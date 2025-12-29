# ========== Импорт библиотек ==========
# 1. Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
# 2. Дата/время и интервалы
from datetime import datetime, timedelta
# 3. Cloud/API
import boto3
from botocore.client import Config
import io
# 4. Для запросов к API YandexGPT
import requests
# 5. Обработка данных
import json
import pandas as pd
import re
import time

# 6. Для статистики
from collections import Counter

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# ========== Функция для хранилища ==========
def get_s3_client():
    """Создает и возвращает настроенный клиент для Yandex Object Storage."""
    conn = BaseHook.get_connection('yandex_object_storage')
    extra_config = conn.extra_dejson
    session = boto3.session.Session()
    return session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=extra_config.get('access_key_id'),
        aws_secret_access_key=extra_config.get('secret_access_key'),
        config=Config(s3={'addressing_style': 'virtual'})
    )

# ========== ЗАДАЧА 1: Поиск файлов (без изменений) ==========
def find_files_in_bucket(**kwargs):
    ti = kwargs['ti']
    print("=== Задача 1: Ищем файлы в бакете ===")

    s3_client = get_s3_client()
    bucket_name = 'n8n-vacancy-bucket'
    folder_prefix = 'vacancies/'

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.csv'):
                files.append(obj['Key'])
                print(f"Найден файл: {obj['Key']}")

    print(f"Всего найдено CSV-файлов: {len(files)}")
    ti.xcom_push(key='file_list', value=files)
    ti.xcom_push(key='bucket_name', value=bucket_name)

# ========== ЗАДАЧА 2: Обработка файла (с отправкой данных дальше) ==========
def process_latest_file(**kwargs):
    ti = kwargs['ti']
    print("\n=== Задача 2: Обрабатываем файлы ===")

    # 1. Получаем список файлов из предыдущей задачи
    files = ti.xcom_pull(task_ids='find_files_in_bucket', key='file_list')
    bucket_name = ti.xcom_pull(task_ids='find_files_in_bucket', key='bucket_name')
    print(f"Полученный список files (сырой): {files}")
    print(f"Тип files: {type(files)}")
    print(f"Длина files: {len(files) if files else 0}")

    if not files:
        print("Ошибка: Нет файлов для обработки!")
        return

    # 2. Оставляем только полные пути к CSV-файлам
    #Игнорируем пустые строки, папки (оканчивающиеся на '/') или слишком короткие имена
    filtered_files = [f for f in files if f and f.endswith('.csv') and len(f) > 10]
    print(f"Отфильтрованный список filtered_files: {filtered_files}")
    print(f"Длина filtered_files: {len(filtered_files)}")

    if not filtered_files:
        print("Ошибка: Нет подходящих CSV-файлов после фильтрации!")
        return

    # 3. Сортируем отфильтрованный список и берём последние 4 файлов
    latest_files = sorted(filtered_files)[-4:]  # Берём два последних
    print(f"Выбраны файлы для обработки: {latest_files}")
    print(f"Выбраны файлы для обработки: {latest_files}")

    # Список для хранения DataFrame каждого файла
    all_dataframes = []

    # Готовим переменную для захода в бакет
    s3_client = get_s3_client()

    # Начинаем переберать 4 файла
    for file_key in latest_files:
        print(f"Читаем файл: {file_key}")
        # Скачиваем и читаем каждый файл
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_data = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_data))
        print(f"   -> Загружено {len(df)} записей.")
        all_dataframes.append(df)

    # 2. Объединяем все DataFrame из списка
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
    else:
        combined_df = pd.DataFrame()  # Пустой DataFrame, если файлов не было

    # 3. Удаляем явные дубликаты (если все колонки одинаковые)
    initial_count = len(combined_df)
    combined_df = combined_df.drop_duplicates()
    deduplicated_count = len(combined_df)
    print(f"Объединено данных: {initial_count} записей.")
    print(f"После удаления дубликатов: {deduplicated_count} записей.")

    # 4. Удаляем дубликаты по ключевому полю, например 'id'
    combined_df = combined_df.drop_duplicates(subset=['id'])

    # 5. Преобразуем весь итоговый DataFrame в список словарей
    all_vacancies_data = combined_df.to_dict('records')
    print(f"Итоговые данные для передачи в GPT. Строк: {len(all_vacancies_data)}")
    print(f"Колонки в данных: {list(all_vacancies_data[0].keys())}")

    # 6. Отправляем все в XCom
    ti.xcom_push(key='vacancies_for_gpt', value=all_vacancies_data)

# ========== ЗАДАЧА 3: Изменение заголовков через YandexGPT (с батчингом и retry) ==========
def title_with_gpt(**kwargs):
    ti = kwargs['ti']
    print("\n=== Задача 3: Нормализация заголовков вакансий через GPT ===")

    raw_data = ti.xcom_pull(task_ids='process_latest_file', key='vacancies_for_gpt')
    if not raw_data:
        print("Ошибка: Нет данных для обработки!")
        return
    
    print(f"Получено {len(raw_data)} записей для нормализации.")
    
    # 1. Собираем все уникальные заголовки
    all_titles = []
    title_to_records = {}
    
    for idx, record in enumerate(raw_data):
        title = record.get('title', '').strip()
        if title:
            all_titles.append(title)
            if title not in title_to_records:
                title_to_records[title] = []
            title_to_records[title].append(idx)
    
    unique_titles = list(set(all_titles))
    print(f"Уникальных заголовков: {len(unique_titles)}")
    print(f"Примеры: {unique_titles[:3]}")
    
    # 2. Получаем API ключи
    conn = BaseHook.get_connection('yandex_gpt')
    api_key = conn.extra_dejson.get('api_key')
    folder_id = conn.extra_dejson.get('folder_id')
    
    # NEW: Функция для обработки батча с retry
    def process_batch_with_retry(batch_items, max_retries=2):
        """Обрабатывает батч заголовков с повторными попытками"""
        
        all_results = []
        current_batch = batch_items.copy()
        
        for attempt in range(max_retries + 1):
            if not current_batch:
                break
                
            print(f"    Попытка {attempt + 1}: {len(current_batch)} заголовков")
            
            # Формируем промпт
            prompt = f"""
            Ты — HR-аналитик, классифицируешь вакансии.
            
            Исходные названия: {', '.join(current_batch)}.

            Приведи каждое название к одной из категорий:

            - Аналитик данных
            - BI-аналитик
            - Системный аналитик
            - Бизнес аналитик
            - Веб-аналитик
            - Финансовый аналитик
            - Продуктовый аналитик
            - ML/AI-инженер
            - Разработчик
            - DevOps-инженер
            - Директор по маркетингу
            - Генеральный директор
            - Коммерческий директор
            - Директор по продукту
            - Маркетолог
            - Главный маркетолог
            - Руководитель по контенту
            - Директор по продажам
            - Специалист по трафику
            - Менеджер продукта
            - Другое
            
            **Правила**
            1. НЕ придумывай новые категории
            2. Если не уверен — ставь "Другое"
            3. Вакансии пиши с большой буквы (как в примере)
            4. НЕ добавляй объяснений, комментариев или примеров.

            Верни ТОЛЬКО JSON-массив, где каждый элемент — объект с полями:
            - "original": исходная строка
            - "normalized_title": выбранная категория
            """
            
            try:
                # Отправляем запрос
                response = requests.post(
                    url,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Api-Key {api_key}"
                    },
                    json={
                        "modelUri": f"gpt://{folder_id}/yandexgpt-lite/rc",
                        "completionOptions": {
                            "stream": False,
                            "temperature": 0.3,
                            "maxTokens": 4000
                        },
                        "messages": [{"role": "user", "text": prompt}]
                    },
                    timeout=60
                )
                
                print(f"      Статус: {response.status_code}")
                response.raise_for_status()
                
                result = response.json()
                gpt_response_text = result['result']['alternatives'][0]['message']['text']
                print(f"      Ответ: {len(gpt_response_text)} символов")

                
                # Парсим JSON
                def safe_json_parse(text):
                    text = text.strip().strip('`')
                    if text.startswith('json'):
                        text = text[4:].strip()
                    
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError:
                        json_match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)
                        if json_match:
                            try:
                                return json.loads(json_match.group())
                            except:
                                pass
                        return []
                
                batch_results = safe_json_parse(gpt_response_text)
                
                if not batch_results:
                    print(f"      Не получили валидный JSON")
                    # Создаём заглушки для всего текущего батча
                    temp_results = []
                    for item in current_batch:
                        temp_results.append({
                            "original": item,
                            "normalized_title": "Не определена"
                        })
                    batch_results = temp_results
                
                # Разделяем успешные и неудачные
                successful = []
                failed_items = []
                
                for item in batch_results:
                    if isinstance(item, dict):
                        title = item.get('normalized_title', '')
                        # Проверяем что категория не "Не определена"
                        if title and title != 'Не определена':
                            successful.append(item)
                        else:
                            failed_items.append(item.get('original', ''))
                
                print(f"      Определено: {len(successful)}, Не определено: {len(failed_items)}")
                
                # Добавляем успешные в общие результаты
                all_results.extend(successful)
                
                # Подготовка к следующей попытке
                current_batch = failed_items
                
                if not failed_items:
                    break  # Все определены, выходим
                    
                if attempt < max_retries:
                    print(f"      Пауза 2 секунды перед повторной попыткой...")
                    time.sleep(2)
                
            except Exception as e:
                print(f"      Ошибка: {e}")
                if attempt == max_retries:
                    # Если это последняя попытка и всё равно ошибка
                    for item in current_batch:
                        all_results.append({
                            "original": item,
                            "normalized_title": "Не определена"
                        })
                    break
                time.sleep(3)  # Пауза при ошибке
        
        # Для оставшихся после всех попыток
        for item in current_batch:
            all_results.append({
                "original": item,
                "normalized_title": "Не определена"
            })
        
        return all_results
    
    # 3. Разбиваем на батчи по 15 заголовков
    batch_size = 15
    all_normalized = []
    
    # URL для запросов (выносим из цикла)
    url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    
    total_batches = (len(unique_titles) + batch_size - 1) // batch_size
    print(f"\n=== Батчинг ===")
    print(f"Всего уникальных заголовков: {len(unique_titles)}")
    print(f"Размер батча: {batch_size}")
    print(f"Количество батчей: {total_batches}")
    
    for batch_num in range(0, len(unique_titles), batch_size):
        batch = unique_titles[batch_num:batch_num + batch_size]
        batch_index = batch_num // batch_size + 1
        
        print(f"\n--- Батч {batch_index}/{total_batches} ---")
        print(f"Заголовков в батче: {len(batch)}")
        print(f"Примеры: {batch[:3]}...")
        
        print(f"👉 Отправляем в GPT с retry механизмом...")
        
        try:
            # Используем функцию с retry
            normalized_batch = process_batch_with_retry(
                batch_items=batch,
                max_retries=1  # Одна дополнительная попытка
            )
            
            # Фильтруем возможные дубликаты
            seen = set()
            unique_results = []
            for item in normalized_batch:
                if isinstance(item, dict) and item.get('original'):
                    if item['original'] not in seen:
                        seen.add(item['original'])
                        unique_results.append(item)
            
            all_normalized.extend(unique_results)
            
            # Статистика для этого батча
            success_count = sum(1 for item in unique_results 
                              if item.get('normalized_title') != 'Не определена')
            print(f"✅ Итог батча: {success_count}/{len(batch)} определено")
            
        except Exception as e:
            print(f"❌ Ошибка в батче #{batch_index} после всех попыток: {e}")
            # Добавляем заглушки для всего батча
            for title in batch:
                all_normalized.append({
                    "original": title,
                    "normalized_title": "Не определена"
                })
    
    # 4. Создаём полный маппинг
    title_mapping = {}
    for item in all_normalized:
        if isinstance(item, dict) and item.get('original'):
            title_mapping[item['original']] = item.get('normalized_title', 'Не определена')
    
    # 5. Применяем ко всем записям
    enriched_data = []
    for record in raw_data:
        original_title = record.get('title', '').strip()
        enriched_record = record.copy()
        enriched_record['normalized_title'] = title_mapping.get(original_title, 'Не определена')
        enriched_data.append(enriched_record)
    
    # 6. Сохраняем
    ti.xcom_push(key='data_with_normalized_titles', value=enriched_data)
    
    # 7. Статистика
    print(f"\n📊 Итоги нормализации заголовков:")
    print(f"Всего записей: {len(enriched_data)}")
    
    from collections import Counter
    normalized_counts = Counter([r['normalized_title'] for r in enriched_data])
    
    print("📈 Распределение по категориям:")
    for category, count in normalized_counts.most_common(15):
        percentage = (count / len(enriched_data)) * 100
        print(f"  {category}: {count} записей ({percentage:.1f}%)")
    
    # Считаем успешность
    success_count = sum(count for cat, count in normalized_counts.items() 
                       if cat not in ['Не определена', 'Другое'])
    success_rate = (success_count / len(enriched_data)) * 100
    
    # Детальная статистика по "Не определена"
    undefined_count = normalized_counts.get('Не определена', 0)
    if undefined_count > 0:
        print(f"\n🔍 Анализ 'Не определена' ({undefined_count} записей):")
        
        # Находим примеры "Не определена"
        undefined_titles = []
        for record in enriched_data[:10]:  # Берём первые 10
            if record['normalized_title'] == 'Не определена':
                title = record.get('title', '')
                if title:
                    undefined_titles.append(title[:50] + '...' if len(title) > 50 else title)
        
        if undefined_titles:
            print(f"  Примеры: {', '.join(undefined_titles[:5])}")
    
    print(f"\n✅ Успешно классифицировано: {success_rate:.1f}% записей")
    
    return f"Обработано {len(enriched_data)} записей, успех: {success_rate:.1f}%"

# ========== ЗАДАЧА 4: Изменение сфер через YandexGPT (с батчингом и retry) ==========
def working_with_gpt(**kwargs):
    ti = kwargs['ti']
    print("\n=== Задача 4: Нормализация сфер деятельности через GPT ===")

    # 1. Получаем данные из предыдущей задачи
    raw_data = ti.xcom_pull(task_ids='title_with_gpt', key='data_with_normalized_titles')
    if not raw_data:
        print("Ошибка: Нет данных для обработки!")
        return
    
    print(f"Получено {len(raw_data)} записей для нормализации.")
    
    # 2. Собираем ВСЕ УНИКАЛЬНЫЕ сферы деятельности
    all_fields = []
    field_to_records = {}
    
    for idx, record in enumerate(raw_data):
        field = record.get('ai_field_of_activity', '').strip()
        if field:  # Только непустые
            all_fields.append(field)
            if field not in field_to_records:
                field_to_records[field] = []
            field_to_records[field].append(idx)
    
    unique_fields = list(set(all_fields))
    print(f"Уникальных сфер деятельности: {len(unique_fields)}")
    print(f"Примеры: {unique_fields[:3]}")
    
    # 3. Получаем API ключи
    conn = BaseHook.get_connection('yandex_gpt')
    api_key = conn.extra_dejson.get('api_key')
    folder_id = conn.extra_dejson.get('folder_id')
    
    print(f"API Key получен: {'Да' if api_key else 'Нет'}")
    
    # Функция для обработки батча с retry
    def process_batch_with_retry(batch_items, max_retries=2):
        """Обрабатывает батч с повторными попытками"""
        
        all_results = []
        current_batch = batch_items.copy()
        
        for attempt in range(max_retries + 1):  # +1 для первой попытки
            if not current_batch:
                break
                
            print(f"    Попытка {attempt + 1}: {len(current_batch)} элементов")
            
            # Формируем промпт
            sample_working_str = ', '.join(current_batch)
            prompt = f"""
            Ты — HR-аналитик, классифицируешь вакансии.
            Исходные сферы деятельности: {sample_working_str}.

            **КАТЕГОРИИ (выбери ОДНУ):**
            - IT (если содержит: технологии, разработка, софт, saas, ai, it, crm, big data и подобные)
            - Финансы (если содержит: мфо, банки, банковские услуги, банкинг, финтех, инвестиции, страхование и подобные)
            - Ритейл (если содержит: розничная торговля, FMCG и подобные)
            - E-commerce (если содержит: интернет-магазины, маркетплейсы, e-commerce и подобные)
            - Производство (если содержит: промышленность, заводы и подобные)
            - Медицина (если содержит: здравоохранение, фармацевтика и подобные)
            - Образование (если содержит: EdTech, курсы, онлайн образование и подобные)
            - Маркетинг (если содержит: реклама, digital, медиа, cpa и подобные)
            - Логистика (если содержит: доставка, транспорт и подобные)
            - Туризм (если содержит: путешествия, гостиницы и подобные)
            - Телеком (если содержит: связь, интернет и подобные)
            - Недвижимость (если содержит: строительство, аренда и подобные)
            - Энергетика (если содержит: нефть, газ, электричество и подобные)
            - Государственный сектор (если содержит: госуслуги, государственный и подобное)
            - Консалтинг (если содержит: консалтинговые услуги и подобные)
            - Развлечения (если содержит: азартные игры, igaming, gambling и подобные)
            - Сфера услуг (если содержит: hr, юридические услуги и подобные)
            - Другое (если не было совпадений с категориями выше)

            **ПРАВИЛА (важно для попытки #{attempt + 1}):**
            1. Выбери ОДНУ основную категорию из списка выше
            2. Для специализации — укажи самое конкретное из названия
            3. Если сомневаешься — ставь категорию "Другое"
            4. Категории и специализации пиши с большой буквы
            5. Когда смотришь на категории в скобках указаны условия для анализа (записывать их в ответ не нужно)
            5. {'⚠️ ВНИМАНИЕ: Эти сферы НЕ УДАЛОСЬ классифицировать с первой попытки! Будь более внимательным!' if attempt > 0 else ''}

            **ВНИМАНИЕ:** Если сфера СЛОЖНАЯ (несколько направлений перечисленные через "." или  "/" ):
            1. Выбери ПЕРВУЮ или ОСНОВНУЮ сферу
            2. Игнорируй второстепенные
            3. Если сомневаешься — ставь "Другое"
            
            Верни ТОЛЬКО JSON-массив, где каждый элемент — объект с полями:
            - "original": исходная строка
            - "category": широкая категория (с большой буквы)
            - "specialization": узкая специализация (с большой буквы)
            """
            
            try:
                # Отправляем запрос
                response = requests.post(
                    url,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Api-Key {api_key}"
                    },
                    json={
                        "modelUri": f"gpt://{folder_id}/yandexgpt-lite/rc",
                        "completionOptions": {
                            "stream": False,
                            "temperature": 0.3,
                            "maxTokens": 4000
                        },
                        "messages": [{"role": "user", "text": prompt}]
                    },
                    timeout=60
                )
                
                print(f"      Статус: {response.status_code}")
                response.raise_for_status()
                
                result = response.json()
                gpt_response_text = result['result']['alternatives'][0]['message']['text']
                print(f"      Ответ: {len(gpt_response_text)} символов")
                
                # Безопасный парсинг JSON
                def safe_json_parse(text):
                    text = text.strip().strip('`')
                    if text.startswith('json'):
                        text = text[4:].strip()
                    
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError:
                        json_match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)
                        if json_match:
                            try:
                                return json.loads(json_match.group())
                            except:
                                pass
                        return []
                
                batch_results = safe_json_parse(gpt_response_text)
                
                if not batch_results:
                    print(f"      Не получили валидный JSON")
                    # Создаём заглушки для всего текущего батча
                    temp_results = []
                    for item in current_batch:
                        temp_results.append({
                            "original": item,
                            "category": "Не определена",
                            "specialization": "Не определена"
                        })
                    batch_results = temp_results
                
                # Фильтруем результаты - оставляем только те, у которых original есть в текущем батче
                current_batch_set = set(current_batch)
                filtered_results = []
                
                for item in batch_results:
                    if isinstance(item, dict):
                        original = item.get('original', '')
                        # Проверяем что original есть в текущем батче
                        if original in current_batch_set:
                            filtered_results.append(item)
                        else:
                            print(f"      Пропускаем чужой элемент: '{original[:50]}...'")
                
                batch_results = filtered_results
                
                if not batch_results:
                    print(f"      После фильтрации осталось 0 записей")
                    # Создаём заглушки для всего текущего батча
                    temp_results = []
                    for item in current_batch:
                        temp_results.append({
                            "original": item,
                            "category": "Не определена",
                            "specialization": "Не определена"
                        })
                    batch_results = temp_results
                
                # Разделяем успешные и неудачные
                successful = []
                failed_items = []
                
                for item in batch_results:
                    if isinstance(item, dict):
                        category = item.get('category', '')
                        # Проверяем что категория не "Не определена" и не "Другое"
                        if category and category != 'Не определена' and category != 'Другое':
                            successful.append(item)
                        else:
                            failed_items.append(item.get('original', ''))
                
                print(f"      Определено: {len(successful)}, Не определено: {len(failed_items)}")
                
                # Добавляем успешные в общие результаты
                all_results.extend(successful)
                
                # Подготовка к следующей попытке
                current_batch = failed_items
                
                if not failed_items:
                    break  # Все определены, выходим
                    
                if attempt < max_retries:
                    print(f"      Пауза 2 секунды перед повторной попыткой...")
                    time.sleep(2)
                
            except Exception as e:
                print(f"      Ошибка: {e}")
                if attempt == max_retries:
                    # Если это последняя попытка и всё равно ошибка
                    for item in current_batch:
                        all_results.append({
                            "original": item,
                            "category": "Не определена",
                            "specialization": "Не определена"
                        })
                    break
                time.sleep(3)  # Пауза при ошибке
        
        # Для оставшихся после всех попыток
        for item in current_batch:
            all_results.append({
                "original": item,
                "category": "Не определена",
                "specialization": "Не определена"
            })
        
        return all_results
    
    # 4. Разбиваем на батчи по 10 сфер
    batch_size = 10
    all_normalized = []
    
    total_batches = (len(unique_fields) + batch_size - 1) // batch_size
    print(f"\n=== Батчинг ===")
    print(f"Всего уникальных сфер: {len(unique_fields)}")
    print(f"Размер батча: {batch_size}")
    print(f"Количество батчей: {total_batches}")
    
    # URL для запросов (выносим из цикла)
    url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    
    for batch_num in range(0, len(unique_fields), batch_size):
        batch = unique_fields[batch_num:batch_num + batch_size]
        batch_index = batch_num // batch_size + 1
        
        print(f"\n--- Батч #{batch_index}/{total_batches} ---")
        print(f"Сфер в батче: {len(batch)}")
        print(f"Примеры: {batch[:3]}...")
        
        print(f"👉 Отправляем в GPT с retry механизмом...")
        
        try:
            # Используем функцию с retry вместо прямого вызова
            normalized_batch = process_batch_with_retry(
                batch_items=batch,
                max_retries=1  # Одна дополнительная попытка
            )
            
            # Фильтруем возможные дубликаты
            seen = set()
            unique_results = []
            for item in normalized_batch:
                if isinstance(item, dict) and item.get('original'):
                    if item['original'] not in seen:
                        seen.add(item['original'])
                        unique_results.append(item)
            
            all_normalized.extend(unique_results)
            
            # Правильный подсчёт успешных результатов
            def count_successful_in_batch(results, original_batch):
                """Считает успешные результаты только для оригинального батча"""
                original_set = set(original_batch)
                successful = 0
                
                for item in results:
                    if isinstance(item, dict):
                        original = item.get('original', '')
                        category = item.get('category', '')
                        
                        # Проверяем что это элемент из нашего батча и он успешен
                        if original in original_set and category not in ['Не определена', 'Другое']:
                            successful += 1
                
                return successful
            
            success_count = count_successful_in_batch(unique_results, batch)
            print(f"✅ Итог батча: {success_count}/{len(batch)} определено")
            
        except Exception as e:
            print(f"❌ Ошибка в батче #{batch_index} после всех попыток: {e}")
            # Добавляем заглушки для всего батча
            for field in batch:
                all_normalized.append({
                    "original": field,
                    "category": "Не определена",
                    "specialization": "Не определена"
                })
    
    # 5. Создаём полные маппинги
    category_mapping = {}
    specialization_mapping = {}
    
    for item in all_normalized:
        if isinstance(item, dict) and item.get('original'):
            category_mapping[item['original']] = item.get('category', 'Не определена')
            specialization_mapping[item['original']] = item.get('specialization', 'Не определена')
    
    # 6. Применяем ко всем записям
    enriched_data = []
    for record in raw_data:
        original_field = record.get('ai_field_of_activity', '').strip()
        if not original_field:
            original_field = 'Не указано'
            
        enriched_record = record.copy()
        enriched_record['category'] = category_mapping.get(original_field, 'Не определена')
        enriched_record['specialization'] = specialization_mapping.get(original_field, 'Не определена')
        enriched_data.append(enriched_record)
    
    # 7. Сохраняем обогащённые данные
    ti.xcom_push(key='data_with_normalized_working', value=enriched_data)
    
    # 8. Статистика
    print(f"\n=== Итоги нормализации сфер ===")
    print(f"Всего записей: {len(enriched_data)}")
    
    from collections import Counter
    category_counts = Counter([r['category'] for r in enriched_data])
    specialization_counts = Counter([r['specialization'] for r in enriched_data])
    
    print(f"\n📊 Широкие категории (топ-5):")
    for category, count in category_counts.most_common(5):
        percentage = (count / len(enriched_data)) * 100
        print(f"  {category}: {count} записей ({percentage:.1f}%)")
    
    print(f"\n🎯 Узкие специализации (топ-5):")
    for spec, count in specialization_counts.most_common(5):
        percentage = (count / len(enriched_data)) * 100
        print(f"  {spec}: {count} записей ({percentage:.1f}%)")
    
    # Считаем успешность
    success_count = sum(count for cat, count in category_counts.items() 
                       if cat not in ['Не определена', 'Не указано', 'Другое'])
    success_rate = (success_count / len(enriched_data)) * 100
    
    # Детальная статистика
    undefined_count = category_counts.get('Не определена', 0)
    if undefined_count > 0:
        undefined_examples = []
        for record in enriched_data[:5]:  # Берём первые 5
            if record['category'] == 'Не определена':
                field = record.get('ai_field_of_activity', '')
                if field:
                    undefined_examples.append(field[:50])
        
        if undefined_examples:
            print(f"\n🔍 Примеры 'Не определена': {', '.join(undefined_examples)}...")
    
    print(f"\n✅ Успешно классифицировано: {success_rate:.1f}% ({success_count}/{len(enriched_data)})")
    
    return f"Обработано {len(enriched_data)} записей, успех: {success_rate:.1f}%"

# ========== ЗАДАЧА 5: Сохранение обогащённых данных в S3 ==========
def save_enriched_data_to_s3(**kwargs):
    ti = kwargs['ti']
    print("\n=== Задача 5: Сохранение обогащённых данных в S3 ===")
    
    # 1. Получаем обогащённые данные
    enriched_data = ti.xcom_pull(task_ids='working_with_gpt', key='data_with_normalized_working')
    if not enriched_data:
        print("Ошибка: Нет обогащённых данных для сохранения!")
        return
    
    print(f"Получено {len(enriched_data)} обогащённых записей")
    
    # 2. Конвертируем в DataFrame
    df = pd.DataFrame(enriched_data)
    
    # 3. Добавляем мета-информацию
    processing_date = datetime.now().strftime('%Y%m%d_%H%M%S')
    df['_processing_date'] = processing_date
    df['_processing_timestamp'] = datetime.now().isoformat()
    
    print(f"Создан DataFrame: {len(df)} строк, {len(df.columns)} колонок")
    print(f"Колонки: {list(df.columns)}")
    
    # 4. Конвертируем в CSV
    csv_buffer = df.to_csv(
        index=False, 
        encoding='utf-8-sig',  # UTF-8 с BOM для лучшей совместимости
        sep=',',               # Явно указываем разделитель
        quotechar='"',         # Символ кавычек
        escapechar='\\'       # Символ экранирования
    )
    
    # 5. Сохраняем в S3
    s3_client = get_s3_client()
    bucket_name = 'n8n-vacancy-bucket'
    
    # Путь для сохранения
    s3_key = f"processed/normalized/vacancies_normalized_{processing_date}.csv"
    
    # Загружаем в S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=csv_buffer.encode('utf-8'),
        ContentType='text/csv'
    )
    
    print(f"Данные сохранены в S3: s3://{bucket_name}/{s3_key}")
    print(f"Размер файла: {len(csv_buffer)} байт")
    
    # 6. Сохраняем путь к файлу для возможного использования
    ti.xcom_push(key='processed_file_path', value=s3_key)
    ti.xcom_push(key='processed_record_count', value=len(df))
    
    return s3_key

# ========== ОПРЕДЕЛЕНИЕ DAG ==========
with DAG(
    'vacancy_pipline_gpt_rerty',
    default_args=default_args,
    description='Пайплайн: поиск, обработка и обогащение вакансий через GPT',
    schedule_interval='@daily',
    start_date=datetime(2024, 5, 1),
    catchup=False,
    tags=['portfolio', 'gpt'],
) as dag:

    task_find = PythonOperator(
        task_id='find_files_in_bucket',
        python_callable=find_files_in_bucket,
    )

    task_process = PythonOperator(
        task_id='process_latest_file',
        python_callable=process_latest_file,
    )

    task_title = PythonOperator(
        task_id='title_with_gpt',
        python_callable=title_with_gpt,
    )

    task_working = PythonOperator(
        task_id='working_with_gpt',
        python_callable=working_with_gpt,
    )

    task_save = PythonOperator(
        task_id='save_enriched_data_to_s3',
        python_callable=save_enriched_data_to_s3,
    )

    # Определяем порядок: task_find -> task_process -> title_with_gpt -> working_with_gpt -> task_load
    task_find >> task_process >> task_title >> task_working  >> task_save