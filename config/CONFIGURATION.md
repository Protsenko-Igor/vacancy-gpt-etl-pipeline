# Конфигурация проекта
⚠️ Никогда не храните реальные ключи в репозитории!

## Airflow Connections

### Yandex Object Storage
```
Connection ID: yandex_object_storage
Connection Type: HTTP
Host: storage.yandexcloud.net
Extra:
{
  "access_key_id": "ваш_key_id",
  "secret_access_key": "ваш_key"
```

### YandexGPT API
```
Connection ID: yandex_gpt
Connection Type: HTTP
Host: llm.api.cloud.yandex.net
Extra:
{
  "api_key": "ваш_api_key",
  "folder_id": "ваш_folder_id"
}
```

## Примеры данных для тестирования

Если нужно протестировать пайплайн, создайте CSV файл со следующей структурой:

```
csv
id,title,ai_field_of_activity,created_at
1,Python разработчик,IT-технологии,2024-01-15
2,Data Scientist,AI/ML разработка,2024-01-15
3,Маркетолог,Digital маркетинг,2024-01-14
```

Параметры DAG
- Батч для заголовков: 15 записей
- Батч для сфер: 10 записей
- Максимальное количество retry: 2
- Расписание: @daily
