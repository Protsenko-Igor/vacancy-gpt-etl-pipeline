-- vacancy_analysis.sql
-- SQL запросы для анализа вакансий в Yandex Datalens
-- Автор: Проценко Игорь
-- Дата: 29.12.2025
-- ============================================
-- ОСНОВНЫЕ МЕТРИКИ
-- ============================================

-- 1. РАСПРЕДЕЛЕНИЕ ПО ДОЛЖНОСТЯМ (вакансиям)
-- Показывает популярность позиций и средние зарплаты
SELECT 
    normalized_title AS vacancy_position,
    COUNT(*) AS vacancy_count,
    ROUND(AVG(salary_to), 0) AS avg_salary_to
FROM processed.normalized_vacancies
WHERE normalized_title != 'Не определена'
GROUP BY normalized_title
ORDER BY vacancy_count DESC
LIMIT 20;

-- 2. РАСПРЕДЕЛЕНИЕ ПО СФЕРАМ ДЕЯТЕЛЬНОСТИ
-- Анализ рынка по отраслям
SELECT 
    category,
    COUNT(*) AS vacancy_count,
    ROUND(AVG(salary_to), 0) AS avg_salary_to,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS market_share_percent
FROM processed.normalized_vacancies
WHERE category NOT IN ('Не определена', 'Другое', 'Не указано')
GROUP BY category
ORDER BY vacancy_count DESC;

-- ============================================
-- ПРИМЕЧАНИЯ:
-- 1. processed.normalized_vacancies - это подключение к данным в Datalens
-- 2. salary_to - поле с зарплатой "до"
-- 3. Можно добавить фильтры по дате, региону и т.д.
-- ============================================