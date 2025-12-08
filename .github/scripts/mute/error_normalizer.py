#!/usr/bin/env python3
"""
Модуль для нормализации текстов ошибок.
Улучшенная версия с более агрессивной нормализацией путей, stack trace и чисел.
"""

import re


def normalize_error_text(text):
    """
    Нормализует текст ошибки, заменяя динамические данные на плейсхолдеры.
    
    Улучшения:
    - Более агрессивная нормализация путей
    - Нормализация stack trace (убирает номера строк)
    - Нормализация таймаутов и длительностей
    - Нормализация имен тестов
    - Нормализация ID операций
    
    Args:
        text: Текст ошибки для нормализации
        
    Returns:
        Нормализованный текст ошибки
    """
    if not text:
        return text
    
    normalized = str(text)
    
    # 1. ISO timestamp (полный формат с временем) - до других замен
    normalized = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z', '<TIMESTAMP>', normalized)
    
    # 2. Нормализация stack trace - ДО нормализации портов, чтобы не конфликтовать
    # Убираем номера строк из stack trace (file.py:1234: -> file.py:<LINE>:)
    normalized = re.sub(r'([a-zA-Z0-9_/]+\.(py|cpp|h|hpp)):\d+:', r'\1:<LINE>:', normalized)
    # Нормализуем пути в stack trace (убираем имена файлов, оставляем только структуру)
    normalized = re.sub(r'([a-zA-Z0-9_/]+/)([^/\s]+\.(py|cpp|h|hpp)):<LINE>:', r'\1<SOURCE_FILE>:<LINE>:', normalized)
    
    # 3. Порты (после stack trace, чтобы не конфликтовать)
    normalized = re.sub(r':\d{4,5}\b', ':<PORT>', normalized)
    
    # 3. UUID/GUID
    normalized = re.sub(
        r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b',
        '<UUID>',
        normalized,
        flags=re.IGNORECASE
    )
    
    # 4. Hex хеши (16+ символов) - до LONG_NUMBER, чтобы не конфликтовать
    normalized = re.sub(r'\b[0-9a-f]{16,}\b', '<HASH>', normalized, flags=re.IGNORECASE)
    
    # 5. IP адреса
    normalized = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '<IP>', normalized)
    
    # 6. Нормализация ID и номеров операций (ДО нормализации LONG_NUMBER)
    # Нормализуем форматы вида number:number (ID операций) - сначала
    normalized = re.sub(r'\b\d{10,}:\d+\b', '<OP_ID>', normalized)
    normalized = re.sub(r'\bopId:\s*\d+:\d+', 'opId: <OP_ID>', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bopId\s*=\s*\d+:\d+', 'opId = <OP_ID>', normalized, flags=re.IGNORECASE)
    
    # 7. Длинные числа (10+ цифр) - после HASH и opId, чтобы не конфликтовать
    normalized = re.sub(r'\b\d{10,}\b', '<NUMBER>', normalized)
    
    # 8. Улучшенная нормализация путей (еще более агрессивная)
    # Сначала нормализуем специфичные директории (testing_out_stuff, test-results и т.д.)
    normalized = re.sub(r'/(testing_out_stuff|test-results)', '/<TEST_DIR>', normalized)
    # Нормализуем стандартные директории сборки
    normalized = re.sub(r'/(tmp|out|build)', '/<BUILD_DIR>', normalized)
    # Нормализуем полные пути к файлам (с расширениями) - лог файлы
    normalized = re.sub(r'/[^/\s]+\.(err|out|log)\b', '/<LOG_FILE>', normalized)
    # Нормализуем пути с числами
    normalized = re.sub(r'/[^/\s]*\d+[^/\s]*', '/<PATH>', normalized)
    # Нормализуем имена исходных файлов в путях (более агрессивно - убираем все имена файлов)
    normalized = re.sub(r'/([^/\s]+\.(py|cpp|h|hpp))', '/<SOURCE_FILE>', normalized)
    # Нормализуем полные пути к файлам в stack trace (убираем имена файлов)
    normalized = re.sub(r'([a-zA-Z0-9_/]+/)([^/\s]+\.(py|cpp|h|hpp))', r'\1<SOURCE_FILE>', normalized)
    # Нормализуем длинные пути - убираем промежуточные директории
    normalized = re.sub(r'/home/runner/actions_runner/_work/ydb/ydb', '/<WORKSPACE>', normalized)
    normalized = re.sub(r'/tmp//-S/', '/<BUILD_PATH>/', normalized)
    
    # 9. Нормализация имен функций в stack trace (убираем параметры и имена)
    normalized = re.sub(r'\bin\s+([a-zA-Z0-9_]+)\([^)]*\)', r' in <FUNCTION>', normalized)
    normalized = re.sub(r'\bin\s+([a-zA-Z0-9_.]+)', r' in <FUNCTION>', normalized)
    # Нормализуем вызовы функций в stack trace (убираем имена методов)
    normalized = re.sub(r'\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\(', r' <OBJECT>.<METHOD>(', normalized)
    normalized = re.sub(r'\s+([a-zA-Z0-9_]+)\(', r' <FUNCTION>(', normalized)
    
    # 10. Нормализация таймаутов и длительностей (более агрессивная)
    normalized = re.sub(r'\b\d+\.?\d*\s*s\b', '<DURATION>s', normalized)  # 600s, 600.95s
    normalized = re.sub(r'duration:\s*\d+\.?\d*\s*s', 'duration: <DURATION>s', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'timeout\s*\(\s*\d+\s*s\s*\)', 'timeout (<TIMEOUT>s)', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'Killed by timeout\s*\(\s*\d+\s*s\s*\)', 'Killed by timeout (<TIMEOUT>s)', normalized, flags=re.IGNORECASE)
    # Нормализуем другие числовые значения в контексте времени
    normalized = re.sub(r'\b\d+\.?\d*\s*(ms|us|ns)\b', '<TIME>', normalized)  # миллисекунды, микросекунды
    # Нормализуем числовые значения в контексте размеров
    normalized = re.sub(r'\b\d+\s*(KB|MB|GB|bytes?)\b', '<SIZE>', normalized, flags=re.IGNORECASE)
    
    # 11. Нормализация имен тестов (более агрессивная)
    # Нормализуем имена тестов в формате TestClass.test_method
    normalized = re.sub(r'([A-Z][a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)', r'<TEST_CLASS>.<TEST_METHOD>', normalized)
    # Нормализуем имена тестов в формате TestClass::TestMethod
    normalized = re.sub(r'([A-Z][a-zA-Z0-9_]+)::([A-Z][a-zA-Z0-9_]+)', r'<TEST_CLASS>::<TEST_METHOD>', normalized)
    # Нормализуем имена тестов в формате test_function_name
    normalized = re.sub(r'\btest_[a-zA-Z0-9_]+', '<TEST_FUNCTION>', normalized)
    # Нормализуем имена классов тестов (убираем конкретные имена)
    normalized = re.sub(r'\b([A-Z][a-zA-Z0-9_]*Test[a-zA-Z0-9_]*)\b', '<TEST_CLASS>', normalized)
    # Нормализуем имена методов тестов
    normalized = re.sub(r'\.([a-zA-Z0-9_]+)\s*\(', '.<TEST_METHOD>(', normalized)
    
    # 12. Версии
    normalized = re.sub(r'\bv?\d+\.\d+\.\d+(-\w+)?\b', '<VERSION>', normalized)
    
    # 13. Email (низкий приоритет)
    normalized = re.sub(r'\b[\w\.-]+@[\w\.-]+\.\w+\b', '<EMAIL>', normalized)
    
    # 14. Нормализация путей к модулям (более агрессивная) - в конце
    # Нормализуем пути вида /ydb/core/<module>/... до /ydb/core/<MODULE>/...
    normalized = re.sub(r'/ydb/(core|library|tests|public)/[^/\s]+', r'/ydb/\1/<MODULE>', normalized)
    
    # 15. Дополнительная нормализация контекстных данных
    # Нормализуем числовые значения в разных контекстах
    normalized = re.sub(r'\b\d+\s*(times?|attempts?|retries?)\b', '<COUNT>', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\b\d+\s*(rows?|records?|items?)\b', '<COUNT>', normalized, flags=re.IGNORECASE)
    # Нормализуем индексы и номера
    normalized = re.sub(r'\b(index|idx|id|num|number)\s*[=:]\s*\d+\b', r'\1 = <ID>', normalized, flags=re.IGNORECASE)
    # Нормализуем проценты
    normalized = re.sub(r'\b\d+\.?\d*%\b', '<PERCENT>', normalized)
    
    # 16. Нормализация имен переменных и параметров
    # Убираем имена переменных из сообщений об ошибках
    normalized = re.sub(r'\b(self|cls|ctx|args|kwargs)\b', '<VAR>', normalized)
    normalized = re.sub(r'\b([a-z][a-zA-Z0-9_]*)\s*=', '<VAR> =', normalized)
    
    # 17. Финальная нормализация путей (убираем оставшиеся детали)
    # Нормализуем оставшиеся пути с именами файлов
    normalized = re.sub(r'/[a-zA-Z0-9_]+\.(py|cpp|h|hpp)', '/<SOURCE_FILE>', normalized)
    # Нормализуем пути с подчеркиваниями и дефисами
    normalized = re.sub(r'/[a-z_]+[a-z0-9_]*', '/<DIR>', normalized)
    
    # 18. Еще более агрессивная нормализация путей (группируем похожие структуры)
    # Нормализуем повторяющиеся паттерны путей
    normalized = re.sub(r'/<DIR>(/<DIR>)+', '/<DIR>', normalized)  # Множественные <DIR> в один
    normalized = re.sub(r'/<BUILD_DIR>(/<BUILD_DIR>)+', '/<BUILD_DIR>', normalized)
    normalized = re.sub(r'/<TEST_DIR>(/<TEST_DIR>)+', '/<TEST_DIR>', normalized)
    
    # 19. Нормализация структуры stack trace (группируем похожие)
    # Нормализуем повторяющиеся паттерны в stack trace
    normalized = re.sub(r'<SOURCE_FILE>:<LINE>:\s+in\s+<FUNCTION>(;;\s*<SOURCE_FILE>:<LINE>:\s+in\s+<FUNCTION>)+', 
                       '<SOURCE_FILE>:<LINE>: in <FUNCTION>', normalized)
    
    # 20. Нормализация числовых значений в разных контекстах (более агрессивная)
    # Нормализуем коды возврата (return code: -4, -6 и т.д.)
    normalized = re.sub(r'return code:\s*-?\d+', 'return code: <RETURN_CODE>', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\(return code:\s*-?\d+\)', '(return code: <RETURN_CODE>)', normalized, flags=re.IGNORECASE)
    # Нормализуем числа в скобках (параметры, индексы) - но не таймауты
    normalized = re.sub(r'\((\d+)\)(?!\s*s\b)', '(<NUM>)', normalized)
    normalized = re.sub(r'\[\d+\]', '[<NUM>]', normalized)
    # Нормализуем числа после двоеточия (индексы, версии) - но не порты (уже обработаны)
    normalized = re.sub(r':(\d+)\b(?!\s*[a-zA-Z])', ':<NUM>', normalized)
    # Нормализуем числа в контексте ошибок
    normalized = re.sub(r'code\s*[=:]\s*\d+', 'code = <CODE>', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'status\s*[=:]\s*\d+', 'status = <STATUS>', normalized, flags=re.IGNORECASE)
    # Нормализуем числа в контексте сравнений
    normalized = re.sub(r'[<>=!]+\s*\d+', '<OP> <NUM>', normalized)
    normalized = re.sub(r'\d+\s*[<>=!]+', '<NUM> <OP>', normalized)
    # Нормализуем короткие числа в контексте (индексы массивов, счетчики)
    normalized = re.sub(r'\[(\d{1,3})\]', '[<INDEX>]', normalized)
    normalized = re.sub(r'errors\[(\d+)\]', 'errors[<INDEX>]', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'args\[(\d+)\]', 'args[<INDEX>]', normalized, flags=re.IGNORECASE)
    
    # 21. Нормализация имен классов и типов
    # Нормализуем имена классов в сообщениях об ошибках
    normalized = re.sub(r'\b[A-Z][a-zA-Z0-9_]*Error\b', '<ERROR_CLASS>', normalized)
    normalized = re.sub(r'\b[A-Z][a-zA-Z0-9_]*Exception\b', '<EXCEPTION_CLASS>', normalized)
    normalized = re.sub(r'\b[A-Z][a-zA-Z0-9_]*Result\b', '<RESULT_CLASS>', normalized)
    # Нормализуем C++ template параметры (убираем конкретные значения)
    normalized = re.sub(r'&lt;(true|false|\d+|[a-zA-Z0-9_]+)&gt;', '<TEMPLATE_PARAM>', normalized)
    normalized = re.sub(r'<true>', '<TEMPLATE_PARAM>', normalized)
    normalized = re.sub(r'<false>', '<TEMPLATE_PARAM>', normalized)
    # Нормализуем C++ namespace и длинные имена классов
    normalized = re.sub(r'::(anonymous namespace)::', '::<NAMESPACE>::', normalized)
    normalized = re.sub(r'[A-Z][a-zA-Z0-9_]*::[A-Z][a-zA-Z0-9_]*::[A-Z][a-zA-Z0-9_]*::', '<CLASS>::<CLASS>::', normalized)
    # Нормализуем длинные сигнатуры функций C++ (убираем параметры)
    normalized = re.sub(r'\([^)]{100,}\)', '(<PARAMS>)', normalized)  # Длинные списки параметров
    normalized = re.sub(r'const\s+[A-Z][a-zA-Z0-9_]*\s*&amp;', 'const <TYPE>&', normalized)
    normalized = re.sub(r'std::[a-zA-Z0-9_]+&lt;[^&]+&gt;', 'std::<TYPE>', normalized)
    
    # 22. Нормализация сообщений об ошибках (убираем специфичные детали)
    # Нормализуем стандартные паттерны ошибок (более агрессивно)
    normalized = re.sub(r'AssertionError:\s*[^;\n]+', 'AssertionError: <MESSAGE>', normalized)
    normalized = re.sub(r'TimeoutError:\s*[^;\n]+', 'TimeoutError: <MESSAGE>', normalized)
    normalized = re.sub(r'ExecutionError:\s*[^;\n]+', 'ExecutionError: <MESSAGE>', normalized)
    # Нормализуем числа в сообщениях об ошибках
    normalized = re.sub(r'Expected\s+\d+\s+but\s+got\s+\d+', 'Expected <NUM> but got <NUM>', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'got\s+\d+', 'got <NUM>', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'expected\s+\d+', 'expected <NUM>', normalized, flags=re.IGNORECASE)
    # Нормализуем короткие числа в разных контекстах (более агрессивно)
    normalized = re.sub(r'\b(\d{1,3})\b(?!\s*[a-zA-Z%])', '<NUM>', normalized)  # Короткие числа, но не перед буквами
    # Но сохраняем важные числа (коды ошибок уже обработаны, таймауты уже обработаны)
    # Откатываем нормализацию для важных контекстов
    normalized = re.sub(r'<NUM>\s*s\b', '<DURATION>s', normalized)  # Восстанавливаем таймауты
    
    # 23. Нормализация путей в сообщениях (еще более агрессивная)
    # Убираем все оставшиеся специфичные пути, оставляем только структуру
    normalized = re.sub(r'/[a-zA-Z0-9_/-]+/[a-zA-Z0-9_/-]+', '/<PATH>', normalized)
    
    # 24. Нормализация длинных сообщений (обрезаем или нормализуем)
    # Если сообщение слишком длинное, обрезаем повторяющиеся части
    if len(normalized) > 2000:
        # Обрезаем до первых 2000 символов и добавляем маркер
        normalized = normalized[:2000] + '...<TRUNCATED>'
    
    # 25. Финальная нормализация повторяющихся паттернов
    # Убираем множественные пробелы
    normalized = re.sub(r'\s+', ' ', normalized)
    # Убираем множественные разделители
    normalized = re.sub(r';;+', ';;', normalized)
    
    return normalized

