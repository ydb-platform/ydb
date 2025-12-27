#!/usr/bin/env python3
"""
Скрипт для тестирования нормализации на реальных данных:
1. Получает данные из БД
2. Сохраняет в JSON
3. Нормализует
4. Сохраняет в формате паттерн -> список ошибок
"""

import json
import sys
from pathlib import Path
from collections import defaultdict

# Добавляем путь к скриптам
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent / 'analytics'))
from ydb_wrapper import YDBWrapper
from error_normalizer import normalize_error_text


def main():
    print("Подключаюсь к YDB...")
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("Ошибка: не удалось проверить credentials")
            return 1
        
        # Запрос для получения текстов ошибок (100,000 записей)
        # Убираем DISTINCT, чтобы получить больше записей для тестирования нормализации
        query = """
        SELECT
            status_description
        FROM `test_results/test_runs_column`
        WHERE branch = 'main'
        AND build_type = 'relwithdebinfo'
        AND run_timestamp >= CurrentUtcDate() - 90*Interval("P1D")
        AND status != 'passed' AND status != 'skipped' AND status != 'muted'
        AND NOT String::Contains(status_description, 'automatically muted based on rules')
        LIMIT 100000
        """
        
        print("Выполняю запрос...")
        results = ydb_wrapper.execute_scan_query(query, query_name="get_error_descriptions")
        
        print(f"Получено {len(results)} записей\n")
        
        # Извлекаем тексты ошибок
        status_descriptions = []
        for row in results:
            desc = row.get('status_description')
            if desc:
                # Декодируем если bytes
                if isinstance(desc, bytes):
                    desc = desc.decode('utf-8', errors='ignore')
                status_descriptions.append(desc)
        
        if not status_descriptions:
            print("Не найдено текстов ошибок")
            return 1
        
        print(f"Обработано {len(status_descriptions)} текстов ошибок")
        
        # Сохраняем исходные данные в JSON
        raw_data_file = "error_raw_data_100k.json"
        with open(raw_data_file, 'w', encoding='utf-8') as f:
            json.dump({
                'total_errors': len(status_descriptions),
                'errors': status_descriptions
            }, f, indent=2, ensure_ascii=False)
        print(f"✓ Исходные данные сохранены в {raw_data_file}")
        
        # Нормализуем и группируем по паттернам
        print("\nНормализуем ошибки...")
        pattern_to_errors = defaultdict(list)
        
        for original_error in status_descriptions:
            normalized = normalize_error_text(original_error)
            pattern_to_errors[normalized].append(original_error)
        
        print(f"✓ Создано {len(pattern_to_errors)} уникальных паттернов")
        
        # Сохраняем нормализованные данные в формате паттерн -> список ошибок
        normalized_data_file = "error_normalized_patterns_100k.json"
        
        # Сортируем по количеству ошибок в паттерне (от большего к меньшему)
        sorted_patterns = sorted(
            pattern_to_errors.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )
        
        # Формируем результат
        result_data = {
            'total_errors': len(status_descriptions),
            'total_patterns': len(pattern_to_errors),
            'compression_ratio': len(status_descriptions) / len(pattern_to_errors) if pattern_to_errors else 0,
            'patterns': []
        }
        
        for pattern, errors in sorted_patterns:
            result_data['patterns'].append({
                'pattern': pattern,
                'error_count': len(errors),
                'errors': errors[:5] if len(errors) > 5 else errors,  # Сохраняем до 5 примеров
                'total_errors_in_pattern': len(errors)
            })
        
        with open(normalized_data_file, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Нормализованные данные сохранены в {normalized_data_file}")
        
        # Выводим статистику
        print("\n" + "=" * 80)
        print("СТАТИСТИКА НОРМАЛИЗАЦИИ")
        print("=" * 80)
        print(f"Всего ошибок: {len(status_descriptions)}")
        print(f"Уникальных паттернов: {len(pattern_to_errors)}")
        print(f"Коэффициент сжатия: {result_data['compression_ratio']:.2f}x")
        
        print("\n" + "=" * 80)
        print("ТОП-20 ПАТТЕРНОВ ПО КОЛИЧЕСТВУ ОШИБОК")
        print("=" * 80)
        
        for i, (pattern, errors) in enumerate(sorted_patterns[:20], 1):
            percentage = (len(errors) / len(status_descriptions)) * 100
            print(f"\n{i}. Паттерн встречается {len(errors)} раз ({percentage:.1f}%)")
            print(f"   Паттерн: {pattern[:200]}...")
            if errors:
                print(f"   Пример ошибки: {errors[0][:200]}...")
        
        return 0

if __name__ == "__main__":
    sys.exit(main())

