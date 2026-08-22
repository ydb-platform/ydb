#!/usr/bin/env python3
"""
Скрипт для тестирования классификации ошибок по типам на реальных данных.
"""

import json
import sys
from pathlib import Path
from collections import Counter, defaultdict

# Добавляем путь к скриптам
sys.path.append(str(Path(__file__).parent))
from error_classifier import ErrorClassifier
from error_normalizer import normalize_error_text


def main():
    print("=" * 80)
    print("ТЕСТИРОВАНИЕ КЛАССИФИКАЦИИ ОШИБОК ПО ТИПАМ")
    print("=" * 80)
    print()
    
    # Загружаем нормализованные паттерны
    patterns_file = "error_normalized_patterns_100k.json"
    try:
        with open(patterns_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Файл {patterns_file} не найден.")
        print("Сначала запустите test_normalization_on_real_data.py")
        return 1
    
    print(f"Загружено {data['total_errors']} ошибок, {data['total_patterns']} паттернов")
    print()
    
    # Инициализируем классификатор
    classifier = ErrorClassifier()
    
    # Классифицируем все паттерны
    print("Классифицирую ошибки...")
    print()
    
    classification_results = defaultdict(lambda: {'count': 0, 'examples': []})
    
    for pattern_data in data['patterns']:
        pattern = pattern_data['pattern']
        error_count = pattern_data['error_count']
        examples = pattern_data.get('errors', [])
        
        # Классифицируем паттерн
        error_type, confidence = classifier.classify(
            examples[0] if examples else pattern,
            normalized_pattern=pattern
        )
        
        classification_results[error_type]['count'] += error_count
        if len(classification_results[error_type]['examples']) < 5:
            classification_results[error_type]['examples'].append({
                'pattern': pattern[:200],
                'original': examples[0][:200] if examples else pattern[:200],
                'count': error_count
            })
    
    # Выводим результаты
    print("=" * 80)
    print("РЕЗУЛЬТАТЫ КЛАССИФИКАЦИИ")
    print("=" * 80)
    print()
    
    total_errors = data['total_errors']
    
    # Сортируем по количеству ошибок
    sorted_results = sorted(
        classification_results.items(),
        key=lambda x: x[1]['count'],
        reverse=True
    )
    
    print(f"{'Тип ошибки':<25} {'Количество':<15} {'Процент':<10} {'Примеры'}")
    print("-" * 80)
    
    for error_type, data_dict in sorted_results:
        count = data_dict['count']
        percentage = (count / total_errors) * 100
        examples_count = len(data_dict['examples'])
        
        print(f"{error_type:<25} {count:<15,} {percentage:>6.1f}%    {examples_count} примеров")
    
    print()
    print("=" * 80)
    print("ДЕТАЛЬНАЯ ИНФОРМАЦИЯ ПО ТИПАМ")
    print("=" * 80)
    print()
    
    for error_type, data_dict in sorted_results[:10]:  # Топ-10 типов
        count = data_dict['count']
        percentage = (count / total_errors) * 100
        
        print(f"\n{error_type.upper()} ({count:,} ошибок, {percentage:.1f}%):")
        print("-" * 80)
        
        for i, example in enumerate(data_dict['examples'][:3], 1):
            print(f"  {i}. Паттерн встречается {example['count']} раз")
            print(f"     Нормализованный: {example['pattern']}...")
            print(f"     Исходный: {example['original']}...")
            print()
    
    # Сохраняем результаты
    result_data = {
        'total_errors': total_errors,
        'total_patterns': data['total_patterns'],
        'classification': {}
    }
    
    for error_type, data_dict in sorted_results:
        result_data['classification'][error_type] = {
            'count': data_dict['count'],
            'percentage': (data_dict['count'] / total_errors) * 100,
            'examples': data_dict['examples']
        }
    
    output_file = "error_classification_results.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Результаты сохранены в {output_file}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

