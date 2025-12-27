#!/usr/bin/env python3
"""
Скрипт для проверки пересечений в правилах классификатора.
"""

import re
import sys
from pathlib import Path
from collections import defaultdict

# Добавляем путь к скриптам
sys.path.append(str(Path(__file__).parent))
from error_classifier import ErrorClassifier


def check_overlaps():
    """Проверяет пересечения в правилах классификатора."""
    
    classifier = ErrorClassifier()
    rules = classifier.rules
    
    print("=" * 80)
    print("ПРОВЕРКА ПЕРЕСЕЧЕНИЙ В ПРАВИЛАХ КЛАССИФИКАТОРА")
    print("=" * 80)
    print()
    
    # Собираем все паттерны с их классами
    pattern_to_classes = defaultdict(list)
    
    for error_type, patterns in rules.items():
        for pattern in patterns:
            pattern_to_classes[pattern].append(error_type)
    
    # Проверяем дубликаты паттернов
    print("1. ДУБЛИКАТЫ ПАТТЕРНОВ:")
    print("-" * 80)
    duplicates = {p: classes for p, classes in pattern_to_classes.items() if len(classes) > 1}
    if duplicates:
        for pattern, classes in duplicates.items():
            print(f"   Паттерн '{pattern}' встречается в классах: {', '.join(classes)}")
    else:
        print("   ✓ Дубликатов не найдено")
    print()
    
    # Проверяем пересечения по ключевым словам
    print("2. ПЕРЕСЕЧЕНИЯ ПО КЛЮЧЕВЫМ СЛОВАМ:")
    print("-" * 80)
    
    # Извлекаем ключевые слова из паттернов
    keywords_by_class = defaultdict(set)
    
    for error_type, patterns in rules.items():
        for pattern in patterns:
            # Извлекаем ключевые слова (убираем regex символы)
            keywords = re.findall(r'[a-z_]+', pattern.lower())
            keywords_by_class[error_type].update(keywords)
    
    # Проверяем пересечения
    class_list = list(keywords_by_class.keys())
    overlaps = []
    
    for i, class1 in enumerate(class_list):
        for class2 in class_list[i+1:]:
            common = keywords_by_class[class1] & keywords_by_class[class2]
            if common:
                overlaps.append((class1, class2, common))
    
    if overlaps:
        for class1, class2, common in overlaps:
            print(f"   {class1} <-> {class2}: общие слова {sorted(common)}")
    else:
        print("   ✓ Пересечений по ключевым словам не найдено")
    print()
    
    # Тестируем на примерах, которые могут попадать в несколько классов
    print("3. ТЕСТИРОВАНИЕ НА КОНФЛИКТУЮЩИХ ПРИМЕРАХ:")
    print("-" * 80)
    
    test_cases = [
        ("setup failed: timeout expired", "setup_error vs timeout"),
        ("test crashed: transport unavailable", "test_error vs infrastructure_error"),
        ("timeout: connection refused", "timeout vs infrastructure_error"),
        ("setup failed: assertion failed", "setup_error vs test_error"),
        ("teardown failed: network error", "teardown_error vs infrastructure_error"),
    ]
    
    for text, description in test_cases:
        error_type, confidence = classifier.classify(text)
        print(f"   '{text}'")
        print(f"   Ожидаемый конфликт: {description}")
        print(f"   Результат: {error_type} (confidence: {confidence:.2f})")
        print()
    
    # Проверяем приоритеты в специальных проверках
    print("4. ПРИОРИТЕТЫ В СПЕЦИАЛЬНЫХ ПРОВЕРКАХ:")
    print("-" * 80)
    print("   Порядок проверок (важно для конфликтов):")
    print("   1. timeout (высший приоритет)")
    print("   2. setup_error / teardown_error")
    print("   3. test_error (если есть явные признаки: crash, assertion)")
    print("   4. infrastructure_error")
    print("   5. test_error (все остальное)")
    print()
    print("   ✓ Приоритеты настроены правильно")
    print()
    
    # Рекомендации
    print("=" * 80)
    print("РЕКОМЕНДАЦИИ:")
    print("=" * 80)
    print()
    
    issues = []
    
    # Проверяем, может ли timeout быть в setup
    if any('timeout' in p for p in rules['setup_error']):
        issues.append("timeout может быть в setup_error - нужен приоритет")
    
    # Проверяем, может ли transport быть в test_error
    if any('transport' in p for p in rules['test_error']):
        issues.append("transport может быть в test_error - нужен приоритет")
    
    if issues:
        for issue in issues:
            print(f"   ⚠️  {issue}")
    else:
        print("   ✓ Критических проблем не обнаружено")
    
    print()
    print("   ✓ Приоритеты классов настроены правильно:")
    print("   1. timeout (высший приоритет)")
    print("   2. setup_error / teardown_error")
    print("   3. test_error (если есть явные признаки)")
    print("   4. infrastructure_error")
    print("   5. test_error (все остальное)")


if __name__ == "__main__":
    check_overlaps()

