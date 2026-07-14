#!/usr/bin/env python3
"""
Модуль для классификации ошибок по основным классам.

Классифицирует ошибки на следующие классы:
- infrastructure_error: инфраструктурные проблемы (сеть, docker, runner)
- timeout: превышение времени выполнения
- test_error: ошибки в тестах (crash, assertion, contract violation)
- setup_error: ошибки настройки тестов
- teardown_error: ошибки очистки после тестов
- unknown: не удалось классифицировать

Использует нормализованные паттерны для определения класса ошибки.
"""

import re
from typing import Tuple, Optional, Dict
from collections import defaultdict


class ErrorClassifier:
    """
    Классификатор ошибок по типам на основе нормализованных паттернов.
    """
    
    def __init__(self):
        """Инициализация классификатора с правилами."""
        self.rules = self._init_rules()
    
    def _init_rules(self) -> Dict[str, list]:
        """
        Инициализирует правила классификации по основным классам.
        
        Returns:
            Словарь {класс_ошибки: [список_паттернов]}
        """
        return {
            'infrastructure_error': [
                # Инфраструктурные проблемы
                r'infrastructure',
                r'runner.*error',
                r'network error',
                r'unauthenticated',
                r'connection.*refused',
                r'endpoint list is empty',
                r'transport_unavailable',
                r'connections to all backends failing',
                r'docker.*failed',
                r'recipe.*error',
            ],
            'timeout': [
                # Таймауты
                r'killed by timeout',
                r'chunk exceeded.*timeout',
                r'timeout.*expired',
                r'exceeded.*timeout',
                r'timed out',
            ],
            'test_error': [
                # Ошибки в тестах
                r'assertion failed',
                r'assertionerror',
                r'assert.*failed',
                r'less-or-equal assertion failed',
                r'test crashed.*return code',
                r'return code:',
                r'terminated by signal',
                r'segmentation fault',
                r'core dumped',
                r'contract violation',
                r'attempt to use result with not successfull status',
                r'tcontractviolation',
                r'event queue is still empty',
                r'emptyeventqueueexception',
            ],
            'setup_error': [
                # Ошибки настройки
                r'setup failed',
                r'setup_class.*failed',
                r'fixture.*setup.*failed',
            ],
            'teardown_error': [
                # Ошибки очистки
                r'teardown failed',
                r'teardown_class.*failed',
                r'fixture.*teardown.*failed',
            ],
        }
    
    def classify(self, status_description: str, normalized_pattern: Optional[str] = None) -> Tuple[str, float]:
        """
        Классифицирует ошибку по типу.
        
        Args:
            status_description: Исходное описание ошибки
            normalized_pattern: Нормализованный паттерн (опционально)
            
        Returns:
            Кортеж (тип_ошибки, уверенность)
        """
        if not status_description:
            return 'unknown', 0.0
        
        # Используем нормализованный паттерн, если он есть, иначе нормализуем сами
        text_to_check = normalized_pattern if normalized_pattern else str(status_description)
        text_lower = text_to_check.lower()
        desc_lower = str(status_description).lower()
        
        # Проверяем правила в порядке приоритета (от высшего к низшему)
        # Приоритет 1: timeout (высший приоритет - если есть timeout, это timeout)
        for pattern in self.rules['timeout']:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return 'timeout', 0.9
        if any(keyword in desc_lower for keyword in ['timeout', 'timed out', 'exceeded time']):
            return 'timeout', 0.7
        
        # Приоритет 2: setup_error / teardown_error (высокий приоритет)
        for pattern in self.rules['setup_error']:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return 'setup_error', 0.9
        if 'setup' in desc_lower and 'failed' in desc_lower:
            return 'setup_error', 0.7
        
        for pattern in self.rules['teardown_error']:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return 'teardown_error', 0.9
        if 'teardown' in desc_lower and 'failed' in desc_lower:
            return 'teardown_error', 0.7
        
        # Приоритет 3: test_error (если есть явные признаки test_error)
        # Проверяем явные признаки test_error (crash, assertion) перед infrastructure
        explicit_test_error_keywords = ['crashed', 'return code', 'signal', 'assertion', 'assert failed', 'contract violation']
        if any(keyword in desc_lower for keyword in explicit_test_error_keywords):
            for pattern in self.rules['test_error']:
                if re.search(pattern, text_lower, re.IGNORECASE):
                    return 'test_error', 0.9
            return 'test_error', 0.7
        
        # Приоритет 4: infrastructure_error (средний приоритет)
        for pattern in self.rules['infrastructure_error']:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return 'infrastructure_error', 0.9
        if any(keyword in desc_lower for keyword in [
            'infrastructure', 'network', 'connection', 'transport', 
            'runner', 'docker', 'recipe'
        ]):
            return 'infrastructure_error', 0.7
        
        # Приоритет 5: test_error (низший приоритет - все остальное)
        for pattern in self.rules['test_error']:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return 'test_error', 0.9
        
        # По умолчанию - неизвестная ошибка
        return 'unknown', 0.0
    
    def classify_batch(self, errors: list, normalized_patterns: Optional[list] = None) -> Dict[str, int]:
        """
        Классифицирует батч ошибок.
        
        Args:
            errors: Список описаний ошибок
            normalized_patterns: Список нормализованных паттернов (опционально)
            
        Returns:
            Словарь {тип_ошибки: количество}
        """
        if normalized_patterns is None:
            normalized_patterns = [None] * len(errors)
        
        classification_counts = defaultdict(int)
        
        for error, normalized in zip(errors, normalized_patterns):
            error_type, _ = self.classify(error, normalized)
            classification_counts[error_type] += 1
        
        return dict(classification_counts)


def classify_error(status_description: str, normalized_pattern: Optional[str] = None) -> Tuple[str, float]:
    """
    Удобная функция для классификации одной ошибки.
    
    Args:
        status_description: Исходное описание ошибки
        normalized_pattern: Нормализованный паттерн (опционально)
        
    Returns:
        Кортеж (тип_ошибки, уверенность)
    """
    classifier = ErrorClassifier()
    return classifier.classify(status_description, normalized_pattern)


def classify_errors_batch(
    errors: list,
    normalized_patterns: Optional[list] = None
) -> Dict[str, int]:
    """
    Удобная функция для классификации батча ошибок.
    
    Args:
        errors: Список описаний ошибок
        normalized_patterns: Список нормализованных паттернов (опционально)
        
    Returns:
        Словарь {тип_ошибки: количество}
    """
    classifier = ErrorClassifier()
    return classifier.classify_batch(errors, normalized_patterns)

