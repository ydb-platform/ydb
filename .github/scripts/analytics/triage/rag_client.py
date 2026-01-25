#!/usr/bin/env python3
"""
Библиотека для работы с RAG индексом (клиент)
Содержит класс RAGSearch для поиска контекста в индексе.
Используется в triage-скрипте и других Python-скриптах.

Для CLI-поиска используйте rag_search.py
"""

import sys
from pathlib import Path
from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass

try:
    # Для langchain >= 1.0 импорты изменились
    try:
        from langchain_community.embeddings import HuggingFaceEmbeddings
        from langchain_community.vectorstores import FAISS
    except ImportError:
        # Fallback для старых версий
        from langchain.embeddings import HuggingFaceEmbeddings
        from langchain.vectorstores import FAISS
    
    try:
        from langchain_core.documents import Document
    except ImportError:
        from langchain.schema import Document
    
    # Исправление docstore при загрузке
    try:
        from langchain_community.docstore.in_memory import InMemoryDocstore
    except ImportError:
        try:
            from langchain.docstore.in_memory import InMemoryDocstore
        except ImportError:
            # Fallback: создаем простой docstore-объект
            class SimpleDocstore:
                def __init__(self):
                    self._dict = {}
                def add(self, ids_and_docs):
                    for id, doc in ids_and_docs:
                        self._dict[id] = doc
                def search(self, search: str):
                    return self._dict.get(search)
            InMemoryDocstore = SimpleDocstore
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Установите зависимости: pip install langchain langchain-community langchain-core faiss-cpu sentence-transformers")
    sys.exit(1)


@dataclass
class SearchResult:
    """Результат поиска с метаданными"""
    path: str
    symbol: Optional[str]
    kind: str
    language: str
    target: Optional[str]
    content: str
    score: float
    relevance: str  # "высокая", "средняя", "низкая"


class RAGSearch:
    """Класс для поиска контекста в RAG индексе"""
    
    def __init__(self, index_path: str):
        """
        Инициализация поиска
        
        Args:
            index_path: Путь к директории с FAISS индексом (содержит index.faiss и index.pkl)
        """
        self.index_path = Path(index_path)
        if not self.index_path.exists():
            raise FileNotFoundError(f"Индекс не найден: {index_path}")
        
        print(f"Загрузка RAG индекса из {index_path}...")
        self.embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
            model_kwargs={'device': 'cpu'}
        )
        
        try:
            self.vectorstore = FAISS.load_local(
                str(self.index_path), 
                self.embeddings, 
                allow_dangerous_deserialization=True
            )
            
            # Исправляем docstore если он загрузился как словарь
            if isinstance(self.vectorstore.docstore, dict):
                print("  Исправление docstore (загружен как dict)...")
                docstore_dict = self.vectorstore.docstore
                new_docstore = InMemoryDocstore()
                
                if hasattr(new_docstore, '_dict'):
                    new_docstore._dict = docstore_dict.copy()
                else:
                    # Пробуем разные варианты
                    for attr_name in ['_dict', 'docstore', 'docs']:
                        if hasattr(new_docstore, attr_name):
                            setattr(new_docstore, attr_name, docstore_dict.copy())
                            break
                    else:
                        # Прямое присваивание
                        new_docstore.__dict__['_dict'] = docstore_dict.copy()
                
                self.vectorstore.docstore = new_docstore
                print("  Docstore исправлен!")
            
            print("Индекс успешно загружен!")
        except Exception as e:
            print(f"Ошибка загрузки индекса: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _determine_relevance(self, score: float) -> str:
        """Определение уровня релевантности по score"""
        if score < 0.5:
            return "высокая"
        elif score < 1.0:
            return "средняя"
        else:
            return "низкая"
    
    def get_context_for_test(
        self, 
        test_name: str, 
        log: str, 
        k: int = 5,
        prefer_test_files: bool = True,
        max_log_length: int = 500
    ) -> str:
        """
        Получить релевантный контекст для теста
        
        Args:
            test_name: Имя теста (например, "test_etl_pipeline")
            log: Лог ошибки теста
            k: Количество чанков для возврата
            prefer_test_files: Приоритет файлам с kind="test"
            max_log_length: Максимальная длина лога для запроса
        
        Returns:
            Строка с форматированным контекстом для промпта LLM
        """
        # Комбинируем запрос из имени теста и лога
        log_truncated = log[:max_log_length] if log else ""
        query = f"{test_name} {log_truncated}".strip()
        
        if not query:
            return "Запрос пуст"
        
        # Векторный поиск (берем больше кандидатов для фильтрации)
        candidate_k = k * 3 if prefer_test_files else k
        results = self.vectorstore.similarity_search_with_score(query, k=candidate_k)
        
        # Фильтруем и сортируем
        if prefer_test_files:
            # Приоритет тестовым файлам
            test_results = [(doc, score) for doc, score in results 
                          if doc.metadata.get('kind') == 'test']
            other_results = [(doc, score) for doc, score in results 
                          if doc.metadata.get('kind') != 'test']
            # Берем половину из тестов, половину из остальных
            test_count = min(len(test_results), k // 2 + 1)
            other_count = k - test_count
            results = test_results[:test_count] + other_results[:other_count]
        
        # Сортируем по возрастанию score (меньший = выше релевантность)
        results = sorted(results, key=lambda x: x[1])[:k]
        
        # Формируем контекст
        context_parts = []
        for i, (doc, score) in enumerate(results, 1):
            relevance = self._determine_relevance(score)
            context_parts.append(
                f"=== Контекст {i} (релевантность: {relevance}, score: {score:.4f}) ===\n"
                f"Файл: {doc.metadata.get('path', 'N/A')}\n"
                f"Символ: {doc.metadata.get('symbol', 'N/A')}\n"
                f"Тип: {doc.metadata.get('kind', 'N/A')}\n"
                f"Язык: {doc.metadata.get('language', 'N/A')}\n"
            )
            if doc.metadata.get('target'):
                context_parts[-1] += f"Таргет: {doc.metadata['target']}\n"
            context_parts[-1] += f"---\n{doc.page_content[:1000]}\n"
        
        return "\n\n".join(context_parts)
    
    def hybrid_search(
        self, 
        query: str, 
        k: int = 5,
        exact_match_boost: float = 2.0,
        path_filter: Optional[str] = None,
        language_filter: Optional[str] = None,
        max_candidates: int = 1000
    ) -> List[SearchResult]:
        """
        Гибридный поиск: векторный + точное совпадение
        
        Args:
            query: Поисковый запрос
            k: Количество результатов
            exact_match_boost: Множитель для точных совпадений (улучшает score)
            path_filter: Фильтр по подстроке в пути (например, ".github/actions")
            language_filter: Фильтр по языку (например, "yaml", "python")
            max_candidates: Максимальное количество кандидатов для фильтрации
        
        Returns:
            Список SearchResult с отсортированными результатами
        """
        if not query:
            return []
        
        # Если есть фильтры, берем намного больше кандидатов для фильтрации
        # Без фильтров: k*3, с фильтрами: 500-1000, чтобы найти нужные файлы
        if path_filter or language_filter:
            candidate_k = max(k * 50, 500)  # Увеличиваем для фильтров
        else:
            candidate_k = k * 3
        candidate_k = min(candidate_k, max_candidates)
        
        # Векторный поиск (берем больше кандидатов для фильтрации)
        vector_results = self.vectorstore.similarity_search_with_score(query, k=candidate_k)
        initial_count = len(vector_results)
        
        # Применяем фильтры
        if path_filter:
            before_count = len(vector_results)
            vector_results = [
                (doc, score) for doc, score in vector_results
                if path_filter in (doc.metadata.get('path', '') or '')
            ]
            # Если ничего не найдено после фильтра пути, попробуем еще раз с большим k
            if len(vector_results) == 0 and before_count > 0:
                # Увеличиваем k еще больше
                larger_k = min(candidate_k * 2, max_candidates)
                vector_results = self.vectorstore.similarity_search_with_score(query, k=larger_k)
                vector_results = [
                    (doc, score) for doc, score in vector_results
                    if path_filter in (doc.metadata.get('path', '') or '')
                ]
        
        if language_filter:
            lang_lower = language_filter.lower()
            before_count = len(vector_results)
            vector_results = [
                (doc, score) for doc, score in vector_results
                if (doc.metadata.get('language', '') or '').lower() == lang_lower
            ]
            # Если ничего не найдено после фильтра языка, попробуем еще раз
            if len(vector_results) == 0 and before_count > 0:
                # Увеличиваем k еще больше
                larger_k = min(candidate_k * 2, max_candidates)
                vector_results = self.vectorstore.similarity_search_with_score(query, k=larger_k)
                if path_filter:
                    vector_results = [
                        (doc, score) for doc, score in vector_results
                        if path_filter in (doc.metadata.get('path', '') or '')
                    ]
                vector_results = [
                    (doc, score) for doc, score in vector_results
                    if (doc.metadata.get('language', '') or '').lower() == lang_lower
                ]
        
        # Точное совпадение подстроки
        query_lower = query.lower()
        exact_matches = []
        other_results = []
        
        for doc, score in vector_results:
            content_lower = doc.page_content.lower()
            if query_lower in content_lower:
                # Улучшаем score для точных совпадений (делим на boost)
                exact_matches.append((doc, score / exact_match_boost))
            else:
                other_results.append((doc, score))
        
        # Комбинируем: сначала точные совпадения, потом остальные
        all_results = exact_matches + other_results
        
        # Если после всех фильтров ничего не найдено, попробуем прямой поиск по метаданным
        if len(all_results) == 0 and (path_filter or language_filter):
            # Fallback: прямой поиск по метаданным через docstore
            # Это медленнее, но гарантирует нахождение файлов по фильтрам
            try:
                if hasattr(self.vectorstore, 'docstore'):
                    docstore_dict = None
                    if hasattr(self.vectorstore.docstore, '_dict'):
                        docstore_dict = self.vectorstore.docstore._dict
                    elif isinstance(self.vectorstore.docstore, dict):
                        docstore_dict = self.vectorstore.docstore
                    
                    if docstore_dict:
                        # Ищем все документы, соответствующие фильтрам
                        matching_docs = []
                        query_embedding = self.embeddings.embed_query(query)
                        
                        for doc_id, doc in docstore_dict.items():
                            matches = True
                            if path_filter and path_filter not in (doc.metadata.get('path', '') or ''):
                                matches = False
                            if language_filter and (doc.metadata.get('language', '') or '').lower() != language_filter.lower():
                                matches = False
                            
                            if matches:
                                # Вычисляем score через эмбеддинги
                                try:
                                    # Получаем индекс документа в FAISS
                                    if hasattr(self.vectorstore, 'index'):
                                        # Пробуем найти индекс документа
                                        doc_idx = int(doc_id) if doc_id.isdigit() else hash(doc_id) % self.vectorstore.index.ntotal
                                        if doc_idx < self.vectorstore.index.ntotal:
                                            doc_embedding = self.vectorstore.index.reconstruct(doc_idx)
                                            # L2 расстояние
                                            import numpy as np
                                            score = float(np.linalg.norm(np.array(query_embedding) - np.array(doc_embedding)))
                                            matching_docs.append((doc, score))
                                except Exception:
                                    # Если не удалось вычислить score, используем большое значение
                                    matching_docs.append((doc, 10.0))
                        
                        if matching_docs:
                            # Сортируем и берем топ-k
                            matching_docs = sorted(matching_docs, key=lambda x: x[1])[:k]
                            all_results = matching_docs
            except Exception as e:
                # Если fallback не сработал, просто продолжаем с пустым результатом
                pass
        
        # Сортируем по возрастанию score и берем топ-k
        all_results = sorted(all_results, key=lambda x: x[1])[:k]
        
        # Преобразуем в SearchResult
        search_results = []
        for doc, score in all_results:
            search_results.append(SearchResult(
                path=doc.metadata.get('path', 'N/A'),
                symbol=doc.metadata.get('symbol'),
                kind=doc.metadata.get('kind', 'N/A'),
                language=doc.metadata.get('language', 'N/A'),
                target=doc.metadata.get('target'),
                content=doc.page_content,
                score=score,
                relevance=self._determine_relevance(score)
            ))
        
        return search_results
    
    def search(
        self,
        query: str,
        k: int = 5,
        use_hybrid: bool = True,
        path_filter: Optional[str] = None,
        language_filter: Optional[str] = None,
        max_candidates: int = 1000
    ) -> List[SearchResult]:
        """
        Универсальный метод поиска
        
        Args:
            query: Поисковый запрос
            k: Количество результатов
            use_hybrid: Использовать гибридный поиск (True) или только векторный (False)
            path_filter: Фильтр по подстроке в пути
            language_filter: Фильтр по языку
            max_candidates: Максимальное количество кандидатов для фильтрации
        
        Returns:
            Список SearchResult
        """
        if use_hybrid:
            return self.hybrid_search(
                query, 
                k=k, 
                path_filter=path_filter, 
                language_filter=language_filter,
                max_candidates=max_candidates
            )
        else:
            # Простой векторный поиск
            # Если есть фильтры, берем больше кандидатов
            candidate_k = max(k * 10, 100) if (path_filter or language_filter) else k
            candidate_k = min(candidate_k, max_candidates)
            
            results = self.vectorstore.similarity_search_with_score(query, k=candidate_k)
            
            # Применяем фильтры
            if path_filter:
                results = [
                    (doc, score) for doc, score in results
                    if path_filter in (doc.metadata.get('path', '') or '')
                ]
            
            if language_filter:
                lang_lower = language_filter.lower()
                results = [
                    (doc, score) for doc, score in results
                    if (doc.metadata.get('language', '') or '').lower() == lang_lower
                ]
            
            # Сортируем и преобразуем
            results = sorted(results, key=lambda x: x[1])[:k]
            return [
                SearchResult(
                    path=doc.metadata.get('path', 'N/A'),
                    symbol=doc.metadata.get('symbol'),
                    kind=doc.metadata.get('kind', 'N/A'),
                    language=doc.metadata.get('language', 'N/A'),
                    target=doc.metadata.get('target'),
                    content=doc.page_content,
                    score=score,
                    relevance=self._determine_relevance(score)
                )
                for doc, score in results
            ]


if __name__ == "__main__":
    # Простой тест
    import argparse
    
    parser = argparse.ArgumentParser(description="Тест RAG поиска")
    parser.add_argument("--index", type=str, default="./rag_index/faiss_index",
                       help="Путь к FAISS индексу")
    parser.add_argument("--query", type=str, required=True,
                       help="Поисковый запрос")
    parser.add_argument("--k", type=int, default=5,
                       help="Количество результатов")
    parser.add_argument("--path-contains", type=str, default=None,
                       help="Фильтр по подстроке в пути")
    parser.add_argument("--language", type=str, default=None,
                       help="Фильтр по языку")
    parser.add_argument("--no-hybrid", action="store_true",
                       help="Отключить гибридный поиск")
    
    args = parser.parse_args()
    
    try:
        search = RAGSearch(args.index)
        results = search.search(
            args.query,
            k=args.k,
            use_hybrid=not args.no_hybrid,
            path_filter=args.path_contains,
            language_filter=args.language
        )
        
        print(f"\nНайдено результатов: {len(results)}")
        print("=" * 60)
        
        for i, result in enumerate(results, 1):
            print(f"\nРезультат {i} (score: {result.score:.4f} - {result.relevance} релевантность):")
            print(f"  PATH: {result.path}")
            print(f"  SYMBOL: {result.symbol or 'N/A'}")
            print(f"  KIND: {result.kind}")
            print(f"  LANGUAGE: {result.language}")
            if result.target:
                print(f"  TARGET: {result.target}")
            print("  ---")
            print(f"  Содержимое (первые 300 символов):")
            print(f"  {result.content[:300]}...")
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
