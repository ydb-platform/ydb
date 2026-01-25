#!/usr/bin/env python3
"""
CLI-утилита для быстрого поиска в RAG индексе
Использование: python3 rag_search.py --query "your search query"
"""

import sys
from pathlib import Path
from rag_client import RAGSearch

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Поиск в RAG индексе кода",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Простой поиск
  python3 rag_search.py --query "test_etl_pipeline"
  
  # Поиск с фильтром по пути
  python3 rag_search.py --query "build graphs" --path-contains ".github/actions"
  
  # Поиск только в YAML файлах
  python3 rag_search.py --query "increment" --language yaml
  
  # Получить контекст для теста
  python3 rag_search.py --query "test_name error_message" --k 10
        """
    )
    parser.add_argument("--index", type=str, default="./rag_index/faiss_index",
                       help="Путь к FAISS индексу (по умолчанию: ./rag_index/faiss_index)")
    parser.add_argument("--query", type=str, required=True,
                       help="Поисковый запрос")
    parser.add_argument("--k", type=int, default=5,
                       help="Количество результатов (по умолчанию: 5)")
    parser.add_argument("--path-contains", type=str, default=None,
                       help="Фильтр по подстроке в пути (например, '.github/actions')")
    parser.add_argument("--language", type=str, default=None,
                       help="Фильтр по языку (например, 'yaml', 'python', 'cpp')")
    parser.add_argument("--no-hybrid", action="store_true",
                       help="Отключить гибридный поиск (только векторный)")
    parser.add_argument("--full-content", action="store_true",
                       help="Показать полное содержимое (не только первые 300 символов)")
    
    args = parser.parse_args()
    
    # Проверяем существование индекса
    index_path = Path(args.index)
    if not index_path.exists():
        print(f"Ошибка: индекс не найден: {args.index}")
        print(f"Убедитесь, что вы запускаете скрипт из директории с rag_index/")
        sys.exit(1)
    
    try:
        print(f"Загрузка индекса из {args.index}...")
        search = RAGSearch(args.index)
        
        print(f"\nПоиск: '{args.query}'")
        if args.path_contains:
            print(f"Фильтр по пути: {args.path_contains}")
        if args.language:
            print(f"Фильтр по языку: {args.language}")
        print("=" * 60)
        
        # Используем больший max_candidates при наличии фильтров
        max_candidates = 2000 if (args.path_contains or args.language) else 1000
        
        results = search.search(
            args.query,
            k=args.k,
            use_hybrid=not args.no_hybrid,
            path_filter=args.path_contains,
            language_filter=args.language,
            max_candidates=max_candidates
        )
        
        if not results:
            print("Результаты не найдены")
            print("\nПопробуйте:")
            print("  - Увеличить k: --k 20")
            print("  - Убрать фильтры и поискать без них")
            print("  - Использовать более общий запрос")
            return
        
        print(f"\nНайдено результатов: {len(results)}")
        print("=" * 60)
        
        for i, result in enumerate(results, 1):
            print(f"\n{'='*60}")
            print(f"Результат {i} (score: {result.score:.4f} - {result.relevance} релевантность)")
            print(f"{'='*60}")
            print(f"PATH: {result.path}")
            print(f"SYMBOL: {result.symbol or 'N/A'}")
            print(f"KIND: {result.kind}")
            print(f"LANGUAGE: {result.language}")
            if result.target:
                print(f"TARGET: {result.target}")
            print("-" * 60)
            print("СОДЕРЖИМОЕ:")
            if args.full_content:
                print(result.content)
            else:
                print(result.content[:500] + ("..." if len(result.content) > 500 else ""))
            print()
    
    except KeyboardInterrupt:
        print("\n\nПрервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
