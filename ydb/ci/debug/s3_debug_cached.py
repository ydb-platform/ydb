#!/usr/bin/env python3
import subprocess
import json
import pickle
import sys
import os
import time
import argparse
from datetime import datetime
import concurrent.futures
from threading import Lock

# Параметры
bucket_name = "ydb-gh-logs"
default_prefix = "ydb-platform/ydb/"
max_keys = 10000
cache_file = "s3_cache.pkl"

def human_readable_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def run_yc_command(continuation_token=None, prefix=None, delimiter=None):
    command = [
        "yc", "storage", "s3api", "list-objects",
        "--bucket", bucket_name,
        "--prefix", prefix or default_prefix,
        "--max-keys", str(max_keys),
        "--format", "json"
    ]
    
    if continuation_token:
        command.extend(["--continuation-token", continuation_token])
    
    if delimiter:
        command.extend(["--delimiter", delimiter])
    
    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Ошибка для префикса {prefix}: {result.stderr}", file=sys.stderr)
        return None
    
    return json.loads(result.stdout)

def get_first_level_folders(prefix=None):
    """Получает папки первого уровня используя delimiter"""
    print(f"🔍 Получение папок первого уровня для префикса: {prefix or default_prefix}", file=sys.stderr)
    
    result = run_yc_command(prefix=prefix, delimiter='/')
    if not result:
        return []
    
    # Получаем common prefixes (папки)
    common_prefixes = result.get("common_prefixes", [])
    folders = [cp.get("prefix", "") for cp in common_prefixes]
    
    print(f"📁 Найдено папок первого уровня: {len(folders)}", file=sys.stderr)
    for folder in folders:
        print(f"   📂 {folder}", file=sys.stderr)
    
    return folders

def get_build_types_from_pr_folders(prefix="ydb-platform/ydb/PR-check/"):
    """Получает все папки из prefix и автоматически определяет типы билдов"""
    print(f"🔍 Получение папок из {prefix}...", file=sys.stderr)
    
    # Получаем все папки
    pr_folders = []
    continuation_token = None
    
    while True:
        result = run_yc_command(continuation_token, prefix, delimiter='/')
        if not result:
            break
            
        common_prefixes = result.get("common_prefixes", [])
        if not common_prefixes:
            break
            
        pr_folders.extend([cp.get("prefix", "") for cp in common_prefixes])
        
        continuation_token = result.get("next_continuation_token")
        if not continuation_token:
            break
    
    print(f"📁 Найдено {len(pr_folders)} папок в {prefix}", file=sys.stderr)
    
    # Автоматически определяем типы билдов из первых папок
    build_types = set()
    sample_size = min(20, len(pr_folders))  # Увеличиваем выборку для лучшего покрытия
    
    print(f"🔍 Анализ типов билдов в {sample_size} папках...", file=sys.stderr)
    
    for i, pr_folder in enumerate(pr_folders[:sample_size]):
        if i % 5 == 0:
            print(f"   📂 Анализ папки {i+1}/{sample_size}: {pr_folder.split('/')[-2]}", file=sys.stderr)
        
        result = run_yc_command(prefix=pr_folder, delimiter='/')
        if result:
            common_prefixes = result.get("common_prefixes", [])
            for cp in common_prefixes:
                build_type = cp.get("prefix", "").split('/')[-2]
                if build_type:
                    build_types.add(build_type)
    
    build_types = sorted(list(build_types))
    print(f"🏗️  Найдено типов билдов: {build_types}", file=sys.stderr)
    
    return pr_folders, build_types

def collect_folder_data_simple(folder_prefix):
    """Упрощенная версия сбора данных для одной папки (без детального анализа структуры)"""
    folder_files = []
    folder_total_files = 0
    folder_total_size = 0
    oldest_date = None
    
    continuation_token = None
    
    while True:
        result = run_yc_command(continuation_token, folder_prefix)
        
        if not result:
            break
            
        contents = result.get("contents", [])
        if not contents:
            break
        
        for obj in contents:
            key = obj.get("key", "")
            size = int(obj.get("size", 0))
            last_modified = obj.get("last_modified", "")
            
            if not key.startswith(folder_prefix):
                continue
            
            folder_total_files += 1
            folder_total_size += size
            
            # Отслеживаем самую старую дату
            if last_modified:
                try:
                    file_date = datetime.fromisoformat(last_modified.replace('Z', '+00:00'))
                    if oldest_date is None or file_date < oldest_date:
                        oldest_date = file_date
                except (ValueError, TypeError):
                    pass  # Игнорируем некорректные даты
            
            file_info = {
                'key': key,
                'size': size,
                'last_modified': last_modified
            }
            folder_files.append(file_info)
        
        continuation_token = result.get("next_continuation_token")
        if not continuation_token:
            break
    
    return {
        'folder_prefix': folder_prefix,
        'files': folder_files,
        'total_files': folder_total_files,
        'total_size': folder_total_size,
        'oldest_date': oldest_date.isoformat() if oldest_date else None
    }

def collect_pr_folder_build_types(pr_folder, known_build_types):
    """Собирает данные по типам билдов для одной папки PR-check"""
    folder_build_data = {}
    
    # Получаем типы билдов в этой папке
    result = run_yc_command(prefix=pr_folder, delimiter='/')
    if not result:
        return folder_build_data
        
    common_prefixes = result.get("common_prefixes", [])
    
    for cp in common_prefixes:
        build_folder = cp.get("prefix", "")
        build_type = build_folder.split('/')[-2]
        
        if build_type not in known_build_types:
            continue
        
        # Собираем данные для этого типа билда
        folder_data = collect_folder_data_simple(build_folder)
        if folder_data:
            folder_build_data[build_type] = {
                'size': folder_data['total_size'],
                'files': folder_data['total_files'],
                'folder_path': build_folder,
                'oldest_date': folder_data['oldest_date']
            }
    
    return folder_build_data

def collect_build_types_data(prefix="ydb-platform/ydb/PR-check/"):
    """Собирает данные агрегированные по типам билдов с параллельной обработкой"""
    start_time = time.time()
    
    print(f"🚀 Параллельный сбор данных по типам билдов из {prefix}...", file=sys.stderr)
    
    # Получаем папки и типы билдов
    pr_folders, build_types = get_build_types_from_pr_folders(prefix)
    
    if not pr_folders or not build_types:
        print("❌ Не найдено папок или типов билдов", file=sys.stderr)
        return None
    
    # Инициализируем структуры данных для агрегации
    build_type_stats = {build_type: {'size': 0, 'files': 0, 'folders': 0, 'oldest_date': None} for build_type in build_types}
    build_type_files = {build_type: [] for build_type in build_types}
    
    total_processed = 0
    completed_folders = 0
    processed_lock = Lock()
    
    # Уменьшаем количество потоков для стабильности
    max_workers = min(15, len(pr_folders))
    print(f"🔄 Параллельная обработка {len(pr_folders)} папок (макс. {max_workers} потоков)...", file=sys.stderr)
    
    def print_progress():
        """Выводит прогресс-бар"""
        if len(pr_folders) == 0:
            return
            
        progress = completed_folders / len(pr_folders) * 100
        bar_length = 50
        filled_length = int(bar_length * completed_folders // len(pr_folders))
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        
        elapsed = time.time() - start_time
        if completed_folders > 0:
            eta = elapsed * (len(pr_folders) - completed_folders) / completed_folders
            eta_str = f"ETA: {eta:.0f}s"
        else:
            eta_str = "ETA: --"
        
        print(f"\r📊 [{bar}] {progress:.1f}% ({completed_folders}/{len(pr_folders)}) | {eta_str} | Билды: {total_processed}", 
              end='', file=sys.stderr, flush=True)
    
    def process_pr_folder(pr_folder):
        nonlocal total_processed, completed_folders
        try:
            folder_build_data = collect_pr_folder_build_types(pr_folder, build_types)
            
            # Обновляем статистику потокобезопасно
            with processed_lock:
                folder_processed = 0
                for build_type, data in folder_build_data.items():
                    build_type_stats[build_type]['size'] += data['size']
                    build_type_stats[build_type]['files'] += data['files']
                    build_type_stats[build_type]['folders'] += 1
                    
                    # Обновляем самую старую дату
                    if data['oldest_date']:
                        try:
                            file_date = datetime.fromisoformat(data['oldest_date'])
                            current_oldest = build_type_stats[build_type]['oldest_date']
                            if current_oldest is None:
                                build_type_stats[build_type]['oldest_date'] = data['oldest_date']
                            else:
                                current_oldest_date = datetime.fromisoformat(current_oldest)
                                if file_date < current_oldest_date:
                                    build_type_stats[build_type]['oldest_date'] = data['oldest_date']
                        except (ValueError, TypeError):
                            pass  # Игнорируем некорректные даты
                    
                    folder_processed += 1
                
                total_processed += folder_processed
                completed_folders += 1
                
                # Обновляем прогресс чаще для лучшей обратной связи
                if completed_folders % 1 == 0:  # Каждую папку
                    print_progress()
            
            return folder_build_data
            
        except Exception as e:
            with processed_lock:
                completed_folders += 1
                pr_name = pr_folder.split('/')[-2] if '/' in pr_folder else pr_folder
                print(f"\n❌ Ошибка в папке {pr_name}: {str(e)[:100]}", file=sys.stderr)
                print_progress()
            return {}
    
    # Параллельная обработка с уменьшенным количеством потоков
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Запускаем задачи
        future_to_folder = {
            executor.submit(process_pr_folder, pr_folder): pr_folder 
            for pr_folder in pr_folders
        }
        
        # Собираем результаты с таймаутом
        try:
            for future in concurrent.futures.as_completed(future_to_folder, timeout=300):  # 5 минут таймаут
                pr_folder = future_to_folder[future]
                try:
                    folder_build_data = future.result(timeout=60)  # 1 минута на папку
                    # Результат уже обработан в process_pr_folder
                except concurrent.futures.TimeoutError:
                    pr_name = pr_folder.split('/')[-2] if '/' in pr_folder else pr_folder
                    with processed_lock:
                        completed_folders += 1
                        print(f"\n⏰ Таймаут для папки {pr_name}", file=sys.stderr)
                        print_progress()
                except Exception as exc:
                    pr_name = pr_folder.split('/')[-2] if '/' in pr_folder else pr_folder
                    with processed_lock:
                        completed_folders += 1
                        print(f"\n❌ Ошибка обработки папки {pr_name}: {str(exc)[:100]}", file=sys.stderr)
                        print_progress()
        except concurrent.futures.TimeoutError:
            print(f"\n⏰ Общий таймаут достигнут, завершаем обработку...", file=sys.stderr)
    
    # Финальный прогресс
    print_progress()
    print("", file=sys.stderr)  # Новая строка после прогресс-бара
    
    collection_time = time.time() - start_time
    
    # Формируем результат
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'prefix': prefix,
        'build_type_stats': build_type_stats,
        'build_type_files': build_type_files,
        'build_types': build_types,
        'total_pr_folders': len(pr_folders),
        'total_processed_folders': total_processed,
        'collection_time': collection_time,
        'collection_method': 'build_types_parallel_aggregation'
    }
    
    # Сохраняем в кэш
    build_types_cache_file = "s3_build_types_cache.pkl"
    with open(build_types_cache_file, 'wb') as f:
        pickle.dump(cache_data, f)
    
    print(f"💾 Данные по типам билдов сохранены в {build_types_cache_file}", file=sys.stderr)
    print(f"⚡ Время сбора: {collection_time:.1f}с", file=sys.stderr)
    print(f"📊 Обработано {total_processed} папок с билдами из {len(pr_folders)} папок", file=sys.stderr)
    
    return cache_data

def analyze_build_types_data(data):
    """Анализирует и выводит данные по типам билдов"""
    build_type_stats = data['build_type_stats']
    build_types = data['build_types']
    
    print("\n" + "="*80)
    print("АГРЕГАЦИЯ ПО ТИПАМ БИЛДОВ")
    print("="*80)
    
    # Сортируем по размеру
    sorted_build_types = sorted(
        build_types, 
        key=lambda bt: build_type_stats[bt]['size'], 
        reverse=True
    )
    
    print(f"{'Тип билда':<35} | {'Размер':>12} | {'Файлов':>10} | {'Папок':>8} | {'Самый старый файл':>16}")
    print("-" * 95)
    
    total_size = 0
    total_files = 0
    total_folders = 0
    
    for build_type in sorted_build_types:
        stats = build_type_stats[build_type]
        size = stats['size']
        files = stats['files']
        folders = stats['folders']
        oldest_date = stats.get('oldest_date')
        
        if oldest_date:
            try:
                oldest_dt = datetime.fromisoformat(oldest_date)
                oldest_str = oldest_dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                oldest_str = "Неизвестно"
        else:
            oldest_str = "Неизвестно"
        
        total_size += size
        total_files += files
        total_folders += folders
        
        print(f"{build_type:<35} | {human_readable_size(size):>12} | {files:>10} | {folders:>8} | {oldest_str:>16}")
    
    print("-" * 95)
    print(f"{'ИТОГО':<35} | {human_readable_size(total_size):>12} | {total_files:>10} | {total_folders:>8} | {'--':>16}")
    
    print(f"\n" + "="*80)
    print("СТАТИСТИКА СБОРА")
    print("="*80)
    print(f"📁 Всего папок: {data['total_pr_folders']}")
    print(f"🏗️  Обработано папок с билдами: {data['total_processed_folders']}")
    print(f"⏱️  Время сбора: {data['collection_time']:.1f} секунд")
    print(f"📅 Дата сбора: {datetime.fromisoformat(data['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")

def load_build_types_cache():
    """Загружает данные по типам билдов из кэша"""
    build_types_cache_file = "s3_build_types_cache.pkl"
    
    if not os.path.exists(build_types_cache_file):
        print(f"❌ Файл кэша {build_types_cache_file} не найден. Запустите сбор данных по типам билдов.", file=sys.stderr)
        return None
    
    try:
        with open(build_types_cache_file, 'rb') as f:
            data = pickle.load(f)
        
        cache_time = datetime.fromisoformat(data['timestamp'])
        print(f"📂 Загружены данные по типам билдов из кэша (собраны: {cache_time.strftime('%Y-%m-%d %H:%M:%S')})", file=sys.stderr)
        return data
    except Exception as e:
        print(f"❌ Ошибка загрузки кэша типов билдов: {e}", file=sys.stderr)
        return None

def collect_all_folders_build_types(prefix="ydb-platform/ydb/"):
    """Собирает данные по типам билдов для всех папок первого уровня"""
    start_time = time.time()
    
    print(f"🚀 Сбор данных по типам билдов для всех папок в {prefix}...", file=sys.stderr)
    
    # Получаем папки первого уровня
    first_level_folders = get_first_level_folders(prefix)
    
    if not first_level_folders:
        print("❌ Не найдено папок первого уровня", file=sys.stderr)
        return None
    
    # Инициализируем общую агрегацию
    all_build_type_stats = {}
    all_build_types = set()
    folder_results = {}
    
    total_folders_processed = 0
    total_build_folders = 0
    total_size_all = 0
    total_files_all = 0
    
    print(f"📁 Найдено {len(first_level_folders)} папок для обработки", file=sys.stderr)
    
    # Обрабатываем каждую папку последовательно
    for i, folder in enumerate(first_level_folders):
        folder_name = folder.split('/')[-2]
        print(f"\n" + "="*80, file=sys.stderr)
        print(f"📂 Обработка папки {i+1}/{len(first_level_folders)}: {folder_name}", file=sys.stderr)
        print("="*80, file=sys.stderr)
        
        # Собираем данные для этой папки
        folder_data = collect_build_types_data(folder)
        
        if folder_data and folder_data['build_type_stats']:
            # Показываем детальную аналитику по этой папке
            print(f"\n🔍 ДЕТАЛЬНАЯ АНАЛИТИКА ПО ПАПКЕ: {folder_name}", file=sys.stderr)
            print("="*80, file=sys.stderr)
            analyze_build_types_data(folder_data)
            
            # Добавляем типы билдов в общий список
            all_build_types.update(folder_data['build_types'])
            
            # Агрегируем статистику
            for build_type, stats in folder_data['build_type_stats'].items():
                if build_type not in all_build_type_stats:
                    all_build_type_stats[build_type] = {'size': 0, 'files': 0, 'folders': 0, 'source_folders': [], 'oldest_date': None}
                
                all_build_type_stats[build_type]['size'] += stats['size']
                all_build_type_stats[build_type]['files'] += stats['files']
                all_build_type_stats[build_type]['folders'] += stats['folders']
                all_build_type_stats[build_type]['source_folders'].append(folder_name)
                
                # Обновляем самую старую дату
                if stats.get('oldest_date'):
                    try:
                        file_date = datetime.fromisoformat(stats['oldest_date'])
                        current_oldest = all_build_type_stats[build_type]['oldest_date']
                        if current_oldest is None:
                            all_build_type_stats[build_type]['oldest_date'] = stats['oldest_date']
                        else:
                            current_oldest_date = datetime.fromisoformat(current_oldest)
                            if file_date < current_oldest_date:
                                all_build_type_stats[build_type]['oldest_date'] = stats['oldest_date']
                    except (ValueError, TypeError):
                        pass  # Игнорируем некорректные даты
            
            # Сохраняем результат папки
            folder_results[folder_name] = folder_data
            
            # Обновляем общие счетчики
            total_build_folders += folder_data['total_processed_folders']
            folder_total_size = sum(stats['size'] for stats in folder_data['build_type_stats'].values())
            folder_total_files = sum(stats['files'] for stats in folder_data['build_type_stats'].values())
            total_size_all += folder_total_size
            total_files_all += folder_total_files
            total_folders_processed += 1
            
            # Показываем краткую сводку по текущей папке
            print(f"\n✅ СВОДКА ПО ПАПКЕ {folder_name}:", file=sys.stderr)
            print(f"   🏗️  Типов билдов: {len(folder_data['build_type_stats'])}", file=sys.stderr)
            print(f"   📊 Папок с билдами: {folder_data['total_processed_folders']}", file=sys.stderr)
            print(f"   💾 Размер: {human_readable_size(folder_total_size)}", file=sys.stderr)
            print(f"   📄 Файлов: {folder_total_files:,}", file=sys.stderr)
            print(f"   ⏱️  Время: {folder_data['collection_time']:.1f}с", file=sys.stderr)
        else:
            print(f"⚠️  Папка {folder_name}: нет данных по типам билдов", file=sys.stderr)
        
        # Показываем общий прогресс
        elapsed = time.time() - start_time
        remaining = len(first_level_folders) - (i + 1)
        if i > 0:
            avg_time = elapsed / (i + 1)
            eta = avg_time * remaining
            eta_str = f"ETA: {eta:.0f}s"
        else:
            eta_str = "ETA: --"
        
        print(f"\n🔄 ОБЩИЙ ПРОГРЕСС: {i+1}/{len(first_level_folders)} папок | {eta_str}", file=sys.stderr)
        
        # Добавляем разделитель между папками
        if i < len(first_level_folders) - 1:  # Не добавляем в конце
            print(f"\n{'='*80}", file=sys.stderr)
            print(f"ПЕРЕХОДИМ К СЛЕДУЮЩЕЙ ПАПКЕ...", file=sys.stderr)
            print(f"{'='*80}", file=sys.stderr)
    
    collection_time = time.time() - start_time
    
    # Формируем итоговый результат
    all_build_types = sorted(list(all_build_types))
    
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'prefix': prefix,
        'all_build_type_stats': all_build_type_stats,
        'all_build_types': all_build_types,
        'folder_results': folder_results,
        'total_first_level_folders': len(first_level_folders),
        'total_folders_with_builds': total_folders_processed,
        'total_build_folders': total_build_folders,
        'total_size': total_size_all,
        'total_files': total_files_all,
        'collection_time': collection_time,
        'collection_method': 'all_folders_build_types_aggregation'
    }
    
    # Сохраняем в отдельный кэш
    all_build_types_cache_file = "s3_all_build_types_cache.pkl"
    with open(all_build_types_cache_file, 'wb') as f:
        pickle.dump(cache_data, f)
    
    print(f"\n" + "="*80, file=sys.stderr)
    print("🎉 ВСЕ ПАПКИ ОБРАБОТАНЫ!", file=sys.stderr)
    print("="*80, file=sys.stderr)
    print(f"💾 Данные сохранены в {all_build_types_cache_file}", file=sys.stderr)
    
    # Показываем финальную статистику
    analyze_all_build_types_data(cache_data)
    
    return cache_data

def analyze_all_build_types_data(data):
    """Анализирует и выводит итоговую статистику по всем папкам"""
    all_build_type_stats = data['all_build_type_stats']
    all_build_types = data['all_build_types']
    folder_results = data['folder_results']
    
    print("\n" + "="*110)
    print("ИТОГОВАЯ АГРЕГАЦИЯ ПО ВСЕМ ТИПАМ БИЛДОВ")
    print("="*110)
    
    # Сортируем по размеру
    sorted_build_types = sorted(
        all_build_types, 
        key=lambda bt: all_build_type_stats.get(bt, {}).get('size', 0), 
        reverse=True
    )
    
    print(f"{'Тип билда':<70} | {'Размер':>12} | {'Файлов':>10} | {'Папок':>8} | {'Источников':>12} | {'Самый старый':>16}")
    print("-" * 110)
    
    total_size = 0
    total_files = 0
    total_folders = 0
    
    for build_type in sorted_build_types:
        if build_type not in all_build_type_stats:
            continue
            
        stats = all_build_type_stats[build_type]
        size = stats['size']
        files = stats['files']
        folders = stats['folders']
        sources = len(set(stats['source_folders']))
        oldest_date = stats.get('oldest_date')
        
        if oldest_date:
            try:
                oldest_dt = datetime.fromisoformat(oldest_date)
                oldest_str = oldest_dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                oldest_str = "Неизвестно"
        else:
            oldest_str = "Неизвестно"
        
        total_size += size
        total_files += files
        total_folders += folders
        
        print(f"{build_type:<70} | {human_readable_size(size):>12} | {files:>10} | {folders:>8} | {sources:>12} | {oldest_str:>16}")
    
    print("-" * 110)
    print(f"{'ИТОГО':<70} | {human_readable_size(total_size):>12} | {total_files:>10} | {total_folders:>8} | {'--':>12} | {'--':>16}")
    
    # Статистика по папкам первого уровня
    print(f"\n" + "="*120)
    print("СТАТИСТИКА ПО ПАПКАМ ПЕРВОГО УРОВНЯ")
    print("="*120)
    
    folder_stats = []
    for folder_name, folder_data in folder_results.items():
        folder_total_size = sum(stats['size'] for stats in folder_data['build_type_stats'].values())
        folder_total_files = sum(stats['files'] for stats in folder_data['build_type_stats'].values())
        
        # Находим самую старую дату в этой папке
        folder_oldest_date = None
        for build_type, stats in folder_data['build_type_stats'].items():
            if stats.get('oldest_date'):
                try:
                    file_date = datetime.fromisoformat(stats['oldest_date'])
                    if folder_oldest_date is None or file_date < folder_oldest_date:
                        folder_oldest_date = file_date
                except (ValueError, TypeError):
                    pass
        
        if folder_oldest_date:
            oldest_str = folder_oldest_date.strftime('%Y-%m-%d')
        else:
            oldest_str = "Неизвестно"
        
        folder_stats.append((folder_name, folder_total_size, folder_total_files, len(folder_data['build_type_stats']), oldest_str))
    
    # Сортируем по размеру
    folder_stats.sort(key=lambda x: x[1], reverse=True)
    
    print(f"{'Папка':<40} | {'Размер':>12} | {'Файлов':>10} | {'Типов билдов':>15} | {'Самый старый файл':>16}")
    print("-" * 90)
    
    for folder_name, size, files, build_types_count, oldest_str in folder_stats:
        print(f"{folder_name:<40} | {human_readable_size(size):>12} | {files:>10} | {build_types_count:>15} | {oldest_str:>16}")
    
    print(f"\n" + "="*100)
    print("ОБЩАЯ СТАТИСТИКА")
    print("="*100)
    print(f"📁 Папок первого уровня: {data['total_first_level_folders']}")
    print(f"🏗️  Папок с типами билдов: {data['total_folders_with_builds']}")
    print(f"📊 Всего папок с билдами: {data['total_build_folders']}")
    print(f"🎯 Уникальных типов билдов: {len(all_build_types)}")
    print(f"💾 Общий размер: {human_readable_size(data['total_size'])}")
    print(f"📄 Общее количество файлов: {data['total_files']:,}")
    print(f"⏱️  Общее время сбора: {data['collection_time']:.1f} секунд")
    print(f"📅 Дата сбора: {datetime.fromisoformat(data['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")

def load_all_build_types_cache():
    """Загружает данные по всем типам билдов из кэша"""
    all_build_types_cache_file = "s3_all_build_types_cache.pkl"
    
    if not os.path.exists(all_build_types_cache_file):
        print(f"❌ Файл кэша {all_build_types_cache_file} не найден. Запустите сбор данных по всем типам билдов.", file=sys.stderr)
        return None
    
    try:
        with open(all_build_types_cache_file, 'rb') as f:
            data = pickle.load(f)
        
        cache_time = datetime.fromisoformat(data['timestamp'])
        print(f"📂 Загружены данные по всем типам билдов из кэша (собраны: {cache_time.strftime('%Y-%m-%d %H:%M:%S')})", file=sys.stderr)
        return data
    except Exception as e:
        print(f"❌ Ошибка загрузки кэша всех типов билдов: {e}", file=sys.stderr)
        return None

def main():
    parser = argparse.ArgumentParser(description='Анализ структуры S3 хранилища по типам билдов')
    parser.add_argument('--collect-build-types', action='store_true', help='Собрать данные агрегированные по типам билдов')
    parser.add_argument('--collect-all-build-types', action='store_true', help='Собрать данные по типам билдов для всех папок первого уровня')
    parser.add_argument('--analyze-build-types', action='store_true', help='Показать анализ по типам билдов')
    parser.add_argument('--analyze-all-build-types', action='store_true', help='Показать анализ по всем типам билдов')
    parser.add_argument('--prefix', type=str, help='Фильтровать по указанному префиксу')
    parser.add_argument('--cache-info', action='store_true', help='Показать информацию о кэше')
    
    args = parser.parse_args()
    
    if args.cache_info:
        # Показываем информацию о кэше типов билдов
        build_types_cache_file = "s3_build_types_cache.pkl"
        if os.path.exists(build_types_cache_file):
            print(f"📂 Файл кэша типов билдов: {build_types_cache_file}")
            data = load_build_types_cache()
            if data:
                cache_time = datetime.fromisoformat(data['timestamp'])
                print(f"📅 Дата создания: {cache_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"🔍 Префикс: {data['prefix']}")
                print(f"🏗️  Типов билдов: {len(data['build_types'])}")
                print(f"📁 Папок: {data['total_pr_folders']}")
                print(f"🔄 Обработано папок: {data['total_processed_folders']}")
        else:
            print(f"❌ Файл кэша {build_types_cache_file} не найден")
        
        # Показываем информацию о кэше всех типов билдов
        all_build_types_cache_file = "s3_all_build_types_cache.pkl"
        if os.path.exists(all_build_types_cache_file):
            print(f"\n📂 Файл кэша всех типов билдов: {all_build_types_cache_file}")
            data = load_all_build_types_cache()
            if data:
                cache_time = datetime.fromisoformat(data['timestamp'])
                print(f"📅 Дата создания: {cache_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"🔍 Префикс: {data['prefix']}")
                print(f"🏗️  Типов билдов: {len(data['all_build_types'])}")
                print(f"📁 Папок первого уровня: {data['total_first_level_folders']}")
                print(f"🔄 Обработано папок с билдами: {data['total_build_folders']}")
                print(f"💾 Общий размер: {human_readable_size(data['total_size'])}")
        else:
            print(f"❌ Файл кэша {all_build_types_cache_file} не найден")
        return
    
    if args.collect_all_build_types:
        # Собираем данные по типам билдов для всех папок
        data = collect_all_folders_build_types(args.prefix or "ydb-platform/ydb/")
        return
    
    if args.analyze_all_build_types:
        # Анализируем данные по всем типам билдов из кэша
        data = load_all_build_types_cache()
        if data:
            analyze_all_build_types_data(data)
        return
    
    if args.collect_build_types:
        # Собираем данные по типам билдов
        data = collect_build_types_data(args.prefix or "ydb-platform/ydb/PR-check/")
        if data:
            analyze_build_types_data(data)
        return
    
    if args.analyze_build_types:
        # Анализируем данные по типам билдов из кэша
        data = load_build_types_cache()
        if data:
            analyze_build_types_data(data)
        return
    
    # Если не указано никаких параметров, показываем справку
    print("💡 Доступные команды:")
    print("  --collect-build-types        - Собрать данные по типам билдов для одной папки")
    print("  --collect-all-build-types    - Собрать данные по типам билдов для всех папок")
    print("  --analyze-build-types        - Показать анализ по типам билдов из кэша")
    print("  --analyze-all-build-types    - Показать анализ по всем типам билдов")
    print("  --cache-info                 - Показать информацию о файлах кэша")
    print("  --prefix <путь>              - Указать конкретный префикс для обработки")

if __name__ == "__main__":
    main() 