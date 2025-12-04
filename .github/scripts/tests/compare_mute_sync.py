#!/usr/bin/env python3
"""
Скрипт для сравнения расхождения между muted_ya.txt и tests_monitor до и после изменений.
Позволяет замерить текущее состояние, сохранить его, и после изменений сравнить результаты.
"""

import argparse
import datetime
import sys
import os
import json
from pathlib import Path
from collections import defaultdict

# Check for required dependencies
try:
    import ydb
except ImportError:
    print("❌ Error: Module 'ydb' is not installed")
    print("   Please install dependencies:")
    print("   pip install ydb[yc]")
    sys.exit(1)

# Add analytics directory to path for ydb_wrapper import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
try:
    from ydb_wrapper import YDBWrapper
except ImportError as e:
    print(f"❌ Error: Failed to import YDBWrapper: {e}")
    print("   Make sure you're on the correct branch with ydb_wrapper.py")
    sys.exit(1)

# Add parent directory to import YaMuteCheck
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from transform_ya_junit import YaMuteCheck
except ImportError as e:
    print(f"❌ Error: Failed to import YaMuteCheck: {e}")
    sys.exit(1)

# Import check_sync from check_mute_sync
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from check_mute_sync import load_muted_ya_tests, get_muted_tests_from_ydb, check_sync
except ImportError as e:
    print(f"❌ Error: Failed to import from check_mute_sync: {e}")
    sys.exit(1)


def save_snapshot(branch, build_type, muted_ya_path, days, output_dir="mute_sync_snapshots"):
    """Сохраняет снимок текущего состояния синхронизации"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_dir = os.path.join(output_dir, f"{branch}_{timestamp}")
    os.makedirs(snapshot_dir, exist_ok=True)
    
    print(f"\n{'='*80}")
    print(f"📸 СОЗДАНИЕ СНИМКА СИНХРОНИЗАЦИИ")
    print(f"{'='*80}")
    print(f"Branch: {branch}")
    print(f"Build type: {build_type}")
    print(f"Snapshot directory: {snapshot_dir}")
    
    # Загружаем тесты из muted_ya.txt
    muted_ya_tests = load_muted_ya_tests(muted_ya_path)
    
    # Получаем тесты из YDB
    ydb_muted_tests = get_muted_tests_from_ydb(branch, build_type, days)
    
    # Проверяем синхронизацию
    stats = check_sync(muted_ya_tests, ydb_muted_tests, branch)
    
    # Сохраняем статистику
    stats_file = os.path.join(snapshot_dir, "stats.json")
    with open(stats_file, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'branch': branch,
            'build_type': build_type,
            'muted_ya_path': muted_ya_path,
            'days': days,
            'stats': stats
        }, f, indent=2)
    
    # Сохраняем списки тестов
    if stats['in_muted_ya_not_muted'] > 0:
        file_path = os.path.join(snapshot_dir, "in_muted_ya_not_muted_in_ydb.txt")
        with open(file_path, 'w') as f:
            for test in sorted(muted_ya_tests - {name for name, is_muted in ydb_muted_tests.items() if is_muted == 1}):
                f.write(f"{test}\n")
    
    if stats['muted_in_ydb_not_in_file'] > 0:
        file_path = os.path.join(snapshot_dir, "muted_in_ydb_not_in_muted_ya.txt")
        with open(file_path, 'w') as f:
            ydb_muted_set = {name for name, is_muted in ydb_muted_tests.items() if is_muted == 1}
            muted_ya_set = set(muted_ya_tests)
            for test in sorted(ydb_muted_set - muted_ya_set):
                f.write(f"{test}\n")
    
    print(f"\n✅ Снимок сохранен в: {snapshot_dir}")
    print(f"   Статистика: {stats_file}")
    
    return snapshot_dir, stats


def compare_snapshots(before_dir, after_dir, branch):
    """Сравнивает два снимка и показывает разницу"""
    print(f"\n{'='*80}")
    print(f"📊 СРАВНЕНИЕ СНИМКОВ")
    print(f"{'='*80}")
    print(f"ДО:  {before_dir}")
    print(f"ПОСЛЕ: {after_dir}")
    
    # Загружаем статистику
    before_stats_file = os.path.join(before_dir, "stats.json")
    after_stats_file = os.path.join(after_dir, "stats.json")
    
    if not os.path.exists(before_stats_file):
        print(f"❌ Файл статистики не найден: {before_stats_file}")
        return
    
    if not os.path.exists(after_stats_file):
        print(f"❌ Файл статистики не найден: {after_stats_file}")
        return
    
    with open(before_stats_file, 'r') as f:
        before_stats = json.load(f)
    
    with open(after_stats_file, 'r') as f:
        after_stats = json.load(f)
    
    # Загружаем списки тестов
    def load_test_list(filename):
        filepath = os.path.join(before_dir if 'before' in filename else after_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                return set(line.strip() for line in f if line.strip())
        return set()
    
    before_muted_in_ydb = load_test_list("muted_in_ydb_not_in_muted_ya.txt")
    after_muted_in_ydb = load_test_list("muted_in_ydb_not_in_muted_ya.txt")
    
    before_in_muted_ya = load_test_list("in_muted_ya_not_muted_in_ydb.txt")
    after_in_muted_ya = load_test_list("in_muted_ya_not_muted_in_ydb.txt")
    
    # Сравниваем статистику
    print(f"\n{'='*80}")
    print(f"📈 СТАТИСТИКА")
    print(f"{'='*80}")
    print(f"{'Метрика':<50} {'ДО':<15} {'ПОСЛЕ':<15} {'Изменение':<15}")
    print("-" * 95)
    
    metrics = [
        ('total_muted_ya', 'Всего в muted_ya.txt'),
        ('total_ydb_muted', 'Замьючено в YDB'),
        ('synced', 'Синхронизировано'),
        ('in_muted_ya_not_muted', 'В muted_ya.txt, но не замьючено'),
        ('muted_in_ydb_not_in_file', 'Замьючено в YDB, но нет в muted_ya.txt'),
        ('in_muted_ya_no_ydb_data', 'В muted_ya.txt, но нет данных в YDB')
    ]
    
    for key, label in metrics:
        before_val = before_stats['stats'].get(key, 0)
        after_val = after_stats['stats'].get(key, 0)
        diff = after_val - before_val
        diff_str = f"{diff:+d}" if diff != 0 else "0"
        print(f"{label:<50} {before_val:<15} {after_val:<15} {diff_str:<15}")
    
    # Показываем исправленные проблемы
    print(f"\n{'='*80}")
    print(f"✅ ИСПРАВЛЕННЫЕ ПРОБЛЕМЫ")
    print(f"{'='*80}")
    
    # Тесты, которые были замьючены в YDB, но не в muted_ya.txt (ДО), и исправились (ПОСЛЕ)
    fixed_muted_in_ydb = before_muted_in_ydb - after_muted_in_ydb
    if fixed_muted_in_ydb:
        print(f"\n🎉 Исправлено: {len(fixed_muted_in_ydb)} тестов больше не замьючены в YDB (были удалены из muted_ya.txt)")
        print(f"   Первые 10 примеров:")
        for test in sorted(list(fixed_muted_in_ydb))[:10]:
            print(f"   - {test}")
        if len(fixed_muted_in_ydb) > 10:
            print(f"   ... и еще {len(fixed_muted_in_ydb) - 10} тестов")
    else:
        print(f"\n⚠️  Нет исправлений в категории 'Замьючено в YDB, но нет в muted_ya.txt'")
    
    # Тесты, которые были в muted_ya.txt, но не замьючены (ДО), и исправились (ПОСЛЕ)
    fixed_in_muted_ya = before_in_muted_ya - after_in_muted_ya
    if fixed_in_muted_ya:
        print(f"\n🎉 Исправлено: {len(fixed_in_muted_ya)} тестов теперь замьючены в YDB (были добавлены в muted_ya.txt)")
        print(f"   Первые 10 примеров:")
        for test in sorted(list(fixed_in_muted_ya))[:10]:
            print(f"   - {test}")
        if len(fixed_in_muted_ya) > 10:
            print(f"   ... и еще {len(fixed_in_muted_ya) - 10} тестов")
    else:
        print(f"\n⚠️  Нет исправлений в категории 'В muted_ya.txt, но не замьючено'")
    
    # Показываем новые проблемы
    print(f"\n{'='*80}")
    print(f"⚠️  НОВЫЕ ПРОБЛЕМЫ")
    print(f"{'='*80}")
    
    new_muted_in_ydb = after_muted_in_ydb - before_muted_in_ydb
    if new_muted_in_ydb:
        print(f"\n❌ Появилось новых: {len(new_muted_in_ydb)} тестов замьючены в YDB, но нет в muted_ya.txt")
        print(f"   Первые 10 примеров:")
        for test in sorted(list(new_muted_in_ydb))[:10]:
            print(f"   - {test}")
        if len(new_muted_in_ydb) > 10:
            print(f"   ... и еще {len(new_muted_in_ydb) - 10} тестов")
    else:
        print(f"\n✅ Нет новых проблем в категории 'Замьючено в YDB, но нет в muted_ya.txt'")
    
    new_in_muted_ya = after_in_muted_ya - before_in_muted_ya
    if new_in_muted_ya:
        print(f"\n❌ Появилось новых: {len(new_in_muted_ya)} тестов в muted_ya.txt, но не замьючены")
        print(f"   Первые 10 примеров:")
        for test in sorted(list(new_in_muted_ya))[:10]:
            print(f"   - {test}")
        if len(new_in_muted_ya) > 10:
            print(f"   ... и еще {len(new_in_muted_ya) - 10} тестов")
    else:
        print(f"\n✅ Нет новых проблем в категории 'В muted_ya.txt, но не замьючено'")
    
    # Итоговая сводка с учетом статистики
    total_fixed = len(fixed_muted_in_ydb) + len(fixed_in_muted_ya)
    total_new = len(new_muted_in_ydb) + len(new_in_muted_ya)
    
    # Вычисляем общее улучшение на основе статистики
    before_desync = before_stats['stats'].get('in_muted_ya_not_muted', 0) + before_stats['stats'].get('muted_in_ydb_not_in_file', 0)
    after_desync = after_stats['stats'].get('in_muted_ya_not_muted', 0) + after_stats['stats'].get('muted_in_ydb_not_in_file', 0)
    total_improvement = before_desync - after_desync
    
    print(f"\n{'='*80}")
    print(f"📊 ИТОГОВАЯ СВОДКА")
    print(f"{'='*80}")
    print(f"Общая рассинхронизация:")
    print(f"  ДО:  {before_desync} проблем")
    print(f"  ПОСЛЕ: {after_desync} проблем")
    print(f"  Улучшение: {total_improvement:+d} проблем")
    
    print(f"\nИсправлено конкретных тестов (из списков): {total_fixed}")
    print(f"  - Замьючено в YDB, но нет в muted_ya.txt: {len(fixed_muted_in_ydb)}")
    print(f"  - В muted_ya.txt, но не замьючено: {len(fixed_in_muted_ya)}")
    print(f"\nПоявилось новых проблем: {total_new}")
    print(f"  - Замьючено в YDB, но нет в muted_ya.txt: {len(new_muted_in_ydb)}")
    print(f"  - В muted_ya.txt, но не замьючено: {len(new_in_muted_ya)}")
    
    # Показываем ключевые улучшения
    print(f"\n{'='*80}")
    print(f"🎯 КЛЮЧЕВЫЕ УЛУЧШЕНИЯ")
    print(f"{'='*80}")
    
    # Улучшение в синхронизации
    before_synced = before_stats['stats'].get('synced', 0)
    after_synced = after_stats['stats'].get('synced', 0)
    synced_improvement = after_synced - before_synced
    if synced_improvement > 0:
        print(f"✅ Синхронизировано тестов: {before_synced} → {after_synced} (+{synced_improvement})")
    
    # Уменьшение замьюченных тестов в YDB (которые не должны быть замьючены)
    before_muted_wrong = before_stats['stats'].get('muted_in_ydb_not_in_file', 0)
    after_muted_wrong = after_stats['stats'].get('muted_in_ydb_not_in_file', 0)
    muted_wrong_improvement = before_muted_wrong - after_muted_wrong
    if muted_wrong_improvement > 0:
        print(f"✅ Исправлено неправильно замьюченных тестов: {before_muted_wrong} → {after_muted_wrong} (-{muted_wrong_improvement})")
    
    # Улучшение в тестах, которые должны быть замьючены
    before_not_muted = before_stats['stats'].get('in_muted_ya_not_muted', 0)
    after_not_muted = after_stats['stats'].get('in_muted_ya_not_muted', 0)
    not_muted_improvement = before_not_muted - after_not_muted
    if not_muted_improvement > 0:
        print(f"✅ Исправлено незамьюченных тестов (должны быть muted): {before_not_muted} → {after_not_muted} (-{not_muted_improvement})")
    
    if total_improvement > 0:
        print(f"\n🎉 ОБЩЕЕ УЛУЧШЕНИЕ: на {total_improvement} проблем меньше!")
        print(f"   Рассинхронизация уменьшилась с {before_desync} до {after_desync} ({total_improvement/before_desync*100:.1f}% улучшение)")
    elif total_improvement < 0:
        print(f"\n⚠️  УХУДШЕНИЕ: на {abs(total_improvement)} проблем больше")
    else:
        print(f"\n➡️  Без изменений в количестве проблем")
    
    # Сохраняем сравнение
    comparison_file = os.path.join(after_dir, "comparison.json")
    with open(comparison_file, 'w') as f:
        json.dump({
            'before_dir': before_dir,
            'after_dir': after_dir,
            'fixed_muted_in_ydb': sorted(list(fixed_muted_in_ydb)),
            'fixed_in_muted_ya': sorted(list(fixed_in_muted_ya)),
            'new_muted_in_ydb': sorted(list(new_muted_in_ydb)),
            'new_in_muted_ya': sorted(list(new_in_muted_ya)),
            'total_fixed': total_fixed,
            'total_new': total_new,
            'total_improvement': total_improvement,
            'before_desync': before_desync,
            'after_desync': after_desync
        }, f, indent=2)
    
    print(f"\n💾 Результаты сравнения сохранены в: {comparison_file}")


def list_snapshots(output_dir="mute_sync_snapshots"):
    """Показывает список всех снимков"""
    if not os.path.exists(output_dir):
        print(f"📁 Директория {output_dir} не существует")
        return
    
    snapshots = []
    for item in os.listdir(output_dir):
        item_path = os.path.join(output_dir, item)
        if os.path.isdir(item_path):
            stats_file = os.path.join(item_path, "stats.json")
            if os.path.exists(stats_file):
                with open(stats_file, 'r') as f:
                    stats = json.load(f)
                    snapshots.append((item_path, stats))
    
    if not snapshots:
        print(f"📁 Нет сохраненных снимков в {output_dir}")
        return
    
    snapshots.sort(key=lambda x: x[1]['timestamp'], reverse=True)
    
    print(f"\n{'='*80}")
    print(f"📸 СПИСОК СНИМКОВ")
    print(f"{'='*80}")
    print(f"{'#':<5} {'Ветка':<20} {'Время':<20} {'Рассинхронизация':<20}")
    print("-" * 80)
    
    for i, (path, stats) in enumerate(snapshots, 1):
        branch = stats.get('branch', 'N/A')
        timestamp = stats.get('timestamp', 'N/A')
        timestamp_display = datetime.datetime.strptime(timestamp, "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        desync = stats['stats'].get('muted_in_ydb_not_in_file', 0) + stats['stats'].get('in_muted_ya_not_muted', 0)
        print(f"{i:<5} {branch:<20} {timestamp_display:<20} {desync:<20}")
        print(f"     {path}")
    
    return snapshots


def main():
    parser = argparse.ArgumentParser(
        description="Сравнение расхождения между muted_ya.txt и tests_monitor до и после изменений"
    )
    subparsers = parser.add_subparsers(dest='command', help='Команда')
    
    # Команда для создания снимка
    snapshot_parser = subparsers.add_parser('snapshot', help='Создать снимок текущего состояния')
    snapshot_parser.add_argument('--branch', required=True, help='Ветка (например: stable-25-3)')
    snapshot_parser.add_argument('--build_type', default='relwithdebinfo', help='Тип сборки')
    snapshot_parser.add_argument('--muted_ya_file', help='Путь к файлу muted_ya.txt')
    snapshot_parser.add_argument('--days', type=int, default=1, help='Количество дней для проверки')
    snapshot_parser.add_argument('--output_dir', default='mute_sync_snapshots', help='Директория для сохранения снимков')
    
    # Команда для сравнения
    compare_parser = subparsers.add_parser('compare', help='Сравнить два снимка')
    compare_parser.add_argument('--before', required=True, help='Директория снимка ДО')
    compare_parser.add_argument('--after', required=True, help='Директория снимка ПОСЛЕ')
    compare_parser.add_argument('--branch', help='Ветка (для информации)')
    
    # Команда для списка снимков
    list_parser = subparsers.add_parser('list', help='Показать список всех снимков')
    list_parser.add_argument('--output_dir', default='mute_sync_snapshots', help='Директория со снимками')
    
    args = parser.parse_args()
    
    if args.command == 'snapshot':
        if args.muted_ya_file:
            muted_ya_path = args.muted_ya_file
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            repo_path = os.path.join(script_dir, '../../../')
            muted_ya_path = os.path.join(repo_path, '.github/config/muted_ya.txt')
        
        save_snapshot(args.branch, args.build_type, muted_ya_path, args.days, args.output_dir)
    
    elif args.command == 'compare':
        compare_snapshots(args.before, args.after, args.branch or 'unknown')
    
    elif args.command == 'list':
        list_snapshots(args.output_dir)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

