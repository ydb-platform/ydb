#!/usr/bin/env python3
"""
Генерация syms.cpp и diff bindings.py для pypdfium2.

Парсит публичные headers pdfium, извлекает экспортируемые функции
и генерирует syms.cpp с SYM() записями. Также показывает расхождения
с bindings.py.

Использование:
    cd arcadia
    python contrib/python/pypdfium2/tools/update_syms.py

    # Или с указанием корня Аркадии:
    python contrib/python/pypdfium2/tools/update_syms.py --arcadia-root /path/to/arcadia
"""

import argparse
import os
import re
import sys
from pathlib import Path


# Относительные пути внутри Аркадии
PDFIUM_PUBLIC_DIR = "contrib/libs/pdfium/public"
PDFIUM_INCLUDE_PREFIX = "contrib/libs/pdfium/public"
SYMS_CPP_PATH = "contrib/python/pypdfium2/syms.cpp"
BINDINGS_PATH = "contrib/python/pypdfium2/pypdfium2_raw/bindings.py"

# Паттерн для извлечения имён функций из headers.
# Ловит оба формата:
#   FPDF_EXPORT int FPDF_CALLCONV FuncName(...)
#   FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV\nFuncName(...)
FUNC_RE = re.compile(
    r"FPDF_EXPORT\s+[\w\s*]+?\s+FPDF_CALLCONV\s*\n?\s*(\w+)\s*\(",
    re.MULTILINE,
)

# ifdef-условия для платформо-зависимых функций, которые не нужны в syms.cpp.
# Функции внутри этих блоков пропускаются.
EXCLUDED_IFDEFS = {"_WIN32", "PDF_ENABLE_V8", "PDF_USE_SKIA", "PDF_ENABLE_XFA"}

# Паттерн для извлечения имён функций из bindings.py
BINDINGS_RE = re.compile(
    r"(\w+)\s*=\s*_libs\['pdfium'\]\['\w+'\]"
)


def find_arcadia_root(start: str) -> str:
    """Ищет корень Аркадии по наличию .arcadia.root или ya.make в корне."""
    path = Path(start).resolve()
    for parent in [path] + list(path.parents):
        if (parent / ".arcadia.root").exists():
            return str(parent)
    # Fallback: ищем по наличию contrib/
    for parent in [path] + list(path.parents):
        if (parent / "contrib").is_dir() and (parent / "taxi").is_dir():
            return str(parent)
    return None


def strip_excluded_ifdefs(content: str) -> str:
    """
    Удаляет из содержимого header-а блоки #ifdef/#if defined(COND)...#endif
    для условий из EXCLUDED_IFDEFS.

    Обрабатывает вложенность: если внутри исключаемого блока есть
    другие #if/#ifdef, они тоже пропускаются до правильного #endif.
    """
    lines = content.split("\n")
    result = []
    skip_depth = 0  # > 0 означает что мы внутри исключаемого блока

    for line in lines:
        stripped = line.strip()

        if skip_depth > 0:
            # Внутри исключаемого блока: считаем вложенность
            if stripped.startswith("#if"):
                skip_depth += 1
            elif stripped.startswith("#endif"):
                skip_depth -= 1
            continue

        # Проверяем, начинается ли исключаемый блок
        # Форматы: #ifdef _WIN32, #if defined(_WIN32), #if defined(PDF_USE_SKIA)
        should_skip = False
        if stripped.startswith("#ifdef "):
            cond = stripped[len("#ifdef "):].strip()
            if cond in EXCLUDED_IFDEFS:
                should_skip = True
        elif stripped.startswith("#if "):
            for exc in EXCLUDED_IFDEFS:
                if f"defined({exc})" in stripped:
                    should_skip = True
                    break

        if should_skip:
            skip_depth = 1
            continue

        result.append(line)

    return "\n".join(result)


def parse_headers(public_dir: str) -> tuple[dict[str, list[str]], list[str]]:
    """
    Парсит .h файлы в public_dir (кроме cpp/ подпапки).

    Возвращает:
        (header_funcs, all_funcs)
        header_funcs: {filename: [func_names]} — функции по файлам (пустой список если нет функций)
        all_funcs: отсортированный список всех функций
    """
    header_funcs = {}
    all_funcs = set()

    for entry in sorted(os.listdir(public_dir)):
        filepath = os.path.join(public_dir, entry)
        if not entry.endswith(".h") or not os.path.isfile(filepath):
            continue

        with open(filepath, "r") as f:
            content = f.read()

        # Убираем платформо-зависимые блоки
        filtered = strip_excluded_ifdefs(content)

        funcs = FUNC_RE.findall(filtered)
        header_funcs[entry] = sorted(funcs)
        all_funcs.update(funcs)

    return header_funcs, sorted(all_funcs)


def generate_syms_cpp(all_headers: list[str], all_funcs: list[str]) -> str:
    """Генерирует содержимое syms.cpp."""
    lines = []

    # Registry include
    lines.append("#include <library/python/symbols/registry/syms.h>")
    lines.append("")

    # Header includes (все .h файлы, включая без функций)
    for header in all_headers:
        lines.append(f"#include <{PDFIUM_INCLUDE_PREFIX}/{header}>")
    lines.append("")

    # SYM entries
    lines.append('BEGIN_SYMS("pdfium")')
    lines.append("")
    for func in all_funcs:
        lines.append(f"SYM({func})")
    lines.append("")
    lines.append("END_SYMS()")
    lines.append("")

    return "\n".join(lines)


def parse_bindings(bindings_path: str) -> set[str]:
    """Извлекает имена функций из bindings.py."""
    with open(bindings_path, "r") as f:
        content = f.read()

    return set(BINDINGS_RE.findall(content))


def main():
    parser = argparse.ArgumentParser(
        description="Генерация syms.cpp и diff bindings.py для pypdfium2"
    )
    parser.add_argument(
        "--arcadia-root",
        help="Корень Аркадии (по умолчанию — автоопределение)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать diff, но не записывать syms.cpp",
    )
    args = parser.parse_args()

    # Определяем корень Аркадии
    if args.arcadia_root:
        arcadia = args.arcadia_root
    else:
        arcadia = find_arcadia_root(os.getcwd())
        if not arcadia:
            print("Ошибка: не удалось определить корень Аркадии.", file=sys.stderr)
            print("Запустите из директории Аркадии или укажите --arcadia-root.", file=sys.stderr)
            sys.exit(1)

    public_dir = os.path.join(arcadia, PDFIUM_PUBLIC_DIR)
    syms_path = os.path.join(arcadia, SYMS_CPP_PATH)
    bindings_path = os.path.join(arcadia, BINDINGS_PATH)

    # Проверяем наличие директории headers
    if not os.path.isdir(public_dir):
        print(f"Ошибка: директория headers не найдена: {public_dir}", file=sys.stderr)
        print("Убедитесь, что contrib/libs/pdfium/public/ доступна (не sparse checkout).", file=sys.stderr)
        sys.exit(1)

    # Парсим headers
    header_funcs, all_funcs = parse_headers(public_dir)
    all_headers = sorted(header_funcs.keys())
    header_count = len(all_headers)
    func_count = len(all_funcs)

    # Генерируем syms.cpp
    new_content = generate_syms_cpp(all_headers, all_funcs)

    if args.dry_run:
        print(f"[dry-run] Найдено {func_count} функций в {header_count} headers")
    else:
        with open(syms_path, "w") as f:
            f.write(new_content)
        print(f"Записан {SYMS_CPP_PATH}: {func_count} функций, {header_count} headers")

    # Diff с bindings.py
    if os.path.isfile(bindings_path):
        bindings_funcs = parse_bindings(bindings_path)
        header_set = set(all_funcs)

        only_headers = sorted(header_set - bindings_funcs)
        only_bindings = sorted(bindings_funcs - header_set)

        if only_headers or only_bindings:
            print(f"\nРасхождения с bindings.py:")
            for func in only_bindings:
                print(f"  - {func}  (есть в bindings.py, нет в headers — удалить)")
            for func in only_headers:
                print(f"  + {func}  (есть в headers, нет в bindings.py — добавить)")
        else:
            print(f"\nbindings.py: расхождений нет, все {len(bindings_funcs)} функций совпадают")
    else:
        print(f"\nПредупреждение: bindings.py не найден: {bindings_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
