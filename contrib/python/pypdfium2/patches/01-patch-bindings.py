#!/usr/bin/env python3
"""
Адаптация bindings.py для Arcadia:
1. search_sys = False -> True (использовать pdfium через symbols/registry)
2. Удаление из bindings.py функций, которых нет в syms.cpp

Запускается во время yamaker reimport.
"""

import re
import sys

SYMS_PATH = "syms.cpp"
BINDINGS_PATH = "pypdfium2_raw/bindings.py"

SYM_RE = re.compile(r"^SYM\((\w+)\)$", re.MULTILINE)
BINDING_RE = re.compile(r"(\w+)\s*=\s*_libs\['pdfium'\]\['\w+'\]")


def get_syms():
    with open(SYMS_PATH) as f:
        return set(SYM_RE.findall(f.read()))


def filter_bindings(syms):
    with open(BINDINGS_PATH) as f:
        lines = f.readlines()

    result = []
    i = 0
    removed = []

    while i < len(lines):
        line = lines[i]
        stripped = line.rstrip("\n")

        m = BINDING_RE.match(stripped)
        if m:
            func_name = m.group(1)
            if func_name not in syms:
                # Удалить комментарий перед биндингом (# ./header.h: N)
                if result and result[-1].strip().startswith("#") and ".h:" in result[-1]:
                    result.pop()

                removed.append(func_name)
                i += 1  # пропустить строку присваивания

                # Пропустить строки атрибутов (argtypes, restype, ...)
                while i < len(lines) and lines[i].strip().startswith(f"{func_name}."):
                    i += 1

                # Пропустить пустую строку после блока
                if i < len(lines) and lines[i].strip() == "":
                    i += 1

                continue

        result.append(line)
        i += 1

    return result, removed


def patch_search_sys(content):
    """search_sys = False -> True для загрузки pdfium через symbols/registry."""
    return content.replace("search_sys = False", "search_sys = True")


PAGE_PATH = "pypdfium2/_helpers/page.py"

GUARD_MARKER = "if not hasattr(pdfium_c, 'FPDFFormObj_RemoveObject')"
GUARD_CODE = (
    "            if not hasattr(pdfium_c, 'FPDFFormObj_RemoveObject'):\n"
    "                raise NotImplementedError("
    "\"FPDFFormObj_RemoveObject is not available in this pdfium version\")\n"
)


def patch_page_guards():
    """Добавить hasattr guards для удалённых функций, используемых в коде."""
    with open(PAGE_PATH) as f:
        content = f.read()

    if GUARD_MARKER in content:
        return

    # Вставить guard перед вызовом FPDFFormObj_RemoveObject
    old = "            ok = pdfium_c.FPDFFormObj_RemoveObject(pageobj.container, pageobj)"
    if old in content:
        content = content.replace(old, GUARD_CODE + old, 1)
        with open(PAGE_PATH, "w") as f:
            f.write(content)
        print("page.py: добавлен hasattr guard для FPDFFormObj_RemoveObject")


def main():
    syms = get_syms()
    if not syms:
        print(f"Ошибка: SYM() записи не найдены в {SYMS_PATH}", file=sys.stderr)
        sys.exit(1)

    # search_sys = False -> True
    with open(BINDINGS_PATH) as f:
        content = f.read()
    patched = patch_search_sys(content)
    if patched != content:
        with open(BINDINGS_PATH, "w") as f:
            f.write(patched)
        print("search_sys = False -> True")

    # Удалить биндинги функций, которых нет в syms.cpp
    result, removed = filter_bindings(syms)

    with open(BINDINGS_PATH, "w") as f:
        f.writelines(result)

    if removed:
        print(f"Удалено {len(removed)} биндингов, отсутствующих в syms.cpp:")
        for name in sorted(removed):
            print(f"  - {name}")
    else:
        print("Нет биндингов для удаления")

    # Добавить guards в код, вызывающий удалённые функции
    patch_page_guards()


if __name__ == "__main__":
    main()
