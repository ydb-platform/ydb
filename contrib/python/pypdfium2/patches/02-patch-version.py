#!/usr/bin/env python3
"""
Добавление pkgutil fallback в version.py для работы в Arcadia.

Изменения:
1. import pkgutil
2. try/except с pkgutil.get_data() fallback в __init__
3. _PKG атрибуты в подклассах
"""

import sys

VERSION_PATH = "pypdfium2/version.py"


def patch_version():
    with open(VERSION_PATH) as f:
        content = f.read()

    original = content

    # 1. Добавить import pkgutil после import json
    if "import pkgutil" not in content:
        content = content.replace(
            "import json\n",
            "import json\nimport pkgutil\n",
            1,
        )

    # 2. Обернуть чтение файла в try/except с pkgutil.get_data fallback
    old_init = (
        "        with open(self._FILE, \"r\") as buf:\n"
        "            data = json.load(buf)"
    )
    new_init = (
        "        try:\n"
        "            with open(self._FILE, \"r\") as buf:\n"
        "                data = json.load(buf)\n"
        "        except (FileNotFoundError, OSError):\n"
        "            raw = pkgutil.get_data(self._PKG, \"version.json\")\n"
        "            if raw is None:\n"
        "                raise\n"
        "            data = json.loads(raw)"
    )
    if old_init in content:
        content = content.replace(old_init, new_init, 1)

    # 3. Добавить _PKG = "pypdfium2" в _version_pypdfium2
    if '_PKG = "pypdfium2"' not in content:
        content = content.replace(
            '_FILE = Path(__file__).parent / "version.json"\n'
            "    _TAG_FIELDS",
            '_FILE = Path(__file__).parent / "version.json"\n'
            '    _PKG = "pypdfium2"\n'
            "    _TAG_FIELDS",
            1,
        )

    # 4. Добавить _PKG = "pypdfium2_raw" в _version_pdfium
    if '_PKG = "pypdfium2_raw"' not in content:
        content = content.replace(
            '_FILE = Path(pypdfium2_raw.__file__).parent / "version.json"\n'
            "    _TAG_FIELDS",
            '_FILE = Path(pypdfium2_raw.__file__).parent / "version.json"\n'
            '    _PKG = "pypdfium2_raw"\n'
            "    _TAG_FIELDS",
            1,
        )

    # 5. Убрать trailing whitespace на всех строках
    content = "\n".join(line.rstrip() for line in content.split("\n"))

    if content == original:
        print("version.py: изменений не требуется (уже пропатчен)")
        return

    with open(VERSION_PATH, "w") as f:
        f.write(content)
    print("version.py: добавлен pkgutil fallback и _PKG атрибуты")


if __name__ == "__main__":
    patch_version()
