#!/bin/bash
set -e

# Добавить pkgutil fallback в version.py для работы в Arcadia
python3 patches/02-patch-version.py
