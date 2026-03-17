#!/bin/bash
set -e

# search_sys = False -> True + удалить функции, которых нет в syms.cpp
python3 patches/01-patch-bindings.py
