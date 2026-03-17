#!/bin/sh

python -B ./genbuild/modules.py "genbuild/python-modules.txt" "base/CMakeModules.lib" "src/config_init.c" "src/config_map.c" "\${PYTHON_SRC_ROOT}/"
