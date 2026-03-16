set -xue
mkdir -p symbols
python3 patches/02-cdefs.py > symbols/symb.c
