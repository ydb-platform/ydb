# Note
При добавлении новых скриптов в данную директорию не забывайте указывать две вещи:

1. Явное разрешать импорт модулей из текущей директории, если это вам необходимо, с помощью строк:
```python3
import os.path, sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
```
2. В командах вызова скриптов прописывать все их зависимые модули через `${input:"build/scripts/module_1.py"}`, `${input:"build/scripts/module_2.py"}` ...
