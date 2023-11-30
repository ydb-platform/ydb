Форк ((termcolor https://github.com/termcolor/termcolor/)) для PY23 с дополнительным функционалом.

Может быть использован для конвертации текстовых спецификаций цвета (например, из markup) в esc-последовательности для корректного отображения в терминале.

Пример использования:
```python
from library.python.color import tcolor
tcolor("some text", "green-bold-on_red") -> '\x1b[32m\x1b[41m\x1b[1msome text\x1b[0m'
```
