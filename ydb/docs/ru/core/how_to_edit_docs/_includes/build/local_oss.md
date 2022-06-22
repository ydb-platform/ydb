## Локальная сборка OpenSource-инструментами

Сборка документации осуществляется утилитой [YFM-Docs](https://github.com/yandex-cloud/yfm-docs).

Порядок установки YFM-Docs описан на [вводной странице документации по этой утилите]{% if lang == "en" %}(https://ydocs.tech/en/tools/docs/){% endif %}{% if lang == "ru" %}(https://ydocs.tech/ru/tools/docs/){% endif %}.

Для сборки OpenSource документации {{ ydb-short-name }} нужно выполнить команду:

``` bash
yfm -i <source_dir> -o <output_dir> --allowHTML
```

Где: 
- `source_dir` - директория, куда склонировано содержимое [{{ ydb-doc-repo }}]({{ ydb-doc-repo }})
- `output_dir` - директория, куда будет выполнена генерация HTML-файлов

Сборка занимает несколько секунд, и не должна выводить сообщений об ошибках в лог (stdout).

В качестве `source_dir` можно задавать `.` (точку), если команда yfm вызывается непосредственно из каталога `source_dir`, например:

``` bash
yfm -i . -o ~/docs/ydboss --allowHTML
```

Для просмотра собранной локально документации можно открыть каталог из браузера, или воспользоваться простым web-сервером, встроенным в Python:

``` bash
python3 -m http.server 8888 -d ~/docs/ydboss
```

При запущенном таким образом сервере собранная локально документация доступна по ссылкам:
- [http://localhost:8888/ru](http://localhost:8888/ru) - на русском языке
- [http://localhost:8888/en](http://localhost:8888/en) - на английском языке

