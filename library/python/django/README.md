## Использование django в Аркадии

### Подключение адаптера

Для того, чтобы заиспользовать django в Аркадии необходимо подключить адаптер: `library/python/django`. Адаптер содержит loader'ы шаблонов и finder'ы статических файлов.

Работа с шаблонами настраивается добавлением соответствующего бэкенда, например так:
```python
TEMPLATES = [
    # тут могут быть использованы и другие бэкенды,
    # ...
    {
        'BACKEND': 'library.python.django.template.backends.arcadia.ArcadiaTemplates',
        'OPTIONS': {
            'debug': False,
            'loaders': [
                'library.python.django.template.loaders.resource.Loader',
                'library.python.django.template.loaders.app_resource.Loader',
            ],
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    }
]
```

Для работы шаблонов с формами джанги, заменить стандартый рендер:
```python
FORM_RENDERER = 'library.python.django.template.backends.forms_renderer.ArcadiaRenderer'
```

Работа со статикой решается добавлением нужных файндеров. Например в следующем примере мы оставили файндеры по умолчанию
и добавили Аркадийный файндер:
```python
STATICFILES_FINDERS = [
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    'library.python.django.contrib.staticfiles.finders.ArcadiaAppFinder',
]
```

### Автоматизация при подключении адаптера

Указанные выше действия можно автоматизировать. Функция **patch_settings_for_arcadia()**,
вызванная из **settings.py** вашего проекта, адаптирует текущие настройки Django к работе после Аркадийной сборки.

Позволяет:
  * использовать шаблоны, встроенные в бинарник;
  * хранить статику в бинарнике для последующей выгрузки (при помощи **collectstatic**) и раздачи сервером;
  * использовать шаблоны в формах.


```python

from library.python.django.utils import patch_settings_for_arcadia


patch_settings_for_arcadia()

# Если используется встроенная в Django система локализации,
# может понадобиться указать LOCALE_PATHS в settings.py проекта
# следующим образом:
from library.python.django.locales import extract_locales

LOCALE_PATHS = extract_locales({'ru'})

```

### Добавление файлов, кроме python кода (статика, шаблоны, переводы, etc)

Все файлы, кроме python кода размещаются в resfs.

Пример добавления шаблонов:
```python
RESOURCE_FILES( PREFIX my/project/arcadia/path/
    rest_framework/templates/rest_framework/docs/auth/basic.html
    rest_framework/templates/rest_framework/docs/auth/session.html
    rest_framework/templates/rest_framework/docs/auth/token.html
)
```

Пример добавления статических файлов:
```python
RESOURCE_FILES( PREFIX my/project/arcadia/path/
    rest_framework/static/rest_framework/css/bootstrap-theme.min.css
    rest_framework/static/rest_framework/css/bootstrap-tweaks.css
    rest_framework/static/rest_framework/css/bootstrap.min.css
)
```

Пример добавления переводов:
```python
RESOURCE_FILES( PREFIX junk/lorekhov/django-rest-framework/
    rest_framework/locale/ach/LC_MESSAGES/django.mo
)
```

Обращаем внимание, что все ресурсы кладем с префиксом == путь до нашего проекта в Аркадии.

### Пример entry point

В Аркадии, для того чтобы собрать приложение, мы пишем ya.make в котором указываем entry point нашего приложения, например так `PY_MAIN(app)`.
Это означает, что при запуске нашего бинарника - вызовется функция main из модуля app.

Обычно мы хотим запускать django внутри какого то wsgi сервера, например gunicorn или uwsgi. Это позволяет нам использовать различные виды
воркеров, хуки и настройки (навроде максимального количества обслуживаемых запросов per worker). Для того чтобы запустить django в gunicorn'е
можно использовать следующий код.

```python
from library.python.gunicorn import run_standalone
from my.project.wsgi_app import application


def main():
    run_standalone(application)
```

Тогда в docker/porto можно будет запустить приложение, передав ему конфигурацию в виде аргументов командной строки, например так:

```sh
app --bind=unix:/tmp/app.sock
    --workers=5
    --timeout=30
    --log-level=info
    --access-logfile=/dev/stdout
    --error-logfile=/dev/stderr
    --max-requests=10000
```

### Рекомендованный layout проекта

```sh
├── common_fixtures
│   └── initial_fixture.json
├── manage
│   ├── manage.py
│   └── ya.make
├── migrations
│   ├── __init__.py
│   └── polls2
│       ├── __init__.py
│       └── ya.make
├── polls
│   ├── __init__.py
│   ├── admin.py
│   ├── apps.py
│   ├── fixtures
│   │   └── 00_initial_polls.json
│   ├── migrations
│   │   ├── 0001_initial.py
│   │   ├── 0002_poll2.py
│   │   ├── __init__.py
│   │   └── ya.make
│   ├── models.py
│   ├── templates
│   │   ├── some-template.html
│   │   └── polls
│   │       ├── base.html
│   │       ├── detail.html
│   │       ├── index.html
│   │       └── results.html
│   ├── urls.py
│   ├── views.py
│   └── ya.make
├── polls2
│   ├── __init__.py
│   ├── apps.py
│   ├── models.py
│   └── ya.make
├── settings.py
├── wsgi
│   ├── app.py
│   └── ya.make
├── wsgi_app.py
└── ya.make
```

Где polls и polls2 - это django applications, каждое из них описывается своим ya.make файлом. Это имеет смысл потому что с одной стороны
каждое django application в философии django это вполне обособленный модуль, с другой стороны результирующие ya.make файлы более компактны,
с ними прощще работать. Сам django проект собирается как библиотека, entrypoint для запуска приложения собирается из директории wsgi, entrypoint
для manage.py собирается из директории manage.

Ссылка на пример django проекта: [проект](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/django/example)


## Особенности

### Миграции
Миграции работают как обычно, ```./app makemigrations``` создаст файл с миграцией в директории migrations приложения (где app это бинарник собранный из manage.py).
Миграции должны описываться ya.make файлами. Это необходимо для автоматического добавления сгенерированных миграций. Можно было бы использовать с этой целью
ya.make файл application, но это не будет работать для миграций, местоположение которых указывается при помощи MIGRATION_MODULES, так как такие миграции могут
лежать в любом месте Аркадии, даже за пределами проекта. Если не следовать этому правилу - нужно будет помнить о необходимости обновления проектного
или application ya.make файла руками.

### Чек лист manage.py команд
Проверка комманд:

[auth]
  - [v] changepassword
  - [v] createsuperuser

[contenttypes]
  - [v] remove_stale_contenttypes

[django]
  - [v] check
  - [ ] compilemessages
  - [ ] createcachetable
  - [v] dbshell
  - [v] diffsettings
  - [v] dumpdata
  - [v] inspectdb
  - [ ] loaddata
  - [ ] makemessages
  - [v] makemigrations
  - [v] migrate
  - [ ] raven
  - [-] sendtestemail не будет реализована
  - [v] shell
  - [v] showmigrations
  - [v] sqlflush
  - [v] sqlmigrate
  - [v] sqlsequencereset
  - [v] squashmigrations
  - [-] startapp не будет реализована
  - [-] startproject не будет реализована
  - [-] test не будет реализована
  - [-] testserver не будет реализована

[staticfiles]
  - [v] collectstatic
  - [v] findstatic
  - [v] runserver пока без поддержки autoreload
