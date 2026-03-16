## example django project

Демонстрирует:
  - layout проекта с автоматически обновляемыми миграциями
  - rest-like api с open-api документацией ([djangorestframework](https://a.yandex-team.ru/arc/trunk/arcadia/contrib/python/djangorestframework) + [coreapi](https://a.yandex-team.ru/arc/trunk/arcadia/contrib/deprecated/python/coreapi))
  - логи в формате qloud ([ylog](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/ylog))
  - трекинг 

## куда смотреть дальше

  - [pgaas](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/django_pgaas)
  - [интеграция с idm](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/django-idm-api)
  - конфигурация:
     - [appconf](https://a.yandex-team.ru/arc/trunk/arcadia/contrib/python/django-appconf)
     - [django-environ](https://a.yandex-team.ru/arc/trunk/arcadia/contrib/python/django-environ/)
  - [трассировка](https://a.yandex-team.ru/arc/trunk/arcadia/contrib/python/django-opentracing)
  - контекстное логирование ([django_tools_log_context](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/django_tools_log_context))
  - [redis-cache](https://a.yandex-team.ru/arc/trunk/arcadia/contrib/python/django-redis-cache)
  - Важно, актуальный mysql клиент: [mysqlclient-python](https://a.yandex-team.ru/arc/trunk/arcadia/contrib/python/mysqlclient)
  
## инструкции по запуску

При запуске энтрипойнтов cmd/manage/manage и cmd/wsgi/app, не забывайте указывать ARCADIA_PATH, так как он используется в settings для вычисления пути до sqlite базы.
