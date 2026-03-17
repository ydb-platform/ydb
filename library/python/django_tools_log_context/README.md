# django_tools_log_context
https://wiki.yandex-team.ru/content/logging/

## Библиотека предоставляет
  * `django_tools_log_context.request_context` общий на джанговых сервисах формат контекста логов чтобы не копипастить постоянно из сервиса в сервис
  * `django_tools_log_context.request_profiler` для профилирования дб и HTTP запросов
  * `django_tools_log_context.celery.CtxAwareMixin` обвязки для передачи контекста в селери задачи и профилирования celery-задач
  * `django_tools_log_context.request_log_context` объединяет функционал `request_context` и `request_profiler`

## Для чего:

По таким логам удобно вычислять:
  * наиболее активных потребителей сервиса через YQL по следующим срезам: по uid, по типу: oauth, tvm, sessionid, по ручке (endpoint) в АПИ, по отрезку времени, по доле от общего числа запросов (`GROUP BY user.uid` или `GROUP BY auth.application`, если речь про TVM, oAuth)
  * долго работающие celery-таски
  * долго работающие обработчики http запросов
    * HTTP запросы с группировкой по endpoint упорядоченные в порядке от более проблемных к менее, [Пример над бекэндом Вики](https://yql.yandex-team.ru/Operations/WwFIlfvJNQmd766GhZp8IguD380FCTXXQVSPVUQW8AY=), TODO: срез по uid, срез по oAuth, TVM приложению
    * HTTP запросы в эндпоинт с группировкой по uid упорядоченные по медиане времени исполния. [Анализ кукера](https://yql.yandex-team.ru/Operations/WbliISRHwc7k_GFY--KxF0EnDqXSBglPz2UmHJzqkpY=)
    * Анализ запросов производимых вьюхой, [Сохранение страницы](https://yql.yandex-team.ru/Operations/5b1a7420fbc935d9c7ee648f). Видны HTTP и SQL запросы упорядеченные от самых "влиятельных" к самых "невлиятельным"
    * Исследовать конкретную view в заданном промежутке. [Просмотр страницы в Вики](https://yql.yandex-team.ru/Operations/WsINJiK7YkjSggHen4rjLKp0hCLsFSKiaKb3ZDk7Gno=), [График](https://charts.yandex-team.ru/preview/editor/YQL/charts/5b061493650e6f762781dc13/1)
    * Исследовать конкретный вызов некоторой View. [Поиск по комитом dogma](https://yql.yandex-team.ru/Operations/WenprR1icVQR9Drd_OxXFyH8f92lIjlrOxnRbGC_9MU=)
    * [Все ошибки в ручке](https://yql.yandex-team.ru/Operations/WvQiGW7BhBf8RtIXw1J6Qhwsk2juuCpOsbHypagtwxU=)

## Использование

  * Подключить к себе в settings:
  ```python
  from django_tools_log_context.settings import *
  ```
  * в конфигурировании LOGGING выставить
  ```python
  LOGGING = {
      'loggers': {
          'django_tools_log_context': {
              'level': 'INFO',
              'propagate': False,
              'handlers': [my_qloud_handler],
          },
      },
  }
  ```
  * для автоматического запуска трекинга запросов, добавить в INSTALLED_APPS
  ```python
  INSTALLED_APPS.append('django_tools_log_context')
  ```
  * для ручного запуска трекинга
  ```python
  from django_tools_log_context.tracking import enable_tracking
  
  enable_tracking()
  ```

### request_context

Для class-based views:
```python
from django_tools_log_context import request_log_context


# объявляем миксин, переопределяем метод dispatch
class GenericMixin(object):
    def dispatch(self, request, *args, **kwargs):
        with request_log_context(request, endpoint=self, threshold=0):
            super(GenericMixin, self).dispatch(request, *args, **kwargs)


# используем миксин во всех вьюхах
class VersionView(GenericMixin, View):
    def get(self, request, *args, **kwargs):
        return HttpResponse(get_version())

```
Для function-based views:
```python
from django_tools_log_context import request_log_context


def add_ctx(view_func):
    @wraps(view_func)
    def wrapped(request, *args, **kwargs):
        with request_log_context(request, endpoint=view_func, **kwargs):
            return view_func(request, *args, **kwargs)
    return wrapped
    
    
@add_ctx
def version_view(request, *args, **kwargs):
    return HttpResponse(get_version())
```
  * можно переопределить как нужно переменную в settings: ```TOOLS_LOG_CONTEXT_PROVIDERS.append('django_tools_log_context.provider.b2b')``` - чтобы добавить/переопределить поле в логах.

Также можно использовать кастомный LogContextWSGIHandler, который автоматически оборачивает вызываемые вьюхи в request_log_context.
Нужно записать в wsgi.py:
```python
from django_tools_log_context.wsgi import log_context_get_wsgi_application


application = log_context_get_wsgi_application()
```

### request_profiler

Выводит в логи именованные куски кода которые выполнялись дольше чем `threshold` мсек. Если превышен threshold, в логах окажутся SQL запросы выполненные внутри профайлера. Любой запрос в рамках request_profiler оказывается в логах с уровнем `INFO`.

```python
from django_tools_log_context import request_profiler

  def dispatch(self, request, *args, **kwargs):
    with request_profiler(request, threshold=100):
      return super(YourView, self).dispatch(request, *args, **kwargs)
```

Настоятельно рекомендуется использовать `request_context` и `request_profiler` вместе, иначе не заработают YQL запросы к логам:

```python
with request_context(request, endpoint=self), request_profiler(request, threshold=100):
    return something(request)
```

### CtxAwareMixin

Позволяет пробросить текущий контекст из http-запроса или из другой селери таски. 
Сохраняет контекст в случае неперехваченного исключения чтобы можно было понять какая именно таска свалилась с исчключением.
Для использования рекомендую настроить LOGGING так, чтобы
```python
CELERYD_HIJACK_ROOT_LOGGER = False
```

```python
from celery import Task, Celery


class MyCeleryTask(CtxAwareMixin, Task):
    pass


app = Celery('wiki', task_cls=MyCeleryTask)
app.config_from_object('django.conf:settings')
```

