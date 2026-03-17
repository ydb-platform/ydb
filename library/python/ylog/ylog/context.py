# coding: utf-8
"""
Контекстное логирование — прозрачное для пользователя добавление в сообщения
лога произвольной строчки -- ID контекста (например веб-запроса). Потом в логе
легко отличать строки, сделанные в разных запросах.

Использование:

    from ylog import log_req_id

    @log_req_id
    def func_that_logs_something():
        log.info('something')

    ...

    func_that_logs_something(req_id=some_id)

В логе будет написано:

    время logger INFO something; req_id=12345
"""


import logging
import sys
import six
from functools import wraps
from collections import defaultdict


if six.PY3:
    from .local_data.contextvar import LocalData

    def force_unicode(obj):
        if isinstance(obj, bytes):
            return obj.decode(sys.getdefaultencoding())
        elif isinstance(obj, str):
            return obj
        return repr(obj)
else:
    from .local_data.thread import LocalData

    def force_unicode(obj):
        if not isinstance(obj, basestring):
            obj = repr(obj)
        return obj.decode(sys.getdefaultencoding())


class ContextFormatter(logging.Formatter):
    """
    ContextFormatter дописывает в запись контекст, если он есть.
    """
    local_data = LocalData()

    def serialize_context(self):
        items = self.local_data.get_data().items()
        join_symbol = u','
        equality = u'%s=%s'
        if six.PY3:
            join_symbol = ','
            equality = '%s=%s'
        return join_symbol.join(
            equality % (force_unicode(key), force_unicode(value[-1]))
            for key, value in items
        )

    def append_log_context(self, result):
        if self.is_context_exist() and self.local_data.get_data():
            if isinstance(result, six.text_type):
                result += u'; ' + self.serialize_context()
            else:
                result += '; ' + self.serialize_context().encode(sys.getdefaultencoding())
        return result

    def format(self, record):
        result = logging.Formatter.format(self, record)
        return self.append_log_context(result)

    @classmethod
    def is_context_exist(cls):
        return cls.local_data.is_exist()

    @classmethod
    def get_or_create_logging_context(cls):
        if not cls.is_context_exist():
            cls.local_data.set_data(defaultdict(list))
        return cls.local_data.get_data()


def put_to_context(key, value):
    """
    Публичная функция, чтобы положить ключ-значение в контекст.
    """
    ctx = ContextFormatter.get_or_create_logging_context()
    ctx[key].append(value)


def pop_from_context(key):
    """
    Публичная функция, чтобы достать значение с вершины стека для ключа
    из контекста.

    Перед использованием функции убедитесь, что вызов не происходит внутри
    контекста менеджера log_context и не манипулирует значениями ключей,
    добавленных вызовом менеджера.
    """
    if not ContextFormatter.is_context_exist():
        return
    ctx = ContextFormatter.get_or_create_logging_context()
    if key not in ctx:
        return
    value = ctx[key].pop()
    if not ctx[key]:
        del ctx[key]
    return value


def start_logging_block_with_req_id(req_id):
    """
    Отмечает начало блока кода, где актуален req_id. После блока требует вызова
    end_logging_block_with_req_id.
    """
    put_to_context('req_id', req_id)


def end_logging_block_with_req_id():
    """
    Завершает блок кода с установленным req_id, удаляя его из стека.
    """
    pop_from_context('req_id')


def log_req_id(func):
    """
    Декоратор, устанавливающий контекст с req_id вокруг вызова функции. req_id
    передаётся в функцию keyword-аргументом. Сама логируемая функция не обязана
    принимать keyword-аргумент "req_id", он вырезается перед вызовом.
    """
    @wraps(func)
    def wrapper(*args, **kw):
        if 'req_id' in kw:
            try:
                start_logging_block_with_req_id(kw.pop('req_id'))
                return func(*args, **kw)
            finally:
                end_logging_block_with_req_id()
        else:
            return func(*args, **kw)
    return wrapper


class LogContext(object):
    context = None

    def __init__(self, **context):
        self.context = context

    def __enter__(self):
        for key, value in self.context.items():
            put_to_context(key, value)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for key in self.context:
            pop_from_context(key)
        return False  # не подавлять исключений


log_context = LogContext


def get_log_context():
    if not ContextFormatter.is_context_exist():
        return {}
    ctx = ContextFormatter.get_or_create_logging_context()
    # В logging_context каждому ключу соответствует стек из значений,
    # нам нужно значение с вершины стека.
    return {key: values[-1] for key, values in ctx.items()}
