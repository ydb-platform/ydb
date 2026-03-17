"""Expose missing context variable

Allows the user to let umongo document return missing rather than None for
empty fields.
"""
from contextvars import ContextVar
from contextlib import AbstractContextManager

import marshmallow as ma


__all__ = (
    'ExposeMissing',
    'RemoveMissingSchema',
)


EXPOSE_MISSING = ContextVar("expose_missing", default=False)


class ExposeMissing(AbstractContextManager):
    """Let Document expose missing values rather than returning None

    By default, getting a document item returns None if the value is missing.
    Inside this context manager, the missing singleton is returned. This can
    be useful is cases where the user want to distinguish between None and
    missing value.
    """
    def __enter__(self):
        self.token = EXPOSE_MISSING.set(True)

    def __exit__(self, *args, **kwargs):
        EXPOSE_MISSING.reset(self.token)


class RemoveMissingSchema(ma.Schema):
    """
    Custom :class:`marshmallow.Schema` subclass that skips missing fields
    rather than returning None for missing fields when dumping umongo
    :class:`umongo.Document`s.
    """
    def dump(self, *args, **kwargs):
        with ExposeMissing():
            return super().dump(*args, **kwargs)
