import re

import sqlalchemy
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base

from ..sql.schema import Table


class ClickHouseDeclarativeMeta(DeclarativeMeta):
    """
    Generates __tablename__ automatically. Taken from flask-sqlalchemy.
    Also adds custom __table_cls__.
    """
    _camelcase_re = re.compile(r'([A-Z]+)(?=[a-z0-9])')

    def __new__(cls, name, bases, d):
        tablename = d.get('__tablename__')

        has_pks = any(
            v.primary_key for k, v in d.items()
            if isinstance(v, sqlalchemy.Column)
        )

        # generate a table name automatically if it's missing and the
        # class dictionary declares a primary key.  We cannot always
        # attach a primary key to support model inheritance that does
        # not use joins.  We also don't want a table name if a whole
        # table is defined
        if not tablename and d.get('__table__') is None and has_pks:
            def _join(match):
                word = match.group()
                if len(word) > 1:
                    return ('_%s_%s' % (word[:-1], word[-1])).lower()
                return '_' + word.lower()
            d['__tablename__'] = cls._camelcase_re.sub(_join, name).lstrip('_')

        if '__table_cls__' not in d:
            d['__table_cls__'] = Table

        return DeclarativeMeta.__new__(cls, name, bases, d)


def get_declarative_base(metadata=None):
    return declarative_base(
        metadata=metadata, metaclass=ClickHouseDeclarativeMeta
    )
