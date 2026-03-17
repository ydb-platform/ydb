# -*- coding: utf-8 -*-
# Copyright: See the LICENSE file.

from __future__ import unicode_literals

from . import base
import warnings

SESSION_PERSISTENCE_COMMIT = 'commit'
SESSION_PERSISTENCE_FLUSH = 'flush'
VALID_SESSION_PERSISTENCE_TYPES = [
    None,
    SESSION_PERSISTENCE_COMMIT,
    SESSION_PERSISTENCE_FLUSH,
]


class SQLAlchemyOptions(base.FactoryOptions):
    def _check_sqlalchemy_session_persistence(self, meta, value):
        if value not in VALID_SESSION_PERSISTENCE_TYPES:
            raise TypeError(
                "%s.sqlalchemy_session_persistence must be one of %s, got %r" %
                (meta, VALID_SESSION_PERSISTENCE_TYPES, value)
            )

    def _check_force_flush(self, meta, value):
        if value:
            warnings.warn(
                "%(meta)s.force_flush has been deprecated as of 2.8.0 and will be removed in 3.0.0. "
                "Please set ``%(meta)s.sqlalchemy_session_persistence = 'flush'`` instead."
                % dict(meta=meta),
                DeprecationWarning,
                # Stacklevel:
                # declaration -> FactoryMetaClass.__new__ -> meta.contribute_to_class
                # -> meta._fill_from_meta -> option.apply -> option.checker
                stacklevel=6,
            )

    def _build_default_options(self):
        return super(SQLAlchemyOptions, self)._build_default_options() + [
            base.OptionDefault('sqlalchemy_session', None, inherit=True),
            base.OptionDefault(
                'sqlalchemy_session_persistence',
                None,
                inherit=True,
                checker=self._check_sqlalchemy_session_persistence,
            ),

            # DEPRECATED as of 2.8.0, remove in 3.0.0
            base.OptionDefault(
                'force_flush',
                False,
                inherit=True,
                checker=self._check_force_flush,
            ),
        ]


class SQLAlchemyModelFactory(base.Factory):
    """Factory for SQLAlchemy models. """

    _options_class = SQLAlchemyOptions

    class Meta:
        abstract = True

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        """Create an instance of the model, and save it to the database."""
        session = cls._meta.sqlalchemy_session
        session_persistence = cls._meta.sqlalchemy_session_persistence
        if cls._meta.force_flush:
            session_persistence = SESSION_PERSISTENCE_FLUSH

        obj = model_class(*args, **kwargs)
        if session is None:
            raise RuntimeError("No session provided.")
        session.add(obj)
        if session_persistence == SESSION_PERSISTENCE_FLUSH:
            session.flush()
        elif session_persistence == SESSION_PERSISTENCE_COMMIT:
            session.commit()
        return obj
