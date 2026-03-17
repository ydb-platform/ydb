from builtins import super
from flask_sqlalchemy.model import Model
from sqlalchemy.orm.session import sessionmaker
from flask_sqlalchemy import SQLAlchemy, BaseQuery, _SessionSignalEvents, get_state
from aws_xray_sdk.ext.sqlalchemy.query import XRaySession, XRayQuery
from aws_xray_sdk.ext.sqlalchemy.util.decorators import xray_on_call, decorate_all_functions


@decorate_all_functions(xray_on_call)
class XRayBaseQuery(BaseQuery):
    BaseQuery.__bases__ = (XRayQuery,)


class XRaySignallingSession(XRaySession):
    """
    .. versionadded:: 2.0
    .. versionadded:: 2.1

    The signalling session is the default session that Flask-SQLAlchemy
    uses. It extends the default session system with bind selection and
    modification tracking.
    If you want to use a different session you can override the
    :meth:`SQLAlchemy.create_session` function.
    The `binds` option was added, which allows a session to be joined
    to an external transaction.
    """
    def __init__(self, db, autocommit=False, autoflush=True, **options):
        #: The application that this session belongs to.
        self.app = app = db.get_app()
        track_modifications = app.config['SQLALCHEMY_TRACK_MODIFICATIONS']
        bind = options.pop('bind', None) or db.engine
        binds = options.pop('binds', db.get_binds(app))

        if track_modifications is None or track_modifications:
            _SessionSignalEvents.register(self)

        XRaySession.__init__(
            self, autocommit=autocommit, autoflush=autoflush,
            bind=bind, binds=binds, **options
        )

    def get_bind(self, mapper=None, clause=None):
        # mapper is None if someone tries to just get a connection
        if mapper is not None:
            info = getattr(mapper.mapped_table, 'info', {})
            bind_key = info.get('bind_key')
            if bind_key is not None:
                state = get_state(self.app)
                return state.db.get_engine(self.app, bind=bind_key)
        return XRaySession.get_bind(self, mapper, clause)


class XRayFlaskSqlAlchemy(SQLAlchemy):
    def __init__(self, app=None, use_native_unicode=True, session_options=None,
                 metadata=None, query_class=XRayBaseQuery, model_class=Model):
        super().__init__(app, use_native_unicode, session_options,
                         metadata, query_class, model_class)

    def create_session(self, options):
        return sessionmaker(class_=XRaySignallingSession, db=self, **options)
