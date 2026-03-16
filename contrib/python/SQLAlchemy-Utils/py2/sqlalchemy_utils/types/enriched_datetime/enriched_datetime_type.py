from sqlalchemy import types

from ..scalar_coercible import ScalarCoercible
from .pendulum_datetime import PendulumDateTime


class EnrichedDateTimeType(types.TypeDecorator, ScalarCoercible):
    """
    Supported for arrow and pendulum.

    Example::


        from sqlalchemy_utils import EnrichedDateTimeType
        import pendulum


        class User(Base):
            __tablename__ = 'user'
            id = sa.Column(sa.Integer, primary_key=True)
            created_at = sa.Column(EnrichedDateTimeType(type="pendulum"))
            # created_at = sa.Column(EnrichedDateTimeType(type="arrow"))


        user = User()
        user.created_at = pendulum.now()
        session.add(user)
        session.commit()
    """
    impl = types.DateTime

    def __init__(self, datetime_processor=PendulumDateTime, *args, **kwargs):
        super(EnrichedDateTimeType, self).__init__(*args, **kwargs)
        self.dt_object = datetime_processor()

    def _coerce(self, value):
        return self.dt_object._coerce(self.impl, value)

    def process_bind_param(self, value, dialect):
        return self.dt_object.process_bind_param(self.impl, value, dialect)

    def process_result_value(self, value, dialect):
        return self.dt_object.process_result_value(self.impl, value, dialect)

    def process_literal_param(self, value, dialect):
        return str(value)

    @property
    def python_type(self):
        return self.impl.type.python_type
