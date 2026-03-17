from sqlalchemy import types

from ..scalar_coercible import ScalarCoercible
from .pendulum_date import PendulumDate


class EnrichedDateType(types.TypeDecorator, ScalarCoercible):
    """
    Supported for pendulum only.

    Example::


        from sqlalchemy_utils import EnrichedDateType
        import pendulum


        class User(Base):
            __tablename__ = 'user'
            id = sa.Column(sa.Integer, primary_key=True)
            birthday = sa.Column(EnrichedDateType(type="pendulum"))


        user = User()
        user.birthday = pendulum.datetime(year=1995, month=7, day=11)
        session.add(user)
        session.commit()
    """
    impl = types.Date
    cache_ok = True

    def __init__(self, date_processor=PendulumDate, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.date_object = date_processor()

    def _coerce(self, value):
        return self.date_object._coerce(self.impl, value)

    def process_bind_param(self, value, dialect):
        return self.date_object.process_bind_param(self.impl, value, dialect)

    def process_result_value(self, value, dialect):
        return self.date_object.process_result_value(self.impl, value, dialect)

    def process_literal_param(self, value, dialect):
        return value

    @property
    def python_type(self):
        return self.impl.type.python_type
