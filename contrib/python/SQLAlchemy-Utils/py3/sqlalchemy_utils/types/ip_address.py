from ipaddress import ip_address

from sqlalchemy import types

from .scalar_coercible import ScalarCoercible


class IPAddressType(ScalarCoercible, types.TypeDecorator):
    """
    Changes IPAddress objects to a string representation on the way in and
    changes them back to IPAddress objects on the way out.

    ::


        from sqlalchemy_utils import IPAddressType


        class User(Base):
            __tablename__ = 'user'
            id = sa.Column(sa.Integer, autoincrement=True)
            name = sa.Column(sa.Unicode(255))
            ip_address = sa.Column(IPAddressType)


        user = User()
        user.ip_address = '123.123.123.123'
        session.add(user)
        session.commit()

        user.ip_address  # IPAddress object
    """

    impl = types.Unicode(50)
    cache_ok = True

    def __init__(self, max_length=50, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.impl = types.Unicode(max_length)

    def process_bind_param(self, value, dialect):
        return str(value) if value else None

    def process_result_value(self, value, dialect):
        return ip_address(value) if value else None

    def _coerce(self, value):
        return ip_address(value) if value else None

    @property
    def python_type(self):
        return self.impl.type.python_type
