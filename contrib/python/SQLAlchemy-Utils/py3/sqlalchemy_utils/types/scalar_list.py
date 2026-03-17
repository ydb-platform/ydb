import sqlalchemy as sa
from sqlalchemy import types


class ScalarListException(Exception):
    pass


class ScalarListType(types.TypeDecorator):
    """
    ScalarListType type provides convenient way for saving multiple scalar
    values in one column. ScalarListType works like list on python side and
    saves the result as comma-separated list in the database (custom separators
    can also be used).

    Example ::


        from sqlalchemy_utils import ScalarListType


        class User(Base):
            __tablename__ = 'user'
            id = sa.Column(sa.Integer, autoincrement=True)
            hobbies = sa.Column(ScalarListType())


        user = User()
        user.hobbies = ['football', 'ice_hockey']
        session.commit()


    You can easily set up integer lists too:

    ::


        from sqlalchemy_utils import ScalarListType


        class Player(Base):
            __tablename__ = 'player'
            id = sa.Column(sa.Integer, autoincrement=True)
            points = sa.Column(ScalarListType(int))


        player = Player()
        player.points = [11, 12, 8, 80]
        session.commit()


    ScalarListType is always stored as text. To use an array field on
    PostgreSQL database use variant construct::

        from sqlalchemy_utils import ScalarListType


        class Player(Base):
            __tablename__ = 'player'
            id = sa.Column(sa.Integer, autoincrement=True)
            points = sa.Column(
                ARRAY(Integer).with_variant(ScalarListType(int), 'sqlite')
            )


    """
    impl = sa.UnicodeText()

    cache_ok = True

    def __init__(self, coerce_func=str, separator=','):
        self.separator = str(separator)
        self.coerce_func = coerce_func

    def process_bind_param(self, value, dialect):
        # Convert list of values to unicode separator-separated list
        # Example: [1, 2, 3, 4] -> '1, 2, 3, 4'
        if value is not None:
            if any(self.separator in str(item) for item in value):
                raise ScalarListException(
                    "List values can't contain string '%s' (its being used as "
                    "separator. If you wish for scalar list values to contain "
                    "these strings, use a different separator string.)"
                    % self.separator
                )
            return self.separator.join(
                map(str, value)
            )

    def process_result_value(self, value, dialect):
        if value is not None:
            if value == '':
                return []
            # coerce each value
            return list(map(
                self.coerce_func, value.split(self.separator)
            ))
