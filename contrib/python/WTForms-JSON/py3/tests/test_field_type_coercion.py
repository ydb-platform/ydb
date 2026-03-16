import six  # noqa
from pytest import mark
from wtforms import Form, IntegerField, SelectMultipleField, StringField
try:
    from wtforms.ext.sqlalchemy.fields import QuerySelectField
    HAS_SQLALCHEMY_SUPPORT = False
except ImportError:
    HAS_SQLALCHEMY_SUPPORT = False
from wtforms.validators import IPAddress

sa = None
try:
    import sqlalchemy as sa
    from sqlalchemy.ext.declarative import declarative_base
except ImportError:
    pass


class TestFieldTypeCoercion(object):
    def test_integer_to_unicode_coercion(self):
        class NetworkForm(Form):
            address = StringField('Address', [IPAddress()])

        network = dict(address=123)

        form = NetworkForm.from_json(network)
        assert not form.validate()

    def test_integer_coercion(self):
        class UserForm(Form):
            age = IntegerField('age')

        network = dict(age=123)

        form = UserForm.from_json(network)
        assert form.validate()


class FooForm(Form):
    items = SelectMultipleField(
        choices=(
            (1, 'a'),
            (2, 'b'),
            (3, 'c'),
        ),
        coerce=int
    )


@mark.skipif('sa is None or six.PY3 or not HAS_SQLALCHEMY_SUPPORT')
class TestQuerySelectField(object):
    def setup_method(self, method):
        self.Base = declarative_base()

        class Team(self.Base):
            __tablename__ = 'team'
            id = sa.Column(sa.Integer, primary_key=True)
            name = sa.Column(sa.Unicode(255))

        class Match(self.Base):
            __tablename__ = 'match'
            id = sa.Column(sa.Integer, primary_key=True)
            name = sa.Column(sa.Unicode(255))
            team_id = sa.Column(sa.BigInteger, sa.ForeignKey(Team.id))
            team = sa.orm.relationship(Team)

        self.Team = Team
        self.Match = Match

    def test_integer_coercion(self):
        class MatchForm(Form):
            team = QuerySelectField(
                query_factory=lambda: [
                    self.Team(id=1, name='Manchester United'),
                    self.Team(id=2, name='FC Barcelona')
                ]
            )

        data = {'team': 1}
        form = MatchForm.from_json(data)
        assert form.validate()
        assert form.team.data


class TestSelectMultipleField(object):
    def test_from_json(self):
        data = {'items': [1, 3]}
        form = FooForm.from_json(data)
        assert form.validate()
        assert form.items.data
