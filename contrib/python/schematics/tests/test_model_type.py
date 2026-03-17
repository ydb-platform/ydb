import pytest

from schematics.models import Model
from schematics.types import IntType, StringType
from schematics.types.compound import ModelType, ListType
from schematics.exceptions import DataError
from schematics.util import ImportStringError


def test_simple_embedded_models():
    class Location(Model):
        country_code = StringType()

    class Player(Model):
        id = IntType()
        location = ModelType(Location)

    p = Player(dict(id=1, location={"country_code": "US"}))

    assert p.id == 1
    assert p.location.country_code == "US"

    p.location = Location({"country_code": "IS"})

    assert isinstance(p.location, Location)
    assert p.location.country_code == "IS"


def test_simple_embedded_models_is_none():
    class Location(Model):
        country_code = StringType()

    class Player(Model):
        id = IntType()
        location = ModelType(Location)

    p = Player(dict(id=1))

    assert p.id == 1
    assert p.location is None


def test_simple_embedded_model_set_to_none():
    class Location(Model):
        country_code = StringType()

    class Player(Model):
        id = IntType()
        location = ModelType(Location)

    p = Player(dict(id=1))
    p.location = None

    assert p.id == 1
    assert p.location is None


def test_simple_embedded_model_is_none_within_listtype():
    class QuestionResources(Model):
        type = StringType()

    class Question(Model):
        id = StringType()
        resources = ModelType(QuestionResources)

    class QuestionPack(Model):
        id = StringType()
        questions = ListType(ModelType(Question))

    question_pack = QuestionPack({
        "id": "1",
        "questions": [
            {
                "id": "1",
            },
        ]
    })

    assert question_pack.questions[0].resources is None


def test_raises_validation_error_on_init_with_partial_submodel():
    class User(Model):
        name = StringType(required=True)
        age = IntType(required=True)

    class Card(Model):
        user = ModelType(User)

    u = User({'name': 'Arthur'})
    c = Card({'user': u})

    with pytest.raises(DataError):
        c.validate()


def test_model_type():
    class User(Model):
        name = StringType()

    class Card(Model):
        user = ModelType(User)

    c = Card({"user": {'name': u'Doggy'}})
    assert isinstance(c.user, User)
    assert c.user.name == "Doggy"


def test_equality_with_embedded_models():
    class Location(Model):
        country_code = StringType()

    class Player(Model):
        id = IntType()
        location = ModelType(Location)

    p1 = Player(dict(id=1, location={"country_code": "US"}))
    p2 = Player(dict(id=1, location={"country_code": "US"}))

    assert id(p1.location) != id(p2.location)
    assert p1.location == p2.location

    assert p1 == p2


def test_default_value_when_embedded_model():
    class Question(Model):
        question_id = StringType(required=True)

        type = StringType(default="text")

    class QuestionPack(Model):

        question = ModelType(Question)

    pack = QuestionPack({
        "question": {
            "question_id": 1
        }
    })

    assert pack.question.question_id == "1"
    assert pack.question.type == "text"


def test_export_loop_with_subclassed_model():
    class Asset(Model):
        file_name = StringType()

    class S3Asset(Asset):
        bucket_name = StringType()

    class Product(Model):
        title = StringType()
        asset = ModelType(Asset)

    asset = S3Asset({'bucket_name': 'assets_bucket', 'file_name': 'bar'})

    product = Product({'title': 'baz', 'asset': asset})

    primitive = product.to_primitive()
    assert 'bucket_name' in primitive['asset']

    native = product.to_native()
    assert 'bucket_name' in native['asset']


def test_conversion_error_recursive_overhead():
    conversions = [0]

    class Leaf(Model):
        pass
    next_model = Leaf
    data = 'not a mapping'

    for i in range(20):
        class Recursive(Model):
            x = ModelType(next_model, required=True)

            def __init__(self, *args, **kwargs):
                super(type(self), self).__init__(*args, **kwargs)
                conversions[0] += 1
                assert conversions[0] < 25
        next_model = Recursive
        data = {'x': data}

    with pytest.raises(DataError):
        next_model(data)


def test_mock_object():
    class User(Model):
        name = StringType(required=True)
        age = IntType(required=True)

    assert ModelType(User, required=True).mock() is not None


def test_specify_model_by_name():

    class M(Model):
        to_one = ModelType('M')
        to_many = ListType(ModelType('M'))
        matrix = ListType(ListType(ModelType('M')))

    assert M.to_one.model_class is M
    assert M.to_many.field.model_class is M
    assert M.matrix.field.field.model_class is M


def test_model_context_pass_to_type():
    from schematics.types import BaseType
    from schematics.datastructures import Context

    class CustomType(BaseType):

        def to_native(self, value, context=None):
            suffix = context.suffix
            return str(value) + suffix

        def to_primitive(self, value, context=None):
            suffix = context.suffix
            return value[:-len(suffix)]

    class Thing(Model):
        x = CustomType()

    context = {'suffix': 'z'}
    input = {'x': 'thingie'}
    thing = Thing(input, context=context)
    assert thing.x == 'thingiez'
    assert thing.to_primitive(context=context) == {'x': 'thingie'}
    # try it with a Context object
    model_context = Context(suffix='z!')
    thing2 = Thing(input, context=model_context)
    assert thing2.x == 'thingiez!'
    export_context = Context(suffix='z!')
    assert thing2.to_primitive(context=export_context) == {'x': 'thingie'}
    with pytest.raises(AttributeError):
        # can't reuse the same Context object as was used for model
        # TODO this may be unexpected to the uninitiated; a custom exception
        #      could explain it better.
        thing2.to_primitive(context=model_context)


def test_model_app_data_pass_to_type():
    from schematics.types import BaseType

    class CustomType(BaseType):

        def to_native(self, value, context=None):
            suffix = context.app_data['suffix']
            return str(value) + suffix

        def to_primitive(self, value, context=None):
            suffix = context.app_data['suffix']
            return value[:-len(suffix)]

    class Thing(Model):
        x = CustomType()

    app_data = {'suffix': 'z'}
    input = {'x': 'thingie'}
    thing = Thing(input, app_data=app_data)
    assert thing.x == 'thingiez'
    assert thing.to_primitive(app_data=app_data) == {'x': 'thingie'}


class OuterModel:
    class InnerModel(Model):
        test = StringType()


def test_deep_string_search():
    class TestModel(Model):
        deep_model = ModelType('test_model_type.OuterModel.InnerModel')

    test = TestModel(dict(deep_model=dict(test='Abc')))
    assert test.validate() is None

    class TestModel2(Model):
        invalid_model = ModelType('a.c.d.e')
    with pytest.raises(ImportStringError):
        TestModel2(dict(invalid_model=dict(a='1')))


def test_recursive_string_self_reference():
    class RecursiveTestModel(Model):
        recursive_model = ModelType('RecursiveTestModel')
        test = StringType()

    test = RecursiveTestModel(dict(recursive_model=dict(test='Abc')))
    assert test.validate() is None
    assert test.recursive_model.test == 'Abc'


def test_circular_string_reference():
    class TestModel1(Model):
        model2 = ModelType('TestModel2')
        name = StringType()

    class TestModel2(Model):
        model1 = ModelType('TestModel1')
        description = StringType()

    data1 = {'name': 'Test1'}
    data2 = {'description': 'Test2', 'model1': data1}

    # TODO: we might want to support locals import late binding someday/somehow
    with pytest.raises(ImportStringError):
        test = TestModel1({
            'name': 'Root',
            'model2': data2
        })
