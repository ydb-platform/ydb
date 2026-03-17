from wtforms import Form, IntegerField, __version__


class MyForm(Form):
    a = IntegerField()


def test_errors():
    form = MyForm.from_json({'a': 'not an integer!'})
    form.validate()
    if __version__ < "3":
        assert form.errors == {'a': [u'Not a valid integer value']}
    else:
        assert form.errors == {'a': [u'Not a valid integer value.']}
