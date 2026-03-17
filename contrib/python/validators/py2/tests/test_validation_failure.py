import six

import validators

obj_repr = (
    "ValidationFailure(func=between"
)


class TestValidationFailure(object):
    def setup_method(self, method):
        self.obj = validators.between(3, min=4, max=5)

    def test_boolean_coerce(self):
        assert not bool(self.obj)
        assert not self.obj

    def test_repr(self):
        assert obj_repr in repr(self.obj)

    def test_unicode(self):
        assert obj_repr in six.text_type(self.obj)

    def test_arguments_as_properties(self):
        assert self.obj.value == 3
        assert self.obj.min == 4
        assert self.obj.max == 5
