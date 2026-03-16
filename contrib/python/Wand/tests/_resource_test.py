# Why the name of this file does start with underscore?
#
# This tests Wand's internal reference counter, so we can't assume
# the initial state after any function of Wand are used.
# That means this tests have to be first, and pytest automatically
# discovers tests just using filenames.  Fortuneately, it seems to run
# tests in lexicographical order, so we simply adds underscore to
# the beginning of the filename.
from pytest import mark

from wand import exceptions, resource


class DummyResource(resource.Resource):

    def set_exception_type(self, idx):
        self.exception_index = idx

    def get_exception(self):
        exc_cls = exceptions.TYPE_MAP[self.exception_index]
        return exc_cls("Dummy exception")


@mark.parametrize('code', exceptions.TYPE_MAP.keys())
def test_raises_exceptions(recwarn, code):
    """Exceptions raise, and warnings warn"""
    res = DummyResource()
    res.set_exception_type(code)
    try:
        res.raise_exception()
    except exceptions.WandException as e:
        assert not e.__class__.__name__.endswith('Warning')
        assert str(e) == 'Dummy exception'
    else:
        w = recwarn.pop()
        assert w.category.__name__.endswith('Warning')
        assert "Dummy exception" in str(w.message)
        assert recwarn.list == []


@mark.skip("Flaky")
def test_limits():
    area_was = resource.limits['area']  # Save state.
    area_expected = area_was - 100
    resource.limits['area'] = area_expected
    assert resource.limits['area'] <= area_expected
    # We have no images loaded, so the current area should be zero.
    assert resource.limits.resource('area') == 0
    del resource.limits['area']
    assert resource.limits['area'] == 0
    resource.limits['area'] = area_was  # To restore for other tests.
    # Non functional smoke test.
    for _ in resource.limits:
        pass
    assert len(resource.limits) > 0
