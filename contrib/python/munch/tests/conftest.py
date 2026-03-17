import pytest
import munch


@pytest.fixture(name='yaml')
def yaml_module():
    try:
        import yaml  # pylint: disable=import-outside-toplevel
        return yaml
    except ImportError:
        pass
    pytest.skip("Module 'PyYAML' is required")


@pytest.fixture(params=[munch.Munch, munch.AutoMunch, munch.DefaultMunch, munch.DefaultFactoryMunch,
                        munch.RecursiveMunch])
def munch_obj(request):
    cls = request.param
    args = tuple()
    if cls == munch.DefaultFactoryMunch:
        args = args + (lambda: None,)
    return cls(*args, hello="world", number=5)
