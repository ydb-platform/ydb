import pytest


class Hookspec:
    def pytest_register_object_hooks(self):
        pass


class ObjectHooksPlugin:
    def __init__(self):
        self._object_hooks = {}

    @property
    def object_hooks(self):
        return self._object_hooks

    def pytest_sessionstart(self, session):
        hooks = session.config.pluginmanager.hook.pytest_register_object_hooks()
        for hook in hooks:
            self._object_hooks.update(hook)

    def pytest_addhooks(self, pluginmanager):
        pluginmanager.add_hookspecs(Hookspec)


def pytest_configure(config):
    config.pluginmanager.register(ObjectHooksPlugin(), 'object_hook_params')


@pytest.fixture(scope='session')
def _base_object_hook(request, pytestconfig, match_operator):
    hooks = {'$match': match_operator}

    plugin = pytestconfig.pluginmanager.get_plugin('object_hook_params')
    hooks.update(plugin.object_hooks)

    def _wrapper():
        return hooks

    return _wrapper


@pytest.fixture
def object_hook(request, _base_object_hook):
    hooks = {}
    for name, hook in _base_object_hook().items():
        if isinstance(hook, dict) and '$fixture' in hook:
            hook = request.getfixturevalue(hook['$fixture'])
        hooks[name] = hook
    return hooks
