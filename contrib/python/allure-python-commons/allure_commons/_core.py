from pluggy import PluginManager
from allure_commons import _hooks


class MetaPluginManager(type):
    _plugin_manager: PluginManager = None

    @staticmethod
    def get_plugin_manager():
        if not MetaPluginManager._plugin_manager:
            MetaPluginManager._plugin_manager = PluginManager('allure')
            MetaPluginManager._plugin_manager.add_hookspecs(_hooks.AllureUserHooks)
            MetaPluginManager._plugin_manager.add_hookspecs(_hooks.AllureDeveloperHooks)

        return MetaPluginManager._plugin_manager

    def __getattr__(cls, attr):
        pm = MetaPluginManager.get_plugin_manager()
        return getattr(pm, attr)


class plugin_manager(metaclass=MetaPluginManager):
    pass
