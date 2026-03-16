import importlib
from ..exceptions.exceptions import MissingPluginNames

module_prefix = 'aws_xray_sdk.core.plugins.'

PLUGIN_MAPPING = {
    'elasticbeanstalkplugin': 'elasticbeanstalk_plugin',
    'ec2plugin': 'ec2_plugin',
    'ecsplugin': 'ecs_plugin'
}


def get_plugin_modules(plugins):
    """
    Get plugin modules from input strings
    :param tuple plugins: a tuple of plugin names in str
    """
    if not plugins:
        raise MissingPluginNames("input plugin names are required")

    modules = []

    for plugin in plugins:
        short_name = PLUGIN_MAPPING.get(plugin.lower(), plugin.lower())
        full_path = '%s%s' % (module_prefix, short_name)
        modules.append(importlib.import_module(full_path))

    return tuple(modules)
