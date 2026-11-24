import yaml


class ConfigDirective:
    """A tagged configuration value interpreted by YDB."""

    def __init__(self, tag, value):
        self.tag = tag
        self.value = value


class ConfigLoader(yaml.SafeLoader):
    pass


class ConfigDumper(yaml.SafeDumper):
    pass


def construct_directive(loader, node):
    if isinstance(node, yaml.MappingNode):
        value = loader.construct_mapping(node, deep=True)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_scalar(node)
    return ConfigDirective(node.tag, value)


def represent_directive(dumper, directive):
    value = directive.value
    if isinstance(value, dict):
        return dumper.represent_mapping(directive.tag, value)
    if isinstance(value, list):
        return dumper.represent_sequence(directive.tag, value)
    return dumper.represent_scalar(directive.tag, value)


for tag in ('!inherit', '!append', '!remove'):
    ConfigLoader.add_constructor(tag, construct_directive)


def construct_keyed_inherit(loader, suffix, node):
    return construct_directive(loader, node)


ConfigLoader.add_multi_constructor('!inherit:', construct_keyed_inherit)
ConfigDumper.add_representer(ConfigDirective, represent_directive)


def load(stream):
    return yaml.load(stream, Loader=ConfigLoader)


def dump(data):
    return yaml.dump(data, Dumper=ConfigDumper, sort_keys=False, allow_unicode=True)
