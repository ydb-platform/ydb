import textwrap
import ruamel.yaml

from ruamel.yaml.scalarstring import LiteralScalarString


yaml = ruamel.yaml.YAML()
yaml.default_flow_style = False
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=2, offset=0)


def LSS(s):
    return LiteralScalarString(textwrap.dedent(s))


def dump(data, *args, **kwargs):
    if isinstance(data, dict):
        if 'kind' in data:
            if data.get('kind').lower() in ['storage', 'database']:
                if 'spec' in data:
                    if 'configuration' in data['spec']:
                        if isinstance(data['spec']['configuration'], str):
                            if len(data['spec']['configuration'].strip()) > 0:
                                data['spec']['configuration'] = LSS(data['spec']['configuration'])
    return yaml.dump(data, *args, **kwargs)


def load(*args, **kwargs):
    return yaml.load(*args, **kwargs)
