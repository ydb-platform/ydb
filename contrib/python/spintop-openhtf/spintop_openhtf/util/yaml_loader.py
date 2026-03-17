import yaml

try:
    from jinja2 import Template 
except ImportError:
    pass

def _parse_net_string(net):
    refered_nets = net.split(',')
    return refered_nets

def load_yml_file(filename, *args, **kwargs):
    with open(filename) as open_file:
        return load_yml(open_file.read(), *args, **kwargs)

def load_yml(yml_string, use_jinja2=False, render_data={}):
    if use_jinja2:
        rendered = render_jinja2_template(yml_string, data=render_data)
    else:
        rendered = yml_string
    content = yaml.load(rendered)
    return content

def render_jinja2_template(content, data={}):
    template = Template(content)
    return template.render(**data)