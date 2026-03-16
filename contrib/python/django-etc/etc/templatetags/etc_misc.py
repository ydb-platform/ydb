from copy import deepcopy
from functools import partial

from django import template
from django.template import TemplateDoesNotExist
from django.template.base import Lexer, Parser
from django.template.loader_tags import do_include, Node

try:
    from django.template.loader_tags import construct_relative_path

    # To prevent AttributeError: 'Parser' object has no attribute 'origin'
    def construct_relative_path_(parser, name):
        return construct_relative_path(parser.origin.template_name, name)

except ImportError:
    construct_relative_path_ = lambda parser, name: name  # Sorry sub here for now.

from ..toolbox import get_site_url

get_lexer = partial(Lexer)
register = template.Library()


@register.simple_tag(takes_context=True)
def site_url(context):
    """Tries to get a site URL from environment and settings.

    See toolbox.get_site_url() for description.

    Example:

        {% load etc_misc %}
        {% site_url %}

    """
    return get_site_url(request=context.get('request', None))


class DynamicIncludeNode(Node):

    context_key = '__include_context'

    def __init__(self, template, *args, **kwargs):
        self.fallback = kwargs.pop('fallback', None)
        self.template = template
        self.extra_context = kwargs.pop('extra_context', {})
        self.isolated_context = kwargs.pop('isolated_context', False)
        super(DynamicIncludeNode, self).__init__(*args, **kwargs)

    def render_(self, tpl_new, context):

        template = deepcopy(self.template)  # Do not mess with global template for threadsafety.
        template.var = tpl_new  # Cheat a little

        # Below is implementation from Django 2.1 generic IncludeNode.

        template = template.resolve(context)
        # Does this quack like a Template?
        if not callable(getattr(template, 'render', None)):
            # If not, try the cache and get_template().
            template_name = template
            cache = context.render_context.dicts[0].setdefault(self, {})
            template = cache.get(template_name)
            if template is None:
                template = context.template.engine.get_template(template_name)
                cache[template_name] = template
        # Use the base.Template of a backends.django.Template.
        elif hasattr(template, 'template'):
            template = template.template
        values = {
            name: var.resolve(context)
            for name, var in self.extra_context.items()
        }
        if self.isolated_context:
            return template.render(context.new(values))
        with context.push(**values):
            return template.render(context)

    def render(self, context):
        render_ = self.render_

        try:
            return render_(
                tpl_new=Parser(get_lexer(self.template.var).tokenize()).parse().render(context),
                context=context)

        except TemplateDoesNotExist:
            fallback = self.fallback

            if not fallback:  # pragma: nocover
                raise

            return render_(tpl_new=fallback.var, context=context)


@register.tag('include_')
def include_(parser, token):
    """Similar to built-in ``include`` template tag, but allowing
    template variables to be used in template name and a fallback template,
    thus making the tag more dynamic.

    .. warning:: Requires Django 1.8+

    Example:

        {% load etc_misc %}
        {% include_ "sub_{{ postfix_var }}.html" fallback "default.html" %}

    """
    bits = token.split_contents()

    dynamic = False

    # We fallback to built-in `include` if a template name contains no variables.
    if len(bits) >= 2:
        dynamic = '{{' in bits[1]

        if dynamic:
            fallback = None
            bits_new = []

            for bit in bits:

                if fallback is True:
                    # This bit is a `fallback` argument.
                    fallback = bit
                    continue

                if bit == 'fallback':
                    fallback = True

                else:
                    bits_new.append(bit)

            if fallback:
                fallback = parser.compile_filter(construct_relative_path_(parser, fallback))

            token.contents = ' '.join(bits_new)

    token.contents = token.contents.replace('include_', 'include')
    include_node = do_include(parser, token)

    if dynamic:
        # swap simple include with dynamic
        include_node = DynamicIncludeNode(
            include_node.template,
            extra_context=include_node.extra_context,
            isolated_context=include_node.isolated_context,
            fallback=fallback or None,
        )

    return include_node
