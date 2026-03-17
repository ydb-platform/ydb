from django.template import Context
from django.template.backends.jinja2 import Template as Jinja2Template

from render_block.exceptions import BlockNotFound


def jinja2_render_block(
    template: Jinja2Template, block_name: str, context: Context
) -> str:
    # Get the underlying jinja2.environment.Template object.
    template = template.template

    # Create a new Context instance.
    context = template.new_context(context)

    # Try to find the wanted block.
    try:
        gen = template.blocks[block_name](context)
    except KeyError:
        raise BlockNotFound(f"block with name '{block_name}' does not exist")

    # The result from above is a generator which yields unicode strings.
    return "".join(s for s in gen)
