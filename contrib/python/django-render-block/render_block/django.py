from copy import copy
from typing import Optional

from django.http import HttpRequest
from django.template import Context, RequestContext
from django.template.backends.django import Template as DjangoTemplate
from django.template.base import Template
from django.template.context import RenderContext
from django.template.loader_tags import (
    BLOCK_CONTEXT_KEY,
    BlockContext,
    BlockNode,
    ExtendsNode,
)

from render_block.exceptions import BlockNotFound


def django_render_block(
    template: DjangoTemplate,
    block_name: str,
    context: Context,
    request: Optional[HttpRequest] = None,
) -> str:
    # Create a Django Context if needed
    if isinstance(context, Context):
        # Make a copy of the context and reset the rendering state.
        # Trying to re-use a RenderContext in multiple renders can
        # lead to TemplateNotFound errors, as Django will skip past
        # any template files it thinks it has already rendered in a
        # template's inheritance stack.
        context_instance = copy(context)
        context_instance.render_context = RenderContext()
    elif request:
        context_instance = RequestContext(request, context)
    else:
        context_instance = Context(context)

    # Get the underlying django.template.base.Template object from the backend.
    template = template.template

    # Bind the template to the context.
    with context_instance.render_context.push_state(template):
        with context_instance.bind_template(template):
            # Before trying to render the template, we need to traverse the tree of
            # parent templates and find all blocks in them.
            _build_block_context(template, context_instance)

            return _render_template_block(block_name, context_instance)


def _build_block_context(template: Template, context: Context) -> None:
    """
    Populate the block context with BlockNodes from this template and parent templates.
    """

    # Ensure there's a BlockContext before rendering. This allows blocks in
    # ExtendsNodes to be found by sub-templates (allowing {{ block.super }} and
    # overriding sub-blocks to work).
    if BLOCK_CONTEXT_KEY not in context.render_context:
        context.render_context[BLOCK_CONTEXT_KEY] = BlockContext()
    block_context = context.render_context[BLOCK_CONTEXT_KEY]

    # Add the template's blocks to the context.
    block_context.add_blocks(
        {n.name: n for n in template.nodelist.get_nodes_by_type(BlockNode)}
    )

    # Check parent nodes (there should only ever be 0 or 1).
    for node in template.nodelist.get_nodes_by_type(ExtendsNode):
        parent = node.get_parent(context)

        # Recurse and search for blocks from the parent.
        _build_block_context(parent, context)


def _render_template_block(block_name: str, context: Context) -> str:
    """Renders a single block from a template."""
    block_node = context.render_context[BLOCK_CONTEXT_KEY].get_block(block_name)

    if block_node is None:
        # The wanted block_name was not found.
        raise BlockNotFound(f"block with name '{block_name}' does not exist")

    return block_node.render(context)  # type: ignore[no-any-return]
