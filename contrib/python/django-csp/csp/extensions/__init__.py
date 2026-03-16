from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from jinja2 import nodes
from jinja2.ext import Extension

from csp.utils import SCRIPT_ATTRS, build_script_tag

if TYPE_CHECKING:
    from jinja2.parser import Parser


class NoncedScript(Extension):
    # a set of names that trigger the extension.
    tags = {"script"}

    def parse(self, parser: Parser) -> nodes.Node:
        # the first token is the token that started the tag.  In our case
        # we only listen to ``'script'`` so this will be a name token with
        # `script` as value.  We get the line number so that we can give
        # that line number to the nodes we create by hand.
        lineno = next(parser.stream).lineno

        # Get the current context and pass along
        kwargs = [nodes.Keyword("ctx", nodes.ContextReference())]

        # Parse until we are done with optional script tag attributes
        while parser.stream.current.value in SCRIPT_ATTRS:
            attr_name = parser.stream.current.value
            parser.stream.skip(2)
            kwargs.append(nodes.Keyword(attr_name, parser.parse_expression()))

        # now we parse the body of the script block up to `endscript` and
        # drop the needle (which would always be `endscript` in that case)
        body = parser.parse_statements(("name:endscript",), drop_needle=True)

        # now return a `CallBlock` node that calls our _render_script
        # helper method on this extension.
        return nodes.CallBlock(self.call_method("_render_script", kwargs=kwargs), [], [], body).set_lineno(lineno)

    def _render_script(self, caller: Callable[[], str], **kwargs: Any) -> str:
        ctx = kwargs.pop("ctx")
        request = ctx.get("request")
        kwargs["nonce"] = str(request.csp_nonce)
        kwargs["content"] = caller().strip()

        return build_script_tag(**kwargs)
