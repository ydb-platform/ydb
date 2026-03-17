from __future__ import annotations

from typing import TYPE_CHECKING

from django import template
from django.template.base import token_kwargs

from csp.utils import build_script_tag

if TYPE_CHECKING:
    from django.template.base import FilterExpression, NodeList, Parser, Token
    from django.template.context import Context

register = template.Library()


def _unquote(s: str) -> str:
    """Helper func that strips single and double quotes from inside strings"""
    return s.replace('"', "").replace("'", "")


@register.tag(name="script")
def script(parser: Parser, token: Token) -> NonceScriptNode:
    # Parse out any keyword args
    token_args = token.split_contents()
    kwargs = token_kwargs(token_args[1:], parser)

    nodelist = parser.parse(("endscript",))
    parser.delete_first_token()

    return NonceScriptNode(nodelist, **kwargs)


class NonceScriptNode(template.Node):
    def __init__(self, nodelist: NodeList, **kwargs: FilterExpression) -> None:
        self.nodelist = nodelist
        self.script_attrs = {}
        for k, v in kwargs.items():
            self.script_attrs[k] = self._get_token_value(v)

    def _get_token_value(self, t: FilterExpression) -> str | None:
        if hasattr(t, "token") and t.token:
            return _unquote(t.token)
        return None

    def render(self, context: Context) -> str:
        output = self.nodelist.render(context).strip()
        request = context.get("request")
        nonce = str(getattr(request, "csp_nonce", ""))
        self.script_attrs.update({"nonce": nonce, "content": output})

        return build_script_tag(**self.script_attrs)
