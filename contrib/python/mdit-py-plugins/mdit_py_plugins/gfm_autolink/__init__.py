"""GFM autolink extension plugin for markdown-it-py.

Implements the `GFM autolink extension
<https://github.github.com/gfm/#autolinks-extension->`_,
which recognises bare URLs (``http://``, ``https://``, ``www.``),
protocol links (``mailto:``, ``xmpp:``),
and bare email addresses without requiring angle brackets.

Ported from the Rust crate
`markdown_it_autolink <https://github.com/markdown-it-rust/markdown-it-plugins.rs>`_.
"""

from .index import gfm_autolink_plugin

__all__ = ("gfm_autolink_plugin",)
