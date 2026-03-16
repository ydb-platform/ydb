import html

from dishka.plotter.model import Group, Node, NodeType, Renderer

HTML_TEMPLATE = """\
<html>
<head>
    <meta charset="UTF-8">
</head>
<body>

<pre class="mermaid">
{diagram}
</pre>

<script type="module">
    import mermaid
    from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
    mermaid.initialize(config);
</script>
</body>
</html>

"""


# these symbols should be escaped additionally to html.escape
MERMAID_SYMBOLS_SUBST = str.maketrans({
    "<": "&lt",
    ">": "&gt",
})


class MermaidRenderer(Renderer):
    def __init__(self) -> None:
        self.nodes: dict[str, Node] = {}

    def _render_node(self, node: Node) -> str:
        if node.type is NodeType.ALIAS:
            return ""
        name = self._node_type(node) + self._escape(node.name)
        if node.type in (NodeType.SELECTOR, NodeType.COLLECTION):
            return  f'class {node.id}["{name}"]'
        source_name = self._escape(node.source_name)
        return "\n".join([
            f'class {node.id}["{name}"]{{',
            f"{source_name}()" if source_name else " ",
            *(
                f"{self._escape(self.nodes[dep].name)}"
                for dep in node.dependencies
            ),
            "}",
        ])

    def _escape(self, line: str) -> str:
        line = line.translate(MERMAID_SYMBOLS_SUBST)
        return html.escape(line, quote=True)

    def _render_node_deps(self, node: Node) -> list[str]:
        res: list[str] = []
        if node.type is NodeType.ALIAS:
            return res
        for dep in node.dependencies:
            dep_node = self.nodes[dep]
            if dep_node.type is NodeType.ALIAS:
                target = dep_node.dependencies[0]
                res.append(
                    f"{target} <.. {node.id}: "
                    f"🔗 {self._escape(dep_node.name)}",
                )
            else:
                res.append(f"{dep} <.. {node.id}")
        return res

    def _node_type(self, node: Node) -> str:
        if node.is_protocol:
            prefix = "Ⓘ "
        else:
            prefix = ""
        if node.type is NodeType.DECORATOR:
            return "🎭 " + prefix
        elif node.type is NodeType.COLLECTION:
            return "🗂 " + prefix
        elif node.type is NodeType.SELECTOR:
            return "🤔 " + prefix
        elif node.type is NodeType.CONTEXT:
            return "📥 " + prefix
        elif node.type is NodeType.ALIAS:
            return "🔗 " + prefix
        return "🏭 " + prefix

    def _render_group(
            self, group: Group, name_prefix: str = "",
    ) -> str:
        if not group.nodes and not group.children:
            return ""
        name = name_prefix + self._escape(group.name or "")
        res = ""
        if group.nodes:
            res = f"namespace {name} {{\n"
            for node in group.nodes:
                res += self._render_node(node) + "\n"
            res += "}\n"
        for child in group.children:
            res += self._render_group(child, name) + "\n"
        return res

    def _render_links(self, group: Group) -> str:
        res = ""
        for node in group.nodes:
            for dep_str in self._render_node_deps(node):
                res += dep_str + "\n"
        for child in group.children:
            res += self._render_links(child)
        return res

    def _fill_nodes(self, groups: list[Group]) -> None:
        for group in groups:
            for node in group.nodes:
                self.nodes[node.id] = node
            self._fill_nodes(group.children)

    def render(self, groups: list[Group]) -> str:
        self._fill_nodes(groups)
        res = (
            "---\n"
            "  config:\n"
            "    class:\n"
            "      hideEmptyMembersBox: true\n"
            "---\n"
        )
        res += "classDiagram\n"
        res += "direction LR\n"
        for group in groups:
            res += self._render_group(group)
            res += self._render_links(group)
        return HTML_TEMPLATE.format(diagram=res)
