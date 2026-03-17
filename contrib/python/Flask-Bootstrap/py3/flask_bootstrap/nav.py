from hashlib import sha1
from dominate import tags
from visitor import Visitor


class BootstrapRenderer(Visitor):
    def __init__(self, html5=True, id=None):
        self.html5 = html5
        self._in_dropdown = False
        self.id = id

    def visit_Navbar(self, node):
        # create a navbar id that is somewhat fixed, but do not leak any
        # information about memory contents to the outside
        node_id = self.id or sha1(str(id(node)).encode()).hexdigest()

        root = tags.nav() if self.html5 else tags.div(role='navigation')
        root['class'] = 'navbar navbar-default'

        cont = root.add(tags.div(_class='container-fluid'))

        # collapse button
        header = cont.add(tags.div(_class='navbar-header'))
        btn = header.add(tags.button())
        btn['type'] = 'button'
        btn['class'] = 'navbar-toggle collapsed'
        btn['data-toggle'] = 'collapse'
        btn['data-target'] = '#' + node_id
        btn['aria-expanded'] = 'false'
        btn['aria-controls'] = 'navbar'

        btn.add(tags.span('Toggle navigation', _class='sr-only'))
        btn.add(tags.span(_class='icon-bar'))
        btn.add(tags.span(_class='icon-bar'))
        btn.add(tags.span(_class='icon-bar'))

        # title may also have a 'get_url()' method, in which case we render
        # a brand-link
        if node.title is not None:
            if hasattr(node.title, 'get_url'):
                header.add(tags.a(node.title.text, _class='navbar-brand',
                                  href=node.title.get_url()))
            else:
                header.add(tags.span(node.title, _class='navbar-brand'))

        bar = cont.add(tags.div(
            _class='navbar-collapse collapse',
            id=node_id,
        ))
        bar_list = bar.add(tags.ul(_class='nav navbar-nav'))

        for item in node.items:
            bar_list.add(self.visit(item))

        return root

    def visit_Text(self, node):
        if not self._in_dropdown:
            return tags.p(node.text, _class='navbar-text')
        return tags.li(node.text, _class='dropdown-header')

    def visit_Link(self, node):
        item = tags.li()
        item.add(tags.a(node.text, href=node.get_url()))

        return item

    def visit_Separator(self, node):
        if not self._in_dropdown:
            raise RuntimeError('Cannot render separator outside Subgroup.')
        return tags.li(role='separator', _class='divider')

    def visit_Subgroup(self, node):
        if not self._in_dropdown:
            li = tags.li(_class='dropdown')
            if node.active:
                li['class'] = 'active'
            a = li.add(tags.a(node.title, href='#', _class='dropdown-toggle'))
            a['data-toggle'] = 'dropdown'
            a['role'] = 'button'
            a['aria-haspopup'] = 'true'
            a['aria-expanded'] = 'false'
            a.add(tags.span(_class='caret'))

            ul = li.add(tags.ul(_class='dropdown-menu'))

            self._in_dropdown = True
            for item in node.items:
                ul.add(self.visit(item))
            self._in_dropdown = False

            return li
        else:
            raise RuntimeError('Cannot render nested Subgroups')

    def visit_View(self, node):
        item = tags.li()
        item.add(tags.a(node.text, href=node.get_url(), title=node.text))
        if node.active:
            item['class'] = 'active'

        return item
