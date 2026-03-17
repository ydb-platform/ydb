"""Tree builder."""

from copy import copy
from xml.etree import ElementTree

from .constants import namespaces, scoping_elements, table_insert_mode_elements

# The scope markers are inserted when entering object elements,
# marquees, table cells, and table captions, and are used to prevent formatting
# from "leaking" into tables, object elements, and marquees.
Marker = None

_html = namespaces["html"]
_list_elements = {
    None: (frozenset(scoping_elements), False),
    "button": (frozenset(scoping_elements | {(_html, "button")}), False),
    "list": (frozenset(scoping_elements | {(_html, "ol"), (_html, "ul")}), False),
    "table": (frozenset([(_html, "html"), (_html, "table")]), False),
    "select": (frozenset([(_html, "optgroup"), (_html, "option")]), True),
}
# XXX td, th and tr are not actually needed.
_implied_end_tags = frozenset(("dd", "dt", "li", "option", "optgroup", "p", "rp", "rt"))


class ActiveFormattingElements(list):
    def append(self, node):
        """Append node to the end of the list."""
        equal_count = 0
        if node is not Marker:
            for element in self[::-1]:
                if element is Marker:
                    break
                nodes_equal = (
                    element.name_tuple == node.name_tuple and
                    element.attributes == node.attributes)
                if nodes_equal:
                    equal_count += 1
                if equal_count == 3:
                    self.remove(element)
                    break
        list.append(self, node)


class Element:
    def __init__(self, name, namespace=None):
        self.name = name
        self.namespace = namespace
        self._element = ElementTree.Element(self._get_etree_tag(name, namespace))
        if namespace is None:
            self.name_tuple = _html, self.name
        else:
            self.name_tuple = self.namespace, self.name
        self._children = []

        # The parent of the current node (or None for the document node).
        self.parent = None

    def _get_etree_tag(self, name, namespace):
        return name if namespace is None else f"{{{namespace}}}{name}"

    def _get_attributes(self):
        return self._element.attrib

    def _set_attributes(self, attributes):
        element_attributes = self._element.attrib
        element_attributes.clear()
        if attributes:
            # Calling .items _always_ allocates, and the above truthy check is
            # cheaper than the allocation on average.
            for key, value in attributes.items():
                name = f"{{{key[2]}}}{key[1]}" if isinstance(key, tuple) else key
                element_attributes[name] = value

    # A dict holding name -> value pairs for attributes of the node.
    attributes = property(_get_attributes, _set_attributes)

    def _get_children(self):
        return self._children

    def _set_children(self, value):
        del self._element[:]
        self._children = []
        for element in value:
            self.insert_child(element)

    # A list of child nodes of the current node. This must include all
    # elements but not necessarily other node types.
    children = property(_get_children, _set_children)

    def has_content(self):
        """Return True if the node has children or text, False otherwise."""
        return bool(self._element.text or len(self._element))

    def append_child(self, node):
        """Insert node as a child of the current node."""
        self._children.append(node)
        self._element.append(node._element)
        node.parent = self

    def insert_before(self, node, reference):
        """Insert node as a child of the current node, before reference.

        Raise ValueError if reference is not a child of the current node.

        """
        index = list(self._element).index(reference._element)
        self._element.insert(index, node._element)
        node.parent = self

    def remove_child(self, node):
        """Remove node from the children of the current node."""
        self._children.remove(node)
        self._element.remove(node._element)
        node.parent = None

    def insert_text(self, text, insert_before=None):
        """Insert data as text in the current node.

        Text is positioned before the start of node insert_before or to the end
        of the node's text.

        If insert_before is a node, insert the text before this node.

        """
        if not len(self._element):
            if not self._element.text:
                self._element.text = ""
            self._element.text += text
        elif insert_before is None:
            # Insert the text as the tail of the last child element
            if not self._element[-1].tail:
                self._element[-1].tail = ""
            self._element[-1].tail += text
        else:
            # Insert the text before the specified node
            children = list(self._element)
            index = children.index(insert_before._element)
            if index > 0:
                if not self._element[index - 1].tail:
                    self._element[index - 1].tail = ""
                self._element[index - 1].tail += text
            else:
                if not self._element.text:
                    self._element.text = ""
                self._element.text += text

    def clone(self):
        """Return a shallow copy of the current node.

        The node has the same name and attributes, but no parent or children.

        """
        element = type(self)(self.name, self.namespace)
        if self._element.attrib:
            element._element.attrib = copy(self._element.attrib)
        return element

    def reparent_children(self, parent):
        """Move all the children of the current node to parent.

        This is needed so that trees that don't store text as nodes move the
        text in the correct way.

        """
        if parent.children:
            parent.children[-1]._element.tail += self._element.text
        else:
            if not parent._element.text:
                parent._element.text = ""
            if self._element.text is not None:
                parent._element.text += self._element.text
        self._element.text = ""
        for child in self.children:
            parent.append_child(child)
        self.children = []


class Comment(Element):
    def __init__(self, data):
        # Use the superclass constructor to set all properties on the
        # wrapper element
        self._element = ElementTree.Comment(data)
        self.parent = None
        self._children = []


class DocumentType(Element):
    def __init__(self, name, public_id, system_id):
        Element.__init__(self, "<!DOCTYPE>")
        self._element.text = name
        self._element.set("publicId", public_id)
        self._element.set("systemId", system_id)
        self.public_id = public_id
        self.system_id = system_id


class Document(Element):
    def __init__(self):
        Element.__init__(self, "DOCUMENT_ROOT")


class DocumentFragment(Element):
    def __init__(self):
        Element.__init__(self, "DOCUMENT_FRAGMENT")


class TreeBuilder:
    """Tree builder."""

    def __init__(self, namespace_html_elements):
        """Create a TreeBuilder.

        If namespace_html_elements is True, namespace HTML elements.

        """
        if namespace_html_elements:
            self.default_namespace = "http://www.w3.org/1999/xhtml"
        else:
            self.default_namespace = None
        self.reset()

    def reset(self):
        self.open_elements = []
        self.active_formatting_elements = ActiveFormattingElements()

        self.head_element = None
        self.form_element = None

        self.insert_from_table = False

        self.document = Document()

    def element_in_scope(self, target, variant=None):
        # If we pass a node in we match that. If we pass a string
        # match any node with that name.
        exact_node = hasattr(target, "name_tuple")
        if not exact_node:
            if isinstance(target, str):
                target = (_html, target)
            assert isinstance(target, tuple)

        list_elements, invert = _list_elements[variant]

        for node in reversed(self.open_elements):
            if exact_node and node == target:
                return True
            elif not exact_node and node.name_tuple == target:
                return True
            elif (invert ^ (node.name_tuple in list_elements)):
                return False

        # We should never reach this point.
        assert False  # pragma: no cover

    def reconstruct_active_formatting_elements(self):
        # Within this algorithm the order of steps described in the
        # specification is not quite the same as the order of steps in the
        # code. It should still do the same though.

        # Step 1: stop the algorithm when there's nothing to do.
        if not self.active_formatting_elements:
            return

        # Step 2 and step 3: we start with the last element. So i is -1.
        i = len(self.active_formatting_elements) - 1
        entry = self.active_formatting_elements[i]
        if entry is Marker or entry in self.open_elements:
            return

        # Step 6.
        while entry is not Marker and entry not in self.open_elements:
            if i == 0:
                # This will be reset to 0 below.
                i = -1
                break
            i -= 1
            # Step 5: let entry be one earlier in the list.
            entry = self.active_formatting_elements[i]

        while True:
            # Step 7.
            i += 1

            # Step 8.
            entry = self.active_formatting_elements[i]
            clone = entry.clone()  # mainly to get a new copy of the attributes

            # Step 9.
            element = self.insert_element({
                "type": "StartTag",
                "name": clone.name,
                "namespace": clone.namespace,
                "data": clone.attributes,
            })

            # Step 10.
            self.active_formatting_elements[i] = element

            # Step 11.
            if element == self.active_formatting_elements[-1]:
                break

    def clear_active_formatting_elements(self):
        entry = self.active_formatting_elements.pop()
        while self.active_formatting_elements and entry is not Marker:
            entry = self.active_formatting_elements.pop()

    def element_in_active_formatting_elements(self, name):
        """Find name between end of active formatting elements and last marker.

        If an element with this name exists, return it. Else return False.

        """
        for item in self.active_formatting_elements[::-1]:
            # Check for Marker first because if it's a Marker it doesn't have a
            # name attribute.
            if item is Marker:
                break
            elif item.name == name:
                return item
        return False

    def insert_root(self, token):
        element = self.create_element(token)
        self.open_elements.append(element)
        self.document.append_child(element)

    def insert_doctype(self, token):
        name = token["name"]
        public_id = token["publicId"]
        system_id = token["systemId"]

        doctype = DocumentType(name, public_id, system_id)
        self.document.append_child(doctype)

    def insert_comment(self, token, parent):
        parent.append_child(Comment(token["data"]))

    def create_element(self, token):
        """Create an element but don't insert it anywhere."""
        name = token["name"]
        namespace = token.get("namespace", self.default_namespace)
        element = Element(name, namespace)
        element.attributes = token["data"]
        return element

    def _get_insert_from_table(self):
        return self._insert_from_table

    def _set_insert_from_table(self, value):
        """Switch the function used to insert an element."""
        self._insert_from_table = value
        if value:
            self.insert_element = self.insert_element_table
        else:
            self.insert_element = self.insert_element_normal

    insert_from_table = property(_get_insert_from_table, _set_insert_from_table)

    def insert_element_normal(self, token):
        name = token["name"]
        assert isinstance(name, str), f"Element {name} not unicode"
        namespace = token.get("namespace", self.default_namespace)
        element = Element(name, namespace)
        element.attributes = token["data"]
        self.open_elements[-1].append_child(element)
        self.open_elements.append(element)
        return element

    def insert_element_table(self, token):
        """Create an element and insert it into the tree."""
        element = self.create_element(token)
        if self.open_elements[-1].name not in table_insert_mode_elements:
            return self.insert_element_normal(token)
        else:
            # We should be in the InTable mode. This means we want to do
            # special magic element rearranging.
            parent, insert_before = self.get_table_misnested_node_position()
            if insert_before is None:
                parent.append_child(element)
            else:
                parent.insert_before(element, insert_before)
            self.open_elements.append(element)
        return element

    def insert_text(self, data, parent=None):
        """Insert text data."""
        if parent is None:
            parent = self.open_elements[-1]

        in_table = (
            self.insert_from_table and
            self.open_elements[-1].name in table_insert_mode_elements)
        if in_table:
            # We should be in the InTable mode. This means we want to do
            # special magic element rearranging.
            parent, insert_before = self.get_table_misnested_node_position()
            parent.insert_text(data, insert_before)
        else:
            parent.insert_text(data)

    def get_table_misnested_node_position(self):
        """Get foster parent element and sibling (or None) to insert before."""

        # The foster parent element is the one which comes before the most
        # recently opened table element.

        # XXX - this is really inelegant.
        last_table = None
        foster_parent = None
        insert_before = None
        for element in self.open_elements[::-1]:
            if element.name == "table":
                last_table = element
                break
        if last_table:
            # XXX - we should really check that this parent is actually a
            # node here.
            if last_table.parent:
                foster_parent = last_table.parent
                insert_before = last_table
            else:
                index = self.open_elements.index(last_table) - 1
                foster_parent = self.open_elements[index]
        else:
            foster_parent = self.open_elements[0]
        return foster_parent, insert_before

    def generate_implied_end_tags(self, exclude=None):
        name = self.open_elements[-1].name
        if name in _implied_end_tags and name != exclude:
            self.open_elements.pop()
            # XXX This is not entirely what the specification says. We should
            # investigate it more closely.
            self.generate_implied_end_tags(exclude)

    def get_document(self, full_tree=False):
        """Return the final tree."""
        if full_tree:
            return self.document._element
        return self.document._element.find(
            "html" if self.default_namespace is None else
            f"{{{self.default_namespace}}}html")

    def get_fragment(self):
        """Return the final fragment."""
        fragment = DocumentFragment()
        self.open_elements[0].reparent_children(fragment)
        return fragment._element
