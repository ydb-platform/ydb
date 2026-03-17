# pylint: disable=missing-docstring

import re
from unittest import TestCase
from os import path
from tree_sitter import Language, Parser

PYTHON = Language("grammars", "python")

class TestParser(TestCase):
    def test_set_language(self):
        parser = Parser()
        parser.set_language(PYTHON)
        tree = parser.parse(b"def foo():\n  bar()")
        self.assertEqual(
            tree.root_node.sexp(),
            trim(
                """(module (function_definition
                name: (identifier)
                parameters: (parameters)
                body: (block (expression_statement (call
                    function: (identifier)
                    arguments: (argument_list))))))"""
            ),
        )

    def test_read_callback(self):
        parser = Parser()
        parser.set_language(PYTHON)
        source_lines = ["def foo():\n", "  bar()"]

        def read_callback(byte_offset, point):
            row, column = point
            if row >= len(source_lines):
                return None
            if column >= len(source_lines[row]):
                return None
            return source_lines[row][column:].encode("utf8")

        tree = parser.parse(read_callback)
        self.assertEqual(
            tree.root_node.sexp(),
            trim(
                """(module (function_definition
                name: (identifier)
                parameters: (parameters)
                body: (block (expression_statement (call
                    function: (identifier)
                    arguments: (argument_list))))))"""
            ),
        )


class TestNode(TestCase):
    def test_child_by_field_id(self):
        parser = Parser()
        parser.set_language(PYTHON)
        tree = parser.parse(b"def foo():\n  bar()")
        root_node = tree.root_node
        fn_node = tree.root_node.children[0]

        self.assertEqual(PYTHON.field_id_for_name("nameasdf"), None)
        name_field = PYTHON.field_id_for_name("name")
        alias_field = PYTHON.field_id_for_name("alias")
        self.assertIsInstance(alias_field, int)
        self.assertIsInstance(name_field, int)
        self.assertEqual(root_node.child_by_field_id(alias_field), None)
        self.assertEqual(root_node.child_by_field_id(name_field), None)
        self.assertEqual(fn_node.child_by_field_id(alias_field), None)
        self.assertEqual(fn_node.child_by_field_id(name_field).type, "identifier")
        self.assertRaises(TypeError, root_node.child_by_field_id, "")
        self.assertRaises(TypeError, root_node.child_by_field_name, True)
        self.assertRaises(TypeError, root_node.child_by_field_name, 1)

        self.assertEqual(fn_node.child_by_field_name("name").type, "identifier")
        self.assertEqual(fn_node.child_by_field_name("asdfasdfname"), None)

        self.assertEqual(
            fn_node.child_by_field_name("name"),
            fn_node.child_by_field_name("name"),
        )

    def test_children(self):
        parser = Parser()
        parser.set_language(PYTHON)
        tree = parser.parse(b"def foo():\n  bar()")

        root_node = tree.root_node
        self.assertEqual(root_node.type, "module")
        self.assertEqual(root_node.start_byte, 0)
        self.assertEqual(root_node.end_byte, 18)
        self.assertEqual(root_node.start_point, (0, 0))
        self.assertEqual(root_node.end_point, (1, 7))

        # List object is reused
        self.assertIs(root_node.children, root_node.children)

        fn_node = root_node.children[0]
        self.assertEqual(fn_node.type, "function_definition")
        self.assertEqual(fn_node.start_byte, 0)
        self.assertEqual(fn_node.end_byte, 18)
        self.assertEqual(fn_node.start_point, (0, 0))
        self.assertEqual(fn_node.end_point, (1, 7))

        def_node = fn_node.children[0]
        self.assertEqual(def_node.type, "def")
        self.assertEqual(def_node.is_named, False)

        id_node = fn_node.children[1]
        self.assertEqual(id_node.type, "identifier")
        self.assertEqual(id_node.is_named, True)
        self.assertEqual(len(id_node.children), 0)

        params_node = fn_node.children[2]
        self.assertEqual(params_node.type, "parameters")
        self.assertEqual(params_node.is_named, True)

        colon_node = fn_node.children[3]
        self.assertEqual(colon_node.type, ":")
        self.assertEqual(colon_node.is_named, False)

        statement_node = fn_node.children[4]
        self.assertEqual(statement_node.type, "block")
        self.assertEqual(statement_node.is_named, True)

    def test_named_and_sibling_and_count_and_parent(self):
        parser = Parser()
        parser.set_language(PYTHON)
        tree = parser.parse(b"[1, 2, 3]")

        root_node = tree.root_node
        self.assertEqual(root_node.type, "module")
        self.assertEqual(root_node.start_byte, 0)
        self.assertEqual(root_node.end_byte, 9)
        self.assertEqual(root_node.start_point, (0, 0))
        self.assertEqual(root_node.end_point, (0, 9))

        exp_stmt_node = root_node.children[0]
        self.assertEqual(exp_stmt_node.type, "expression_statement")
        self.assertEqual(exp_stmt_node.start_byte, 0)
        self.assertEqual(exp_stmt_node.end_byte, 9)
        self.assertEqual(exp_stmt_node.start_point, (0, 0))
        self.assertEqual(exp_stmt_node.end_point, (0, 9))
        self.assertEqual(exp_stmt_node.parent,
                         root_node)

        list_node = exp_stmt_node.children[0]
        self.assertEqual(list_node.type, "list")
        self.assertEqual(list_node.start_byte, 0)
        self.assertEqual(list_node.end_byte, 9)
        self.assertEqual(list_node.start_point, (0, 0))
        self.assertEqual(list_node.end_point, (0, 9))
        self.assertEqual(list_node.parent,
                         exp_stmt_node)

        named_children = list_node.named_children

        open_delim_node = list_node.children[0]
        self.assertEqual(open_delim_node.type, "[")
        self.assertEqual(open_delim_node.start_byte, 0)
        self.assertEqual(open_delim_node.end_byte, 1)
        self.assertEqual(open_delim_node.start_point, (0, 0))
        self.assertEqual(open_delim_node.end_point, (0, 1))
        self.assertEqual(open_delim_node.parent,
                         list_node)

        first_num_node = list_node.children[1]
        self.assertEqual(first_num_node,
                         open_delim_node.next_named_sibling)
        self.assertEqual(first_num_node.parent,
                         list_node)
        self.assertEqual(named_children[0],
                         first_num_node)

        first_comma_node = list_node.children[2]
        self.assertEqual(first_comma_node,
                         first_num_node.next_sibling)
        self.assertEqual(first_num_node,
                         first_comma_node.prev_sibling)
        self.assertEqual(first_comma_node.parent,
                         list_node)

        second_num_node = list_node.children[3]
        self.assertEqual(second_num_node,
                         first_comma_node.next_sibling)
        self.assertEqual(second_num_node,
                         first_num_node.next_named_sibling)
        self.assertEqual(first_num_node,
                         second_num_node.prev_named_sibling)
        self.assertEqual(second_num_node.parent,
                         list_node)
        self.assertEqual(named_children[1],
                         second_num_node)

        second_comma_node = list_node.children[4]
        self.assertEqual(second_comma_node,
                         second_num_node.next_sibling)
        self.assertEqual(second_num_node,
                         second_comma_node.prev_sibling)
        self.assertEqual(second_comma_node.parent,
                         list_node)

        third_num_node = list_node.children[5]
        self.assertEqual(third_num_node,
                         second_comma_node.next_sibling)
        self.assertEqual(third_num_node,
                         second_num_node.next_named_sibling)
        self.assertEqual(second_num_node,
                         third_num_node.prev_named_sibling)
        self.assertEqual(third_num_node.parent,
                         list_node)
        self.assertEqual(named_children[2],
                         third_num_node)

        close_delim_node = list_node.children[6]
        self.assertEqual(close_delim_node.type, "]")
        self.assertEqual(close_delim_node.start_byte, 8)
        self.assertEqual(close_delim_node.end_byte, 9)
        self.assertEqual(close_delim_node.start_point, (0, 8))
        self.assertEqual(close_delim_node.end_point, (0, 9))
        self.assertEqual(close_delim_node,
                         third_num_node.next_sibling)
        self.assertEqual(third_num_node,
                         close_delim_node.prev_sibling)
        self.assertEqual(third_num_node,
                         close_delim_node.prev_named_sibling)
        self.assertEqual(close_delim_node.parent,
                         list_node)

        self.assertEqual(list_node.child_count, 7)
        self.assertEqual(list_node.named_child_count, 3)

    def test_node_text(self):
        parser = Parser()
        parser.set_language(PYTHON)
        tree = parser.parse(b"[0, [1, 2, 3]]")

        self.assertEqual(tree.text, b"[0, [1, 2, 3]]")

        root_node = tree.root_node
        self.assertEqual(root_node.text, b'[0, [1, 2, 3]]')

        exp_stmt_node = root_node.children[0]
        self.assertEqual(exp_stmt_node.text, b'[0, [1, 2, 3]]')

        list_node = exp_stmt_node.children[0]
        self.assertEqual(list_node.text, b'[0, [1, 2, 3]]')

        open_delim_node = list_node.children[0]
        self.assertEqual(open_delim_node.text, b'[')

        first_num_node = list_node.children[1]
        self.assertEqual(first_num_node.text, b'0')

        first_comma_node = list_node.children[2]
        self.assertEqual(first_comma_node.text, b',')

        child_list_node = list_node.children[3]
        self.assertEqual(child_list_node.text, b'[1, 2, 3]')

        close_delim_node = list_node.children[4]
        self.assertEqual(close_delim_node.text, b']')

        edit_offset = len(b"[0, [")
        tree.edit(
            start_byte=edit_offset,
            old_end_byte=edit_offset,
            new_end_byte=edit_offset + 2,
            start_point=(0, edit_offset),
            old_end_point=(0, edit_offset),
            new_end_point=(0, edit_offset + 2),
        )
        self.assertEqual(tree.text, None)

        root_node_again = tree.root_node
        self.assertEqual(root_node_again.text, None)

        tree_text_false = parser.parse(b"[0, [1, 2, 3]]", keep_text=False)
        self.assertIsNone(tree_text_false.text)
        root_node_text_false = tree_text_false.root_node
        self.assertIsNone(root_node_text_false.text)

        tree_text_true = parser.parse(b"[0, [1, 2, 3]]", keep_text=True)
        self.assertEqual(tree_text_true.text, b"[0, [1, 2, 3]]")
        root_node_text_true = tree_text_true.root_node
        self.assertEqual(root_node_text_true.text, b"[0, [1, 2, 3]]")

    def test_tree(self):
        code = b"def foo():\n  bar()\n\ndef foo():\n  bar()"
        parser = Parser()
        parser.set_language(PYTHON)

        def parse_root(bytes_):
            tree = parser.parse(bytes_)
            return tree.root_node

        root = parse_root(code)
        for item in root.children:
            self.assertIsNotNone(item.is_named)

        def parse_root_children(bytes_):
            tree = parser.parse(bytes_)
            return tree.root_node.children

        children = parse_root_children(code)
        for item in children:
            self.assertIsNotNone(item.is_named)


class TestTree(TestCase):
    def test_tree_cursor_without_tree(self):
        parser = Parser()
        parser.set_language(PYTHON)

        def parse():
            tree = parser.parse(b"def foo():\n  bar()")
            return tree.walk()

        cursor = parse()
        self.assertIs(cursor.node, cursor.node)
        for item in cursor.node.children:
            self.assertIsNotNone(item.is_named)

        cursor = cursor.copy()
        self.assertIs(cursor.node, cursor.node)
        for item in cursor.node.children:
            self.assertIsNotNone(item.is_named)

    def test_walk(self):
        parser = Parser()
        parser.set_language(PYTHON)
        tree = parser.parse(b"def foo():\n  bar()")
        cursor = tree.walk()

        # Node always returns the same instance
        self.assertIs(cursor.node, cursor.node)

        self.assertEqual(cursor.node.type, "module")
        self.assertEqual(cursor.node.start_byte, 0)
        self.assertEqual(cursor.node.end_byte, 18)
        self.assertEqual(cursor.node.start_point, (0, 0))
        self.assertEqual(cursor.node.end_point, (1, 7))
        self.assertEqual(cursor.current_field_name(), None)

        self.assertTrue(cursor.goto_first_child())
        self.assertEqual(cursor.node.type, "function_definition")
        self.assertEqual(cursor.node.start_byte, 0)
        self.assertEqual(cursor.node.end_byte, 18)
        self.assertEqual(cursor.node.start_point, (0, 0))
        self.assertEqual(cursor.node.end_point, (1, 7))
        self.assertEqual(cursor.current_field_name(), None)

        self.assertTrue(cursor.goto_first_child())
        self.assertEqual(cursor.node.type, "def")
        self.assertEqual(cursor.node.is_named, False)
        self.assertEqual(cursor.node.sexp(), '("def")')
        self.assertEqual(cursor.current_field_name(), None)
        def_node = cursor.node

        # Node remains cached after a failure to move
        self.assertFalse(cursor.goto_first_child())
        self.assertIs(cursor.node, def_node)

        self.assertTrue(cursor.goto_next_sibling())
        self.assertEqual(cursor.node.type, "identifier")
        self.assertEqual(cursor.node.is_named, True)
        self.assertEqual(cursor.current_field_name(), "name")
        self.assertFalse(cursor.goto_first_child())

        self.assertTrue(cursor.goto_next_sibling())
        self.assertEqual(cursor.node.type, "parameters")
        self.assertEqual(cursor.node.is_named, True)
        self.assertEqual(cursor.current_field_name(), "parameters")

    def test_edit(self):
        parser = Parser()
        parser.set_language(PYTHON)
        tree = parser.parse(b"def foo():\n  bar()")

        edit_offset = len(b"def foo(")
        tree.edit(
            start_byte=edit_offset,
            old_end_byte=edit_offset,
            new_end_byte=edit_offset + 2,
            start_point=(0, edit_offset),
            old_end_point=(0, edit_offset),
            new_end_point=(0, edit_offset + 2),
        )

        fn_node = tree.root_node.children[0]
        self.assertEqual(fn_node.type, "function_definition")
        self.assertTrue(fn_node.has_changes)
        self.assertFalse(fn_node.children[0].has_changes)
        self.assertFalse(fn_node.children[1].has_changes)
        self.assertFalse(fn_node.children[3].has_changes)

        params_node = fn_node.children[2]
        self.assertEqual(params_node.type, "parameters")
        self.assertTrue(params_node.has_changes)
        self.assertEqual(params_node.start_point, (0, edit_offset - 1))
        self.assertEqual(params_node.end_point, (0, edit_offset + 3))

        new_tree = parser.parse(b"def foo(ab):\n  bar()", tree)
        self.assertEqual(
            new_tree.root_node.sexp(),
            trim(
                """(module (function_definition
                name: (identifier)
                parameters: (parameters (identifier))
                body: (block
                    (expression_statement (call
                        function: (identifier)
                        arguments: (argument_list))))))"""
            ),
        )

    def test_get_changed_ranges(self):
        parser = Parser()
        parser.set_language(PYTHON)
        tree = parser.parse(b"def foo():\n  bar()")

        edit_offset = len(b"def foo(")
        tree.edit(
            start_byte=edit_offset,
            old_end_byte=edit_offset,
            new_end_byte=edit_offset + 2,
            start_point=(0, edit_offset),
            old_end_point=(0, edit_offset),
            new_end_point=(0, edit_offset + 2),
        )

        new_tree = parser.parse(b"def foo(ab):\n  bar()", tree)
        changed_ranges = tree.get_changed_ranges(new_tree)

        self.assertEqual(len(changed_ranges), 1)
        self.assertEqual(changed_ranges[0].start_byte, edit_offset)
        self.assertEqual(changed_ranges[0].start_point, (0, edit_offset))
        self.assertEqual(changed_ranges[0].end_byte, edit_offset + 2)
        self.assertEqual(changed_ranges[0].end_point, (0, edit_offset + 2))


class TestQuery(TestCase):
    def test_errors(self):
        with self.assertRaisesRegex(NameError, "Invalid node type foo"):
            PYTHON.query("(list (foo))")
        with self.assertRaisesRegex(NameError, "Invalid field name buzz"):
            PYTHON.query("(function_definition buzz: (identifier))")
        with self.assertRaisesRegex(NameError, "Invalid capture name garbage"):
            PYTHON.query("((function_definition) (eq? @garbage foo))")
        with self.assertRaisesRegex(SyntaxError, "Invalid syntax at offset 6"):
            PYTHON.query("(list))")
        PYTHON.query("(function_definition)")

    def test_captures(self):
        parser = Parser()
        parser.set_language(PYTHON)
        source = b"def foo():\n  bar()\ndef baz():\n  quux()\n"
        tree = parser.parse(source)
        query = PYTHON.query(
            """
            (function_definition name: (identifier) @func-def)
            (call function: (identifier) @func-call)
            """
        )

        captures = query.captures(tree.root_node)

        self.assertEqual(captures[0][0].start_point, (0, 4))
        self.assertEqual(captures[0][0].end_point, (0, 7))
        self.assertEqual(captures[0][1], "func-def")

        self.assertEqual(captures[1][0].start_point, (1, 2))
        self.assertEqual(captures[1][0].end_point, (1, 5))
        self.assertEqual(captures[1][1], "func-call")

        self.assertEqual(captures[2][0].start_point, (2, 4))
        self.assertEqual(captures[2][0].end_point, (2, 7))
        self.assertEqual(captures[2][1], "func-def")

        self.assertEqual(captures[3][0].start_point, (3, 2))
        self.assertEqual(captures[3][0].end_point, (3, 6))
        self.assertEqual(captures[3][1], "func-call")

    def test_byte_range_captures(self):
        parser = Parser()
        parser.set_language(PYTHON)
        source = b"def foo():\n  bar()\ndef baz():\n  quux()\n"
        tree = parser.parse(source)
        query = PYTHON.query(
            """
            (function_definition name: (identifier) @func-def)
            (call function: (identifier) @func-call)
            """
        )

        captures = query.captures(tree.root_node, start_byte=10, end_byte=20)
        self.assertEqual(captures[0][0].start_point, (1, 2))
        self.assertEqual(captures[0][0].end_point, (1, 5))
        self.assertEqual(captures[0][1], "func-call")

    def test_point_range_captures(self):
        parser = Parser()
        parser.set_language(PYTHON)
        source = b"def foo():\n  bar()\ndef baz():\n  quux()\n"
        tree = parser.parse(source)
        query = PYTHON.query(
            """
            (function_definition name: (identifier) @func-def)
            (call function: (identifier) @func-call)
            """
        )

        captures = query.captures(tree.root_node, start_point=(1, 0), end_point=(2, 0))
        # FIXME: this test is incorrect
        self.assertEqual(captures[1][0].start_point, (1, 2))
        self.assertEqual(captures[1][0].end_point, (1, 5))
        self.assertEqual(captures[1][1], "func-call")


def trim(string):
    return re.sub(r"\s+", " ", string).strip()
