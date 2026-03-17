#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import unittest
import weakref

from py23_utils import *

from llist import dllist, dllistnode, sllistnode


class testdllist(unittest.TestCase):

    def test_init_empty(self):
        ll = dllist()
        self.assertEqual(len(ll), 0)
        self.assertEqual(ll.size, 0)
        self.assertEqual(list(ll), [])

    def test_init_with_sequence(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        self.assertEqual(len(ll), len(ref))
        self.assertEqual(ll.size, len(ref))
        self.assertEqual(list(ll), ref)

    def test_init_with_non_sequence(self):
        self.assertRaises(TypeError, dllist, None);
        self.assertRaises(TypeError, dllist, 1);
        self.assertRaises(TypeError, dllist, 1.5);

    def test_str(self):
        a = dllist([])
        self.assertEqual(str(a), 'dllist()')
        b = dllist([None, 1, 'abc'])
        self.assertEqual(str(b), 'dllist([None, 1, abc])')

    def test_repr(self):
        a = dllist([])
        self.assertEqual(repr(a), 'dllist()')
        b = dllist([None, 1, 'abc'])
        self.assertEqual(repr(b), 'dllist([None, 1, \'abc\'])')

    def test_node_str(self):
        a = dllist([None, None]).first
        self.assertEqual(str(a), 'dllistnode(None)')
        b = dllist([1, None]).first
        self.assertEqual(str(b), 'dllistnode(1)')
        c = dllist(['abc', None]).first
        self.assertEqual(str(c), 'dllistnode(abc)')

    def test_node_repr(self):
        a = dllist([None]).first
        self.assertEqual(repr(a), '<dllistnode(None)>')
        b = dllist([1, None]).first
        self.assertEqual(repr(b), '<dllistnode(1)>')
        c = dllist(['abc', None]).first
        self.assertEqual(repr(c), '<dllistnode(\'abc\')>')

    def test_str_recursive_list(self):
        ll = dllist()
        ll.append(dllistnode(ll))
        self.assertEqual(str(ll), 'dllist([dllist(<...>)])')

    def test_str_recursive_node(self):
        ll = self.make_recursive_node_list()
        self.assertEqual(str(ll), 'dllist([dllistnode(dllistnode(<...>))])')

    def test_repr_recursive_list(self):
        ll = dllist()
        ll.append(dllistnode(ll))
        self.assertEqual(repr(ll), 'dllist([dllist(<...>)])')

    def test_repr_recursive_node(self):
        ll = self.make_recursive_node_list()
        self.assertEqual(repr(ll), 'dllist([<dllistnode(dllistnode(<...>))>])')

    def make_recursive_node_list(self):
        ll = dllist()
        node = dllistnode()
        node.value = node
        ll.append(node)
        return ll

    def test_cmp(self):
        a = dllist(py23_xrange(0, 1100))
        b = dllist(py23_xrange(0, 1101))
        c = dllist([1, 2, 3, 4])
        d = dllist([1, 2, 3, 5])
        e = dllist([1, 0, 0, 0])
        f = dllist([0, 0, 0, 0])
        self.assertEqual(cmp(a, a), 0)
        self.assertEqual(cmp(a, b), -1)
        self.assertEqual(cmp(b, a), 1)
        self.assertEqual(cmp(c, d), -1)
        self.assertEqual(cmp(d, c), 1)
        self.assertEqual(cmp(e, f), 1)
        self.assertEqual(cmp(f, e), -1)

    def test_cmp_nonlist(self):
        a = dllist(py23_xrange(0, 1100))
        b = [py23_xrange(0, 1100)]
        if sys.hexversion < 0x03000000:
            # actual order is not specified by language
            self.assertNotEqual(cmp(a, b), 0)
            self.assertNotEqual(cmp(b, a), 0)
            self.assertNotEqual(cmp([], a), 0)
            self.assertNotEqual(cmp(a, []), 0)

    def test_eq(self):
        a = dllist(py23_xrange(0, 1100))
        b = dllist(py23_xrange(0, 1101))
        c = dllist([1, 2, 3, 4])
        d = dllist([1, 2, 3, 5])
        e = dllist([1, 0, 0, 0])
        f = dllist([0, 0, 0, 0])
        self.assertTrue(dllist() == dllist())
        self.assertTrue(a == a)
        self.assertFalse(dllist() == a)
        self.assertFalse(a == dllist())
        self.assertFalse(a == b)
        self.assertFalse(b == a)
        self.assertFalse(c == d)
        self.assertFalse(d == c)
        self.assertFalse(e == f)
        self.assertFalse(f == e)

    def test_ne(self):
        a = dllist(py23_xrange(0, 1100))
        b = dllist(py23_xrange(0, 1101))
        c = dllist([1, 2, 3, 4])
        d = dllist([1, 2, 3, 5])
        e = dllist([1, 0, 0, 0])
        f = dllist([0, 0, 0, 0])
        self.assertFalse(dllist() != dllist())
        self.assertFalse(a != a)
        self.assertTrue(dllist() != a)
        self.assertTrue(a != dllist())
        self.assertTrue(a != b)
        self.assertTrue(b != a)
        self.assertTrue(c != d)
        self.assertTrue(d != c)
        self.assertTrue(e != f)
        self.assertTrue(f != e)

    def test_lt(self):
        a = dllist(py23_xrange(0, 1100))
        b = dllist(py23_xrange(0, 1101))
        c = dllist([1, 2, 3, 4])
        d = dllist([1, 2, 3, 5])
        e = dllist([1, 0, 0, 0])
        f = dllist([0, 0, 0, 0])
        self.assertFalse(dllist() < dllist())
        self.assertFalse(a < a)
        self.assertTrue(dllist() < a)
        self.assertFalse(a < dllist())
        self.assertTrue(a < b)
        self.assertFalse(b < a)
        self.assertTrue(c < d)
        self.assertFalse(d < c)
        self.assertFalse(e < f)
        self.assertTrue(f < e)

    def test_gt(self):
        a = dllist(py23_xrange(0, 1100))
        b = dllist(py23_xrange(0, 1101))
        c = dllist([1, 2, 3, 4])
        d = dllist([1, 2, 3, 5])
        e = dllist([1, 0, 0, 0])
        f = dllist([0, 0, 0, 0])
        self.assertFalse(dllist() > dllist())
        self.assertFalse(a > a)
        self.assertFalse(dllist() > a)
        self.assertTrue(a > dllist())
        self.assertFalse(a > b)
        self.assertTrue(b > a)
        self.assertFalse(c > d)
        self.assertTrue(d > c)
        self.assertTrue(e > f)
        self.assertFalse(f > e)

    def test_le(self):
        a = dllist(py23_xrange(0, 1100))
        b = dllist(py23_xrange(0, 1101))
        c = dllist([1, 2, 3, 4])
        d = dllist([1, 2, 3, 5])
        e = dllist([1, 0, 0, 0])
        f = dllist([0, 0, 0, 0])
        self.assertTrue(dllist() <= dllist())
        self.assertTrue(a <= a)
        self.assertTrue(dllist() <= a)
        self.assertFalse(a <= dllist())
        self.assertTrue(a <= b)
        self.assertFalse(b <= a)
        self.assertTrue(c <= d)
        self.assertFalse(d <= c)
        self.assertFalse(e <= f)
        self.assertTrue(f <= e)

    def test_ge(self):
        a = dllist(py23_xrange(0, 1100))
        b = dllist(py23_xrange(0, 1101))
        c = dllist([1, 2, 3, 4])
        d = dllist([1, 2, 3, 5])
        e = dllist([1, 0, 0, 0])
        f = dllist([0, 0, 0, 0])
        self.assertTrue(dllist() >= dllist())
        self.assertTrue(a >= a)
        self.assertFalse(dllist() >= a)
        self.assertTrue(a >= dllist())
        self.assertFalse(a >= b)
        self.assertTrue(b >= a)
        self.assertFalse(c >= d)
        self.assertTrue(d >= c)
        self.assertTrue(e >= f)
        self.assertFalse(f >= e)

    def test_nodeat(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        for idx in py23_xrange(len(ll)):
            self.assertTrue(isinstance(ll.nodeat(idx), dllistnode))
            self.assertEqual(ll.nodeat(idx).value, ref[idx])
        for idx in py23_xrange(len(ll)):
            self.assertTrue(isinstance(ll.nodeat(idx), dllistnode))
            self.assertEqual(ll.nodeat(-idx - 1).value, ref[-idx - 1])
        self.assertRaises(TypeError, ll.nodeat, None)
        self.assertRaises(TypeError, ll.nodeat, 'abc')
        self.assertRaises(IndexError, ll.nodeat, len(ref))
        self.assertRaises(IndexError, ll.nodeat, -len(ref) - 1)

    def test_nodeat_empty(self):
        ll = dllist()
        self.assertRaises(TypeError, ll.nodeat, None)
        self.assertRaises(TypeError, ll.nodeat, 'abc')
        self.assertRaises(IndexError, ll.nodeat, 0)
        self.assertRaises(IndexError, ll.nodeat, -1)

    def test_iter(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        idx = 0
        for val in ll:
            self.assertNotIsInstance(val, dllistnode)
            self.assertEqual(val, ref[idx])
            idx += 1
        self.assertEqual(idx, len(ref))

    def test_iter_on_iterator_returns_same_object(self):
        ll = dllist([0, 1, 2, 3])
        first_iter = iter(ll)
        second_iter = iter(first_iter)
        self.assertIsNotNone(second_iter)
        self.assertIs(first_iter, second_iter)

    def test_iter_with_empty_list(self):
        ll = dllist()
        count = 0
        for val in ll:
            count += 1
        self.assertEqual(count, 0)

    def test_iter_with_appended_node(self):
        ll = dllist(['initial item'])

        appended_item_visited = False

        for x in ll:
            if x == 'initial item':
                ll.append('new item')
            elif x == 'new item':
                appended_item_visited = True

        self.assertTrue(appended_item_visited)

    def test_iter_with_removed_node(self):
        ll = dllist(['x', 'removed item'])

        for x in ll:
            self.assertNotEqual(x, 'removed item')
            ll.remove(ll.last)

    def test_itervalues(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        idx = 0
        for val in ll.itervalues():
            self.assertNotIsInstance(val, dllistnode)
            self.assertEqual(val, ref[idx])
            idx += 1
        self.assertEqual(idx, len(ref))

    def test_iter_on_itervalues_iterator_returns_same_object(self):
        ll = dllist([0, 1, 2, 3])
        first_iter = ll.itervalues()
        second_iter = iter(first_iter)
        self.assertIsNotNone(second_iter)
        self.assertIs(first_iter, second_iter)

    def test_itervalues_with_empty_list(self):
        ll = dllist()
        count = 0
        for val in ll.itervalues():
            count += 1
        self.assertEqual(count, 0)

    def test_itervalues_with_appended_node(self):
        ll = dllist(['initial item'])

        appended_item_visited = False

        for x in ll.itervalues():
            if x == 'initial item':
                ll.append('new item')
            elif x == 'new item':
                appended_item_visited = True

        self.assertTrue(appended_item_visited)

    def test_itervalues_with_removed_node(self):
        ll = dllist(['x', 'removed item'])

        for x in ll.itervalues():
            self.assertNotEqual(x, 'removed item')
            ll.remove(ll.last)

    def test_iternodes(self):
        ref = list(py23_range(0, 1024, 4))
        ll = dllist(ref)
        idx = 0
        for node in ll.iternodes():
            self.assertIsInstance(node, dllistnode)
            self.assertIs(node, ll.nodeat(idx))
            idx += 1
        self.assertEqual(idx, len(ref))

    def test_iter_on_iternodes_iterator_returns_same_object(self):
        ll = dllist([0, 1, 2, 3])
        first_iter = ll.iternodes()
        second_iter = iter(first_iter)
        self.assertIsNotNone(second_iter)
        self.assertIs(first_iter, second_iter)

    def test_iternodes_with_empty_list(self):
        ll = dllist()
        count = 0
        for node in ll.iternodes():
            count += 1
        self.assertEqual(count, 0)

    def test_iternodes_with_appended_node(self):
        ll = dllist(['initial item'])

        appended_node = None
        appended_node_visited = False

        for node in ll.iternodes():
            if node == ll.first:
                appended_node = ll.append('new item')
            elif node is not None and node is appended_node:
                appended_node_visited = True

        self.assertTrue(appended_node_visited)

    def test_iternodes_with_removed_node(self):
        ll = dllist(['x', 'removed item'])
        removed_node = ll.last

        for node in ll.iternodes():
            self.assertNotEqual(node, removed_node)
            ll.remove(removed_node)

    def test_reversed(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        idx = len(ref) - 1
        for val in reversed(ll):
            self.assertFalse(isinstance(val, dllistnode))
            self.assertEqual(val, ref[idx])
            idx -= 1
        self.assertEqual(idx, -1)

    def test_reversed_empty(self):
        ll = dllist()
        count = 0
        for val in reversed(ll):
            count += 1
        self.assertEqual(count, 0)

    def test_insert_value(self):
        ll = dllist(py23_xrange(4))
        ref = dllist([0, 1, 2, 3, 10])
        prev = ll.nodeat(-1)
        arg_node = dllistnode(10)
        new_node = ll.insert(arg_node)
        self.assertNotEqual(new_node, arg_node)
        self.assertEqual(new_node.value, 10)
        self.assertEqual(new_node.prev, prev)
        self.assertEqual(new_node.next, None)
        self.assertEqual(prev.next, new_node)
        self.assertEqual(new_node, ll.last)
        self.assertEqual(ll, ref)

    def test_insert_value_before(self):
        ll = dllist(py23_xrange(4))
        ref = dllist([0, 1, 10, 2, 3])
        prev = ll.nodeat(1)
        next = ll.nodeat(2)
        arg_node = dllistnode(10)
        new_node = ll.insert(arg_node, ll.nodeat(2))
        self.assertNotEqual(new_node, arg_node)
        self.assertEqual(new_node.value, 10)
        self.assertEqual(new_node.prev, prev)
        self.assertEqual(new_node.next, next)
        self.assertEqual(prev.next, new_node)
        self.assertEqual(next.prev, new_node)
        self.assertEqual(ll, ref)

    def test_insert_value_before_first(self):
        ll = dllist(py23_xrange(4))
        ref = dllist([10, 0, 1, 2, 3])
        next = ll.nodeat(0)
        arg_node = dllistnode(10)
        new_node = ll.insert(arg_node, ll.nodeat(0))
        self.assertNotEqual(new_node, arg_node)
        self.assertEqual(new_node.value, 10)
        self.assertEqual(new_node.prev, None)
        self.assertEqual(new_node.next, next)
        self.assertEqual(next.prev, new_node)
        self.assertEqual(new_node, ll.first)
        self.assertEqual(ll, ref)

    def test_insert_invalid_ref(self):
        ll = dllist()
        self.assertRaises(TypeError, ll.insert, 10, 1)
        self.assertRaises(TypeError, ll.insert, 10, 'abc')
        self.assertRaises(TypeError, ll.insert, 10, [])
        self.assertRaises(ValueError, ll.insert, 10, dllistnode())

    def test_insertbefore_without_ref_node(self):
        ll = dllist()
        self.assertRaises(TypeError, ll.insertbefore, 1234)

    def test_insertbefore_with_invalid_ref_node(self):
        ll = dllist()
        other_list = dllist(['node in other list'])
        self.assertRaises(TypeError, ll.insertbefore, 1234, None)
        self.assertRaises(TypeError, ll.insertbefore, 1234, 'not a dllist node')
        self.assertRaises(ValueError, ll.insertbefore, 1234, dllistnode())
        self.assertRaises(ValueError, ll.insertbefore, 1234, other_list.first)

    def test_insertbefore_adds_item_in_correct_position(self):
        ll = dllist([0, 1, 2, 3, 4])
        ref_node = ll.nodeat(2)
        ll.insertbefore(1234, ref_node)
        self.assertEqual(ll, dllist([0, 1, 1234, 2, 3, 4]))

    def test_insertbefore_returns_node_with_inserted_value(self):
        ll = dllist([0])
        value = 'inserted value'
        new_node = ll.insertbefore(value, ll.first)
        self.assertIs(new_node.value, value)

    def test_insertbefore_extracts_value_from_inserted_node(self):
        ll = dllist([0])
        free_node = dllistnode('free node')
        other_list = dllist(['node in other list'])
        new_node = ll.insertbefore(free_node, ll.first)
        self.assertIsNot(new_node, free_node)
        self.assertIs(new_node.value, free_node.value)
        new_node = ll.insertbefore(other_list.first, ll.first)
        self.assertIsNot(new_node, other_list.first)
        self.assertIs(new_node.value, other_list.first.value)

    def test_insertbefore_correctly_links_items(self):
        ll = dllist([0, 1, 2, 3])
        prev_node = ll.nodeat(1)
        next_node = ll.nodeat(2)
        new_node = ll.insertbefore(1234, next_node)
        self.assertIs(new_node.prev, prev_node)
        self.assertIs(prev_node.next, new_node)
        self.assertIs(new_node.next, next_node)
        self.assertIs(next_node.prev, new_node)

    def test_insertbefore_before_first_item_makes_node_without_prev(self):
        ll = dllist([0])
        new_node = ll.insertbefore(1234, ll.first)
        self.assertIs(new_node.prev, None)

    def test_insertbefore_before_first_item_updates_list_head(self):
        ll = dllist([0])
        original_head = ll.first
        new_node = ll.insertbefore(1234, ll.first)
        self.assertIs(ll.first, new_node)

    def test_insertbefore_after_first_item_does_not_update_list_head(self):
        ll = dllist([0, 1])
        original_head = ll.first
        ll.insertbefore(1234, ll.last)
        self.assertIs(ll.first, original_head)

    def test_insertbefore_does_not_update_list_tail(self):
        ll = dllist([0])
        original_tail = ll.last
        ll.insertbefore(1234, ll.last)
        self.assertIs(ll.last, original_tail)

    def test_insertbefore_updates_list_length(self):
        ll = dllist([0])
        self.assertEqual(len(ll), 1)
        ll.insertbefore(1234, ll.first)
        self.assertEqual(len(ll), 2)

    def test_insertafter_without_ref_node(self):
        ll = dllist()
        self.assertRaises(TypeError, ll.insertafter, 1234)

    def test_insertafter_with_invalid_ref_node(self):
        ll = dllist()
        other_list = dllist(['node in other list'])
        self.assertRaises(TypeError, ll.insertafter, 1234, None)
        self.assertRaises(TypeError, ll.insertafter, 1234, 'not a dllist node')
        self.assertRaises(ValueError, ll.insertafter, 1234, dllistnode())
        self.assertRaises(ValueError, ll.insertafter, 1234, other_list.first)

    def test_insertafter_adds_item_in_correct_position(self):
        ll = dllist([0, 1, 2, 3, 4])
        ref_node = ll.nodeat(2)
        ll.insertafter(1234, ref_node)
        self.assertEqual(ll, dllist([0, 1, 2, 1234, 3, 4]))

    def test_insertafter_returns_node_with_inserted_value(self):
        ll = dllist([0])
        value = 'inserted value'
        new_node = ll.insertafter(value, ll.first)
        self.assertIs(new_node.value, value)

    def test_insertafter_extracts_value_from_inserted_node(self):
        ll = dllist([0])
        free_node = dllistnode('free node')
        other_list = dllist(['node in other list'])
        new_node = ll.insertafter(free_node, ll.first)
        self.assertIsNot(new_node, free_node)
        self.assertIs(new_node.value, free_node.value)
        new_node = ll.insertafter(other_list.first, ll.first)
        self.assertIsNot(new_node, other_list.first)
        self.assertIs(new_node.value, other_list.first.value)

    def test_insertafter_correctly_links_items(self):
        ll = dllist([0, 1, 2, 3])
        prev_node = ll.nodeat(1)
        next_node = ll.nodeat(2)
        new_node = ll.insertafter(1234, prev_node)
        self.assertIs(new_node.prev, prev_node)
        self.assertIs(prev_node.next, new_node)
        self.assertIs(new_node.next, next_node)
        self.assertIs(next_node.prev, new_node)

    def test_insertafter_after_last_item_makes_node_without_next(self):
        ll = dllist([0])
        new_node = ll.insertafter(1234, ll.last)
        self.assertIs(new_node.next, None)

    def test_insertafter_after_last_item_updates_list_tail(self):
        ll = dllist([0])
        original_tail = ll.last
        new_node = ll.insertafter(1234, ll.last)
        self.assertIs(ll.last, new_node)

    def test_insertafter_before_last_item_does_not_update_list_tail(self):
        ll = dllist([0, 1])
        original_tail = ll.last
        ll.insertafter(1234, ll.first)
        self.assertIs(ll.last, original_tail)

    def test_insertafter_does_not_update_list_head(self):
        ll = dllist([0])
        original_head = ll.first
        ll.insertafter(1234, ll.first)
        self.assertIs(ll.first, original_head)

    def test_insertafter_updates_list_length(self):
        ll = dllist([0])
        self.assertEqual(len(ll), 1)
        ll.insertafter(1234, ll.first)
        self.assertEqual(len(ll), 2)

    def test_insert_node(self):
        ll = dllist(py23_xrange(4))
        ref = dllist([0, 1, 2, 3, 10])
        prev = ll.nodeat(-1)
        arg_node = dllistnode(10)
        new_node = ll.insertnode(arg_node)
        self.assertEqual(new_node, arg_node)
        self.assertEqual(new_node.value, 10)
        self.assertEqual(new_node.prev, prev)
        self.assertEqual(new_node.next, None)
        self.assertEqual(prev.next, new_node)
        self.assertEqual(new_node, ll.last)
        self.assertEqual(ll, ref)

    def test_insert_node_value_before(self):
        ll = dllist(py23_xrange(4))
        ref = dllist([0, 1, 10, 2, 3])
        prev = ll.nodeat(1)
        next = ll.nodeat(2)
        arg_node = dllistnode(10)
        new_node = ll.insertnode(arg_node, ll.nodeat(2))
        self.assertEqual(new_node, arg_node)
        self.assertEqual(new_node.value, 10)
        self.assertEqual(new_node.prev, prev)
        self.assertEqual(new_node.next, next)
        self.assertEqual(prev.next, new_node)
        self.assertEqual(next.prev, new_node)
        self.assertEqual(ll, ref)

    def test_insert_node_value_before_first(self):
        ll = dllist(py23_xrange(4))
        ref = dllist([10, 0, 1, 2, 3])
        next = ll.nodeat(0)
        arg_node = dllistnode(10)
        new_node = ll.insertnode(arg_node, ll.nodeat(0))
        self.assertEqual(new_node, arg_node)
        self.assertEqual(new_node.value, 10)
        self.assertEqual(new_node.prev, None)
        self.assertEqual(new_node.next, next)
        self.assertEqual(next.prev, new_node)
        self.assertEqual(new_node, ll.first)
        self.assertEqual(ll, ref)

    def test_insert_node_with_bad_argument_type(self):
        ll = dllist([1234])
        self.assertRaises(
            TypeError, ll.insertnode, 'non-node argument', ll.first)

    def test_insert_node_with_already_owned_node(self):
        ll = dllist([1234])
        other_list = dllist([5678])
        self.assertRaises(
            ValueError, ll.insertnode, other_list.first, ll.first)

    def test_insert_node_with_invalid_ref(self):
        ll = dllist()
        self.assertRaises(TypeError, ll.insertnode, dllistnode(10), 1)
        self.assertRaises(TypeError, ll.insertnode, dllistnode(10), 'abc')
        self.assertRaises(TypeError, ll.insertnode, dllistnode(10), [])
        self.assertRaises(
            ValueError, ll.insertnode, dllistnode(10), dllistnode())

    def test_insert_node_refcount_update(self):
        ll = dllist([1234])
        node = ll.insertnode(dllistnode(5678), ll.nodeat(0))
        self.assertGreaterEqual(sys.getrefcount(node), 3)

    def test_insertnodebefore_with_invalid_type_of_inserted_node(self):
        ll = dllist([0])
        self.assertRaises(
            TypeError, ll.insertnodebefore, None, ll.first)
        self.assertRaises(
            TypeError, ll.insertnodebefore, 'non-node argument', ll.first)
        self.assertRaises(
            TypeError, ll.insertnodebefore, sllistnode(1234), ll.first)

    def test_insertnodebefore_with_already_owned_inserted_node(self):
        ll = dllist([0])
        other_list = dllist([0])
        self.assertRaises(ValueError, ll.insertnodebefore, ll.first, ll.first)
        self.assertRaises(
            ValueError, ll.insertnodebefore, other_list.first, ll.first)

    def test_insertnodebefore_without_ref_node(self):
        ll = dllist([0])
        self.assertRaises(TypeError, ll.insertnodebefore, dllistnode(1234))

    def test_insertnodebefore_with_invalid_type_of_ref_node(self):
        ll = dllist([0])
        self.assertRaises(
            TypeError, ll.insertnodebefore, dllistnode(1234), None)
        self.assertRaises(
            TypeError, ll.insertnodebefore, dllistnode(1234), 'not a dllist node')
        self.assertRaises(
            TypeError, ll.insertnodebefore, dllistnode(1234), sllistnode(1234))

    def test_insertnodebefore_with_unowned_ref_node(self):
        ll = dllist([0])
        self.assertRaises(
            ValueError, ll.insertnodebefore, dllistnode(1234), dllistnode())

    def test_insertnodebefore_with_ref_node_from_different_list(self):
        ll = dllist([0])
        other_list = dllist(['node in other list'])
        self.assertRaises(
            ValueError, ll.insertnodebefore, dllistnode(1234), other_list.first)

    def test_insertnodebefore_adds_node_in_correct_position(self):
        ll = dllist([0, 1, 2, 3, 4])
        inserted_node = dllistnode(1234)
        ref_node = ll.nodeat(2)
        ll.insertnodebefore(inserted_node, ref_node)
        self.assertIs(ll.nodeat(2), inserted_node)
        self.assertIs(ll.nodeat(2).value, inserted_node.value)
        self.assertEqual(ll, dllist([0, 1, 1234, 2, 3, 4]))

    def test_insertnodebefore_returns_inserted_node(self):
        ll = dllist([0])
        inserted_value = 'inserted value'
        inserted_node = dllistnode(inserted_value)
        returned_node = ll.insertnodebefore(inserted_node, ll.first)
        self.assertIs(returned_node, inserted_node)
        self.assertIs(returned_node.value, inserted_value)

    def test_insertnodebefore_correctly_links_items(self):
        ll = dllist([0, 1, 2, 3])
        prev_node = ll.nodeat(1)
        next_node = ll.nodeat(2)
        inserted_node = dllistnode(1234)
        ll.insertnodebefore(inserted_node, next_node)
        self.assertIs(inserted_node.prev, prev_node)
        self.assertIs(prev_node.next, inserted_node)
        self.assertIs(inserted_node.next, next_node)
        self.assertIs(next_node.prev, inserted_node)

    def test_insertnodebefore_sets_prev_in_inserted_node_to_none_if_it_becomes_head(self):
        ll = dllist([0])
        inserted_node = dllistnode(1234)
        ll.insertnodebefore(inserted_node, ll.first)
        self.assertIs(inserted_node.prev, None)

    def test_insertnodebefore_updates_list_head_if_inserted_node_becomes_head(self):
        ll = dllist([0])
        inserted_node = dllistnode(1234)
        new_node = ll.insertnodebefore(inserted_node, ll.first)
        self.assertIs(ll.first, inserted_node)

    def test_insertnodebefore_after_first_item_does_not_update_list_head(self):
        ll = dllist([0, 1])
        original_head = ll.first
        inserted_node = dllistnode(1234)
        ll.insertnodebefore(inserted_node, ll.last)
        self.assertIs(ll.first, original_head)

    def test_insertnodebefore_does_not_update_list_tail(self):
        ll = dllist([0])
        original_tail = ll.last
        inserted_node = dllistnode(1234)
        ll.insertnodebefore(inserted_node, ll.last)
        self.assertIs(ll.last, original_tail)

    def test_insertnodebefore_updates_list_length(self):
        ll = dllist([0])
        self.assertEqual(len(ll), 1)
        ll.insertnodebefore(dllistnode(1234), ll.first)
        self.assertEqual(len(ll), 2)

    def test_insertnodeafter_with_invalid_type_of_inserted_node(self):
        ll = dllist([0])
        self.assertRaises(
            TypeError, ll.insertnodeafter, None, ll.first)
        self.assertRaises(
            TypeError, ll.insertnodeafter, 'non-node argument', ll.first)
        self.assertRaises(
            TypeError, ll.insertnodeafter, sllistnode(1234), ll.first)

    def test_insertnodeafter_with_already_owned_inserted_node(self):
        ll = dllist([0])
        other_list = dllist([0])
        self.assertRaises(ValueError, ll.insertnodeafter, ll.first, ll.first)
        self.assertRaises(
            ValueError, ll.insertnodeafter, other_list.first, ll.first)

    def test_insertnodeafter_without_ref_node(self):
        ll = dllist([0])
        self.assertRaises(TypeError, ll.insertnodeafter, dllistnode(1234))

    def test_insertnodeafter_with_invalid_type_of_ref_node(self):
        ll = dllist([0])
        self.assertRaises(TypeError, ll.insertnodeafter, dllistnode(1234), None)
        self.assertRaises(
            TypeError, ll.insertnodeafter, dllistnode(1234), 'not a dllist node')
        self.assertRaises(
            TypeError, ll.insertnodeafter, dllistnode(1234), sllistnode(1234))

    def test_insertnodeafter_with_unowned_ref_node(self):
        ll = dllist([0])
        self.assertRaises(
            ValueError, ll.insertnodeafter, dllistnode(1234), dllistnode())

    def test_insertnodeafter_with_ref_node_from_different_list(self):
        ll = dllist([0])
        other_list = dllist(['node in other list'])
        self.assertRaises(
            ValueError, ll.insertnodeafter, dllistnode(1234), other_list.first)

    def test_insertnodeafter_adds_node_in_correct_position(self):
        ll = dllist([0, 1, 2, 3, 4])
        inserted_node = dllistnode(1234)
        ref_node = ll.nodeat(2)
        ll.insertnodeafter(inserted_node, ref_node)
        self.assertIs(ll.nodeat(3), inserted_node)
        self.assertIs(ll.nodeat(3).value, inserted_node.value)
        self.assertEqual(ll, dllist([0, 1, 2, 1234, 3, 4]))

    def test_insertnodeafter_returns_inserted_node(self):
        ll = dllist([0])
        inserted_value = 'inserted value'
        inserted_node = dllistnode(inserted_value)
        returned_node = ll.insertnodeafter(inserted_node, ll.first)
        self.assertIs(returned_node, inserted_node)
        self.assertIs(returned_node.value, inserted_value)

    def test_insertnodeafter_correctly_links_items(self):
        ll = dllist([0, 1, 2, 3])
        prev_node = ll.nodeat(1)
        next_node = ll.nodeat(2)
        inserted_node = dllistnode(1234)
        ll.insertnodeafter(inserted_node, prev_node)
        self.assertIs(inserted_node.prev, prev_node)
        self.assertIs(prev_node.next, inserted_node)
        self.assertIs(inserted_node.next, next_node)
        self.assertIs(next_node.prev, inserted_node)

    def test_insertnodeafter_sets_next_in_inserted_node_to_none_if_it_becomes_tail(self):
        ll = dllist([0])
        inserted_node = dllistnode(1234)
        ll.insertnodeafter(inserted_node, ll.last)
        self.assertIs(inserted_node.next, None)

    def test_insertnodeafter_updates_list_tail_if_inserted_node_becomes_tail(self):
        ll = dllist([0])
        inserted_node = dllistnode(1234)
        new_node = ll.insertnodeafter(inserted_node, ll.last)
        self.assertIs(ll.last, inserted_node)

    def test_insertnodeafter_before_last_item_does_not_update_list_tail(self):
        ll = dllist([0, 1])
        original_tail = ll.last
        inserted_node = dllistnode(1234)
        ll.insertnodeafter(inserted_node, ll.first)
        self.assertIs(ll.last, original_tail)

    def test_insertnodeafter_does_not_update_list_head(self):
        ll = dllist([0])
        original_head = ll.first
        inserted_node = dllistnode(1234)
        ll.insertnodeafter(inserted_node, ll.first)
        self.assertIs(ll.first, original_head)

    def test_insertnodeafter_updates_list_length(self):
        ll = dllist([0])
        self.assertEqual(len(ll), 1)
        ll.insertnodeafter(dllistnode(1234), ll.first)
        self.assertEqual(len(ll), 2)

    def test_append(self):
        ll = dllist(py23_xrange(4))
        ref = dllist([0, 1, 2, 3, 10])
        prev = ll.nodeat(-1)
        arg_node = dllistnode(10)
        new_node = ll.append(arg_node)
        self.assertNotEqual(new_node, arg_node)
        self.assertEqual(new_node.value, 10);
        self.assertEqual(new_node.prev, prev)
        self.assertEqual(new_node.next, None)
        self.assertEqual(prev.next, new_node)
        self.assertEqual(ll.last, new_node)
        self.assertEqual(ll, ref)

    def test_appendleft(self):
        ll = dllist(py23_xrange(4))
        ref = dllist([10, 0, 1, 2, 3])
        next = ll.nodeat(0)
        arg_node = dllistnode(10)
        new_node = ll.appendleft(arg_node)
        self.assertNotEqual(new_node, arg_node)
        self.assertEqual(new_node.value, 10);
        self.assertEqual(new_node.prev, None)
        self.assertEqual(new_node.next, next)
        self.assertEqual(next.prev, new_node)
        self.assertEqual(ll.first, new_node)
        self.assertEqual(ll, ref)

    def test_appendright(self):
        ll = dllist(py23_xrange(4))
        ref = dllist([0, 1, 2, 3, 10])
        prev = ll.nodeat(-1)
        arg_node = dllistnode(10)
        new_node = ll.appendright(arg_node)
        self.assertNotEqual(new_node, arg_node)
        self.assertEqual(new_node.value, 10);
        self.assertEqual(new_node.prev, prev)
        self.assertEqual(new_node.next, None)
        self.assertEqual(prev.next, new_node)
        self.assertEqual(ll.last, new_node)
        self.assertEqual(ll, ref)

    def test_appendnode(self):
        ll = dllist([1, 2, 3, 4])
        node = dllistnode(5)
        ll.appendnode(node)
        self.assertEqual([1, 2, 3, 4, 5], list(ll))
        self.assertIs(node, ll.last)

    def test_appendnode_with_bad_argument_type(self):
        ll = dllist()
        self.assertRaises(TypeError, ll.appendnode, 'non-node argument')

    def test_appendnode_with_already_owned_node(self):
        ll = dllist()
        other_list = dllist([1234])
        self.assertRaises(ValueError, ll.appendnode, other_list.first)

    def test_appendnode_refcount_update(self):
        ll = dllist()
        node = ll.appendnode(dllistnode(1234))
        self.assertGreaterEqual(sys.getrefcount(node), 3)

    def test_extend(self):
        a_ref = py23_range(0, 1024, 4)
        b_ref = py23_range(8092, 8092 + 1024, 4)
        b = dllist(b_ref)
        ab_ref = dllist(a_ref + b_ref)
        a = dllist(a_ref)
        a.extend(b)
        self.assertEqual(a, ab_ref)
        self.assertEqual(len(a), len(ab_ref))
        a = dllist(a_ref)
        a.extend(b_ref)
        self.assertEqual(a, ab_ref)
        self.assertEqual(len(a), len(ab_ref))
        a = dllist(a_ref)
        a.extend(a)
        self.assertEqual(a, dllist(a_ref + a_ref))
        self.assertEqual(len(a), len(a_ref) * 2)

    def test_extend_empty(self):
        filled_ref = py23_range(0, 1024, 4)
        filled = dllist(filled_ref)
        empty = dllist()
        empty.extend(empty)
        self.assertEqual(empty, dllist([] + []))
        self.assertEqual(len(empty), 0)
        empty = dllist()
        empty.extend(filled)
        self.assertEqual(empty, dllist([] + filled_ref))
        self.assertEqual(len(empty), len(filled_ref))
        empty = dllist()
        filled.extend(empty)
        self.assertEqual(filled, dllist(filled_ref + []))
        self.assertEqual(len(filled), len(filled_ref))

    def test_extendleft(self):
        a_ref = py23_range(0, 1024, 4)
        b_ref = py23_range(8092, 8092 + 1024, 4)
        b = dllist(b_ref)
        ab_ref = dllist(list(reversed(b_ref)) + a_ref)
        a = dllist(a_ref)
        a.extendleft(b)
        self.assertEqual(a, ab_ref)
        self.assertEqual(len(a), len(ab_ref))
        a = dllist(a_ref)
        a.extendleft(b_ref)
        self.assertEqual(a, ab_ref)
        self.assertEqual(len(a), len(ab_ref))
        a = dllist(a_ref)
        a.extendleft(a)
        self.assertEqual(a, dllist(list(reversed(a_ref)) + a_ref))
        self.assertEqual(len(a), len(a_ref) * 2)

    def test_extendleft_empty(self):
        filled_ref = py23_range(0, 1024, 4)
        filled = dllist(filled_ref)
        empty = dllist()
        empty.extendleft(empty)
        self.assertEqual(empty, dllist([] + []))
        self.assertEqual(len(empty), 0)
        empty = dllist()
        empty.extendleft(filled)
        self.assertEqual(empty, dllist(list(reversed(filled_ref)) + []))
        self.assertEqual(len(empty), len(filled_ref))
        empty = dllist()
        filled.extendleft(empty)
        self.assertEqual(filled, dllist(list(reversed([])) + filled_ref))
        self.assertEqual(len(filled), len(filled_ref))

    def test_extendright(self):
        a_ref = py23_range(0, 1024, 4)
        b_ref = py23_range(8092, 8092 + 1024, 4)
        b = dllist(b_ref)
        ab_ref = dllist(a_ref + b_ref)
        a = dllist(a_ref)
        a.extendright(b)
        self.assertEqual(a, ab_ref)
        self.assertEqual(len(a), len(ab_ref))
        a = dllist(a_ref)
        a.extendright(b_ref)
        self.assertEqual(a, ab_ref)
        self.assertEqual(len(a), len(ab_ref))
        a = dllist(a_ref)
        a.extendright(a)
        self.assertEqual(a, dllist(a_ref + a_ref))
        self.assertEqual(len(a), len(a_ref) * 2)

    def test_extendright_empty(self):
        filled_ref = py23_range(0, 1024, 4)
        filled = dllist(filled_ref)
        empty = dllist()
        empty.extendright(empty)
        self.assertEqual(empty, dllist([] + []))
        self.assertEqual(len(empty), 0)
        empty = dllist()
        empty.extendright(filled)
        self.assertEqual(empty, dllist([] + filled_ref))
        self.assertEqual(len(empty), len(filled_ref))
        empty = dllist()
        filled.extendright(empty)
        self.assertEqual(filled, dllist(filled_ref + []))
        self.assertEqual(len(filled), len(filled_ref))

    def test_clear_empty(self):
        empty_list = dllist()
        empty_list.clear()
        self.assertEqual(empty_list.first, None)
        self.assertEqual(empty_list.last, None)
        self.assertEqual(empty_list.size, 0)
        self.assertEqual(list(empty_list), [])

    def test_clear(self):
        ll = dllist(py23_xrange(0, 1024, 4))
        del_node = ll.nodeat(4)
        ll.clear()
        self.assertEqual(ll.first, None)
        self.assertEqual(ll.last, None)
        self.assertEqual(ll.size, 0)
        self.assertEqual(list(ll), [])
        self.assertEqual(del_node.prev, None)
        self.assertEqual(del_node.next, None)

    def test_pop(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        result = ll.pop();
        self.assertEqual(result, ref[-1])
        self.assertEqual(len(ll), len(ref) - 1)
        self.assertEqual(ll.size, len(ref) - 1)
        self.assertEqual(ll.last.value, ref[-2])
        self.assertEqual(list(ll), ref[:-1])

    def test_node_after_pop(self):
        ll = dllist([1, 2])
        del_node = ll.last
        ll.pop()
        self.assertIs(del_node.prev, None)
        self.assertIs(del_node.next, None)
        self.assertIs(del_node.owner, None)

    def test_popleft(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        result = ll.popleft()
        self.assertEqual(result, ref[0])
        self.assertEqual(len(ll), len(ref) - 1)
        self.assertEqual(ll.size, len(ref) - 1)
        self.assertEqual(ll.first.value, ref[1])
        self.assertEqual(list(ll), ref[1:])

    def test_node_after_popleft(self):
        ll = dllist([1, 2])
        del_node = ll.first
        ll.popleft()
        self.assertIs(del_node.prev, None)
        self.assertIs(del_node.next, None)
        self.assertIs(del_node.owner, None)

    def test_popright(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        result = ll.popright()
        self.assertEqual(result, ref[-1])
        self.assertEqual(len(ll), len(ref) - 1)
        self.assertEqual(ll.size, len(ref) - 1)
        self.assertEqual(ll.last.value, ref[-2])
        self.assertEqual(list(ll), ref[:-1])

    def test_node_after_popright(self):
        ll = dllist([1, 2])
        del_node = ll.last
        ll.popright()
        self.assertIs(del_node.prev, None)
        self.assertIs(del_node.next, None)
        self.assertIs(del_node.owner, None)

    def test_pop_from_empty_list(self):
        ll = dllist()
        self.assertRaises(ValueError, ll.pop)
        self.assertRaises(ValueError, ll.popleft)
        self.assertRaises(ValueError, ll.popright)

    def test_remove(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        prev_node = ll.nodeat(3)
        del_node = ll.nodeat(4)
        next_node = ll.nodeat(5)
        result = ll.remove(del_node)
        ref_result = ref[4]
        del ref[4]
        self.assertEqual(list(ll), ref)
        self.assertEqual(result, ref_result)
        self.assertEqual(len(ll), len(ref))
        self.assertEqual(ll.size, len(ref))
        self.assertEqual(prev_node.next, next_node)
        self.assertEqual(next_node.prev, prev_node)

    def test_remove_after_remove(self):
        ll = dllist([1, 2, 3])
        del_node = ll.nodeat(1)
        ll.remove(del_node)
        self.assertIs(del_node.prev, None)
        self.assertIs(del_node.next, None)
        self.assertIs(del_node.owner, None)

    def test_remove_from_empty_list(self):
        ll = dllist()
        self.assertRaises(ValueError, ll.remove, dllistnode())

    def test_remove_invalid_node(self):
        ll = dllist([1, 2, 3, 4])
        self.assertRaises(ValueError, ll.remove, dllistnode())
        self.assertEqual(len(ll), 4)

    def test_remove_already_deleted_node(self):
        ll = dllist([1, 2, 3, 4])
        node = ll.nodeat(2)
        ll.remove(node)
        self.assertRaises(ValueError, ll.remove, node)

    def test_rotate_left(self):
        for n in py23_xrange(128):
            ref = py23_range(32)
            split = n % len(ref)
            ref_result = ref[split:] + ref[:split]
            ll = dllist(ref)
            new_first = ll.nodeat(split)
            new_last = ll.nodeat(split - 1)
            # touch future middle element to initialize cache
            cached_idx = (len(ll) // 2 + n) % len(ll)
            ll[cached_idx]
            ll.rotate(-n)
            self.assertEqual(list(ll), ref_result)
            self.assertEqual(ll.first, new_first)
            self.assertEqual(ll.last, new_last)
            self.assertEqual(ll.size, len(ref))
            self.assertEqual(ll.first.prev, None)
            self.assertEqual(ll.first.next.prev, ll.first)
            self.assertEqual(ll.last.next, None)
            self.assertEqual(ll.last.prev.next, ll.last)
            # check if cached index is updated correctly
            self.assertEqual(ll[len(ll) // 2], ref_result[len(ref_result) // 2])

    def test_rotate_right(self):
        for n in py23_xrange(128):
            ref = py23_range(32)
            split = n % len(ref)
            ref_result = ref[-split:] + ref[:-split]
            ll = dllist(ref)
            new_first = ll.nodeat(-split)
            last_idx = -split - 1
            new_last = ll.nodeat(last_idx)
            # touch future middle element to initialize cache
            cached_idx = len(ll) - (len(ll) // 2 + n) % len(ll) - 1
            ll[cached_idx]
            ll.rotate(n)
            self.assertEqual(list(ll), ref_result)
            self.assertEqual(ll.first, new_first)
            self.assertEqual(ll.last, new_last)
            self.assertEqual(ll.size, len(ref))
            self.assertEqual(ll.first.prev, None)
            self.assertEqual(ll.first.next.prev, ll.first)
            self.assertEqual(ll.last.next, None)
            self.assertEqual(ll.last.prev.next, ll.last)
            # check if cached index is updated correctly
            self.assertEqual(ll[len(ll) // 2], ref_result[len(ref_result) // 2])

    def test_rotate_left_empty(self):
        for n in py23_xrange(4):
            ll = dllist()
            ll.rotate(-n)
            self.assertEqual(ll.first, None)
            self.assertEqual(ll.last, None)
            self.assertEqual(ll.size, 0)

    def test_rotate_right_empty(self):
        for n in py23_xrange(4):
            ll = dllist()
            ll.rotate(n)
            self.assertEqual(ll.first, None)
            self.assertEqual(ll.last, None)
            self.assertEqual(ll.size, 0)

    def test_getitem(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        for idx in py23_xrange(len(ll)):
            self.assertFalse(isinstance(ll[idx], dllistnode))
            self.assertEqual(ll[idx], ref[idx])
        for idx in py23_xrange(len(ll)):
            self.assertFalse(isinstance(ll[idx], dllistnode))
            self.assertEqual(ll[-idx - 1], ref[-idx - 1])
        self.assertRaises(TypeError, ll.__getitem__, None)
        self.assertRaises(TypeError, ll.__getitem__, 'abc')
        self.assertRaises(IndexError, ll.__getitem__, len(ref))
        self.assertRaises(IndexError, ll.__getitem__, -len(ref) - 1)

    def test_getitem_empty(self):
        ll = dllist()
        self.assertRaises(TypeError, ll.__getitem__, None)
        self.assertRaises(TypeError, ll.__getitem__, 'abc')
        self.assertRaises(IndexError, ll.__getitem__, 0)
        self.assertRaises(IndexError, ll.__getitem__, -1)

    def test_del(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        del ll[0]
        del ref[0]
        self.assertEqual(list(ll), ref)
        del ll[len(ll) - 1]
        del ref[len(ref) - 1]
        self.assertEqual(list(ll), ref)
        del ll[(len(ll) - 1) // 2]
        del ref[(len(ref) - 1) // 2]
        self.assertEqual(list(ll), ref)

        def del_item(idx):
            del ll[idx]
        self.assertRaises(IndexError, del_item, len(ll))

        for i in py23_xrange(len(ll)):
            del ll[0]
        self.assertEqual(len(ll), 0)

    def test_node_after_del(self):
        ll = dllist([1, 2, 3])
        del_node = ll.nodeat(1)
        del ll[1]
        self.assertIs(del_node.prev, None)
        self.assertIs(del_node.next, None)
        self.assertIs(del_node.owner, None)

    def test_concat(self):
        a_ref = py23_range(0, 1024, 4)
        a = dllist(a_ref)
        b_ref = py23_range(8092, 8092 + 1024, 4)
        b = dllist(b_ref)
        ab_ref = dllist(a_ref + b_ref)
        c = a + b
        self.assertEqual(c, ab_ref)
        self.assertEqual(len(c), len(ab_ref))
        c = a + b_ref
        self.assertEqual(c, ab_ref)
        self.assertEqual(len(c), len(a_ref) * 2)

    def test_concat_empty(self):
        empty = dllist()
        filled_ref = py23_range(0, 1024, 4)
        filled = dllist(filled_ref)
        res = empty + empty
        self.assertEqual(res, dllist([] + []))
        self.assertEqual(len(res), 0)
        res = empty + filled
        self.assertEqual(res, dllist([] + filled_ref))
        self.assertEqual(len(res), len(filled_ref))
        res = filled + empty
        self.assertEqual(res, dllist(filled_ref + []))
        self.assertEqual(len(res), len(filled_ref))

    def test_concat_inplace(self):
        a_ref = py23_range(0, 1024, 4)
        b_ref = py23_range(8092, 8092 + 1024, 4)
        b = dllist(b_ref)
        ab_ref = dllist(a_ref + b_ref)
        a = dllist(a_ref)
        a += b
        self.assertEqual(a, ab_ref)
        self.assertEqual(len(a), len(ab_ref))
        a = dllist(a_ref)
        a += b_ref
        self.assertEqual(a, ab_ref)
        self.assertEqual(len(a), len(ab_ref))
        a = dllist(a_ref)
        a += a
        self.assertEqual(a, dllist(a_ref + a_ref))
        self.assertEqual(len(a), len(a_ref) * 2)

    def test_concat_inplace_empty(self):
        filled_ref = py23_range(0, 1024, 4)
        filled = dllist(filled_ref)
        empty = dllist()
        empty += empty
        self.assertEqual(empty, dllist([] + []))
        self.assertEqual(len(empty), 0)
        empty = dllist()
        empty += filled
        self.assertEqual(empty, dllist([] + filled_ref))
        self.assertEqual(len(empty), len(filled_ref))
        empty = dllist()
        filled += empty
        self.assertEqual(filled, dllist(filled_ref + []))
        self.assertEqual(len(filled), len(filled_ref))

    def test_repeat(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        self.assertEqual(ll * 4, dllist(ref * 4))

    def test_repeat_empty(self):
        ll = dllist()
        self.assertEqual(ll * 4, dllist([] * 4))

    def test_repeat_inplace(self):
        ref = py23_range(0, 1024, 4)
        ll = dllist(ref)
        ll *= 4
        self.assertEqual(ll, dllist(ref * 4))

    def test_repeat_inplace_empty(self):
        ll = dllist()
        ll *= 4
        self.assertEqual(ll, dllist([] * 4))

    def test_list_readonly_attributes(self):
        if sys.hexversion >= 0x03000000:
            expected_error = AttributeError
        else:
            expected_error = TypeError

        ll = dllist(py23_range(4))
        self.assertRaises(expected_error, setattr, ll, 'first', None)
        self.assertRaises(expected_error, setattr, ll, 'last', None)
        self.assertRaises(expected_error, setattr, ll, 'size', None)

    def test_node_readonly_attributes(self):
        if sys.hexversion >= 0x03000000:
            expected_error = AttributeError
        else:
            expected_error = TypeError

        ll = dllistnode()
        self.assertRaises(expected_error, setattr, ll, 'prev', None)
        self.assertRaises(expected_error, setattr, ll, 'next', None)

    def test_node_owner(self):
        ll = dllist([1234])
        owner_ref = ll.first.owner
        self.assertIsInstance(owner_ref, weakref.ref)
        self.assertIs(owner_ref(), ll)
        del ll
        self.assertIsNone(owner_ref())

    def test_list_hash(self):
        self.assertEqual(hash(dllist()), hash(dllist()))
        self.assertEqual(hash(dllist(py23_range(0, 1024, 4))),
            hash(dllist(py23_range(0, 1024, 4))))
        self.assertEqual(hash(dllist([0, 2])), hash(dllist([0.0, 2.0])))
        self.assertNotEqual(hash(dllist([1, 2])), hash(dllist([2, 1])))

    def test_list_can_be_subclassed(self):
        class DerivedList(dllist):
            pass

    def test_list_node_can_be_subclassed(self):
        class DerivedNode(dllistnode):
            pass

    def test_cyclic_list_destruction_does_not_release_extra_None_refs(self):
        original_ref_count = sys.getrefcount(None)

        for _ in range(original_ref_count * 10):
            ll = dllist()
            ll.append(dllistnode(ll))
            del ll

        self.assertGreater(sys.getrefcount(None), 0)

    def test_cyclic_node_destruction_does_not_release_extra_None_refs(self):
        original_ref_count = sys.getrefcount(None)

        for _ in range(original_ref_count * 10):
            ll = self.make_recursive_node_list()
            del ll

        self.assertGreater(sys.getrefcount(None), 0)
