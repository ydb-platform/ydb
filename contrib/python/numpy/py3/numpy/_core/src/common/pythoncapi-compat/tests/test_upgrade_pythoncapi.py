#!/usr/bin/env python3
import io
import os
import sys
import tempfile
import textwrap
import unittest

# Get upgrade_pythoncapi.py of the parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import upgrade_pythoncapi   # noqa


def operations(disable=None):
    if isinstance(disable, str):
        disable = (disable,)
    elif not disable:
        disable = ()
    operations = ["all"]
    for op in upgrade_pythoncapi.EXCLUDE_FROM_ALL:
        if op.NAME in disable:
            continue
        operations.append(op.NAME)
    for name in disable:
        operations.append(f'-{name}')
    operations = ','.join(operations)
    return operations


def patch(source, no_compat=False, disable=None):
    args = ['script', 'mod.c', '-o', operations(disable=disable)]
    if no_compat:
        args.append('--no-compat')

    patcher = upgrade_pythoncapi.Patcher(args)
    return patcher.patch(source)


def reformat(source):
    return textwrap.dedent(source).strip()


class Tests(unittest.TestCase):
    maxDiff = 80 * 30

    def _test_patch_file(self, tmp_dir):
        # test Patcher.patcher()
        source = """
            PyTypeObject*
            test_type(PyObject *obj, PyTypeObject *type)
            {
                Py_TYPE(obj) = type;
                return Py_TYPE(obj);
            }
        """
        expected = """
            #include "pythoncapi_compat.h"

            PyTypeObject*
            test_type(PyObject *obj, PyTypeObject *type)
            {
                Py_SET_TYPE(obj, type);
                return Py_TYPE(obj);
            }
        """
        source = reformat(source)
        expected = reformat(expected)

        filename = tempfile.mktemp(suffix='.c', dir=tmp_dir)
        old_filename = filename + ".old"
        try:
            with open(filename, "w", encoding="utf-8") as fp:
                fp.write(source)

            old_stderr = sys.stderr
            old_argv = list(sys.argv)
            try:
                # redirect stderr
                sys.stderr = io.StringIO()

                if tmp_dir is not None:
                    arg = tmp_dir
                else:
                    arg = filename
                sys.argv = ['script', arg]
                try:
                    upgrade_pythoncapi.Patcher().main()
                except SystemExit as exc:
                    self.assertEqual(exc.code, 0)
                else:
                    self.fail("SystemExit not raised")
            finally:
                sys.stderr = old_stderr
                sys.argv = old_argv

            with open(filename, encoding="utf-8") as fp:
                new_contents = fp.read()

            with open(old_filename, encoding="utf-8") as fp:
                old_contents = fp.read()
        finally:
            try:
                os.unlink(filename)
            except FileNotFoundError:
                pass
            try:
                os.unlink(old_filename)
            except FileNotFoundError:
                pass

        self.assertEqual(new_contents, expected)
        self.assertEqual(old_contents, source)

    def test_patch_file(self):
        self._test_patch_file(None)

    def test_patch_directory(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._test_patch_file(tmp_dir)

    def check_replace(self, source, expected, **kwargs):
        source = reformat(source)
        expected = reformat(expected)
        self.assertEqual(patch(source, **kwargs), expected)

    def check_dont_replace(self, source, disable=None):
        source = reformat(source)
        self.assertEqual(patch(source, disable=disable), source)

    def test_expr_regex(self):
        # Test EXPR_REGEX
        self.check_replace("a->b->ob_type", "Py_TYPE(a->b)")
        self.check_replace("a.b->ob_type", "Py_TYPE(a.b)")
        self.check_replace("array[2]->ob_type", "Py_TYPE(array[2])")

        # Don't match function calls
        self.check_dont_replace("func()->ob_type")

    def test_pythoncapi_compat(self):
        # If pythoncapi_compat.h is included, avoid compatibility includes
        # and macros.
        #
        # Otherise, Py_SET_TYPE() requires a macro and PyFrame_GetBack()
        # requires 2 macros and an include.
        HEADERS = (
            '<pythoncapi_compat.h>',
            '"pythoncapi_compat.h"',
        )
        for header in HEADERS:
            # There is no empty line between the include and the function
            # on purpose.
            self.check_replace("""
                #include %s
                void test_set_type(PyObject *obj, PyTypeObject *type)
                {
                    Py_TYPE(obj) = type;
                }
                PyFrameObject* frame_back_borrowed(PyFrameObject *frame)
                {
                    return frame->f_back;
                }
            """ % header, """
                #include %s
                void test_set_type(PyObject *obj, PyTypeObject *type)
                {
                    Py_SET_TYPE(obj, type);
                }
                PyFrameObject* frame_back_borrowed(PyFrameObject *frame)
                {
                    return _PyFrame_GetBackBorrow(frame);
                }
            """ % header)

    def test_py_type(self):
        source = """
            PyTypeObject* get_type(PyObject *obj)
            { return obj->ob_type; }
        """
        expected = """
            PyTypeObject* get_type(PyObject *obj)
            { return Py_TYPE(obj); }
        """
        self.check_replace(source, expected)

    def test_py_size(self):
        source = """
            Py_ssize_t get_size(PyVarObject *obj)
            { return obj->ob_size; }
        """
        expected = """
            Py_ssize_t get_size(PyVarObject *obj)
            { return Py_SIZE(obj); }
        """
        self.check_replace(source, expected)

    def test_py_refcnt(self):
        source = """
            Py_ssize_t get_refcnt(PyObject *obj)
            { return obj->ob_refcnt; }
        """
        expected = """
            Py_ssize_t get_refcnt(PyObject *obj)
            { return Py_REFCNT(obj); }
        """
        self.check_replace(source, expected)

    def test_py_set_type(self):
        source = """
            void test_type(PyObject *obj, PyTypeObject *type)
            {
                obj->ob_type = type;
                Py_TYPE(obj) = type;
            }
        """
        expected = """
            #include "pythoncapi_compat.h"

            void test_type(PyObject *obj, PyTypeObject *type)
            {
                Py_SET_TYPE(obj, type);
                Py_SET_TYPE(obj, type);
            }
        """
        self.check_replace(source, expected)

        self.check_dont_replace("""
            PyTypeObject* get_type(PyObject *obj, PyTypeObject *check_type)
            {
                assert(Py_TYPE(args) == check_type);
                return Py_TYPE(obj);
            }
        """)

    def test_py_set_size(self):
        source = """\
            void test_size(PyVarObject *obj)
            {
                obj->ob_size = 3;
                Py_SIZE(obj) = 4;
            }
        """
        expected = """\
            #include "pythoncapi_compat.h"

            void test_size(PyVarObject *obj)
            {
                Py_SET_SIZE(obj, 3);
                Py_SET_SIZE(obj, 4);
            }
        """
        self.check_replace(source, expected)

        self.check_dont_replace("""
            Py_ssize_t
            get_size(PyObject *obj)
            {
                assert(Py_SIZE(args) == 1);
                return Py_SIZE(obj);
            }
        """)

    def test_py_set_refcnt(self):
        source = """\
            void set_refcnt(PyObject *obj)
            {
                obj->ob_refcnt = 1;
                Py_REFCNT(obj) = 2;
            }
        """
        expected = """\
            #include "pythoncapi_compat.h"

            void set_refcnt(PyObject *obj)
            {
                Py_SET_REFCNT(obj, 1);
                Py_SET_REFCNT(obj, 2);
            }
        """
        self.check_replace(source, expected)

        self.check_dont_replace("""
            Py_ssize_t
            get_refcnt(PyObject *obj)
            {
                assert(Py_REFCNT(args) == 1);
                return Py_REFCNT(obj);
            }
        """)

    def test_pyobject_new(self):
        source = """\
            capsule = PyObject_NEW(PyCapsule, &PyCapsule_Type);
            pattern = PyObject_NEW_VAR(PatternObject, Pattern_Type, n);
        """
        expected = """\
            capsule = PyObject_New(PyCapsule, &PyCapsule_Type);
            pattern = PyObject_NewVar(PatternObject, Pattern_Type, n);
        """
        self.check_replace(source, expected)

        self.check_dont_replace("""
            func = PyObject_NEW;
            capsule2 = PyObject_NEW2(PyCapsule, &PyCapsule_Type);

            func2 = PyObject_NEW_VAR;
            pattern2 = PyObject_NEW_VAR2(PatternObject, Pattern_Type, n);
        """)

    def test_pymem_malloc(self):
        source = """\
            void *ptr = PyMem_MALLOC(10);
            ptr = PyMem_REALLOC(ptr, 20);
            PyMem_FREE(ptr);
            PyMem_DEL(ptr);
            PyMem_Del(ptr);
        """
        expected = """\
            void *ptr = PyMem_Malloc(10);
            ptr = PyMem_Realloc(ptr, 20);
            PyMem_Free(ptr);
            PyMem_Free(ptr);
            PyMem_Free(ptr);
        """
        self.check_replace(source, expected)

    def test_pyobject_malloc(self):
        source = """\
            void *ptr = PyObject_MALLOC(10);
            ptr = PyObject_REALLOC(ptr, 20);
            PyObject_FREE(ptr);
            PyObject_DEL(ptr);
            PyObject_Del(ptr);
        """
        expected = """\
            void *ptr = PyObject_Malloc(10);
            ptr = PyObject_Realloc(ptr, 20);
            PyObject_Free(ptr);
            PyObject_Free(ptr);
            PyObject_Free(ptr);
        """
        self.check_replace(source, expected)

    def test_pyframe_getback(self):
        source = """\
            PyFrameObject* frame_back_borrowed(PyFrameObject *frame)
            {
                return frame->f_back;
            }
        """
        expected = """\
            #include "pythoncapi_compat.h"

            PyFrameObject* frame_back_borrowed(PyFrameObject *frame)
            {
                return _PyFrame_GetBackBorrow(frame);
            }
        """
        self.check_replace(source, expected)

    def test_pyframe_getcode(self):
        self.check_replace("""\
            PyCodeObject* frame_code_borrowed(PyFrameObject *frame)
            {
                return frame->f_code;
            }
        """, """\
            #include "pythoncapi_compat.h"

            PyCodeObject* frame_code_borrowed(PyFrameObject *frame)
            {
                return _PyFrame_GetCodeBorrow(frame);
            }
        """)

    def test_get_member_regex(self):
        # Use PyFrame_GetCode() to test get_member_regex()
        self.check_dont_replace("""
            void frame_set_code(PyFrameObject *frame, PyCodeObject *code)
            {
                frame->f_code = code;
            }

            void frame_clear_code(PyFrameObject *frame)
            {
                Py_CLEAR(frame->f_code);
            }
        """)

    def test_pythreadstate_getinterpreter(self):
        self.check_replace("""
            PyInterpreterState* get_interp(PyThreadState *tstate)
            { return tstate->interp; }
        """, """
            #include "pythoncapi_compat.h"

            PyInterpreterState* get_interp(PyThreadState *tstate)
            { return PyThreadState_GetInterpreter(tstate); }
        """)

    def test_pythreadstate_getframe(self):
        self.check_replace("""
            PyFrameObject* get_frame(PyThreadState *tstate)
            { return tstate->frame; }
        """, """
            #include "pythoncapi_compat.h"

            PyFrameObject* get_frame(PyThreadState *tstate)
            { return _PyThreadState_GetFrameBorrow(tstate); }
        """)

    def test_py_newref_return(self):
        self.check_replace("""
            PyObject* new_ref(PyObject *obj) {
                Py_INCREF(obj);
                return obj;
            }

            PyObject* same_line(PyObject *obj) {
                Py_INCREF(obj); return obj;
            }

            PyObject* new_xref(PyObject *obj) {
                Py_XINCREF(obj);
                return obj;
            }

            PyObject* cast(PyLongObject *obj) {
                Py_XINCREF(obj);
                return (PyObject *)obj;
            }
        """, """
            #include "pythoncapi_compat.h"

            PyObject* new_ref(PyObject *obj) {
                return Py_NewRef(obj);
            }

            PyObject* same_line(PyObject *obj) {
                return Py_NewRef(obj);
            }

            PyObject* new_xref(PyObject *obj) {
                return Py_XNewRef(obj);
            }

            PyObject* cast(PyLongObject *obj) {
                return Py_XNewRef(obj);
            }
        """)

    def test_py_newref(self):
        # INCREF, assign
        self.check_replace("""
            void set_attr(MyStruct *obj, PyObject *value, int test)
            {
                // 1
                Py_INCREF(value);
                obj->attr = value;
                // 2
                obj->attr = value;
                Py_INCREF(value);
                // 3
                obj->attr = value;
                Py_INCREF(obj->attr);
            }
        """, """
            #include "pythoncapi_compat.h"

            void set_attr(MyStruct *obj, PyObject *value, int test)
            {
                // 1
                obj->attr = Py_NewRef(value);
                // 2
                obj->attr = Py_NewRef(value);
                // 3
                obj->attr = Py_NewRef(value);
            }
        """)

        # Same line
        self.check_replace("""
            void set_attr(MyStruct *obj, PyObject *value, int test)
            {
                // same line 1
                obj->attr = value; Py_INCREF(value);
                // same line 2
                if (test) { obj->attr = value; Py_INCREF(obj->attr); }
                // same line 3
                if (test) { Py_INCREF(value); obj->attr = value; }
            }
        """, """
            #include "pythoncapi_compat.h"

            void set_attr(MyStruct *obj, PyObject *value, int test)
            {
                // same line 1
                obj->attr = Py_NewRef(value);
                // same line 2
                if (test) { obj->attr = Py_NewRef(value); }
                // same line 3
                if (test) { obj->attr = Py_NewRef(value); }
            }
        """)

        # Cast
        self.check_replace("""
            void set_attr(MyStruct *obj, PyObject *value, int test)
            {
                // cast 1
                Py_INCREF(value);
                obj->attr = (PyObject*)value;
                // cast 2
                obj->attr = (PyObject*)value;
                Py_INCREF(value);

                // assign var, incref
                PyCodeObject *code_obj = (PyCodeObject *)code;
                Py_INCREF(code_obj);
                // assign var, incref
                PyCodeObject* code_obj = (PyCodeObject *)code;
                Py_INCREF(code);
                // assign var, xincref
                PyCodeObject * code_obj = (PyCodeObject *)code;
                Py_XINCREF(code_obj);

                // incref, assign var
                Py_INCREF(code);
                PyCodeObject* code_obj = (PyCodeObject *)code;
                // xincref, assign var
                Py_XINCREF(code);
                PyCodeObject *code_obj = (PyCodeObject *)code;
            }
        """, """
            #include "pythoncapi_compat.h"

            void set_attr(MyStruct *obj, PyObject *value, int test)
            {
                // cast 1
                obj->attr = Py_NewRef(value);
                // cast 2
                obj->attr = Py_NewRef(value);

                // assign var, incref
                PyCodeObject *code_obj = (PyCodeObject *)Py_NewRef(code);
                // assign var, incref
                PyCodeObject* code_obj = (PyCodeObject *)Py_NewRef(code);
                // assign var, xincref
                PyCodeObject * code_obj = (PyCodeObject *)Py_XNewRef(code);

                // incref, assign var
                PyCodeObject* code_obj = (PyCodeObject *)Py_NewRef(code);
                // xincref, assign var
                PyCodeObject *code_obj = (PyCodeObject *)Py_XNewRef(code);
            }
        """)

        # Py_XINCREF
        self.check_replace("""
            void set_xattr(MyStruct *obj, PyObject *value)
            {
                // 1
                Py_XINCREF(value);
                obj->attr = value;
                // 2
                obj->attr = value;
                Py_XINCREF(value);
                // 3
                obj->attr = value;
                Py_XINCREF(obj->attr);
            }
        """, """
            #include "pythoncapi_compat.h"

            void set_xattr(MyStruct *obj, PyObject *value)
            {
                // 1
                obj->attr = Py_XNewRef(value);
                // 2
                obj->attr = Py_XNewRef(value);
                // 3
                obj->attr = Py_XNewRef(value);
            }
        """)

        # the first Py_INCREF should be replaced before the second one,
        # otherwise the first Py_INCREF is not replaced.
        self.check_replace("""
            void set(void)
            {
                PyObject *x, *y;
                Py_INCREF(Py_None);
                x = Py_None;
                Py_INCREF(Py_None);
                x = Py_None;
                Py_DECREF(x);
                Py_DECREF(y);
            }
        """, """
            #include "pythoncapi_compat.h"

            void set(void)
            {
                PyObject *x, *y;
                x = Py_NewRef(Py_None);
                x = Py_NewRef(Py_None);
                Py_DECREF(x);
                Py_DECREF(y);
            }
        """)

        # Indentation matters for conditional code
        self.check_dont_replace("""
            void test1(int test)
            {
                PyObject *res;
                if (test)
                    res = Py_True;
                else
                    res = Py_False;
                Py_INCREF(res);

                Py_DECREF(res);
            }

            int test2(struct datetime* result, PyObject *tzinfo)
            {
                int res = 0;
                if (test)
                 res = 1;
                else
                 Py_INCREF(tzinfo);
                result->tzinfo = tzinfo;
                return res;
            }
        """)

    def test_py_clear(self):
        self.check_replace("""
            void clear(int test)
            {
                PyObject *obj;

                // two lines
                Py_XDECREF(obj);
                obj = NULL;

                // inside if
                if (test) { Py_XDECREF(obj); obj = NULL; }
            }
        """, """
            void clear(int test)
            {
                PyObject *obj;

                // two lines
                Py_CLEAR(obj);

                // inside if
                if (test) { Py_CLEAR(obj); }
            }
        """)

        # Don't replace Py_DECREF()
        self.check_dont_replace("""
            void dont_clear(void)
            {
                PyObject *obj;
                Py_DECREF(obj);
                obj = NULL;
            }
        """, disable="Py_SETREF")

    def test_py_setref(self):
        self.check_replace("""
            void set(PyObject **obj, PyObject *t)
            {
                // DECREF
                Py_DECREF(*obj);
                *obj = t;

                // XDECREF
                Py_XDECREF(*obj);
                *obj = t;

                // DECREF, INCREF
                Py_DECREF(*obj);
                Py_INCREF(t);
                *obj = t;
            }
        """, """
            #include "pythoncapi_compat.h"

            void set(PyObject **obj, PyObject *t)
            {
                // DECREF
                Py_SETREF(*obj, t);

                // XDECREF
                Py_XSETREF(*obj, t);

                // DECREF, INCREF
                Py_SETREF(*obj, Py_NewRef(t));
            }
        """)

        self.check_replace("""
            void set(PyObject **obj, PyObject *value)
            {
                // 1
                PyObject *old = *obj;
                *obj = value;
                Py_DECREF(old);
                // 2
                PyObject *old = *obj;
                *obj = Py_XNewRef(value);
                Py_DECREF(old);
                // 3
                PyObject *old = *obj;
                *obj = value;
                Py_XDECREF(old);
                // 4
                PyObject *old = *obj;
                *obj = Py_NewRef(value);
                Py_XDECREF(old);
            }
        """, """
            #include "pythoncapi_compat.h"

            void set(PyObject **obj, PyObject *value)
            {
                // 1
                Py_SETREF(*obj, value);
                // 2
                Py_SETREF(*obj, Py_XNewRef(value));
                // 3
                Py_XSETREF(*obj, value);
                // 4
                Py_XSETREF(*obj, Py_NewRef(value));
            }
        """)

        # INCREF, DECREF, assign
        self.check_replace("""
            void set(void)
            {
                // 1
                Py_INCREF(value);
                Py_DECREF(obj);
                obj = value;
                // 2
                Py_INCREF(value);
                Py_XDECREF(obj);
                obj = value;
                // 3
                Py_XINCREF(value);
                Py_DECREF(obj);
                obj = value;
                // 4
                Py_XINCREF(value);
                Py_XDECREF(obj);
                obj = value;
            }
        """, """
            #include "pythoncapi_compat.h"

            void set(void)
            {
                // 1
                Py_SETREF(obj, Py_NewRef(value));
                // 2
                Py_XSETREF(obj, Py_NewRef(value));
                // 3
                Py_SETREF(obj, Py_XNewRef(value));
                // 4
                Py_XSETREF(obj, Py_XNewRef(value));
            }
        """)

        # old variable
        self.check_replace("""
            void set(PyObject **obj, PyObject *value)
            {
                // 1
                PyObject *old_next = (PyObject*)self->tb_next;
                self->tb_next = (PyTracebackObject *)Py_XNewRef(new_next);
                Py_XDECREF(old_next);
                // 2
                old_next = (PyObject*)self->tb_next;
                self->tb_next = (PyTracebackObject *)Py_XNewRef(new_next);
                Py_XDECREF(old_next);
            }
        """, """
            #include "pythoncapi_compat.h"

            void set(PyObject **obj, PyObject *value)
            {
                // 1
                Py_XSETREF(self->tb_next, (PyTracebackObject *)Py_XNewRef(new_next));
                // 2
                Py_XSETREF(self->tb_next, (PyTracebackObject *)Py_XNewRef(new_next));
            }
        """)

        # Py_CLEAR
        self.check_replace("""
            void set(PyObject **obj, PyObject *value)
            {
                // 1
                Py_CLEAR(self->tb_next);
                self->tb_next = value;
                // 2
                Py_INCREF(value);
                Py_CLEAR(self->tb_next);
                self->tb_next = value;
                // 3
                Py_XINCREF(value);
                Py_CLEAR(self->tb_next);
                self->tb_next = value;
            }
        """, """
            #include "pythoncapi_compat.h"

            void set(PyObject **obj, PyObject *value)
            {
                // 1
                Py_XSETREF(self->tb_next, value);
                // 2
                Py_XSETREF(self->tb_next, Py_NewRef(value));
                // 3
                Py_XSETREF(self->tb_next, Py_XNewRef(value));
            }
        """)

    def test_py_is(self):
        self.check_replace("""
            void test_py_is(PyObject *x)
            {
                if (x == Py_None) {
                    return 1;
                }
                if (x == Py_True) {
                    return 2;
                }
                if (x == Py_False) {
                    return 3;
                }
                return 0;
            }

            void test_py_is_not(PyObject *x)
            {
                if (x != Py_None) {
                    return 1;
                }
                if (x != Py_True) {
                    return 2;
                }
                if (x != Py_False) {
                    return 3;
                }
                return 0;
            }
        """, """
            #include "pythoncapi_compat.h"

            void test_py_is(PyObject *x)
            {
                if (Py_IsNone(x)) {
                    return 1;
                }
                if (Py_IsTrue(x)) {
                    return 2;
                }
                if (Py_IsFalse(x)) {
                    return 3;
                }
                return 0;
            }

            void test_py_is_not(PyObject *x)
            {
                if (!Py_IsNone(x)) {
                    return 1;
                }
                if (!Py_IsTrue(x)) {
                    return 2;
                }
                if (!Py_IsFalse(x)) {
                    return 3;
                }
                return 0;
            }
        """)

        self.check_replace("""
            void test_expr(struct MyStruct *obj, PyObject **obj2)
            {
                if (obj->attr1 == Py_None) {
                    return 1;
                }
                if (obj->attr2.name == Py_None) {
                    return 1;
                }
                if (*obj2 == Py_None) {
                    return 1;
                }
                return 0;
            }
        """, """
            #include "pythoncapi_compat.h"

            void test_expr(struct MyStruct *obj, PyObject **obj2)
            {
                if (Py_IsNone(obj->attr1)) {
                    return 1;
                }
                if (Py_IsNone(obj->attr2.name)) {
                    return 1;
                }
                if (Py_IsNone(*obj2)) {
                    return 1;
                }
                return 0;
            }
        """)


    def test_no_compat(self):
        # Don't add "#include "pythoncapi_compat.h"
        source = """
            void test_type(PyObject *obj, PyTypeObject *type)
            {
                obj->ob_type = type;
            }
        """
        expected = """
            void test_type(PyObject *obj, PyTypeObject *type)
            {
                Py_SET_TYPE(obj, type);
            }
        """
        self.check_replace(source, expected, no_compat=True)

if __name__ == "__main__":
    unittest.main()
