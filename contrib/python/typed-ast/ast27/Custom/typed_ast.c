#include "Python.h"
#include "../Include/Python-ast.h"
#include "../Include/compile.h"
#include "../Include/node.h"
#include "../Include/grammar.h"
#include "../Include/token.h"
#include "../Include/ast.h"
#include "../Include/parsetok.h"
#include "../Include/errcode.h"
#include "../Include/graminit.h"

extern grammar _Ta27Parser_Grammar; /* from graminit.c */

// from Python/bltinmodule.c
static const char *
source_as_string(PyObject *cmd, const char *funcname, const char *what, PyCompilerFlags *cf, PyObject **cmd_copy)
{
    const char *str;
    Py_ssize_t size;
    Py_buffer view;

    *cmd_copy = NULL;
    if (PyUnicode_Check(cmd)) {
        cf->cf_flags |= PyCF_IGNORE_COOKIE;
        str = PyUnicode_AsUTF8AndSize(cmd, &size);
        if (str == NULL)
            return NULL;
    }
    else if (PyBytes_Check(cmd)) {
        str = PyBytes_AS_STRING(cmd);
        size = PyBytes_GET_SIZE(cmd);
    }
    else if (PyByteArray_Check(cmd)) {
        str = PyByteArray_AS_STRING(cmd);
        size = PyByteArray_GET_SIZE(cmd);
    }
    else if (PyObject_GetBuffer(cmd, &view, PyBUF_SIMPLE) == 0) {
        /* Copy to NUL-terminated buffer. */
        *cmd_copy = PyBytes_FromStringAndSize(
            (const char *)view.buf, view.len);
        PyBuffer_Release(&view);
        if (*cmd_copy == NULL) {
            return NULL;
        }
        str = PyBytes_AS_STRING(*cmd_copy);
        size = PyBytes_GET_SIZE(*cmd_copy);
    }
    else {
        PyErr_Format(PyExc_TypeError,
          "%s() arg 1 must be a %s object",
          funcname, what);
        return NULL;
    }

    if (strlen(str) != (size_t)size) {
        PyErr_SetString(PyExc_ValueError,
                        "source code string cannot contain null bytes");
        Py_CLEAR(*cmd_copy);
        return NULL;
    }
    return str;
}

// from Python/pythonrun.c
/* compute parser flags based on compiler flags */
static int PARSER_FLAGS(PyCompilerFlags *flags)
{
    int parser_flags = 0;
    if (!flags)
        return 0;
    if (flags->cf_flags & PyCF_DONT_IMPLY_DEDENT)
        parser_flags |= PyPARSE_DONT_IMPLY_DEDENT;
    if (flags->cf_flags & PyCF_IGNORE_COOKIE)
        parser_flags |= PyPARSE_IGNORE_COOKIE;
    return parser_flags;
}

// from Python/pythonrun.c
/* Set the error appropriate to the given input error code (see errcode.h) */
static void
err_input(perrdetail *err)
{
    PyObject *v, *w, *errtype, *errtext;
    PyObject *msg_obj = NULL;
    char *msg = NULL;
    int offset = err->offset;

    errtype = PyExc_SyntaxError;
    switch (err->error) {
    case E_ERROR:
        return;
    case E_SYNTAX:
        errtype = PyExc_IndentationError;
        if (err->expected == INDENT)
            msg = "expected an indented block";
        else if (err->token == INDENT)
            msg = "unexpected indent";
        else if (err->token == DEDENT)
            msg = "unexpected unindent";
        else {
            errtype = PyExc_SyntaxError;
            if (err->token == TYPE_COMMENT)
              msg = "misplaced type annotation";
            else
              msg = "invalid syntax";
        }
        break;
    case E_TOKEN:
        msg = "invalid token";
        break;
    case E_EOFS:
        msg = "EOF while scanning triple-quoted string literal";
        break;
    case E_EOLS:
        msg = "EOL while scanning string literal";
        break;
    case E_INTR:
        if (!PyErr_Occurred())
            PyErr_SetNone(PyExc_KeyboardInterrupt);
        goto cleanup;
    case E_NOMEM:
        PyErr_NoMemory();
        goto cleanup;
    case E_EOF:
        msg = "unexpected EOF while parsing";
        break;
    case E_TABSPACE:
        errtype = PyExc_TabError;
        msg = "inconsistent use of tabs and spaces in indentation";
        break;
    case E_OVERFLOW:
        msg = "expression too long";
        break;
    case E_DEDENT:
        errtype = PyExc_IndentationError;
        msg = "unindent does not match any outer indentation level";
        break;
    case E_TOODEEP:
        errtype = PyExc_IndentationError;
        msg = "too many levels of indentation";
        break;
    case E_DECODE: {
        PyObject *type, *value, *tb;
        PyErr_Fetch(&type, &value, &tb);
        msg = "unknown decode error";
        if (value != NULL)
            msg_obj = PyObject_Str(value);
        Py_XDECREF(type);
        Py_XDECREF(value);
        Py_XDECREF(tb);
        break;
    }
    case E_LINECONT:
        msg = "unexpected character after line continuation character";
        break;
    default:
        fprintf(stderr, "error=%d\n", err->error);
        msg = "unknown parsing error";
        break;
    }
    /* err->text may not be UTF-8 in case of decoding errors.
       Explicitly convert to an object. */
    if (!err->text) {
        errtext = Py_None;
        Py_INCREF(Py_None);
    } else {
        errtext = PyUnicode_DecodeUTF8(err->text, err->offset,
                                       "replace");
        if (errtext != NULL) {
            Py_ssize_t len = strlen(err->text);
            offset = (int)PyUnicode_GET_LENGTH(errtext);
            if (len != err->offset) {
                Py_DECREF(errtext);
                errtext = PyUnicode_DecodeUTF8(err->text, len,
                                               "replace");
            }
        }
    }
    v = Py_BuildValue("(OiiN)", err->filename,
                      err->lineno, offset, errtext);
    if (v != NULL) {
        if (msg_obj)
            w = Py_BuildValue("(OO)", msg_obj, v);
        else
            w = Py_BuildValue("(sO)", msg, v);
    } else
        w = NULL;
    Py_XDECREF(v);
    PyErr_SetObject(errtype, w);
    Py_XDECREF(w);
cleanup:
    Py_XDECREF(msg_obj);
    if (err->text != NULL) {
        PyObject_FREE(err->text);
        err->text = NULL;
    }
}

// from Python/pythonrun.c
static void
err_free(perrdetail *err)
{
    Py_CLEAR(err->filename);
}

// copy of PyParser_ASTFromStringObject in Python/pythonrun.c
/* Preferred access to parser is through AST. */
static mod_ty
string_object_to_c_ast(const char *s, PyObject *filename, int start,
                             PyCompilerFlags *flags, PyArena *arena)
{
    mod_ty mod;
    PyCompilerFlags localflags;
    perrdetail err;
    int iflags = PARSER_FLAGS(flags);

    node *n = Ta27Parser_ParseStringObject(s, filename,
                                         &_Ta27Parser_Grammar, start, &err,
                                         &iflags);
    if (flags == NULL) {
        localflags.cf_flags = 0;
        flags = &localflags;
    }
    if (n) {
        flags->cf_flags |= iflags & PyCF_MASK;
        mod = Ta27AST_FromNode(n, flags, PyUnicode_AsUTF8(filename), arena);
        Ta27Node_Free(n);
    }
    else {
        err_input(&err);
        mod = NULL;
    }
    err_free(&err);
    return mod;
}

// adapted from Py_CompileStringObject in Python/pythonrun.c
static PyObject *
string_object_to_py_ast(const char *str, PyObject *filename, int start,
                       PyCompilerFlags *flags)
{
    mod_ty mod;
    PyObject *result;
    PyArena *arena = PyArena_New();
    if (arena == NULL)
        return NULL;

    mod = string_object_to_c_ast(str, filename, start, flags, arena);
    if (mod == NULL) {
        PyArena_Free(arena);
        return NULL;
    }

    result = Ta27AST_mod2obj(mod);
    PyArena_Free(arena);
    return result;
}

// adapted from builtin_compile_impl in Python/bltinmodule.c
static PyObject *
ast27_parse_impl(PyObject *source,
                 PyObject *filename, const char *mode)
{
    PyObject *source_copy;
    const char *str;
    int compile_mode = -1;
    PyCompilerFlags cf;
    int start[] = {file_input, eval_input, single_input, func_type_input };
    PyObject *result;

    cf.cf_flags = PyCF_ONLY_AST | PyCF_SOURCE_IS_UTF8;

    if (strcmp(mode, "exec") == 0)
        compile_mode = 0;
    else if (strcmp(mode, "eval") == 0)
        compile_mode = 1;
    else if (strcmp(mode, "single") == 0)
        compile_mode = 2;
    else if (strcmp(mode, "func_type") == 0)
        compile_mode = 3;
    else {
        PyErr_SetString(PyExc_ValueError,
                        "parse() mode must be 'exec', 'eval', 'single', for 'func_type'");
        goto error;
    }

    str = source_as_string(source, "parse", "string or bytes", &cf, &source_copy);
    if (str == NULL)
        goto error;

    result = string_object_to_py_ast(str, filename, start[compile_mode], &cf);
    Py_XDECREF(source_copy);
    goto finally;

error:
    result = NULL;
finally:
    Py_DECREF(filename);
    return result;
}

// adapted from builtin_compile in Python/clinic/bltinmodule.c.h
PyObject *
ast27_parse(PyObject *self, PyObject *args)
{
    PyObject *return_value = NULL;
    PyObject *source;
    PyObject *filename;
    const char *mode;

    if (PyArg_ParseTuple(args, "OO&s:parse", &source, PyUnicode_FSDecoder, &filename, &mode))
        return_value = ast27_parse_impl(source, filename, mode);

    return return_value;
}
