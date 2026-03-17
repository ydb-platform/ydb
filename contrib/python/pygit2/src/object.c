/*
 * Copyright 2010-2025 The pygit2 contributors
 *
 * This file is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 *
 * In addition to the permissions in the GNU General Public License,
 * the authors give you unlimited permission to link the compiled
 * version of this file into combinations with other programs,
 * and to distribute those combinations without any restriction
 * coming from the use of this file.  (The General Public License
 * restrictions do apply in other respects; for example, they cover
 * modification of the file, and distribution when not linked into
 * a combined executable.)
 *
 * This file is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file COPYING.  If not, write to
 * the Free Software Foundation, 51 Franklin Street, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <git2.h>
#include "error.h"
#include "types.h"
#include "utils.h"
#include "odb.h"
#include "oid.h"
#include "repository.h"
#include "object.h"

extern PyTypeObject TreeType;
extern PyTypeObject CommitType;
extern PyTypeObject BlobType;
extern PyTypeObject TagType;
extern PyObject *FileModeEnum;

PyTypeObject ObjectType;

void
Object_dealloc(Object* self)
{
    Py_CLEAR(self->repo);
    git_object_free(self->obj);
    git_tree_entry_free((git_tree_entry*)self->entry);
    Py_TYPE(self)->tp_free(self);
}


git_object*
Object__load(Object *self)
{
    if (self->obj == NULL) {
        int err = git_tree_entry_to_object(&self->obj, self->repo->repo, self->entry);
        if (err < 0) {
            Error_set(err);
            return NULL;
        }
    }

    return self->obj;
}

const git_oid*
Object__id(Object *self)
{
    return (self->obj) ? git_object_id(self->obj) : git_tree_entry_id(self->entry);
}


git_object_t
Object__type(Object *self)
{
    return (self->obj) ? git_object_type(self->obj) : git_tree_entry_type(self->entry);
}


PyDoc_STRVAR(Object_id__doc__,
    "The object id, an instance of the Oid type.");

PyObject *
Object_id__get__(Object *self)
{
    return git_oid_to_python(Object__id(self));
}


PyDoc_STRVAR(Object_short_id__doc__,
    "An unambiguous short (abbreviated) hex Oid string for the object.");

PyObject *
Object_short_id__get__(Object *self)
{
    if (Object__load(self) == NULL) { return NULL; } // Lazy load

    git_buf short_id = { NULL, 0, 0 };
    int err = git_object_short_id(&short_id, self->obj);
    if (err != GIT_OK)
        return Error_set(err);

    PyObject *py_short_id = to_unicode_n(short_id.ptr, short_id.size, NULL, "strict");
    git_buf_dispose(&short_id);
    return py_short_id;
}


PyDoc_STRVAR(Object_type__doc__,
    "One of the enums.ObjectType.COMMIT, TREE, BLOB or TAG constants.");

PyObject *
Object_type__get__(Object *self)
{
    return PyLong_FromLong(Object__type(self));
}

PyDoc_STRVAR(Object_type_str__doc__,
    "One of the 'commit', 'tree', 'blob' or 'tag' strings.");

PyObject *
Object_type_str__get__(Object *self)
{
    return PyUnicode_DecodeFSDefault(git_object_type2string(Object__type(self)));
}

PyDoc_STRVAR(Object__pointer__doc__, "Get the object's pointer. For internal use only.");
PyObject *
Object__pointer__get__(Object *self)
{
    /* Bytes means a raw buffer */
    if (Object__load(self) == NULL) { return NULL; } // Lazy load
    return PyBytes_FromStringAndSize((char *) &self->obj, sizeof(git_object *));
}

PyDoc_STRVAR(Object_name__doc__,
             "Name (or None if the object was not reached through a tree)");
PyObject *
Object_name__get__(Object *self)
{
    if (self->entry == NULL)
        Py_RETURN_NONE;

    return PyUnicode_DecodeFSDefault(git_tree_entry_name(self->entry));
}

PyDoc_STRVAR(Object_raw_name__doc__, "Name (bytes).");

PyObject *
Object_raw_name__get__(Object *self)
{
    if (self->entry == NULL)
        Py_RETURN_NONE;

    return PyBytes_FromString(git_tree_entry_name(self->entry));
}

PyDoc_STRVAR(Object_filemode__doc__,
             "An enums.FileMode constant (or None if the object was not reached through a tree)");
PyObject *
Object_filemode__get__(Object *self)
{
    if (self->entry == NULL)
        Py_RETURN_NONE;

    return pygit2_enum(FileModeEnum, git_tree_entry_filemode(self->entry));
}


PyDoc_STRVAR(Object_read_raw__doc__,
  "read_raw() -> bytes\n"
  "\n"
  "Returns the byte string with the raw contents of the object.");

PyObject *
Object_read_raw(Object *self)
{
    int err;
    git_odb *odb;
    PyObject *aux;

    err = git_repository_odb(&odb, self->repo->repo);
    if (err < 0)
        return Error_set(err);

    const git_oid *oid = Object__id(self);
    git_odb_object *obj = Odb_read_raw(odb, oid, GIT_OID_HEXSZ);
    git_odb_free(odb);
    if (obj == NULL)
        return NULL;

    aux = PyBytes_FromStringAndSize(
        git_odb_object_data(obj),
        git_odb_object_size(obj));

    git_odb_object_free(obj);
    return aux;
}

PyDoc_STRVAR(Object_peel__doc__,
  "peel(target_type) -> Object\n"
  "\n"
  "Peel the current object and returns the first object of the given type.\n"
  "\n"
  "If you pass None as the target type, then the object will be peeled\n"
  "until the type changes. A tag will be peeled until the referenced object\n"
  "is no longer a tag, and a commit will be peeled to a tree. Any other\n"
  "object type will raise InvalidSpecError.\n");

PyObject *
Object_peel(Object *self, PyObject *py_type)
{
    int err;
    git_otype otype;
    git_object *peeled;

    if (Object__load(self) == NULL) { return NULL; } // Lazy load

    otype = py_object_to_otype(py_type);
    if (otype == GIT_OBJECT_INVALID)
        return NULL;

    err = git_object_peel(&peeled, self->obj, otype);
    if (err < 0)
        return Error_set(err);

    return wrap_object(peeled, self->repo, NULL);
}

Py_hash_t
Object_hash(Object *self)
{
    const git_oid *oid = Object__id(self);
    PyObject *py_oid = git_oid_to_py_str(oid);
    Py_hash_t ret = PyObject_Hash(py_oid);
    Py_DECREF(py_oid);
    return ret;
}

PyObject *
Object_repr(Object *self)
{
    char hex[GIT_OID_HEXSZ + 1];

    git_oid_fmt(hex, Object__id(self));
    hex[GIT_OID_HEXSZ] = '\0';

    return PyUnicode_FromFormat("<pygit2.Object{%s:%s}>",
        git_object_type2string(Object__type(self)),
        hex
    );
}

PyObject *
Object_richcompare(PyObject *o1, PyObject *o2, int op)
{
    PyObject *res;

    if (!PyObject_TypeCheck(o2, &ObjectType)) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    int equal = git_oid_equal(Object__id((Object *)o1), Object__id((Object *)o2));
    switch (op) {
        case Py_NE:
            res = (equal) ? Py_False : Py_True;
            break;
        case Py_EQ:
            res = (equal) ? Py_True : Py_False;
            break;
        case Py_LT:
        case Py_LE:
        case Py_GT:
        case Py_GE:
            Py_INCREF(Py_NotImplemented);
            return Py_NotImplemented;
        default:
            PyErr_Format(PyExc_RuntimeError, "Unexpected '%d' op", op);
            return NULL;
    }

    Py_INCREF(res);
    return res;
}

PyGetSetDef Object_getseters[] = {
    GETTER(Object, id),
    GETTER(Object, short_id),
    GETTER(Object, type),
    GETTER(Object, type_str),
    GETTER(Object, _pointer),
    // These come from git_tree_entry
    GETTER(Object, name),
    GETTER(Object, raw_name),
    GETTER(Object, filemode),
    {NULL}
};

PyMethodDef Object_methods[] = {
    METHOD(Object, read_raw, METH_NOARGS),
    METHOD(Object, peel, METH_O),
    {NULL}
};


PyDoc_STRVAR(Object__doc__, "Base class for Git objects.");

PyTypeObject ObjectType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Object",                          /* tp_name           */
    sizeof(Object),                            /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)Object_dealloc,                /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    (reprfunc)Object_repr,                     /* tp_repr           */
    0,                                         /* tp_as_number      */
    0,                                         /* tp_as_sequence    */
    0,                                         /* tp_as_mapping     */
    (hashfunc)Object_hash,                     /* tp_hash           */
    0,                                         /* tp_call           */
    0,                                         /* tp_str            */
    0,                                         /* tp_getattro       */
    0,                                         /* tp_setattro       */
    0,                                         /* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,  /* tp_flags          */
    Object__doc__,                             /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    (richcmpfunc)Object_richcompare,           /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    Object_methods,                            /* tp_methods        */
    0,                                         /* tp_members        */
    Object_getseters,                          /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyObject *
wrap_object(git_object *c_object, Repository *repo, const git_tree_entry *entry)
{
    Object *py_obj = NULL;

    git_object_t obj_type = (c_object) ? git_object_type(c_object) : git_tree_entry_type(entry);

    switch (obj_type) {
        case GIT_OBJECT_COMMIT:
            py_obj = PyObject_New(Object, &CommitType);
            break;
        case GIT_OBJECT_TREE:
            py_obj = PyObject_New(Object, &TreeType);
            break;
        case GIT_OBJECT_BLOB:
            py_obj = PyObject_New(Object, &BlobType);
            break;
        case GIT_OBJECT_TAG:
            py_obj = PyObject_New(Object, &TagType);
            break;
        default:
            assert(0);
    }

    if (py_obj) {
        py_obj->obj = c_object;
        if (repo) {
            py_obj->repo = repo;
            Py_INCREF(repo);
        }
        py_obj->entry = entry;
    }
    return (PyObject *)py_obj;
}
