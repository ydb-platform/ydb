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
#include "object.h"
#include "error.h"
#include "types.h"
#include "utils.h"
#include "signature.h"
#include "oid.h"


PyDoc_STRVAR(Tag_target__doc__, "Tagged object.");

PyObject *
Tag_target__get__(Tag *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    const git_oid *oid = git_tag_target_id(self->tag);

    return git_oid_to_python(oid);
}


PyDoc_STRVAR(Tag_get_object__doc__,
  "get_object() -> Object\n"
  "\n"
  "Retrieves the object the current tag is pointing to.");

PyObject *
Tag_get_object(Tag *self)
{
    git_object* obj;

    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    int err = git_tag_peel(&obj, self->tag);
    if (err < 0)
        return Error_set(err);

    return wrap_object(obj, self->repo, NULL);
}


PyDoc_STRVAR(Tag_name__doc__, "Tag name.");

PyObject *
Tag_name__get__(Tag *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    const char *name = git_tag_name(self->tag);
    if (!name)
        Py_RETURN_NONE;

    return to_unicode(name, "utf-8", "strict");
}


PyDoc_STRVAR(Tag_raw_name__doc__, "Tag name (bytes).");

PyObject *
Tag_raw_name__get__(Tag *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    const char *name = git_tag_name(self->tag);
    if (!name)
        Py_RETURN_NONE;

    return PyBytes_FromString(name);
}


PyDoc_STRVAR(Tag_tagger__doc__, "Tagger.");

PyObject *
Tag_tagger__get__(Tag *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    const git_signature *signature = git_tag_tagger(self->tag);
    if (!signature)
        Py_RETURN_NONE;

    return build_signature((Object*)self, signature, "utf-8");
}


PyDoc_STRVAR(Tag_message__doc__, "Tag message.");

PyObject *
Tag_message__get__(Tag *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    const char *message = git_tag_message(self->tag);
    if (!message)
        Py_RETURN_NONE;

    return to_unicode(message, "utf-8", "strict");
}


PyDoc_STRVAR(Tag_raw_message__doc__, "Tag message (bytes).");

PyObject *
Tag_raw_message__get__(Tag *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    const char *message = git_tag_message(self->tag);
    if (!message)
        Py_RETURN_NONE;

    return PyBytes_FromString(message);
}

PyMethodDef Tag_methods[] = {
    METHOD(Tag, get_object, METH_NOARGS),
    {NULL}
};

PyGetSetDef Tag_getseters[] = {
    GETTER(Tag, target),
    GETTER(Tag, name),
    GETTER(Tag, raw_name),
    GETTER(Tag, tagger),
    GETTER(Tag, message),
    GETTER(Tag, raw_message),
    {NULL}
};


PyDoc_STRVAR(Tag__doc__, "Tag objects.");

PyTypeObject TagType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Tag",                             /* tp_name           */
    sizeof(Tag),                               /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    0,                                         /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    0,                                         /* tp_repr           */
    0,                                         /* tp_as_number      */
    0,                                         /* tp_as_sequence    */
    0,                                         /* tp_as_mapping     */
    0,                                         /* tp_hash           */
    0,                                         /* tp_call           */
    0,                                         /* tp_str            */
    0,                                         /* tp_getattro       */
    0,                                         /* tp_setattro       */
    0,                                         /* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT,                        /* tp_flags          */
    Tag__doc__,                                /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    Tag_methods,                               /* tp_methods        */
    0,                                         /* tp_members        */
    Tag_getseters,                             /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};
