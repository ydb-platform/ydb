/* typecast.h - definitions for typecasters
 *
 * Copyright (C) 2003-2019 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2020-2021 The Psycopg Team
 *
 * This file is part of psycopg.
 *
 * psycopg2 is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link this program with the OpenSSL library (or with
 * modified versions of OpenSSL that use the same license as OpenSSL),
 * and distribute linked combinations including the two.
 *
 * You must obey the GNU Lesser General Public License in all respects for
 * all of the code used other than OpenSSL.
 *
 * psycopg2 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 */

#ifndef PSYCOPG_TYPECAST_H
#define PSYCOPG_TYPECAST_H 1

#ifdef __cplusplus
extern "C" {
#endif

/* type of type-casting functions (both C and Python) */
typedef PyObject *(*typecast_function)(const char *str, Py_ssize_t len,
                                       PyObject *cursor);

/** typecast type **/

extern HIDDEN PyTypeObject typecastType;

typedef struct {
    PyObject_HEAD

    PyObject *name;    /* the name of this type */
    PyObject *values;  /* the different types this instance can match */

    typecast_function  ccast;  /* the C casting function */
    PyObject          *pcast;  /* the python casting function */
    PyObject          *bcast;  /* base cast, used by array typecasters */
} typecastObject;

/* the initialization values are stored here */

typedef struct {
    char *name;
    long int *values;
    typecast_function cast;

    /* base is the base typecaster for arrays */
    char *base;
} typecastObject_initlist;

/* the type dictionary, much faster to access it globally */
extern HIDDEN PyObject *psyco_types;
extern HIDDEN PyObject *psyco_binary_types;

/* the default casting objects, used when no other objects are available */
extern HIDDEN PyObject *psyco_default_cast;
extern HIDDEN PyObject *psyco_default_binary_cast;

/** exported functions **/

/* used by module.c to init the type system and register types */
RAISES_NEG HIDDEN int typecast_init(PyObject *dict);
RAISES_NEG HIDDEN int typecast_add(PyObject *obj, PyObject *dict, int binary);

/* the C callable typecastObject creator function */
HIDDEN PyObject *typecast_from_c(typecastObject_initlist *type, PyObject *d);

/* the python callable typecast creator functions */
HIDDEN PyObject *typecast_from_python(
    PyObject *self, PyObject *args, PyObject *keywds);
HIDDEN PyObject *typecast_array_from_python(
    PyObject *self, PyObject *args, PyObject *keywds);

/* the function used to dispatch typecasting calls */
HIDDEN PyObject *typecast_cast(
    PyObject *self, const char *str, Py_ssize_t len, PyObject *curs);

#endif /* !defined(PSYCOPG_TYPECAST_H) */
