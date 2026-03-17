/* Copyright (c) 2011-2018 Adam Jakubek, Rafał Gałczyński
 * Released under the MIT license (see attached LICENSE file).
 */

#ifndef DLLIST_H
#define DLLIST_H

#include "config.h"

LLIST_INTERNAL int  dllist_init_type(void);
LLIST_INTERNAL void dllist_register(PyObject* module);

#endif /* DLLIST_H */
