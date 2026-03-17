/* Copyright (c) 2011-2018 Adam Jakubek, Rafał Gałczyński
 * Released under the MIT license (see attached LICENSE file).
 */

#ifndef SLLIST_H
#define SLLIST_H

#include "config.h"

LLIST_INTERNAL int  sllist_init_type(void);
LLIST_INTERNAL void sllist_register(PyObject* module);

#endif /* SLLIST_H */
