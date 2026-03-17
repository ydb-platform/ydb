/* Copyright (c) 2011-2018 Adam Jakubek, Rafał Gałczyński
 * Released under the MIT license (see attached LICENSE file).
 */

#ifndef CONFIG_H
#define CONFIG_H

#include <limits.h>

#if ULONG_MAX > 0xffffffffUL
#define HAVE_64_BIT_LONG
#else
#undef HAVE_64_BIT_LONG
#endif

#if defined(__GNUC__) && __GNUC__ >= 4
#define LLIST_INTERNAL __attribute__((visibility("hidden")))
#else
#define LLIST_INTERNAL
#endif

#endif /* CONFIG_H */
