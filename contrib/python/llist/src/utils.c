/* Copyright (c) 2011-2018 Adam Jakubek, Rafał Gałczyński
 * Released under the MIT license (see attached LICENSE file).
 */

#include "config.h"

#ifdef HAVE_64_BIT_LONG
#define HASH_MIX_CONSTANT 0x9e3779b97f4a7c15
#else
#define HASH_MIX_CONSTANT 0x9e3779b9
#endif

LLIST_INTERNAL long hash_combine(long h1, long h2)
{
    unsigned long uh1 = (unsigned long)h1;
    unsigned long uh2 = (unsigned long)h2;
    unsigned long c = uh1 ^ ((uh1 << 6) + (uh1 >> 2) + HASH_MIX_CONSTANT + uh2);
    return (long)c;
}
