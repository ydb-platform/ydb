/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"

/* Note: all non-alpha are mapped to 0 */
static const int mapping[] = {
    0,  /* 00 */
    0,  /* 01 */
    0,  /* 02 */
    0,  /* 03 */
    0,  /* 04 */
    0,  /* 05 */
    0,  /* 06 */
    0,  /* 07 */
    0,  /* 08 */
    0,  /* 09 */
    0,  /* 0a */
    0,  /* 0b */
    0,  /* 0c */
    0,  /* 0d */
    0,  /* 0e */
    0,  /* 0f */
    0,  /* 10 */
    0,  /* 11 */
    0,  /* 12 */
    0,  /* 13 */
    0,  /* 14 */
    0,  /* 15 */
    0,  /* 16 */
    0,  /* 17 */
    0,  /* 18 */
    0,  /* 19 */
    0,  /* 1a */
    0,  /* 1b */
    0,  /* 1c */
    0,  /* 1d */
    0,  /* 1e */
    0,  /* 1f */
    0,  /* 20 */
    0,  /* 21 */
    0,  /* 22 */
    38, /* # */
    0,  /* 24 */
    0,  /* 25 */
    0,  /* 26 */
    0,  /* 27 */
    0,  /* 28 */
    0,  /* 29 */
    0,  /* 2a */
    0,  /* 2b */
    0,  /* 2c */
    0,  /* 2d */
    0,  /* 2e */
    0,  /* 2f */
    1,  /* 0 */
    2,  /* 1 */
    3,  /* 2 */
    4,  /* 3 */
    5,  /* 4 */
    6,  /* 5 */
    7,  /* 6 */
    8,  /* 7 */
    9,  /* 8 */
    10, /* 9 */
    0,  /* 3a */
    0,  /* 3b */
    0,  /* 3c */
    0,  /* 3d */
    0,  /* 3e */
    0,  /* 3f */
    0,  /* 40 */
    11, /* A */
    12, /* B */
    13, /* C */
    14, /* D */
    15, /* E */
    16, /* F */
    17, /* G */
    18, /* H */
    19, /* I */
    20, /* J */
    21, /* K */
    22, /* L */
    23, /* M */
    24, /* N */
    25, /* O */
    26, /* P */
    27, /* Q */
    28, /* R */
    29, /* S */
    30, /* T */
    31, /* U */
    32, /* V */
    33, /* W */
    34, /* X */
    35, /* Y */
    36, /* Z */
    0,  /* 5b */
    0,  /* 5c */
    0,  /* 5d */
    0,  /* 5e */
    37, /* _ */
    0,  /* 60 */
    11, /* a */
    12, /* b */
    13, /* c */
    14, /* d */
    15, /* e */
    16, /* f */
    17, /* g */
    18, /* h */
    19, /* i */
    20, /* j */
    21, /* k */
    22, /* l */
    23, /* m */
    24, /* n */
    25, /* o */
    26, /* p */
    27, /* q */
    28, /* r */
    29, /* s */
    30, /* t */
    31, /* u */
    32, /* v */
    33, /* w */
    34, /* x */
    35, /* y */
    36, /* z */
    0,  /* 7b */
    0,  /* 7c */
    0,  /* 7d */
    0,  /* 7e */
    0,  /* 7f */
    0,  /* 80 */
    0,  /* 81 */
    0,  /* 82 */
    0,  /* 83 */
    0,  /* 84 */
    0,  /* 85 */
    0,  /* 86 */
    0,  /* 87 */
    0,  /* 88 */
    0,  /* 89 */
    0,  /* 8a */
    0,  /* 8b */
    0,  /* 8c */
    0,  /* 8d */
    0,  /* 8e */
    0,  /* 8f */
    0,  /* 90 */
    0,  /* 91 */
    0,  /* 92 */
    0,  /* 93 */
    0,  /* 94 */
    0,  /* 95 */
    0,  /* 96 */
    0,  /* 97 */
    0,  /* 98 */
    0,  /* 99 */
    0,  /* 9a */
    0,  /* 9b */
    0,  /* 9c */
    0,  /* 9d */
    0,  /* 9e */
    0,  /* 9f */
    0,  /* a0 */
    0,  /* a1 */
    0,  /* a2 */
    0,  /* a3 */
    0,  /* a4 */
    0,  /* a5 */
    0,  /* a6 */
    0,  /* a7 */
    0,  /* a8 */
    0,  /* a9 */
    0,  /* aa */
    0,  /* ab */
    0,  /* ac */
    0,  /* ad */
    0,  /* ae */
    0,  /* af */
    0,  /* b0 */
    0,  /* b1 */
    0,  /* b2 */
    0,  /* b3 */
    0,  /* b4 */
    0,  /* b5 */
    0,  /* b6 */
    0,  /* b7 */
    0,  /* b8 */
    0,  /* b9 */
    0,  /* ba */
    0,  /* bb */
    0,  /* bc */
    0,  /* bd */
    0,  /* be */
    0,  /* bf */
    0,  /* c0 */
    0,  /* c1 */
    0,  /* c2 */
    0,  /* c3 */
    0,  /* c4 */
    0,  /* c5 */
    0,  /* c6 */
    0,  /* c7 */
    0,  /* c8 */
    0,  /* c9 */
    0,  /* ca */
    0,  /* cb */
    0,  /* cc */
    0,  /* cd */
    0,  /* ce */
    0,  /* cf */
    0,  /* d0 */
    0,  /* d1 */
    0,  /* d2 */
    0,  /* d3 */
    0,  /* d4 */
    0,  /* d5 */
    0,  /* d6 */
    0,  /* d7 */
    0,  /* d8 */
    0,  /* d9 */
    0,  /* da */
    0,  /* db */
    0,  /* dc */
    0,  /* dd */
    0,  /* de */
    0,  /* df */
    0,  /* e0 */
    0,  /* e1 */
    0,  /* e2 */
    0,  /* e3 */
    0,  /* e4 */
    0,  /* e5 */
    0,  /* e6 */
    0,  /* e7 */
    0,  /* e8 */
    0,  /* e9 */
    0,  /* ea */
    0,  /* eb */
    0,  /* ec */
    0,  /* ed */
    0,  /* ee */
    0,  /* ef */
    0,  /* f0 */
    0,  /* f1 */
    0,  /* f2 */
    0,  /* f3 */
    0,  /* f4 */
    0,  /* f5 */
    0,  /* f6 */
    0,  /* f7 */
    0,  /* f8 */
    0,  /* f9 */
    0,  /* fa */
    0,  /* fb */
    0,  /* fc */
    0,  /* fd */
    0,  /* fe */
    0,  /* ff */
};

/* ECC-388 */
#ifdef DEBUG
static const size_t NUM_MAPPINGS = sizeof(mapping) / sizeof(mapping[0]);

#define DebugCheckBounds(index, value)                                                                  \
    do {                                                                                                \
        if (!((index) >= 0 && (index) < NUM_MAPPINGS)) {                                                \
            printf("ERROR: string='%s' index=%ld @ %s +%d \n", value, (long)index, __FILE__, __LINE__); \
            abort();                                                                                    \
        }                                                                                               \
    } while (0)
#else
#define DebugCheckBounds(index, value)
#endif


#define SIZE 39

#if GRIB_PTHREADS
static pthread_once_t once   = PTHREAD_ONCE_INIT;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void init_mutex()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex;

static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_grib_trie_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex);
            once = 1;
        }
    }
}
#endif

struct grib_trie
{
    grib_trie* next[SIZE];
    grib_context* context;
    int first;
    int last;
    void* data;
};

grib_trie* grib_trie_new(grib_context* c)
{
#ifdef RECYCLE_TRIE
    grib_trie* t = grib_context_malloc_clear_persistent(c, sizeof(grib_trie));
#else
    grib_trie* t = (grib_trie*)grib_context_malloc_clear(c, sizeof(grib_trie));
#endif
    t->context = c;
    t->first   = SIZE;
    t->last    = -1;
    return t;
}

void grib_trie_delete_container(grib_trie* t)
{
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);
    if (t) {
        int i;
        for (i = t->first; i <= t->last; i++)
            if (t->next[i]) {
                grib_trie_delete_container(t->next[i]);
            }
#ifdef RECYCLE_TRIE
        grib_context_free_persistent(t->context, t);
#else
        grib_context_free(t->context, t);
#endif
    }
    GRIB_MUTEX_UNLOCK(&mutex);
}

void grib_trie_delete(grib_trie* t)
{
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);
    if (t) {
        int i;
        for (i = t->first; i <= t->last; i++)
            if (t->next[i]) {
                grib_context_free(t->context, t->next[i]->data);
                grib_trie_delete(t->next[i]);
            }
#ifdef RECYCLE_TRIE
        grib_context_free_persistent(t->context, t);
#else
        grib_context_free(t->context, t);
#endif
    }
    GRIB_MUTEX_UNLOCK(&mutex);
}

void grib_trie_clear(grib_trie* t)
{
    if (t) {
        int i;
        t->data = NULL;
        for (i = t->first; i <= t->last; i++)
            if (t->next[i])
                grib_trie_clear(t->next[i]);
    }
}

void* grib_trie_insert(grib_trie* t, const char* key, void* data)
{
    grib_trie* last = t;
    const char* k   = key;
    void* old       = NULL;

    if (!t) {
        ECCODES_ASSERT(!"grib_trie_insert: grib_trie==NULL");
        return NULL;
    }

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);

    while (*k && t) {
        last = t;
        DebugCheckBounds((int)*k, key);
        t = t->next[mapping[(int)*k]];
        if (t)
            k++;
    }

    if (*k == 0) {
        old     = t->data;
        t->data = data;
    }
    else {
        t = last;
        while (*k) {
            int j = 0;
            DebugCheckBounds((int)*k, key);
            j = mapping[(int)*k++];
            if (j < t->first)
                t->first = j;
            if (j > t->last)
                t->last = j;
            t = t->next[j] = grib_trie_new(t->context);
        }
        old     = t->data;
        t->data = data;
    }
    GRIB_MUTEX_UNLOCK(&mutex);
    return data == old ? NULL : old;
}

void* grib_trie_insert_no_replace(grib_trie* t, const char* key, void* data)
{
    grib_trie* last = t;
    const char* k   = key;
    ECCODES_ASSERT(t);

    while (*k && t) {
        last = t;
        DebugCheckBounds((int)*k, key);
        t = t->next[mapping[(int)*k]];
        if (t)
            k++;
    }

    if (*k != 0) {
        t = last;
        while (*k) {
            int j = 0;
            DebugCheckBounds((int)*k, key);
            j = mapping[(int)*k++];
            if (j < t->first)
                t->first = j;
            if (j > t->last)
                t->last = j;
            t = t->next[j] = grib_trie_new(t->context);
        }
    }

    if (!t->data)
        t->data = data;

    return t->data;
}

void* grib_trie_get(grib_trie* t, const char* key)
{
    const char* k = key;
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);

    while (*k && t) {
        DebugCheckBounds((int)*k, key);
        t = t->next[mapping[(int)*k++]];
    }

    if (*k == 0 && t != NULL && t->data != NULL) {
        GRIB_MUTEX_UNLOCK(&mutex);
        return t->data;
    }
    GRIB_MUTEX_UNLOCK(&mutex);
    return NULL;
}

/*
To be rewritten.
void grib_trie_remove(grib_trie* trie,const char* key)
{
  const char *k = key;
  grib_trie_internal *t=trie->trie;

  grib_context *c = trie->context;

  while(*k && t)
    t = t->next[mapping[(int)*k++]];

  if(*k == 0 && t != NULL)
    t->data->data = NULL;
  else
    grib_context_log(c,GRIB_LOG_WARNING,"grib_trie_remove: key not found [%s]",key);

}
*/
