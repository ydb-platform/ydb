/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2012-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2018 IBM Corporation. All rights reserved.
 * Copyright (c) 2017-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <string.h>
#include <errno.h>
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <limits.h>
#include <ctype.h>
#ifdef HAVE_SYS_UTSNAME_H
#include <sys/utsname.h>
#endif
#include <assert.h>

#include "opal/util/argv.h"
#include "opal/util/opal_getcwd.h"
#include "opal/util/output.h"
#include "opal/util/strncpy.h"
#include "opal/util/info_subscriber.h"

static char* opal_infosubscribe_inform_subscribers(opal_infosubscriber_t * object, char *key, char *new_value, int *found_callback);
static void infosubscriber_construct(opal_infosubscriber_t *obj);
static void infosubscriber_destruct(opal_infosubscriber_t *obj);

/*
 * Local structures
 */

typedef struct opal_callback_list_t opal_callback_list_t;

struct opal_callback_list_item_t {
    opal_list_item_t super;
    char *default_value;
    opal_key_interest_callback_t *callback;
};
typedef struct opal_callback_list_item_t opal_callback_list_item_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_infosubscriber_t);
OBJ_CLASS_INSTANCE(opal_infosubscriber_t,
                   opal_object_t,
                   infosubscriber_construct,
                   infosubscriber_destruct);

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_callback_list_item_t);
static void opal_callback_list_item_destruct(opal_callback_list_item_t *obj);
OBJ_CLASS_INSTANCE(opal_callback_list_item_t,
                   opal_list_item_t,
                   NULL,
                   opal_callback_list_item_destruct);

static void infosubscriber_construct(opal_infosubscriber_t *obj) {
    OBJ_CONSTRUCT(&obj->s_subscriber_table, opal_hash_table_t);
    opal_hash_table_init(&obj->s_subscriber_table, 10);
}

static void infosubscriber_destruct(opal_infosubscriber_t *obj) {
    opal_hash_table_t *table = &obj->s_subscriber_table;
    void *node = NULL;
    int err;
    char *next_key;
    size_t key_size;
    opal_list_t *list = NULL;

    err = opal_hash_table_get_first_key_ptr(table,
        (void**) &next_key, &key_size, (void**) &list, &node);
    while (list && err == OPAL_SUCCESS) {
        OPAL_LIST_RELEASE(list);

        err = opal_hash_table_get_next_key_ptr(table,
          (void**) &next_key, &key_size, (void**) &list, node, &node);
    }

    OBJ_DESTRUCT(&obj->s_subscriber_table);

    if (NULL != obj->s_info) {
        OBJ_RELEASE(obj->s_info);
    }
}

static void opal_callback_list_item_destruct(opal_callback_list_item_t *obj) {
    if (obj->default_value) {
        free(obj->default_value); // came from a strdup()
    }
}

static char* opal_infosubscribe_inform_subscribers(opal_infosubscriber_t *object, char *key, char *new_value, int *found_callback)
{
    opal_hash_table_t *table = &object->s_subscriber_table;
    opal_list_t *list = NULL;
    opal_callback_list_item_t *item;
    char *updated_value = NULL;

    if (found_callback) { *found_callback = 0; }
/*
 * Present the new value to each subscriber.  They can decide to accept it, ignore it, or
 * over-ride it with their own value (like ignore, but they specify what value they want it to have).
 *
 * Since multiple subscribers could set values, only the last setting is kept as the
 * returned value.
 */
    if (table) {
        opal_hash_table_get_value_ptr(table, key, strlen(key), (void**) &list);

        if (list) {
            updated_value = new_value;
            OPAL_LIST_FOREACH(item, list, opal_callback_list_item_t) {
                updated_value = item->callback(object, key, updated_value);
                if (found_callback) { *found_callback = 1; }
            }
        }
    }

    return updated_value;
}




/*
 * Testing-only static data, all paths using this code should be
 * inactive in a normal run.  In particular ntesting_callbacks is 0
 * unless testing is in play.
 */
static int ntesting_callbacks = 0;
static opal_key_interest_callback_t *testing_callbacks[5];
static char *testing_keys[5];
static char *testing_initialvals[5];
// User-level call, user adds their own callback function to be subscribed
// to every object:
int opal_infosubscribe_testcallback(opal_key_interest_callback_t *callback,
  char *key, char *val);

int
opal_infosubscribe_testcallback(opal_key_interest_callback_t *callback,
  char *key, char *val)
{
    int i = ntesting_callbacks;
    if (ntesting_callbacks >= 5) { return -1; }

    testing_callbacks[i] = callback;
    testing_keys[i] = key;
    testing_initialvals[i] = val;
    ++ntesting_callbacks;
    return 0;
}

int opal_infosubscribe_testregister(opal_infosubscriber_t *object);
int
opal_infosubscribe_testregister(opal_infosubscriber_t *object)
{
    opal_hash_table_t *table = &object->s_subscriber_table;
    opal_callback_list_item_t *item;
    opal_list_t *list = NULL;

// The testing section should only ever be activated if the testing callback
// above is used.
    if (ntesting_callbacks != 0) {
        int i;
        for (i=0; i<ntesting_callbacks; ++i) {
// The testing-code only wants to add test-subscriptions
// once for an obj.  So before adding a test-subscription, see
// if it's already there.
            int found = 0;
            opal_hash_table_get_value_ptr(table, testing_keys[i],
                strlen(testing_keys[i]), (void**) &list);
            if (list) {
                OPAL_LIST_FOREACH(item, list, opal_callback_list_item_t) {
                    if (0 ==
                        strcmp(item->default_value, testing_initialvals[i])
                        &&
                        item->callback == testing_callbacks[i])
                    {
                        found = 1;
                    }
                }
            }
            list = NULL;

            if (!found) {
                opal_infosubscribe_subscribe(object,
                    testing_keys[i],
                    testing_initialvals[i], testing_callbacks[i]);
            }
        }
    }

// For testing-mode only, while we're here, lets walk the whole list
// to see if there are any duplicates.
    if (ntesting_callbacks != 0) {
        int err;
        void *node = NULL;
        size_t key_size;
        char *next_key;
        opal_callback_list_item_t *item1, *item2;

        err = opal_hash_table_get_first_key_ptr(table, (void**) &next_key,
            &key_size, (void**) &list, &node);
        while (list && err == OPAL_SUCCESS) {
            int counter = 0;
            OPAL_LIST_FOREACH(item1, list, opal_callback_list_item_t) {
                OPAL_LIST_FOREACH(item2, list, opal_callback_list_item_t) {
                    if (0 ==
                        strcmp(item1->default_value, item2->default_value)
                        &&
                        item1->callback == item2->callback)
                    {
                        ++counter;
                    }
                }
            }
            if (counter > 1) {
                printf("ERROR: duplicate info key/val subscription found "
                    "in hash table\n");
                exit(-1);
            }

            err = opal_hash_table_get_next_key_ptr(table,
                (void**) &next_key, &key_size, (void**) &list, node, &node);
        }
    }

    return OPAL_SUCCESS;
}

// This routine is to be used after making a callback for a
// key/val pair. The callback would have ggiven a new value to associate
// with <key>, and this function saves the previous value under
// __IN_<key>.
//
// The last argument indicates whether to overwrite a previous
// __IN_<key> or not.
static int
save_original_key_val(opal_info_t *info, char *key, char *val, int overwrite)
{
    char modkey[OPAL_MAX_INFO_KEY];
    int flag, err;

    // Checking strlen, even though it should be unnecessary.
    // This should only happen on predefined keys with short lengths.
    if (strlen(key) + strlen(OPAL_INFO_SAVE_PREFIX) < OPAL_MAX_INFO_KEY) {
        snprintf(modkey, OPAL_MAX_INFO_KEY,
            OPAL_INFO_SAVE_PREFIX "%s", key);
// (the prefix macro is a string, so the unreadable part above is a string concatenation)
        flag = 0;
        opal_info_get(info, modkey, 0, NULL, &flag);
        if (!flag || overwrite) {
            err = opal_info_set(info, modkey, val);
            if (OPAL_SUCCESS != err) {
                return err;
            }
        }
// FIXME: use whatever the Open MPI convention is for DEBUG options like this
// Even though I don't expect this codepath to happen, if it somehow DID happen
// in a real run with user-keys, I'd rather it be silent at that point rather
// being noisy and/or aborting.
#ifdef OMPI_DEBUG
    } else {
        printf("WARNING: Unexpected key length [%s]\n", key);
#endif
    }
    return OPAL_SUCCESS;
}

int
opal_infosubscribe_change_info(opal_infosubscriber_t *object, opal_info_t *new_info)
{
    int err;
    opal_info_entry_t *iterator;
    char *updated_value;

    /* for each key/value in new info, let subscribers know of new value */
    int found_callback;

    if (!object->s_info) {
        object->s_info = OBJ_NEW(opal_info_t);
    }

    if (NULL != new_info) {
    OPAL_LIST_FOREACH(iterator, &new_info->super, opal_info_entry_t) {

        updated_value = opal_infosubscribe_inform_subscribers(object, iterator->ie_key, iterator->ie_value, &found_callback);
        if (updated_value) {
            err = opal_info_set(object->s_info, iterator->ie_key, updated_value);
        } else {
// This path would happen if there was no callback for this key,
// or if there was a callback and it returned null. One way the
// setting was unrecognized the other way it was recognized and ignored,
// either way it shouldn't be set, which we'll ensure with an unset
// in case a previous value exists.
            err = opal_info_delete(object->s_info, iterator->ie_key);
            err = OPAL_SUCCESS; // we don't care if the key was found or not
        }
        if (OPAL_SUCCESS != err) {
            return err;
        }
// Save the original at "__IN_<key>":"original"
// And if multiple set-info calls happen, the last would be the most relevant
// to save, so overwrite a previously saved value if there is one.
        save_original_key_val(object->s_info,
            iterator->ie_key, iterator->ie_value, 1);
    }}

    return OPAL_SUCCESS;
}

// Callers can provide a callback for processing info k/v pairs.
//
// Currently the callback() is expected to return a static string, and the
// callers of callback() do not try to free the string it returns. for example
// current callbacks do things like
//     return some_condition ? "true" : "false";
// the caller of callback() uses the return value in an opal_info_set() which
// strdups the string. The string returned from callback() is not kept beyond
// that. Currently if the callback() did malloc/strdup/etc for its return value
// the caller of callback() would have no way to know whether it needed freeing
// or not, so that string would be leaked.
//
// For future consideration I'd propose a model where callback() is expected
// to always strdup() its return value, so the value returned by callback()
// would either be NULL or it would be a string that needs free()ed. It seems
// to me this might be required if the strings become more dynamic than the
// simple true/false values seen in the current code. It'll be an easy change,
// callback() is only used two places.
int opal_infosubscribe_subscribe(opal_infosubscriber_t *object, char *key, char *value, opal_key_interest_callback_t *callback)
{
    opal_list_t *list = NULL;
    opal_hash_table_t *table = &object->s_subscriber_table;
    opal_callback_list_item_t *callback_list_item;
    size_t max_len = OPAL_MAX_INFO_KEY - strlen(OPAL_INFO_SAVE_PREFIX);

    if (strlen(key) > max_len) {
        opal_output(0, "DEVELOPER WARNING: Unexpected MPI info key length [%s]: "
                    "OMPI internal callback keys are limited to %" PRIsize_t " chars.",
                    key, max_len);
#if OPAL_ENABLE_DEBUG
        opal_output(0, "Aborting because this is a developer / debugging build.  Go fix this error.");
        // Do not assert() / dump core.  Just exit un-gracefully.
        exit(1);
#else
        opal_output(0, "The \"%s\" MPI info key almost certainly will not work properly.  You should inform an Open MPI developer about this.", key);
        key[max_len] = '\0';
#endif
    }

    if (table) {
        opal_hash_table_get_value_ptr(table, key, strlen(key), (void**) &list);

        if (!list) {
            list = OBJ_NEW(opal_list_t);
            opal_hash_table_set_value_ptr(table, key, strlen(key), list);
        }

        callback_list_item = OBJ_NEW(opal_callback_list_item_t);
        callback_list_item->callback = callback;
        if (value) {
            callback_list_item->default_value = strdup(value);
        } else {
            callback_list_item->default_value = NULL;
        }

        opal_list_append(list, (opal_list_item_t*) callback_list_item);

// Trigger callback() on either the default value or the info that's in the
// object if there is one. Unfortunately there's some code duplication as
// this is similar to the job of opal_infosubscribe_change_info().
//
// The value we store for key is whatever the callback() returns.
// We also leave a backup __IN_* key with the previous value.

//  - is there an info object yet attached to this object
        if (NULL == object->s_info) {
            object->s_info = OBJ_NEW(opal_info_t);
        }
// - is there a value already associated with key in this obj's info:
//   to use in the callback()
        char *buffer = malloc(OPAL_MAX_INFO_VAL+1); // (+1 shouldn't be needed)
        char *val = value; // start as default value
        int flag = 0;
        char *updated_value;
        int err;
        opal_info_get(object->s_info, key, OPAL_MAX_INFO_VAL, buffer, &flag);
        if (flag) {
            val = buffer; // become info value if this key was in info
        }
// - callback() and modify the val in info
        updated_value = callback(object, key, val);
        if (updated_value) {
            err = opal_info_set(object->s_info, key, updated_value);
        } else {
            err = opal_info_delete(object->s_info, key);
            err = OPAL_SUCCESS; // we don't care if the key was found or not
        }
        if (OPAL_SUCCESS != err) {
            free(buffer);
            return err;
        }
// - save the previous val under key __IN_*
//   This function might be called separately for the same key multiple
//   times (multiple modules might register an interest in the same key),
//   so we only save __IN_<key> for the first.
//   Note we're saving the first k/v regardless of whether it was the default
//   or whether it came from info. This means system settings will show
//   up if the user queries later with get_info.
        save_original_key_val(object->s_info, key, val, 0);

        free(buffer);
    } else {
/*
 * TODO: This should not happen
 */
    }

    return OPAL_SUCCESS;
}

/*
    OBJ_DESTRUCT(&opal_comm_info_hashtable);
    OBJ_DESTRUCT(&opal_win_info_hashtable);
    OBJ_DESTRUCT(&opal_file_info_hashtable);
*/
