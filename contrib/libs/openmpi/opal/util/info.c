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
 * Copyright (c) 2007-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2012-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2018 IBM Corporation. All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
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

#include "opal/util/info.h"

/*
 * Local functions
 */
static void info_constructor(opal_info_t *info);
static void info_destructor(opal_info_t *info);
static void info_entry_constructor(opal_info_entry_t *entry);
static void info_entry_destructor(opal_info_entry_t *entry);
static opal_info_entry_t *info_find_key (opal_info_t *info, const char *key);


/*
 * opal_info_t classes
 */
OBJ_CLASS_INSTANCE(opal_info_t,
                   opal_list_t,
                   info_constructor,
                   info_destructor);

/*
 * opal_info_entry_t classes
 */
OBJ_CLASS_INSTANCE(opal_info_entry_t,
                   opal_list_item_t,
                   info_entry_constructor,
                   info_entry_destructor);



/*
 * Duplicate an info
 */
int opal_info_dup (opal_info_t *info, opal_info_t **newinfo)
{
    int err;
    opal_info_entry_t *iterator;

    OPAL_THREAD_LOCK(info->i_lock);
    OPAL_LIST_FOREACH(iterator, &info->super, opal_info_entry_t) {
        err = opal_info_set(*newinfo, iterator->ie_key, iterator->ie_value);
        if (OPAL_SUCCESS != err) {
            OPAL_THREAD_UNLOCK(info->i_lock);
            return err;
        }
     }
    OPAL_THREAD_UNLOCK(info->i_lock);
    return OPAL_SUCCESS;
}

static void opal_info_get_nolock (opal_info_t *info, const char *key, int valuelen,
                                 char *value, int *flag)
{
    opal_info_entry_t *search;
    int value_length;

    search = info_find_key (info, key);
    if (NULL == search){
        *flag = 0;
    } else if (value && valuelen) {
        /*
         * We have found the element, so we can return the value
         * Set the flag, value_length and value
         */
         *flag = 1;
         value_length = strlen(search->ie_value);
         /*
          * If the stored value is shorter than valuelen, then
          * we can copy the entire value out. Else, we have to
          * copy ONLY valuelen bytes out
          */
          if (value_length < valuelen ) {
               strcpy(value, search->ie_value);
          } else {
               opal_strncpy(value, search->ie_value, valuelen);
               if (OPAL_MAX_INFO_VAL == valuelen) {
                   value[valuelen-1] = 0;
               } else {
                   value[valuelen] = 0;
               }
          }
    }
}

static int opal_info_set_nolock (opal_info_t *info, const char *key, const char *value)
{
    char *new_value;
    opal_info_entry_t *new_info;
    opal_info_entry_t *old_info;

    new_value = strdup(value);
    if (NULL == new_value) {
      return OPAL_ERR_OUT_OF_RESOURCE;
    }

    old_info = info_find_key (info, key);
    if (NULL != old_info) {
        /*
         * key already exists. remove the value associated with it
         */
        free(old_info->ie_value);
        old_info->ie_value = new_value;
    } else {
        new_info = OBJ_NEW(opal_info_entry_t);
        if (NULL == new_info) {
            free(new_value);
            OPAL_THREAD_UNLOCK(info->i_lock);
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        strncpy (new_info->ie_key, key, OPAL_MAX_INFO_KEY);
        new_info->ie_value = new_value;
        opal_list_append (&(info->super), (opal_list_item_t *) new_info);
    }
    return OPAL_SUCCESS;
}
/*
 * An object's info can be set, but those settings can be modified by
 * system callbacks. When those callbacks happen, we save a "__IN_<key>"/"val"
 * copy of changed or erased values.
 *
 * extra options for how to dup:
 *   include_system_extras (default 1)
 *   omit_ignored          (default 1)
 *   show_modifications    (default 0)
 */
static
int opal_info_dup_mode (opal_info_t *info, opal_info_t **newinfo,
    int include_system_extras,  // (k/v with no corresponding __IN_k)
    int omit_ignored,           // (__IN_k with no k/v)
    int show_modifications)     // (pick v from k/v or __IN_k/v)
{
    int err, flag;
    opal_info_entry_t *iterator;
    char savedkey[OPAL_MAX_INFO_KEY + 1]; // iterator->ie_key has this as its size
    char savedval[OPAL_MAX_INFO_VAL];
    char *valptr, *pkey;
    int is_IN_key;
    int exists_IN_key, exists_reg_key;

    OPAL_THREAD_LOCK(info->i_lock);
    OPAL_LIST_FOREACH(iterator, &info->super, opal_info_entry_t) {
// If we see an __IN_<key> key but no <key>, decide what to do based on mode.
// If we see an __IN_<key> and a <key>, skip since it'll be handled when
// we process <key>.
         is_IN_key = 0;
         exists_IN_key = 0;
         exists_reg_key = 0;
         pkey = iterator->ie_key;
         if (0 == strncmp(iterator->ie_key, OPAL_INFO_SAVE_PREFIX,
             strlen(OPAL_INFO_SAVE_PREFIX)))
        {
             pkey += strlen(OPAL_INFO_SAVE_PREFIX);

             is_IN_key = 1;
             exists_IN_key = 1;
             opal_info_get_nolock (info, pkey, 0, NULL, &flag);
             if (flag) {
                 exists_reg_key = 1;
             }
         } else {
             is_IN_key = 0;
             exists_reg_key = 1;

// see if there is an __IN_<key> for the current <key>
             if (strlen(OPAL_INFO_SAVE_PREFIX) + strlen(pkey) < OPAL_MAX_INFO_KEY) {
                 snprintf(savedkey, OPAL_MAX_INFO_KEY+1,
                     OPAL_INFO_SAVE_PREFIX "%s", pkey);
// (the prefix macro is a string, so the unreadable part above is a string concatenation)
                 opal_info_get_nolock (info, savedkey, OPAL_MAX_INFO_VAL,
                                       savedval, &flag);
             } else {
                 flag = 0;
             }
             if (flag) {
                 exists_IN_key = 1;
             }
         }

         if (is_IN_key) {
             if (exists_reg_key) {
// we're processing __IN_<key> and there exists a <key> so we'll handle it then
                 continue;
             } else {
// we're processing __IN_<key> and no <key> exists
// this would mean <key> was set by the user but ignored by the system
// so base our behavior on the omit_ignored
                 if (!omit_ignored) {
                     err = opal_info_set_nolock(*newinfo, pkey, iterator->ie_value);
                     if (OPAL_SUCCESS != err) {
                         OPAL_THREAD_UNLOCK(info->i_lock);
                         return err;
                     }
                 }
             }
         } else {
             valptr = 0;
             if (!exists_IN_key) {
// we're processing <key> and no __IN_<key> <key> exists
// this would mean it's a system setting, not something that came from the user
                 if (include_system_extras) {
                     valptr = iterator->ie_value;
                 }
             } else {
// we're processing <key> and __IN_<key> also exists
// pick which value to use
                 if (!show_modifications) {
                     valptr = savedval;
                 } else {
                     valptr = iterator->ie_value;
                 }
             }
             if (valptr) {
                 err = opal_info_set_nolock(*newinfo, pkey, valptr);
                 if (OPAL_SUCCESS != err) {
                     OPAL_THREAD_UNLOCK(info->i_lock);
                     return err;
                 }
             }
         }
     }
     OPAL_THREAD_UNLOCK(info->i_lock);
     return OPAL_SUCCESS;
}

/*
 * Implement opal_info_dup_mpistandard by using whatever mode
 * settings represent our interpretation of the standard
 */
int opal_info_dup_mpistandard (opal_info_t *info, opal_info_t **newinfo)
{
    return opal_info_dup_mode (info, newinfo, 1, 1, 0);
}

/*
 * Set a value on the info
 */
int opal_info_set (opal_info_t *info, const char *key, const char *value)
{
    int ret;

    OPAL_THREAD_LOCK(info->i_lock);
    ret = opal_info_set_nolock(info, key, value);
    OPAL_THREAD_UNLOCK(info->i_lock);
    return ret;
}


int opal_info_set_value_enum (opal_info_t *info, const char *key, int value,
                              mca_base_var_enum_t *var_enum)
{
    char *string_value;
    int ret;

    ret = var_enum->string_from_value (var_enum, value, &string_value);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    return opal_info_set (info, key, string_value);
}


/*
 * Get a value from an info
 */
int opal_info_get (opal_info_t *info, const char *key, int valuelen,
                   char *value, int *flag)
{
    OPAL_THREAD_LOCK(info->i_lock);
    opal_info_get_nolock(info, key, valuelen, value, flag);
    OPAL_THREAD_UNLOCK(info->i_lock);
    return OPAL_SUCCESS;
}

int opal_info_get_value_enum (opal_info_t *info, const char *key, int *value,
                              int default_value, mca_base_var_enum_t *var_enum,
                              int *flag)
{
    opal_info_entry_t *search;
    int ret;

    *value = default_value;

    OPAL_THREAD_LOCK(info->i_lock);
    search = info_find_key (info, key);
    if (NULL == search){
        OPAL_THREAD_UNLOCK(info->i_lock);
        *flag = 0;
        return OPAL_SUCCESS;
    }

    /* we found a mathing key. pass the string value to the enumerator and
     * return */
    *flag = 1;

    ret = var_enum->value_from_string (var_enum, search->ie_value, value);
    OPAL_THREAD_UNLOCK(info->i_lock);

    return ret;
}


/*
 * Similar to opal_info_get(), but cast the result into a boolean
 * using some well-defined rules.
 */
int opal_info_get_bool(opal_info_t *info, char *key, bool *value, int *flag)
{
    char str[256];

    str[sizeof(str) - 1] = '\0';
    opal_info_get(info, key, sizeof(str) - 1, str, flag);
    if (*flag) {
        *value = opal_str_to_bool(str);
    }

    return OPAL_SUCCESS;
}


bool
opal_str_to_bool(char *str)
{
    bool result = false;
    char *ptr;

    /* Trim whitespace */
    ptr = str + sizeof(str) - 1;
    while (ptr >= str && isspace(*ptr)) {
        *ptr = '\0';
        --ptr;
    }
    ptr = str;
    while (ptr < str + sizeof(str) - 1 && *ptr != '\0' &&
           isspace(*ptr)) {
        ++ptr;
    }
    if ('\0' != *ptr) {
        if (isdigit(*ptr)) {
            result = (bool) atoi(ptr);
        } else if (0 == strcasecmp(ptr, "yes") ||
                   0 == strcasecmp(ptr, "true")) {
            result = true;
        } else if (0 != strcasecmp(ptr, "no") &&
                   0 != strcasecmp(ptr, "false")) {
            /* RHC unrecognized value -- print a warning? */
        }
    }
    return result;
}

/*
 * Delete a key from an info
 */
int opal_info_delete(opal_info_t *info, const char *key)
{
    opal_info_entry_t *search;

    OPAL_THREAD_LOCK(info->i_lock);
    search = info_find_key (info, key);
    if (NULL == search){
         OPAL_THREAD_UNLOCK(info->i_lock);
         return OPAL_ERR_NOT_FOUND;
    } else {
         /*
          * An entry with this key value was found. Remove the item
          * and free the memory allocated to it.
          * As this key *must* be available, we do not check for errors.
          */
          opal_list_remove_item (&(info->super),
                                 (opal_list_item_t *)search);
          OBJ_RELEASE(search);
    }
    OPAL_THREAD_UNLOCK(info->i_lock);
    return OPAL_SUCCESS;
}


/*
 * Return the length of a value
 */
int opal_info_get_valuelen (opal_info_t *info, const char *key, int *valuelen,
                            int *flag)
{
    opal_info_entry_t *search;

    OPAL_THREAD_LOCK(info->i_lock);
    search = info_find_key (info, key);
    if (NULL == search){
        *flag = 0;
    } else {
        /*
         * We have found the element, so we can return the value
         * Set the flag, value_length and value
         */
         *flag = 1;
         *valuelen = strlen(search->ie_value);
    }
    OPAL_THREAD_UNLOCK(info->i_lock);
    return OPAL_SUCCESS;
}


/*
 * Get the nth key
 */
int opal_info_get_nthkey (opal_info_t *info, int n, char *key)
{
    opal_info_entry_t *iterator;

    /*
     * Iterate over and over till we get to the nth key
     */
    OPAL_THREAD_LOCK(info->i_lock);
    for (iterator = (opal_info_entry_t *)opal_list_get_first(&(info->super));
         n > 0;
         --n) {
         iterator = (opal_info_entry_t *)opal_list_get_next(iterator);
         if (opal_list_get_end(&(info->super)) ==
             (opal_list_item_t *) iterator) {
             OPAL_THREAD_UNLOCK(info->i_lock);
             return OPAL_ERR_BAD_PARAM;
         }
    }
    /*
     * iterator is of the type opal_list_item_t. We have to
     * cast it to opal_info_entry_t before we can use it to
     * access the value
     */
    strncpy(key, iterator->ie_key, OPAL_MAX_INFO_KEY);
    OPAL_THREAD_UNLOCK(info->i_lock);
    return OPAL_SUCCESS;
}



/*
 * This function is invoked when OBJ_NEW() is called. Here, we add this
 * info pointer to the table and then store its index as the handle
 */
static void info_constructor(opal_info_t *info)
{
    info->i_lock = OBJ_NEW(opal_mutex_t);
}

/*
 * This function is called during OBJ_DESTRUCT of "info". When this
 * done, we need to remove the entry from the opal fortran to C
 * translation table
 */
static void info_destructor(opal_info_t *info)
{
    opal_list_item_t *item;
    opal_info_entry_t *iterator;

    /* Remove every key in the list */

    for (item = opal_list_remove_first(&(info->super));
         NULL != item;
         item = opal_list_remove_first(&(info->super))) {
        iterator = (opal_info_entry_t *) item;
        OBJ_RELEASE(iterator);
    }

    /* Release the lock */

    OBJ_RELEASE(info->i_lock);
}


/*
 * opal_info_entry_t interface functions
 */
static void info_entry_constructor(opal_info_entry_t *entry)
{
    memset(entry->ie_key, 0, sizeof(entry->ie_key));
    entry->ie_key[OPAL_MAX_INFO_KEY] = 0;
}


static void info_entry_destructor(opal_info_entry_t *entry)
{
    if (NULL != entry->ie_value) {
        free(entry->ie_value);
    }
}


/*
 * Find a key
 *
 * Do NOT thread lock in here -- the calling function is responsible
 * for that.
 */
static opal_info_entry_t *info_find_key (opal_info_t *info, const char *key)
{
    opal_info_entry_t *iterator;

    /* No thread locking in here! */

    /* Iterate over all the entries. If the key is found, then
     * return immediately. Else, the loop will fall of the edge
     * and NULL is returned
     */
    OPAL_LIST_FOREACH(iterator, &info->super, opal_info_entry_t) {
        if (0 == strcmp(key, iterator->ie_key)) {
            return iterator;
        }
    }
    return NULL;
}


int
opal_info_value_to_int(char *value, int *interp)
{
    long tmp;
    char *endp;

    if (NULL == value || '\0' == value[0]) return OPAL_ERR_BAD_PARAM;

    errno = 0;
    tmp = strtol(value, &endp, 10);
    /* we found something not a number */
    if (*endp != '\0') return OPAL_ERR_BAD_PARAM;
    /* underflow */
    if (tmp == 0 && errno == EINVAL) return OPAL_ERR_BAD_PARAM;

    *interp = (int) tmp;

    return OPAL_SUCCESS;
}


int
opal_info_value_to_bool(char *value, bool *interp)
{
    int tmp;

    /* idiot case */
    if (NULL == value || NULL == interp) return OPAL_ERR_BAD_PARAM;

    /* is it true / false? */
    if (0 == strcmp(value, "true")) {
        *interp = true;
        return OPAL_SUCCESS;
    } else if (0 == strcmp(value, "false")) {
        *interp = false;
        return OPAL_SUCCESS;

    /* is it a number? */
    } else if (OPAL_SUCCESS == opal_info_value_to_int(value, &tmp)) {
        if (tmp == 0) {
            *interp = false;
        } else {
            *interp = true;
        }
        return OPAL_SUCCESS;
    }

    return OPAL_ERR_BAD_PARAM;
}
