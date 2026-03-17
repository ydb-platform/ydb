/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2013-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Bull SAS.  All rights reserved.
 * Copyright (c) 2015      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal/mca/base/mca_base_pvar.h"
#include "opal/mca/base/mca_base_vari.h"

#include <stddef.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "opal/class/opal_pointer_array.h"
#include "opal/class/opal_hash_table.h"

static opal_hash_table_t mca_base_pvar_index_hash;
static opal_pointer_array_t registered_pvars;
static bool mca_base_pvar_initialized = false;
static int pvar_count = 0;

#define min(a,b) ((a) < (b) ? (a) : (b))
#define max(a,b) ((a) > (b) ? (a) : (b))

static int mca_base_pvar_get_internal (int index, mca_base_pvar_t **pvar, bool invalidok);

/* string representations of class names */
static const char *pvar_class_names[] = {
    "state",
    "level",
    "size",
    "percentage",
    "high watermark",
    "low watermark",
    "counter",
    "aggregate",
    "timer",
    "generic"
};

int mca_base_pvar_init (void)
{
    int ret = OPAL_SUCCESS;

    if (!mca_base_pvar_initialized) {
        mca_base_pvar_initialized = true;

        OBJ_CONSTRUCT(&registered_pvars, opal_pointer_array_t);
        opal_pointer_array_init(&registered_pvars, 128, 2048, 128);

        OBJ_CONSTRUCT(&mca_base_pvar_index_hash, opal_hash_table_t);
        ret = opal_hash_table_init (&mca_base_pvar_index_hash, 1024);
        if (OPAL_SUCCESS != ret) {
            mca_base_pvar_initialized = false;
            OBJ_DESTRUCT(&registered_pvars);
            OBJ_DESTRUCT(&mca_base_pvar_index_hash);
        }
    }

    return ret;
}

int mca_base_pvar_find (const char *project, const char *framework, const char *component, const char *name)
{
    char *full_name;
    int ret, index;

    ret = mca_base_var_generate_full_name4 (NULL, framework, component, name, &full_name);
    if (OPAL_SUCCESS != ret) {
        return OPAL_ERROR;
    }

    ret = mca_base_pvar_find_by_name (full_name, MCA_BASE_PVAR_CLASS_ANY, &index);
    free (full_name);

    /* NTH: should we verify the name components match the returned variable? */

    return (OPAL_SUCCESS != ret) ? ret : index;
}

int mca_base_pvar_find_by_name (const char *full_name, int var_class, int *index)
{
    mca_base_pvar_t *pvar;
    void *tmp;
    int rc;

    rc = opal_hash_table_get_value_ptr (&mca_base_pvar_index_hash, full_name, strlen (full_name),
                                        &tmp);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    rc = mca_base_pvar_get_internal ((int)(uintptr_t) tmp, &pvar, false);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    if (MCA_BASE_PVAR_CLASS_ANY != var_class && pvar->var_class != var_class) {
        return OPAL_ERR_NOT_FOUND;
    }

    *index = (int)(uintptr_t) tmp;

    return OPAL_SUCCESS;
}

int mca_base_pvar_finalize (void)
{
    int i;

    if (mca_base_pvar_initialized)  {
        mca_base_pvar_initialized = false;

        for (i = 0 ; i < pvar_count ; ++i) {
            mca_base_pvar_t *pvar = opal_pointer_array_get_item (&registered_pvars, i);
            if (pvar) {
                OBJ_RELEASE(pvar);
            }
        }

        pvar_count = 0;

        OBJ_DESTRUCT(&registered_pvars);
        OBJ_DESTRUCT(&mca_base_pvar_index_hash);
    }

    return OPAL_SUCCESS;
}

int mca_base_pvar_get_count (int *count)
{
    *count = pvar_count;
    return OPAL_SUCCESS;
}

static int mca_base_pvar_default_get_value (const mca_base_pvar_t *pvar, void *value, void *obj_handle)
{
    /* not used */
    (void) obj_handle;

    memmove (value, pvar->ctx, ompi_var_type_sizes[pvar->type]);

    return OPAL_SUCCESS;
}

static int mca_base_pvar_default_set_value (mca_base_pvar_t *pvar, const void *value, void *obj_handle)
{
    /* not used */
    (void) obj_handle;

    memmove (pvar->ctx, value, ompi_var_type_sizes[pvar->type]);

    return OPAL_SUCCESS;
}

static int mca_base_pvar_notify_ignore (mca_base_pvar_t *pvar, mca_base_pvar_event_t event, void *obj_handle, int *count)
{
    /* silence compiler warnings */
    (void) pvar;
    (void) obj_handle;

    /* default is only one value */
    if (MCA_BASE_PVAR_HANDLE_BIND == event) {
        *count = 1;
    }

    return OPAL_SUCCESS;
}

int mca_base_pvar_register (const char *project, const char *framework, const char *component, const char *name,
                            const char *description, mca_base_var_info_lvl_t verbosity,
                            int var_class, mca_base_var_type_t type, mca_base_var_enum_t *enumerator,
                            int bind, mca_base_pvar_flag_t flags, mca_base_get_value_fn_t get_value,
                            mca_base_set_value_fn_t set_value, mca_base_notify_fn_t notify, void *ctx)
{
    int ret, group_index, pvar_index;
    mca_base_pvar_t *pvar;

    /* assert on usage errors */
    if (!get_value && !ctx) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* ensure the caller did not set an invalid flag */
    assert (!(flags & 0x3f));

    flags &= ~MCA_BASE_PVAR_FLAG_INVALID;

    /* check that the datatype matches what is permitted for the variable class */
    switch (var_class) {
    case MCA_BASE_PVAR_CLASS_STATE:
        /* states MUST be integers */
        if (MCA_BASE_VAR_TYPE_INT != type) {
            return OPAL_ERR_BAD_PARAM;
        }
        break;
    case MCA_BASE_PVAR_CLASS_COUNTER:
        /* counters can have the any of types in the fall-through except double */
        if (MCA_BASE_VAR_TYPE_DOUBLE == type) {
            return OPAL_ERR_BAD_PARAM;
        }
        /* fall-through */
    case MCA_BASE_PVAR_CLASS_LEVEL:
    case MCA_BASE_PVAR_CLASS_SIZE:
    case MCA_BASE_PVAR_CLASS_HIGHWATERMARK:
    case MCA_BASE_PVAR_CLASS_LOWWATERMARK:
    case MCA_BASE_PVAR_CLASS_AGGREGATE:
    case MCA_BASE_PVAR_CLASS_TIMER:
        if (MCA_BASE_VAR_TYPE_UNSIGNED_INT != type &&
            MCA_BASE_VAR_TYPE_UNSIGNED_LONG != type &&
            MCA_BASE_VAR_TYPE_UNSIGNED_LONG_LONG != type &&
            MCA_BASE_VAR_TYPE_DOUBLE != type) {
            return OPAL_ERR_BAD_PARAM;
        }
        break;
    case MCA_BASE_PVAR_CLASS_PERCENTAGE:
        /* percentages must be doubles */
        if (MCA_BASE_VAR_TYPE_DOUBLE != type) {
            return OPAL_ERR_BAD_PARAM;
        }
        break;
    case MCA_BASE_PVAR_CLASS_GENERIC:
        /* there are no additional restrictions on the type of generic
           variables */
        break;
    default:
        return OPAL_ERR_BAD_PARAM;
    }

    /* update this assert if more MPIT verbosity levels are added */
    assert (verbosity >= OPAL_INFO_LVL_1 && verbosity <= OPAL_INFO_LVL_9);

    /* check if this variable is already registered */
    ret = mca_base_pvar_find (project, framework, component, name);
    if (OPAL_SUCCESS <= ret) {
        ret = mca_base_pvar_get_internal (ret, &pvar, true);
        if (OPAL_SUCCESS != ret) {
            /* inconsistent internal state */
            return OPAL_ERROR;
        }

        if (pvar->enumerator) {
            OBJ_RELEASE(pvar->enumerator);
        }
    } else {
        /* find/register an MCA parameter group for this performance variable */
        group_index = mca_base_var_group_register (project, framework, component, NULL);
        if (-1 > group_index) {
            return group_index;
        }

        /* create a new parameter entry */
        pvar = OBJ_NEW(mca_base_pvar_t);
        if (NULL == pvar) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }

        do {
            /* generate the variable's full name */
            ret = mca_base_var_generate_full_name4 (NULL, framework, component, name, &pvar->name);
            if (OPAL_SUCCESS != ret) {
                ret = OPAL_ERR_OUT_OF_RESOURCE;
                break;
            }

            if (NULL != description) {
                pvar->description = strdup(description);
                if (NULL == pvar->description) {
                    ret = OPAL_ERR_OUT_OF_RESOURCE;
                    break;
                }
            }

            pvar_index = opal_pointer_array_add (&registered_pvars, pvar);
            if (0 > pvar_index) {
                break;
            }
            pvar->pvar_index = pvar_index;

            /* add this performance variable to the MCA variable group */
            if (0 <= group_index) {
                ret = mca_base_var_group_add_pvar (group_index, pvar_index);
                if (0 > ret) {
                    break;
                }
            }

            pvar->pvar_index = pvar_count;
            opal_hash_table_set_value_ptr (&mca_base_pvar_index_hash, pvar->name, strlen (pvar->name),
                                           (void *)(uintptr_t) pvar->pvar_index);

            pvar_count++;
            ret = OPAL_SUCCESS;
        } while (0);

        if (OPAL_SUCCESS != ret) {
            OBJ_RELEASE(pvar);
            return ret;
        }

        pvar->group_index = group_index;
    }

    pvar->verbosity   = verbosity;
    pvar->var_class   = var_class;
    pvar->type        = type;
    pvar->enumerator  = enumerator;
    if (enumerator) {
        OBJ_RETAIN(enumerator);
    }

    pvar->bind = bind;
    pvar->flags = flags;

    pvar->get_value = get_value ? get_value : mca_base_pvar_default_get_value;
    pvar->notify = notify ? notify : mca_base_pvar_notify_ignore;

    if (!(flags & MCA_BASE_PVAR_FLAG_READONLY)) {
        pvar->set_value = set_value ? set_value : mca_base_pvar_default_set_value;
    }

    pvar->ctx        = ctx;

    return pvar->pvar_index;
}

int mca_base_component_pvar_register (const mca_base_component_t *component, const char *name,
                                      const char *description, mca_base_var_info_lvl_t verbosity,
                                      int var_class, mca_base_var_type_t type, mca_base_var_enum_t *enumerator,
                                      int bind, mca_base_pvar_flag_t flags, mca_base_get_value_fn_t get_value,
                                      mca_base_set_value_fn_t set_value, mca_base_notify_fn_t notify, void *ctx)
{
    /* invalidate this variable if the component's group is deregistered */
    return mca_base_pvar_register(component->mca_project_name, component->mca_type_name, component->mca_component_name,
                                  name, description, verbosity, var_class, type, enumerator, bind,
                                  flags | MCA_BASE_PVAR_FLAG_IWG, get_value, set_value, notify, ctx);
}

static int mca_base_pvar_get_internal (int index, mca_base_pvar_t **pvar, bool invalidok)
{
    if (index >= pvar_count) {
        return OPAL_ERR_VALUE_OUT_OF_BOUNDS;
    }

    *pvar = opal_pointer_array_get_item (&registered_pvars, index);

    /* variables should never be removed per MPI 3.0 § 14.3.7 */
    assert (*pvar);

    if (((*pvar)->flags & MCA_BASE_PVAR_FLAG_INVALID) && !invalidok) {
        *pvar = NULL;
        return OPAL_ERR_VALUE_OUT_OF_BOUNDS;
    }

    return OPAL_SUCCESS;
}

int mca_base_pvar_get (int index, const mca_base_pvar_t **pvar)
{
    return mca_base_pvar_get_internal (index, (mca_base_pvar_t **) pvar, false);
}

int mca_base_pvar_mark_invalid (int index)
{
    mca_base_pvar_t *pvar;
    int ret;

    ret = mca_base_pvar_get_internal (index, &pvar, false);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    pvar->flags |= MCA_BASE_PVAR_FLAG_INVALID;

    return OPAL_SUCCESS;
}

int mca_base_pvar_notify (mca_base_pvar_handle_t *handle, mca_base_pvar_event_t event, int *count)
{
    if (mca_base_pvar_is_invalid (handle->pvar)) {
        return OPAL_ERR_NOT_BOUND;
    }

    return handle->pvar->notify (handle->pvar, event, handle->obj_handle, count);
}

int mca_base_pvar_update_all_handles (int index, const void *obj)
{
    mca_base_pvar_handle_t *handle, *next;
    mca_base_pvar_t *pvar;
    int ret;

    ret = mca_base_pvar_get_internal (index, &pvar, false);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    if (0 == opal_list_get_size (&pvar->bound_handles)) {
        /* nothing to do */
        return OPAL_SUCCESS;
    }

    /* TODO -- probably need to add a handle/variable lock */
    OPAL_LIST_FOREACH_SAFE(handle, next, &pvar->bound_handles, mca_base_pvar_handle_t) {
        handle = (mca_base_pvar_handle_t *)((char *) handle - offsetof (mca_base_pvar_handle_t, list2));

        if (handle->obj_handle != obj) {
            continue;
        }

        (void) mca_base_pvar_handle_update (handle);
    }

    return OPAL_SUCCESS;
}

int mca_base_pvar_handle_alloc (mca_base_pvar_session_t *session, int index, void *obj_handle,
                                mca_base_pvar_handle_t **handle, int *count)
{
    mca_base_pvar_handle_t *pvar_handle = NULL;
    size_t datatype_size;
    mca_base_pvar_t *pvar;
    int ret;

    do {
        /* find the requested performance variable */
        ret = mca_base_pvar_get_internal (index, &pvar, false);
        if (OPAL_SUCCESS != ret) {
            break;
        }

        if (0 == pvar->bind) {
            /* ignore binding object */
            obj_handle = NULL;
        } else if (0 != pvar->bind && NULL == obj_handle) {
            /* this is an application error. what is the correct error code? */
            ret = OPAL_ERR_BAD_PARAM;
            break;
        }

        /* allocate and initialize the handle */
        pvar_handle = OBJ_NEW(mca_base_pvar_handle_t);
        if (NULL == pvar_handle) {
            ret = OPAL_ERR_OUT_OF_RESOURCE;
            break;
        }

        pvar_handle->obj_handle = (NULL == obj_handle ? NULL : *(void**)obj_handle);
        pvar_handle->pvar = pvar;

        *handle = pvar_handle;

        /* notify the variable that a handle has been bound and determine
           how many values this handle has. NTH: finding the count should
           probably be pushed into a separate function. */
        ret = mca_base_pvar_notify (pvar_handle, MCA_BASE_PVAR_HANDLE_BIND, count);
        if (0 > ret) {
            ret = OPAL_ERROR;
            break;
        }

        pvar_handle->count = *count;

        /* get the size of this datatype since read functions will expect an
           array of datatype not mca_base_pvar_value_t's. */
        datatype_size = ompi_var_type_sizes[pvar->type];
        if (0 == datatype_size) {
            ret = OPAL_ERROR;
            break;
        }

        if (!mca_base_pvar_is_continuous (pvar) || mca_base_pvar_is_sum (pvar) ||
            mca_base_pvar_is_watermark (pvar)) {
            /* if a variable is not continuous we will need to keep track of its last value
               to support start->stop->read correctly. use calloc to initialize the current
               value to 0. */
            pvar_handle->current_value = calloc (*count, datatype_size);
            if (NULL == pvar_handle->current_value) {
                ret = OPAL_ERR_OUT_OF_RESOURCE;
                break;
            }
        }

        if (mca_base_pvar_is_sum (pvar) || mca_base_pvar_is_watermark (pvar)) {
            /* for sums (counters, timers, etc) we need to keep track of
               what the last value of the underlying counter was. this allows
               us to push the computation of handle values from the event(s)
               (which could be in a critical path) to pvar read/stop/reset/etc */
            pvar_handle->tmp_value = calloc (*count, datatype_size);
            if (NULL == pvar_handle->tmp_value) {
                ret = OPAL_ERR_OUT_OF_RESOURCE;
                break;
            }

            pvar_handle->last_value = calloc (*count, datatype_size);
            if (NULL == pvar_handle->last_value) {
                ret = OPAL_ERR_OUT_OF_RESOURCE;
                break;
            }

            /* get the current value of the performance variable if this is a
               continuous sum or watermark. if this variable needs to be started first the
               current value is not relevant. */
            if (mca_base_pvar_is_continuous (pvar)) {
                if (mca_base_pvar_is_sum (pvar)) {
                    ret = pvar->get_value (pvar, pvar_handle->last_value, pvar_handle->obj_handle);
                } else {
                    /* the initial value of a watermark is the current value of the variable */
                    ret = pvar->get_value (pvar, pvar_handle->current_value, pvar_handle->obj_handle);
                }

                if (OPAL_SUCCESS != ret) {
                    return ret;
                }
            }
        }

        pvar_handle->session = session;

        /* the handle is ready. add it to the appropriate lists */
        opal_list_append (&session->handles, &pvar_handle->super);
        opal_list_append (&pvar->bound_handles, &pvar_handle->list2);

        if (mca_base_pvar_is_continuous (pvar)) {
            /* mark this variable as started */
            pvar_handle->started = true;
        }

        ret = OPAL_SUCCESS;
    } while (0);

    if (OPAL_SUCCESS != ret && pvar_handle) {
        OBJ_RELEASE(pvar_handle);
    }

    return ret;
}

int mca_base_pvar_handle_free (mca_base_pvar_handle_t *handle)
{
    OBJ_RELEASE(handle);

    return OPAL_SUCCESS;
}

int mca_base_pvar_handle_update (mca_base_pvar_handle_t *handle)
{
    int i, ret;
    void *tmp;

    if (mca_base_pvar_is_invalid (handle->pvar)) {
        return OPAL_ERR_NOT_BOUND;
    }

    if (!mca_base_pvar_handle_is_running (handle)) {
        return OPAL_SUCCESS;
    }

    if (mca_base_pvar_is_sum (handle->pvar) || mca_base_pvar_is_watermark (handle->pvar)) {
        ret = handle->pvar->get_value (handle->pvar, handle->tmp_value, handle->obj_handle);
        if (OPAL_SUCCESS != ret) {
            return OPAL_ERROR;
        }

        if (mca_base_pvar_is_sum (handle->pvar)) {
            for (i = 0 ; i < handle->count ; ++i) {
                /* the instance started at 0. need to subract the initial value off the
                   result. */
                switch (handle->pvar->type) {
                case MCA_BASE_VAR_TYPE_UNSIGNED_INT:
                    ((unsigned *) handle->current_value)[i] += ((unsigned *) handle->tmp_value)[i] -
                        ((unsigned *) handle->last_value)[i];
                    break;
                case MCA_BASE_VAR_TYPE_UNSIGNED_LONG:
                    ((unsigned long *) handle->current_value)[i] += ((unsigned long *) handle->tmp_value)[i] -
                        ((unsigned long *) handle->last_value)[i];
                    break;
                case MCA_BASE_VAR_TYPE_UNSIGNED_LONG_LONG:
                    ((unsigned long long *) handle->current_value)[i] += ((unsigned long long *) handle->tmp_value)[i] -
                        ((unsigned long long *) handle->last_value)[i];
                    break;
                case MCA_BASE_VAR_TYPE_DOUBLE:
                    ((double *) handle->current_value)[i] += ((double *) handle->tmp_value)[i] -
                        ((double *) handle->last_value)[i];
                    break;
                default:
                    /* shouldn't happen */
                    break;
                }
            }

            tmp = handle->tmp_value;
            handle->tmp_value = handle->last_value;
            handle->last_value = tmp;
        } else {
            for (i = 0 ; i < handle->count ; ++i) {
                if (MCA_BASE_PVAR_CLASS_LOWWATERMARK == handle->pvar->var_class) {
                    switch (handle->pvar->type) {
                    case MCA_BASE_VAR_TYPE_UNSIGNED_INT:
                        ((unsigned *) handle->current_value)[i] = min(((unsigned *) handle->tmp_value)[i],
                                                                      ((unsigned *) handle->current_value)[i]);
                        break;
                    case MCA_BASE_VAR_TYPE_UNSIGNED_LONG:
                        ((unsigned long *) handle->current_value)[i] = min(((unsigned long *) handle->tmp_value)[i],
                                                                           ((unsigned long *) handle->current_value)[i]);
                        break;
                    case MCA_BASE_VAR_TYPE_UNSIGNED_LONG_LONG:
                        ((unsigned long long *) handle->current_value)[i] = min(((unsigned long long *) handle->tmp_value)[i],
                                                                                ((unsigned long long *) handle->current_value)[i]);
                        break;
                    case MCA_BASE_VAR_TYPE_DOUBLE:
                        ((double *) handle->current_value)[i] = min(((double *) handle->tmp_value)[i],
                                                                    ((double *) handle->current_value)[i]);
                        break;
                    default:
                        /* shouldn't happen */
                        break;
                    }
                } else {
                    switch (handle->pvar->type) {
                    case MCA_BASE_VAR_TYPE_UNSIGNED_INT:
                        ((unsigned *) handle->current_value)[i] = max(((unsigned *) handle->tmp_value)[i],
                                                                      ((unsigned *) handle->current_value)[i]);
                        break;
                    case MCA_BASE_VAR_TYPE_UNSIGNED_LONG:
                        ((unsigned long *) handle->current_value)[i] = max(((unsigned long *) handle->tmp_value)[i],
                                                                           ((unsigned long *) handle->current_value)[i]);
                        break;
                    case MCA_BASE_VAR_TYPE_UNSIGNED_LONG_LONG:
                        ((unsigned long long *) handle->current_value)[i] = max(((unsigned long long *) handle->tmp_value)[i],
                                                                                ((unsigned long long *) handle->current_value)[i]);
                        break;
                    case MCA_BASE_VAR_TYPE_DOUBLE:
                        ((double *) handle->current_value)[i] = max(((double *) handle->tmp_value)[i],
                                                                    ((double *) handle->current_value)[i]);
                        break;
                    default:
                        /* shouldn't happen */
                        break;
                    }
                }
            }
        }
    } else if (!mca_base_pvar_is_continuous (handle->pvar)) {
        /* cache the current value */
        ret = handle->pvar->get_value (handle->pvar, handle->current_value, handle->obj_handle);
        if (OPAL_SUCCESS != ret) {
            return ret;
        }
    }

    /* XXX -- TODO -- For watermarks this function will have to be invoked for each handle whenever the underlying value is updated. */

    return OPAL_SUCCESS;
}

int mca_base_pvar_handle_read_value (mca_base_pvar_handle_t *handle, void *value)
{
    int ret;

    if (mca_base_pvar_is_invalid (handle->pvar)) {
        return OPAL_ERR_NOT_BOUND;
    }

    /* ensure this handle's value is up to date. */
    ret = mca_base_pvar_handle_update (handle);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    if (mca_base_pvar_is_sum (handle->pvar) || mca_base_pvar_is_watermark (handle->pvar) ||
        !mca_base_pvar_handle_is_running (handle)) {
        /* read the value cached in the handle. */
        memmove (value, handle->current_value, handle->count * ompi_var_type_sizes[handle->pvar->type]);
    } else {
        /* read the value directly from the variable. */
        ret = handle->pvar->get_value (handle->pvar, value, handle->obj_handle);
    }

    return ret;
}

int mca_base_pvar_handle_write_value (mca_base_pvar_handle_t *handle, const void *value)
{
    int ret;

    if (mca_base_pvar_is_invalid (handle->pvar)) {
        return OPAL_ERR_NOT_BOUND;
    }

    if (mca_base_pvar_is_readonly (handle->pvar)) {
        return OPAL_ERR_PERM;
    }

    /* write the value directly from the variable. */
    ret = handle->pvar->set_value (handle->pvar, value, handle->obj_handle);

    ret = mca_base_pvar_handle_update (handle);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    memmove (handle->current_value, value, handle->count * ompi_var_type_sizes[handle->pvar->type]);
    /* read the value directly from the variable. */
    ret = handle->pvar->set_value (handle->pvar, value, handle->obj_handle);

    return OPAL_SUCCESS;
}

int mca_base_pvar_handle_start (mca_base_pvar_handle_t *handle)
{
    int ret;

    /* Can't start a continuous or an already started variable */
    if ((handle->pvar->flags & MCA_BASE_PVAR_FLAG_CONTINUOUS) ||
        handle->started) {
        return OPAL_ERR_NOT_SUPPORTED;
    }

    /* Notify the variable that a handle has started */
    ret = mca_base_pvar_notify (handle, MCA_BASE_PVAR_HANDLE_START, NULL);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    handle->started = true;

    if (mca_base_pvar_is_sum (handle->pvar)) {
        /* Keep track of the counter value from when this counter started. */
        ret = handle->pvar->get_value (handle->pvar, handle->last_value, handle->obj_handle);
        if (OPAL_SUCCESS != ret) {
            return ret;
        }
    } else if (mca_base_pvar_is_watermark (handle->pvar)) {
        /* Find the current watermark. is this correct in the case where a watermark is started, stopped,
           then restarted? Probably will need to add a check. */
        ret = handle->pvar->get_value (handle->pvar, handle->current_value, handle->obj_handle);
        if (OPAL_SUCCESS != ret) {
            return ret;
        }
    }

    return OPAL_SUCCESS;
}

int mca_base_pvar_handle_stop (mca_base_pvar_handle_t *handle)
{
    int ret;

    if (mca_base_pvar_is_invalid (handle->pvar)) {
        return OPAL_ERR_NOT_BOUND;
    }

    /* Can't stop a continuous or an already stopped variable */
    if (!mca_base_pvar_handle_is_running (handle) || mca_base_pvar_is_continuous (handle->pvar)) {
        return OPAL_ERR_NOT_SUPPORTED;
    }

    ret = mca_base_pvar_handle_update (handle);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    /* Notify the variable that a handle has stopped */
    (void) mca_base_pvar_notify (handle, MCA_BASE_PVAR_HANDLE_STOP, NULL);

    /* Handle is stopped */
    handle->started = false;

    return OPAL_SUCCESS;
}

int mca_base_pvar_handle_reset (mca_base_pvar_handle_t *handle)
{
    int ret = OPAL_SUCCESS;

    if (mca_base_pvar_is_invalid (handle->pvar)) {
        return OPAL_ERR_NOT_BOUND;
    }

    /* reset this handle to a state analagous to when it was created */
    if (mca_base_pvar_is_sum (handle->pvar)) {
        /* reset the running sum to 0 */
        memset (handle->current_value, 0, handle->count * ompi_var_type_sizes[handle->pvar->type]);

        if (mca_base_pvar_handle_is_running (handle)) {
            ret = handle->pvar->get_value (handle->pvar, handle->last_value, handle->obj_handle);
        }
    } else if (mca_base_pvar_handle_is_running (handle) && mca_base_pvar_is_watermark (handle->pvar)) {
            /* watermarks should get set to the current value if runnning. */

        ret = handle->pvar->get_value (handle->pvar, handle->current_value, handle->obj_handle);
    } else if (mca_base_pvar_is_readonly (handle->pvar)) {
        return OPAL_ERR_PERM;
    }
    /* NTH: TODO -- Actually write the value for variable of other types */

    return ret;
}

int mca_base_pvar_dump(int index, char ***out, mca_base_var_dump_type_t output_type)
{
    const char *framework, *component, *full_name;
    mca_base_var_group_t *group;
    int line = 0, line_count, i;
    const mca_base_pvar_t *pvar;
    int ret, enum_count = 0;
    char *tmp;

    ret = mca_base_pvar_get (index, &pvar);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    ret = mca_base_var_group_get_internal (pvar->group_index, &group, true);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    framework = group->group_framework;
    component = group->group_component ? group->group_component : "base";
    full_name = pvar->name;

    if (NULL != pvar->enumerator) {
        (void) pvar->enumerator->get_count(pvar->enumerator, &enum_count);
    }

    if (MCA_BASE_VAR_DUMP_PARSABLE == output_type) {
        line_count = 5 + !!(pvar->description) + enum_count;

        *out = (char **) calloc (line_count + 1, sizeof (char *));
        if (NULL == *out) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }

        /* build the message*/
        (void)asprintf(&tmp, "mca:%s:%s:pvar:%s:", framework, component, full_name);

        (void)asprintf(out[0] + line++, "%sclass:%s", tmp, pvar_class_names[pvar->var_class]);
        (void)asprintf(out[0] + line++, "%sread-only:%s", tmp, mca_base_pvar_is_readonly(pvar) ? "true" : "false");
        (void)asprintf(out[0] + line++, "%scontinuous:%s", tmp, mca_base_pvar_is_continuous(pvar) ? "true" : "false");
        (void)asprintf(out[0] + line++, "%satomic:%s", tmp, mca_base_pvar_is_atomic(pvar) ? "true" : "false");

        /* if it has a help message, output the help message */
        if (pvar->description) {
            (void)asprintf(out[0] + line++, "%shelp:%s", tmp, pvar->description);
        }

        if (NULL != pvar->enumerator) {
            for (i = 0 ; i < enum_count ; ++i) {
                const char *enum_string = NULL;
                int enum_value;

                ret = pvar->enumerator->get_value(pvar->enumerator, i, &enum_value,
                                                     &enum_string);
                if (OPAL_SUCCESS != ret) {
                    continue;
                }

                (void)asprintf(out[0] + line++, "%senumerator:value:%d:%s", tmp, enum_value, enum_string);
            }
        }

        (void)asprintf(out[0] + line++, "%stype:%s", tmp, ompi_var_type_names[pvar->type]);
        free(tmp);  // release tmp storage
    } else {
        /* there will be at most three lines in the pretty print case */
        *out = (char **) calloc (3, sizeof (char *));
        if (NULL == *out) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }

        (void)asprintf (out[0] + line++, "performance \"%s\" (type: %s, class: %s)", full_name,
                        ompi_var_type_names[pvar->type], pvar_class_names[pvar->var_class]);

        if (pvar->description) {
            (void)asprintf(out[0] + line++, "%s", pvar->description);
        }

        if (NULL != pvar->enumerator) {
            char *values;

            ret = pvar->enumerator->dump(pvar->enumerator, &values);
            if (OPAL_SUCCESS == ret) {
                (void)asprintf (out[0] + line++, "Values: %s", values);
                free (values);
            }
        }
    }

    return OPAL_SUCCESS;
}

/* mca_base_pvar_t class */
static void mca_base_pvar_contructor (mca_base_pvar_t *pvar)
{
    memset ((char *) pvar + sizeof (pvar->super), 0, sizeof (*pvar) - sizeof (pvar->super));
    OBJ_CONSTRUCT(&pvar->bound_handles, opal_list_t);
}

static void mca_base_pvar_destructor (mca_base_pvar_t *pvar)
{
    if (pvar->name) {
        free (pvar->name);
    }

    if (pvar->description) {
        free (pvar->description);
    }

    if (NULL != pvar->enumerator) {
        OBJ_RELEASE(pvar->enumerator);
    }

    OBJ_DESTRUCT(&pvar->bound_handles);
}

OBJ_CLASS_INSTANCE(mca_base_pvar_t, opal_object_t, mca_base_pvar_contructor, mca_base_pvar_destructor);

/* mca_base_pvar_session_t class */
static void opal_mpi_pvar_session_constructor (mca_base_pvar_session_t *session)
{
    OBJ_CONSTRUCT(&session->handles, opal_list_t);
}

static void opal_mpi_pvar_session_destructor (mca_base_pvar_session_t *session)
{
    mca_base_pvar_handle_t *handle, *next;

    /* it is likely a user error if there are any allocated handles when the session
     * is freed. clean it up anyway. The handle destructor will remove the handle from
     * the session's handle list. */
    OPAL_LIST_FOREACH_SAFE(handle, next, &session->handles, mca_base_pvar_handle_t) {
        OBJ_DESTRUCT(handle);
    }

    OBJ_DESTRUCT(&session->handles);
}

OBJ_CLASS_INSTANCE(mca_base_pvar_session_t, opal_object_t, opal_mpi_pvar_session_constructor,
                   opal_mpi_pvar_session_destructor);

/* mca_base_pvar_handle_t class */
static void mca_base_pvar_handle_constructor (mca_base_pvar_handle_t *handle)
{
    memset ((char *) handle + sizeof (handle->super), 0, sizeof (*handle) - sizeof (handle->super));

    OBJ_CONSTRUCT(&handle->list2, opal_list_item_t);
}

static void mca_base_pvar_handle_destructor (mca_base_pvar_handle_t *handle)
{
    if (handle->pvar) {
        (void) mca_base_pvar_notify (handle, MCA_BASE_PVAR_HANDLE_UNBIND, NULL);
    }

    if (NULL != handle->last_value) {
        free (handle->last_value);
    }

    if (NULL != handle->current_value) {
        free (handle->current_value);
    }

    if (NULL != handle->tmp_value) {
        free (handle->tmp_value);
    }

    /* remove this handle from the pvar list */
    if (handle->pvar) {
        opal_list_remove_item (&handle->pvar->bound_handles, &handle->list2);
    }

    OBJ_DESTRUCT(&handle->list2);

    /* remove this handle from the session */
    if (handle->session) {
        opal_list_remove_item (&handle->session->handles, &handle->super);
    }
}

OBJ_CLASS_INSTANCE(mca_base_pvar_handle_t, opal_list_item_t, mca_base_pvar_handle_constructor,
                   mca_base_pvar_handle_destructor);
