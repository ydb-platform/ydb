/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018 The University of Tennessee and The University
 *                    of Tennessee Research Foundation.  All rights
 *                    reserved.
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OPAL_MPIT_PVAR_H)
#define OPAL_MPIT_PVAR_H

#include "opal/mca/base/mca_base_var.h"

/*
 * These flags are used when registering a new pvar.
 */
typedef enum {
    /** This variable should be marked as invalid when the containing
        group is deregistered (IWG = "invalidate with group").  This
        flag is set automatically when you register a variable with
        mca_base_component_pvvar_register(), but can also be set
        manually when you register a variable with
        mca_base_pvar_register().  Analogous to the
        MCA_BASE_VAR_FLAG_DWG flag. */
    MCA_BASE_PVAR_FLAG_IWG        = 0x040,
    /** This variable can not be written. Will be ignored for counter,
        timer, and aggregate variables. These variable handles will be
        updated relative to the value reported by the get_value()
        function provided at registration time. */
    MCA_BASE_PVAR_FLAG_READONLY   = 0x080,
    /** This variable runs continuously after being bound to a handle. */
    MCA_BASE_PVAR_FLAG_CONTINUOUS = 0x100,
    /** This variable can be updated atomically. This flag is ignored
        by mca_base_pvar_register() at this time. */
    MCA_BASE_PVAR_FLAG_ATOMIC     = 0x200,
    /** This variable has been marked as invalid. This flag is ignored
        by mca_base_pvar_register(). */
    MCA_BASE_PVAR_FLAG_INVALID    = 0x400,
} mca_base_pvar_flag_t;

/*
 * These flags are passed to the mca_base_notify_fn_t function.
 */
typedef enum {
    /** A handle has been created an bound to this variable. The
        return value must be the number of values associated with
        the bound object or an OPAL error. For example, if the variable
        is the number of messages sent to each peer in a communicator then the
        return value should be the size of the bound communicator. */
    MCA_BASE_PVAR_HANDLE_BIND,
    /** A handle associated with this variable has been started. It is
        recommended that any computation that might affect perfomance
        only be performed when a bound handle has been started. */
    MCA_BASE_PVAR_HANDLE_START,
    /** A handle associated with this variable has been stopped */
    MCA_BASE_PVAR_HANDLE_STOP,
    /** A handle associated with this variable has been freed */
    MCA_BASE_PVAR_HANDLE_UNBIND
} mca_base_pvar_event_t;

/*
 * The class type is passed when registering a new pvar.
 */
enum {
    /** Variable represents a state */
    MCA_BASE_PVAR_CLASS_STATE,
    /** Variable represents a the utilization level of a resource */
    MCA_BASE_PVAR_CLASS_LEVEL,
    /** Variable represents the fixed size of a resource */
    MCA_BASE_PVAR_CLASS_SIZE,
    /** Variable represents the utilization level of a resource.
       Valid type: double */
    MCA_BASE_PVAR_CLASS_PERCENTAGE,
    /** Variable describes the high-watermark of the utilization
       of a resource */
    MCA_BASE_PVAR_CLASS_HIGHWATERMARK,
    /** Variable describes the low-watermark of the utilization
       of a resource */
    MCA_BASE_PVAR_CLASS_LOWWATERMARK,
    /** Variable counts the number of occurences of a specific event */
    MCA_BASE_PVAR_CLASS_COUNTER,
    /** Variable represents a sum of arguments processed during a
       specific event */
    MCA_BASE_PVAR_CLASS_AGGREGATE,
    /** Variable represents the aggregated time that is spent
       executing an specific event */
    MCA_BASE_PVAR_CLASS_TIMER,
    /** Variable doesn't fit any other class */
    MCA_BASE_PVAR_CLASS_GENERIC
};

#define MCA_BASE_PVAR_CLASS_ANY -1

/*
 * Reserved bindings; passed when registering a new pvar. OPAL will
 * ignore any other binding type.
 */
enum {
    MCA_BASE_VAR_BIND_NO_OBJECT,
    MCA_BASE_VAR_BIND_MPI_COMM,
    MCA_BASE_VAR_BIND_MPI_DATATYPE,
    MCA_BASE_VAR_BIND_MPI_ERRHANDLER,
    MCA_BASE_VAR_BIND_MPI_FILE,
    MCA_BASE_VAR_BIND_MPI_GROUP,
    MCA_BASE_VAR_BIND_MPI_OP,
    MCA_BASE_VAR_BIND_MPI_REQUEST,
    MCA_BASE_VAR_BIND_MPI_WIN,
    MCA_BASE_VAR_BIND_MPI_MESSAGE,
    MCA_BASE_VAR_BIND_MPI_INFO,
    MCA_BASE_VAR_BIND_FIRST_AVAILABLE,
};

struct mca_base_pvar_t;

/**
 * Function to retrieve the current value of a variable.
 *
 * @param[in]  pvar  Performance variable to get the value of.
 * @param[out] value Current value of the variable.
 * @param[in]  obj   Bound object
 *
 * This function will be called to get the current value of a variable. The value
 * pointer will be large enough to hold the datatype and count specified when this
 * variable was created and bound.
 */
typedef int (*mca_base_get_value_fn_t) (const struct mca_base_pvar_t *pvar, void *value, void *obj);

/**
 * Function to set the current value of a variable.
 *
 * @param[in] pvar  Performance variable to set the value of.
 * @param[in] value Value to write.
 * @param[in] obj   Bound object.
 *
 * This function will be called to set the current value of a variable. The value
 * pointer will be large enough to hold the datatype and count specified when this
 * variable was created and bound. Read-only variables are not expected to provide
 * this function.
 */
typedef int (*mca_base_set_value_fn_t) (struct mca_base_pvar_t *pvar, const void *value, void *obj);

/**
 * Function to notify of a pvar handle event.
 *
 * @param[in]  pvar  Performance variable the handle is assocaited with
 * @param[in]  event Event that has occurred. See mca_base_pvar_event_t.
 * @param[in]  obj   Bound object
 * @param[out] count Value count for this object (on MCA_BASE_PVAR_HANDLE_BIND)
 *
 * Depending on the event this functions is expected to:
 * On MCA_BASE_PVAR_HANDLE_BIND: depending on the bound object returns the number of values
 *                            needed for get_value().
 * On MCA_BASE_PVAR_HANDLE_START: enable the performance variable.
 * On MCA_BASE_PVAR_HANDLE_STOP: XXX -- TODO -- finish me
 */
typedef int (*mca_base_notify_fn_t) (struct mca_base_pvar_t *pvar, mca_base_pvar_event_t event, void *obj, int *count);

/**
 * Structure representing an OPAL performance variable.
 */
typedef struct mca_base_pvar_t {
    /** Make this an opal object */
    opal_object_t super;

    /** Variable index */
    int pvar_index;

    /** Full name of the variable: form is framework_component_name */
    char *name;

    /** Description of this performance variable */
    char *description;

    /** MCA variable group this variable is associated with */
    int group_index;

    /** Verbosity level of this variable */
    mca_base_var_info_lvl_t verbosity;

    /** Variable class. See mpi.h.in MPIT pvar classes */
    int var_class;

    /** MPI datatype of the information stored in the performance variable */
    mca_base_var_type_t type;

    /** Enumerator for integer values */
    mca_base_var_enum_t *enumerator;

    /** Type of object to which this variable must be bound or MCA_BASE_VAR_BIND_NULL */
    int bind;

    /** Flags for this variable */
    mca_base_pvar_flag_t flags;

    /** Get the current value of this variable */
    mca_base_get_value_fn_t get_value;

    /** Set the current value of this variable. Only valid for read-write variables. */
    mca_base_set_value_fn_t set_value;

    /** Notify the creator of this variable of a change */
    mca_base_notify_fn_t notify;

    /** Context of this variable */
    void *ctx;

    /** List of bound pvar handles. NOTE: The items in this list are
        offsetof(mca_base_pvar_handle_t, list2) into a pvar handle. */
    opal_list_t bound_handles;
} mca_base_pvar_t;
OBJ_CLASS_DECLARATION(mca_base_pvar_t);

/**
 * Performance variable session
 */
typedef struct mca_base_pvar_session_t {
    /** Make this an opal object */
    opal_object_t super;

    /** List of all handles in the session */
    opal_list_t handles;
} mca_base_pvar_session_t;
OBJ_CLASS_DECLARATION(mca_base_pvar_session_t);

/**
 * Performance variable handle
 *
 * Handles are used to bind performance variables to objects, read, write, and
 * reset values.
 */
typedef struct mca_base_pvar_handle_t {
    /** List item in pvar session */
    opal_list_item_t super;

    /** XXX -- use me -- add this list item to the associated variable */
    opal_list_item_t list2;

    /** session this handle is associated with */
    mca_base_pvar_session_t *session;

    /** performance variable this handle is associated with */
    mca_base_pvar_t *pvar;

    /** MPI object handle */
    void *obj_handle;

    /** Number of values for this handle */
    int count;

    /** Last value read from the variable */
    void *last_value;

    /** Current sum for counters and timers */
    void *current_value;

    /** Temporary buffer for counters. Used to calculate deltas between
        the last value and the current value. */
    void *tmp_value;

    /** Has this handle been started (or is continuous) */
    bool started;
} mca_base_pvar_handle_t;
OBJ_CLASS_DECLARATION(mca_base_pvar_handle_t);

/****************************************************************************
 * The following functions are public functions, and are intended to
 * be used by components and frameworks to expose their performance
 * variables.
 ****************************************************************************/

/**
 * Register a performance variable
 *
 * @param[in] framework   Name of registering framework
 * @param[in] component   Name of registering component
 * @param[in] name        Name of performance variable
 * @param[in] description Description of the performance variable. Verbose
 *                        is good!
 * @param[in] verbosity   Opal info verbosity of this variable. Equivalent to
 *                        MPI_T verbosity.
 * @param[in] var_class   Class of performance variable. See mpi.h.in MPI_T_PVAR_CLASS_*
 * @param[in] type        Type of this performance variable
 * @param[in] enumerator  Enumerator for this variable. Will be
 *                        OBJ_RETAIN'd (created by, for example,
 *                        mca_base_var_enum_create()).
 * @param[in] bind        Object type this variable should be bound to. See mpi.h.in
 *                        MPI_T_BIND_*
 * @param[in] flags       Flags for this variable. See mca_base_pvar_flag_t for acceptable
 *                        flags.
 * @param[in] get_value   Function for reading the value of this variable. If this function
 *                        is NULL a default function that reads 1 value from ctx will be used.
 *                        See mca_base_get_value_fn_t.
 * @param[in] set_value   Function for writing the value of this variable. This pointer is
 *                        ignored if the \flags includes \MCA_BASE_PVAR_FLAG_READONLY. If this
 *                        function is NULL a default function that writes 1 value from ctx will
 *                        be used. See mca_base_set_value_fn_t.
 * @param[in] notify      Function for notifying about variable handle events. If this function
 *                        is NULL then a default function that ignores all events will be used.
 *                        See mca_base_notify_fn_t.
 * @param[in] ctx         Context for this variable. Will be stored in the resulting variable
 *                        for future use.
 *
 * @returns index         On success returns the index of this variable.
 * @returns OPAL_ERROR    On error.
 *
 * Note: if used incorrectly this function may fail an assert(); see
 * MPI 3.0 14.3 to see acceptable values for datatype given the class.
 */
OPAL_DECLSPEC int mca_base_pvar_register (const char *project, const char *framework, const char *component, const char *name,
                            const char *description, mca_base_var_info_lvl_t verbosity,
                            int var_class, mca_base_var_type_t type, mca_base_var_enum_t *enumerator,
                            int bind, mca_base_pvar_flag_t flags, mca_base_get_value_fn_t get_value,
                            mca_base_set_value_fn_t set_value, mca_base_notify_fn_t notify, void *ctx);

/**
 * Convinience function for registering a performance variable
 * associated with a component.
 *
 * While quite similar to mca_base_pvar_register(), there is one key
 * difference: pvars registered with this function will automatically
 * be unregistered / made unavailable when that component is closed by
 * its framework.
 */
OPAL_DECLSPEC int mca_base_component_pvar_register (const mca_base_component_t *component, const char *name,
                            const char *description, mca_base_var_info_lvl_t verbosity, int var_class,
                            mca_base_var_type_t type, mca_base_var_enum_t *enumerator, int bind,
                            mca_base_pvar_flag_t flags, mca_base_get_value_fn_t get_value,
                            mca_base_set_value_fn_t set_value, mca_base_notify_fn_t notify, void *ctx);


/**
 * Find the index for an MCA performance variable based on its names.
 *
 * @param project   Name of the project
 * @param type      Name of the type containing the variable.
 * @param component Name of the component containing the variable.
 * @param param     Name of the variable.
 *
 * @retval OPAL_ERROR If the variable was not found.
 * @retval index If the variable was found.
 *
 * It is not always convenient to widely propagate a variable's index
 * value, or it may be necessary to look up the variable from a
 * different component. This function can be used to look up the index
 * of any registered variable.  The returned index can be used with
 * mca_base_pvar_get(), mca_base_pvar_handle_alloc(), and
 * mca_base_pvar_dump().
 */
OPAL_DECLSPEC int mca_base_pvar_find (const char *project, const char *framework, const char *component, const char *name);

/**
 * Find the index for a performance variable based on its full name
 *
 * @param full_name [in] Full name of the variable
 * @param index [out]    Index of the variable
 *
 * See mca_base_pvar_find().
 */
OPAL_DECLSPEC int mca_base_pvar_find_by_name (const char *full_name, int var_class, int *index);

/****************************************************************************
 * The following functions are the back-end to the MPI_T API functions
 * and/or the internals of the performance variable system itself.
 ****************************************************************************/

/**
 * Return the number or registered performance variables.
 *
 * @param[out] count Number of registered performance variables.
 *
 * This function can be called before mca_base_pvar_init() and after
 * mca_base_pvar_finalize().
 */
OPAL_DECLSPEC int mca_base_pvar_get_count (int *count);

/**
 * Update the handles associated with the specified performance variable and MPI object
 *
 * @param[in] index  Index of the performance variable
 * @param[in] value  New value of the variable.
 * @param[in] obj    Object updated handles should be bound to.
 *
 * This function will obtain and hold the mpit big lock until all handles are updated. It
 * is recommended this function not be called from within any critical code path. Calling
 * this function should only be necessary to update watermarks.
 */
OPAL_DECLSPEC int mca_base_pvar_update_all_handles (int index, const void *obj);

/**
 * Get the variable at an index
 *
 * @param[in]  index Index of variable to get.
 * @param[out] pvar  Performance variable from index on success.
 *
 * @returns OPAL_SUCCESS on success
 * @returns OPAL_ERR_VALUE_OUT_OF_BOUNDS on if index is out of range
 */
OPAL_DECLSPEC int mca_base_pvar_get (int index, const mca_base_pvar_t **pvar);

/**
 * Dump strings describing the performance variable at an index
 *
 * @param[in]  index       Variable index
 * @param[out] out         Array of strings representing this variable
 * @param[in]  output_type Type of output desired
 *
 * This function returns an array of strings describing the variable. All strings
 * and the array must be freed by the caller. The \output_type may be either
 * MCA_BASE_VAR_DUMP_READABLE or MCA_BASE_VAR_DUMP_PARSABLE.
 */
OPAL_DECLSPEC int mca_base_pvar_dump(int index, char ***out, mca_base_var_dump_type_t output_type);

/**
 * Mark a performance variable as invalid
 *
 * @param[in] index Variable index
 *
 * A performance variable that has been marked as invalid will not be available. To
 * restore a performance variable it has to be re-registered using mca_base_pvar_register().
 */
int mca_base_pvar_mark_invalid (int index);

/**
 * Convienience functions for performance variables
 */
static inline bool mca_base_pvar_is_sum (const mca_base_pvar_t *pvar)
{
    return (MCA_BASE_PVAR_CLASS_COUNTER == pvar->var_class ||
            MCA_BASE_PVAR_CLASS_TIMER == pvar->var_class ||
            MCA_BASE_PVAR_CLASS_AGGREGATE == pvar->var_class);
}

static inline bool mca_base_pvar_is_watermark (const mca_base_pvar_t *pvar)
{
    return (MCA_BASE_PVAR_CLASS_HIGHWATERMARK == pvar->var_class ||
            MCA_BASE_PVAR_CLASS_LOWWATERMARK == pvar->var_class);
}

static inline bool mca_base_pvar_is_readonly (const mca_base_pvar_t *pvar)
{
    return !!(pvar->flags & MCA_BASE_PVAR_FLAG_READONLY);
}

static inline bool mca_base_pvar_is_continuous (const mca_base_pvar_t *pvar)
{
    return !!(pvar->flags & MCA_BASE_PVAR_FLAG_CONTINUOUS);
}

static inline bool mca_base_pvar_is_atomic (const mca_base_pvar_t *pvar)
{
    return !!(pvar->flags & MCA_BASE_PVAR_FLAG_ATOMIC);
}

static inline bool mca_base_pvar_is_invalid (const mca_base_pvar_t *pvar)
{
    return !!(pvar->flags & MCA_BASE_PVAR_FLAG_INVALID);
}

/* Handle functions */

/**
 * Bind a new handle and object to a performance variable
 *
 * @param[in]  session    Valid pvar session
 * @param[in]  index      Variable index
 * @param[in]  obj_handle Object handle
 * @param[out] handle     New handle
 * @param[out] count      Number of values associated with this object
 *
 * This function allocates a new performance variable handle and binds it
 * to \obj_handle (if the variable binding is 0 the object is ignored). On
 * success a new handle is returned in \handle and the number of values i
 * returned in \count. Calls to read/write must provide buffers that will
 * hold \count values of the type of the performance variable specified by
 * \index.
 */
OPAL_DECLSPEC int mca_base_pvar_handle_alloc (mca_base_pvar_session_t *session, int index, void *obj_handle,
                                mca_base_pvar_handle_t **handle, int *count);

/**
 * Free an allocated performance variable handle
 *
 * @param[in] handle Handle to free
 *
 * After calling this function the performance variable will no longer be valid.
 */
OPAL_DECLSPEC int mca_base_pvar_handle_free (mca_base_pvar_handle_t *handle);

/**
 * Update a performance variable handle.
 *
 * @param[in] handle Handle to update
 *
 * The new value of the handle will depend on the class of performance variable.
 * For counters and timers the new value will be the current handle value plus the
 * difference between the last and the current variable value. For high/low watermarks
 * the new value will be the greater/lesser of the current handle value and the
 * current variable value. This call does not update other types of handles.
 */
OPAL_DECLSPEC int mca_base_pvar_handle_update (mca_base_pvar_handle_t *handle);

/**
 * Read the current value of a handle
 *
 * @param[in]  handle Handle to read from
 * @param[out] value  Buffer to store the current value in
 *
 * Read the current value of the handle or variable (depending on the variable class)
 * and return it in the buffer specified by value. The buffer must be large enough to
 * hold the correct number and type of this handle's value (see mca_base_pvar_handle_update()).
 */
OPAL_DECLSPEC int mca_base_pvar_handle_read_value (mca_base_pvar_handle_t *handle, void *value);

/**
 * Write a value to a read-write handle
 *
 * @param[in] handle Handle to update
 * @param[in] value  Value to write
 *
 * If the underlying variable is read-only this function will fail with OPAL_ERR_PERM.
 */
OPAL_DECLSPEC int mca_base_pvar_handle_write_value (mca_base_pvar_handle_t *handle, const void *value);

/**
 * Convienience function for sending notification of a handle change
 *
 * @param[in]  handle Handle event occurred on
 * @param[in]  event  Event that occurred
 * @param[out] count  Value count returned when binding a handle
 */
OPAL_DECLSPEC int mca_base_pvar_notify (mca_base_pvar_handle_t *handle, mca_base_pvar_event_t event, int *count);

/**
 * Start a performance variable handle
 *
 * @param[in] handle Handle to start
 *
 * @returns OPAL_SUCCESS on success
 * @returns OPAL_ERR_NOT_SUPPORTED if the handle could not be started
 */
OPAL_DECLSPEC int mca_base_pvar_handle_start (mca_base_pvar_handle_t *handle);

/**
 * Stop a performance variable handle
 *
 * @param[in] handle Handle to stop (must be started)
 *
 * @return OPAL_SUCCESS on success
 * @returns OPAL_ERR_NOT_SUPPORTED if the handle could not be started
 */
OPAL_DECLSPEC int mca_base_pvar_handle_stop (mca_base_pvar_handle_t *handle);

/**
 * Reset a performance variable handle
 *
 * @param[in] handle Handle to reset
 *
 * Reset the handle to a value equivalent to when the handle was first allocated.
 */
OPAL_DECLSPEC int mca_base_pvar_handle_reset (mca_base_pvar_handle_t *handle);

static inline bool mca_base_pvar_handle_is_running (mca_base_pvar_handle_t *handle)
{
    return handle->started || !!(handle->pvar->flags & MCA_BASE_PVAR_FLAG_CONTINUOUS);
}

#endif
