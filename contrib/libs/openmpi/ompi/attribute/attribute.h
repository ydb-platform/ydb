/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *
 * Implementation for taking care of the attribute that can hang off a comm,
 * win or datatype.
 */

#ifndef OMPI_ATTRIBUTE_H
#define OMPI_ATTRIBUTE_H

#include <string.h>
#include "mpi.h"

#include "ompi_config.h"
#include "ompi/constants.h"
#include "opal/class/opal_object.h"
#include "opal/class/opal_hash_table.h"

#define ATTR_HASH_SIZE 10

/*
 * Flags for keyvals
 */
#define OMPI_KEYVAL_PREDEFINED     0x0001
#define OMPI_KEYVAL_F77            0x0002
#define OMPI_KEYVAL_F77_INT        0x0004


BEGIN_C_DECLS

enum ompi_attribute_type_t {
    UNUSED_ATTR = 0, /**< Make the compilers happy when we have to construct
                      * an attribute */
    COMM_ATTR,       /**< The attribute belongs to a comm object. Starts
                      * with 1 so that we can have it initialized to 0
                      * using memset in the constructor */
    TYPE_ATTR,       /**< The attribute belongs to datatype object */
    WIN_ATTR         /**< The attribute belongs to a win object */
};
typedef enum ompi_attribute_type_t ompi_attribute_type_t;


/* Old-style MPI-1 Fortran function pointer declarations for copy and
   delete. These will only be used here and not in the front end
   functions. */

typedef void (ompi_fint_copy_attr_function)(MPI_Fint *oldobj,
                                                    MPI_Fint *keyval,
                                                    MPI_Fint *extra_state,
                                                    MPI_Fint *attr_in,
                                                    MPI_Fint *attr_out,
                                                    ompi_fortran_logical_t *flag,
                                                    MPI_Fint *ierr);
typedef void (ompi_fint_delete_attr_function)(MPI_Fint *obj,
                                                      MPI_Fint *keyval,
                                                      MPI_Fint *attr_in,
                                                      MPI_Fint *extra_state,
                                                      MPI_Fint *ierr);

/* New-style MPI-2 Fortran function pointer declarations for copy and
   delete. These will only be used here and not in the front end
   functions. */

typedef void (ompi_aint_copy_attr_function)(MPI_Fint *oldobj,
                                            MPI_Fint *keyval,
                                            void *extra_state,
                                            void *attr_in,
                                            void *attr_out,
                                            ompi_fortran_logical_t *flag,
                                            MPI_Fint *ierr);
typedef void (ompi_aint_delete_attr_function)(MPI_Fint *obj,
                                              MPI_Fint *keyval,
                                              void *attr_in,
                                              void *extra_state,
                                              MPI_Fint *ierr);
/*
 * Internally the copy function for all kinds of MPI objects has one more
 * argument, the pointer to the new object. Therefore, we can do on the
 * flight modifications of the new communicator based on attributes stored
 * on the main communicator.
 */
typedef int (MPI_Comm_internal_copy_attr_function)(MPI_Comm, int, void *,
                                                   void *, void *, int *,
                                                   MPI_Comm);
typedef int (MPI_Type_internal_copy_attr_function)(MPI_Datatype, int, void *,
                                                   void *, void *, int *,
                                                   MPI_Datatype);
typedef int (MPI_Win_internal_copy_attr_function)(MPI_Win, int, void *,
                                                  void *, void *, int *,
                                                  MPI_Win);

typedef void (ompi_attribute_keyval_destructor_fn_t)(int);

/* Union to take care of proper casting of the function pointers
   passed from the front end functions depending on the type. This
   will avoid casting function pointers to void*  */

union ompi_attribute_fn_ptr_union_t {
    MPI_Comm_delete_attr_function          *attr_communicator_delete_fn;
    MPI_Type_delete_attr_function          *attr_datatype_delete_fn;
    MPI_Win_delete_attr_function           *attr_win_delete_fn;

    MPI_Comm_internal_copy_attr_function   *attr_communicator_copy_fn;
    MPI_Type_internal_copy_attr_function   *attr_datatype_copy_fn;
    MPI_Win_internal_copy_attr_function    *attr_win_copy_fn;

    /* For Fortran old MPI-1 callback functions */

    ompi_fint_delete_attr_function *attr_fint_delete_fn;
    ompi_fint_copy_attr_function   *attr_fint_copy_fn;

    /* For Fortran new MPI-2 callback functions */

    ompi_aint_delete_attr_function *attr_aint_delete_fn;
    ompi_aint_copy_attr_function   *attr_aint_copy_fn;
};

typedef union ompi_attribute_fn_ptr_union_t ompi_attribute_fn_ptr_union_t;


/**
 * Union to help convert between Fortran attributes (which must be
 * stored by value) and C pointers (which is the back-end storage of
 * all attributes).
 */
union ompi_attribute_fortran_ptr_t {
    void *c_ptr;
    MPI_Fint f_integer;
    MPI_Aint f_address;
};
/**
 * Convenience typedef
 */
typedef union ompi_attribute_fortran_ptr_t ompi_attribute_fortran_ptr_t;

struct ompi_attribute_keyval_t {
    opal_object_t super;
    ompi_attribute_type_t attr_type; /**< One of COMM/WIN/DTYPE. This
                                       will be used to cast the
                                       copy/delete attribute functions
                                       properly and error checking */
    int attr_flag; /**< flag field: contains "OMPI_KEYVAL_PREDEFINED",
                      "OMPI_KEYVAL_F77"  */
    ompi_attribute_fn_ptr_union_t copy_attr_fn; /**< Copy function for the
                                             attribute */
    ompi_attribute_fn_ptr_union_t delete_attr_fn; /**< Delete function for the
                                               attribute */
    ompi_attribute_fortran_ptr_t extra_state; /**< Extra state of the attribute */
    int key; /**< Keep a track of which key this item belongs to, so that
                the key can be deleted when this object is destroyed */

    /** Extra state for bindings to hang data on.  If non-NULL, will be
        freed by the C base when the keyval is destroyed. */
    void *bindings_extra_state;
};

typedef struct ompi_attribute_keyval_t ompi_attribute_keyval_t;


/* Functions */



/**
 * Convenient way to initialize the attribute hash table per MPI-Object
 */

static inline
int ompi_attr_hash_init(opal_hash_table_t **hash)
{
    *hash = OBJ_NEW(opal_hash_table_t);
    if (NULL == *hash) {
        fprintf(stderr, "Error while creating the local attribute list\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    if (OMPI_SUCCESS != opal_hash_table_init(*hash, ATTR_HASH_SIZE)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    return MPI_SUCCESS;
}

/**
 * Initialize the main attribute hash that stores the keyvals and meta data
 *
 * @return OMPI return code
 */

int ompi_attr_init(void);

/**
 * Destroy the main attribute hash that stores the keyvals and meta data
 */

int ompi_attr_finalize(void);


/**
 * Create a new key for use by attribute of Comm/Win/Datatype
 *
 * @param type           Type of attribute (COMM/WIN/DTYPE) (IN)
 * @param copy_attr_fn   Union variable containing the function pointer
 *                       to be used in order to copy the attribute (IN)
 * @param delete_attr_fn Function pointer to be used for deleting the
 *                       attribute (IN)
 * @param key            The newly created key is returned here (OUT)
 * @param extra_state    Extra state to hang off/do some special things (IN)
 * @param flags          Flags for the key -- flags contain OMPI_KEYVAL_F77,
 *                       OMPI_KEYVAL_PREDEFINED
 * @param bindings_extra_state Extra state that, if non-NULL, will
 *                       automatically be free()'ed by the C base when
 *                       the keyval is destroyed.
 *
 * NOTE: I have taken the assumption that user cannot modify/delete
 * any predefined keys or the attributes attached. To accomplish this,
 * all MPI* calls will have OMPI_KEYVAL_PREDEFINED set as 0. MPI
 * implementors who will need to play with the predefined keys and
 * attributes would call the ompi* functions here and not the MPI*
 * functions, with OMPI_KEYVAL_PREDEFINED set to 1.
 * END OF NOTE
 *
 * NOTE: For the function pointers, you need to create a variable of the
 * union type "ompi_attribute_fn_ptr_union_t" and assign the proper field.
 * to be passed into this function
 * END OF NOTE
 *
 * @return OMPI return code

 *
 */

OMPI_DECLSPEC int ompi_attr_create_keyval(ompi_attribute_type_t type,
                                          ompi_attribute_fn_ptr_union_t copy_attr_fn,
                                          ompi_attribute_fn_ptr_union_t delete_attr_fn,
                                          int *key, void *extra_state, int flags,
                                          void *bindings_extra_state);

/**
 * Same as ompi_attr_create_keyval, but extra_state is a Fortran default integer.
 */

OMPI_DECLSPEC int ompi_attr_create_keyval_fint(ompi_attribute_type_t type,
                                               ompi_attribute_fn_ptr_union_t copy_attr_fn,
                                               ompi_attribute_fn_ptr_union_t delete_attr_fn,
                                               int *key, MPI_Fint extra_state, int flags,
                                               void *bindings_extra_state);

/**
 * Same as ompi_attr_create_keyval, but extra_state is a Fortran address integer.
 */

OMPI_DECLSPEC int ompi_attr_create_keyval_aint(ompi_attribute_type_t type,
                                               ompi_attribute_fn_ptr_union_t copy_attr_fn,
                                               ompi_attribute_fn_ptr_union_t delete_attr_fn,
                                               int *key, MPI_Aint extra_state, int flags,
                                               void *bindings_extra_state);

/**
 * Free an attribute keyval
 * @param type           Type of attribute (COMM/WIN/DTYPE) (IN)
 * @param key            key, which is set to MPI_KEY_INVALID (IN/OUT)
 * @return OMPI error code
 */

int ompi_attr_free_keyval(ompi_attribute_type_t type, int *key,
                          bool predefined);

/**
 * Set an attribute on the comm/win/datatype in a form valid for C.
 *
 * @param type           Type of attribute (COMM/WIN/DTYPE) (IN)
 * @param object         The actual Comm/Win/Datatype object (IN)
 * @param attr_hash      The attribute hash table hanging on the object(IN/OUT)
 * @param key            Key val for the attribute (IN)
 * @param attribute      The actual attribute pointer (IN)
 * @param predefined     Whether the key is predefined or not 0/1 (IN)
 * @return OMPI error code
 *
 * If (*attr_hash) == NULL, a new hash will be created and
 * initialized.
 *
 * All four of these functions (ompi_attr_set_c(), ompi_attr_set_int(),
 * ompi_attr_set_fint(), and ompi_attr_set_aint())
 * could have been combined into one function that took some kind of
 * (void*) and an enum to indicate which way to translate the final
 * representation, but that just seemed to make an already complicated
 * situation more complicated through yet another layer of
 * indirection.
 *
 * So yes, this is more code, but it's clearer and less error-prone
 * (read: better) this way.
 */
int ompi_attr_set_c(ompi_attribute_type_t type, void *object,
                    opal_hash_table_t **attr_hash,
                    int key, void *attribute, bool predefined);

/**
 * Set an int predefined attribute in a form valid for C.
 *
 * @param type           Type of attribute (COMM/WIN/DTYPE) (IN)
 * @param object         The actual Comm/Win/Datatype object (IN)
 * @param attr_hash      The attribute hash table hanging on the object(IN/OUT)
 * @param key            Key val for the attribute (IN)
 * @param attribute      The actual attribute value (IN)
 * @param predefined     Whether the key is predefined or not 0/1 (IN)
 * @return OMPI error code
 *
 * If (*attr_hash) == NULL, a new hash will be created and
 * initialized.
 *
 * All four of these functions (ompi_attr_set_c(), ompi_attr_set_int(),
 * ompi_attr_set_fint(), and ompi_attr_set_aint())
 * could have been combined into one function that took some kind of
 * (void*) and an enum to indicate which way to translate the final
 * representation, but that just seemed to make an already complicated
 * situation more complicated through yet another layer of
 * indirection.
 *
 * So yes, this is more code, but it's clearer and less error-prone
 * (read: better) this way.
 */
int ompi_attr_set_int(ompi_attribute_type_t type, void *object,
                      opal_hash_table_t **attr_hash,
                      int key, int attribute, bool predefined);

/**
 * Set an attribute on the comm/win/datatype in a form valid for
 * Fortran MPI-1.
 *
 * @param type           Type of attribute (COMM/WIN/DTYPE) (IN)
 * @param object         The actual Comm/Win/Datatype object (IN)
 * @param attr_hash      The attribute hash table hanging on the object(IN/OUT)
 * @param key            Key val for the attribute (IN)
 * @param attribute      The actual attribute pointer (IN)
 * @param predefined     Whether the key is predefined or not 0/1 (IN)
 * @return OMPI error code
 *
 * If (*attr_hash) == NULL, a new hash will be created and
 * initialized.
 *
 * All four of these functions (ompi_attr_set_c(), ompi_attr_set_int(),
 * ompi_attr_set_fint(), and ompi_attr_set_aint())
 * could have been combined into one function that took some kind of
 * (void*) and an enum to indicate which way to translate the final
 * representation, but that just seemed to make an already complicated
 * situation more complicated through yet another layer of
 * indirection.
 *
 * So yes, this is more code, but it's clearer and less error-prone
 * (read: better) this way.
 */
OMPI_DECLSPEC int ompi_attr_set_fint(ompi_attribute_type_t type, void *object,
                                     opal_hash_table_t **attr_hash,
                                     int key, MPI_Fint attribute,
                                     bool predefined);

/**
 * Set an attribute on the comm/win/datatype in a form valid for
 * Fortran MPI-2.
 *
 * @param type           Type of attribute (COMM/WIN/DTYPE) (IN)
 * @param object         The actual Comm/Win/Datatype object (IN)
 * @param attr_hash      The attribute hash table hanging on the object(IN/OUT)
 * @param key            Key val for the attribute (IN)
 * @param attribute      The actual attribute pointer (IN)
 * @param predefined     Whether the key is predefined or not 0/1 (IN)
 * @return OMPI error code
 *
 * If (*attr_hash) == NULL, a new hash will be created and
 * initialized.
 *
 * All four of these functions (ompi_attr_set_c(), ompi_attr_set_int(),
 * ompi_attr_set_fint(), and ompi_attr_set_aint())
 * could have been combined into one function that took some kind of
 * (void*) and an enum to indicate which way to translate the final
 * representation, but that just seemed to make an already complicated
 * situation more complicated through yet another layer of
 * indirection.
 *
 * So yes, this is more code, but it's clearer and less error-prone
 * (read: better) this way.
 */
OMPI_DECLSPEC int ompi_attr_set_aint(ompi_attribute_type_t type, void *object,
                                     opal_hash_table_t **attr_hash,
                                     int key, MPI_Aint attribute,
                                     bool predefined);

/**
 * Get an attribute on the comm/win/datatype in a form valid for C.
 *
 * @param attr_hash      The attribute hash table hanging on the object(IN)
 * @param key            Key val for the attribute (IN)
 * @param attribute      The actual attribute pointer (OUT)
 * @param flag           Flag whether an attribute is associated
 *                       with the key (OUT)
 * @return OMPI error code
 *
 * All three of these functions (ompi_attr_get_c(),
 * ompi_attr_get_fint(), and ompi_attr_get_aint())
 * could have been combined into one function that took some kind of
 * (void*) and an enum to indicate which way to translate the final
 * representation, but that just seemed to make an already complicated
 * situation more complicated through yet another layer of
 * indirection.
 *
 * So yes, this is more code, but it's clearer and less error-prone
 * (read: better) this way.
 */

int ompi_attr_get_c(opal_hash_table_t *attr_hash, int key,
                    void **attribute, int *flag);


/**
 * Get an attribute on the comm/win/datatype in a form valid for
 * Fortran MPI-1.
 *
 * @param attr_hash      The attribute hash table hanging on the object(IN)
 * @param key            Key val for the attribute (IN)
 * @param attribute      The actual attribute pointer (OUT)
 * @param flag           Flag whether an attribute is associated
 *                       with the key (OUT)
 * @return OMPI error code
 *
 * All three of these functions (ompi_attr_get_c(),
 * ompi_attr_get_fint(), and ompi_attr_get_aint())
 * could have been combined into one function that took some kind of
 * (void*) and an enum to indicate which way to translate the final
 * representation, but that just seemed to make an already complicated
 * situation more complicated through yet another layer of
 * indirection.
 *
 * So yes, this is more code, but it's clearer and less error-prone
 * (read: better) this way.
 */

    OMPI_DECLSPEC int ompi_attr_get_fint(opal_hash_table_t *attr_hash, int key,
                                         MPI_Fint *attribute, int *flag);


/**
 * Get an attribute on the comm/win/datatype in a form valid for
 * Fortran MPI-2.
 *
 * @param attr_hash      The attribute hash table hanging on the object(IN)
 * @param key            Key val for the attribute (IN)
 * @param attribute      The actual attribute pointer (OUT)
 * @param flag           Flag whether an attribute is associated
 *                       with the key (OUT)
 * @return OMPI error code
 *
 * All three of these functions (ompi_attr_get_c(),
 * ompi_attr_get_fint(), and ompi_attr_get_aint())
 * could have been combined into one function that took some kind of
 * (void*) and an enum to indicate which way to translate the final
 * representation, but that just seemed to make an already complicated
 * situation more complicated through yet another layer of
 * indirection.
 *
 * So yes, this is more code, but it's clearer and less error-prone
 * (read: better) this way.
 */

OMPI_DECLSPEC int ompi_attr_get_aint(opal_hash_table_t *attr_hash, int key,
                                     MPI_Aint *attribute, int *flag);


/**
 * Delete an attribute on the comm/win/datatype
 * @param type           Type of attribute (COMM/WIN/DTYPE) (IN)
 * @param object         The actual Comm/Win/Datatype object (IN)
 * @param attr_hash      The attribute hash table hanging on the object(IN)
 * @param key            Key val for the attribute (IN)
 * @param predefined     Whether the key is predefined or not 0/1 (IN)
 * @return OMPI error code
 *
 */

int ompi_attr_delete(ompi_attribute_type_t type, void *object,
                     opal_hash_table_t *attr_hash , int key,
                     bool predefined);


/**
 * This to be used from functions like MPI_*_DUP in order to copy all
 * the attributes from the old Comm/Win/Dtype object to a new
 * object.
 * @param type         Type of attribute (COMM/WIN/DTYPE) (IN)
 * @param old_object   The old COMM/WIN/DTYPE object (IN)
 * @param new_object   The new COMM/WIN/DTYPE object (IN)
 * @param oldattr_hash The attribute hash table hanging on old object(IN)
 * @param newattr_hash The attribute hash table hanging on new object(IN)
 * @return OMPI error code
 *
 */

int ompi_attr_copy_all(ompi_attribute_type_t type, void *old_object,
                       void *new_object, opal_hash_table_t *oldattr_hash,
                       opal_hash_table_t *newattr_hash);


/**
 * This to be used to delete all the attributes from the Comm/Win/Dtype
 * object in one shot
 * @param type         Type of attribute (COMM/WIN/DTYPE) (IN)
 * @param object       The COMM/WIN/DTYPE object (IN)
 * @param attr_hash    The attribute hash table hanging on the object(IN)
 * @return OMPI error code
 *
 */

int ompi_attr_delete_all(ompi_attribute_type_t type, void *object,
                        opal_hash_table_t *attr_hash);


/**
 * \internal
 *
 * Create all the predefined attributes
 *
 * @returns OMPI_SUCCESS
 */
int ompi_attr_create_predefined(void);

/**
 * \internal
 *
 * Free all the predefined attributes
 *
 * @returns OMPI_SUCCESS
 */
int ompi_attr_free_predefined(void);


END_C_DECLS

#endif /* OMPI_ATTRIBUTE_H */
