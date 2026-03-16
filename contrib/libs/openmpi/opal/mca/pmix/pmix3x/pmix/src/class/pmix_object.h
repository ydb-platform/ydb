/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file:
 *
 * A simple C-language object-oriented system with single inheritance
 * and ownership-based memory management using a retain/release model.
 *
 * A class consists of a struct and singly-instantiated class
 * descriptor.  The first element of the struct must be the parent
 * class's struct.  The class descriptor must be given a well-known
 * name based upon the class struct name (if the struct is sally_t,
 * the class descriptor should be sally_t_class) and must be
 * statically initialized as discussed below.
 *
 * (a) To define a class
 *
 * In a interface (.h) file, define the class.  The first element
 * should always be the parent class, for example
 * @code
 *   struct sally_t
 *   {
 *     parent_t parent;
 *     void *first_member;
 *     ...
 *   };
 *   typedef struct sally_t sally_t;
 *
 *   PMIX_CLASS_DECLARATION(sally_t);
 * @endcode
 * All classes must have a parent which is also class.
 *
 * In an implementation (.c) file, instantiate a class descriptor for
 * the class like this:
 * @code
 *   PMIX_CLASS_INSTANCE(sally_t, parent_t, sally_construct, sally_destruct);
 * @endcode
 * This macro actually expands to
 * @code
 *   pmix_class_t sally_t_class = {
 *     "sally_t",
 *     PMIX_CLASS(parent_t),  // pointer to parent_t_class
 *     sally_construct,
 *     sally_destruct,
 *     0, 0, NULL, NULL,
 *     sizeof ("sally_t")
 *   };
 * @endcode
 * This variable should be declared in the interface (.h) file using
 * the PMIX_CLASS_DECLARATION macro as shown above.
 *
 * sally_construct, and sally_destruct are function pointers to the
 * constructor and destructor for the class and are best defined as
 * static functions in the implementation file.  NULL pointers maybe
 * supplied instead.
 *
 * Other class methods may be added to the struct.
 *
 * (b) Class instantiation: dynamic
 *
 * To create a instance of a class (an object) use PMIX_NEW:
 * @code
 *   sally_t *sally = PMIX_NEW(sally_t);
 * @endcode
 * which allocates memory of sizeof(sally_t) and runs the class's
 * constructors.
 *
 * Use PMIX_RETAIN, PMIX_RELEASE to do reference-count-based
 * memory management:
 * @code
 *   PMIX_RETAIN(sally);
 *   PMIX_RELEASE(sally);
 *   PMIX_RELEASE(sally);
 * @endcode
 * When the reference count reaches zero, the class's destructor, and
 * those of its parents, are run and the memory is freed.
 *
 * N.B. There is no explicit free/delete method for dynamic objects in
 * this model.
 *
 * (c) Class instantiation: static
 *
 * For an object with static (or stack) allocation, it is only
 * necessary to initialize the memory, which is done using
 * PMIX_CONSTRUCT:
 * @code
 *   sally_t sally;
 *
 *   PMIX_CONSTRUCT(&sally, sally_t);
 * @endcode
 * The retain/release model is not necessary here, but before the
 * object goes out of scope, PMIX_DESTRUCT should be run to release
 * initialized resources:
 * @code
 *   PMIX_DESTRUCT(&sally);
 * @endcode
 */

#ifndef PMIX_OBJECT_H
#define PMIX_OBJECT_H

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include <assert.h>
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif  /* HAVE_STDLIB_H */

#include "src/threads/thread_usage.h"

BEGIN_C_DECLS

#if PMIX_ENABLE_DEBUG
/* Any kind of unique ID should do the job */
#define PMIX_OBJ_MAGIC_ID ((0xdeafbeedULL << 32) + 0xdeafbeedULL)
#endif

/* typedefs ***********************************************************/

typedef struct pmix_object_t pmix_object_t;
typedef struct pmix_class_t pmix_class_t;
typedef void (*pmix_construct_t) (pmix_object_t *);
typedef void (*pmix_destruct_t) (pmix_object_t *);


/* types **************************************************************/

/**
 * Class descriptor.
 *
 * There should be a single instance of this descriptor for each class
 * definition.
 */
struct pmix_class_t {
    const char *cls_name;           /**< symbolic name for class */
    pmix_class_t *cls_parent;       /**< parent class descriptor */
    pmix_construct_t cls_construct; /**< class constructor */
    pmix_destruct_t cls_destruct;   /**< class destructor */
    int cls_initialized;            /**< is class initialized */
    int cls_depth;                  /**< depth of class hierarchy tree */
    pmix_construct_t *cls_construct_array;
                                    /**< array of parent class constructors */
    pmix_destruct_t *cls_destruct_array;
                                    /**< array of parent class destructors */
    size_t cls_sizeof;              /**< size of an object instance */
};

PMIX_EXPORT extern int pmix_class_init_epoch;

/**
 * For static initializations of OBJects.
 *
 * @param NAME   Name of the class to initialize
 */
#if PMIX_ENABLE_DEBUG
#define PMIX_OBJ_STATIC_INIT(BASE_CLASS) { PMIX_OBJ_MAGIC_ID, PMIX_CLASS(BASE_CLASS), 1, __FILE__, __LINE__ }
#else
#define PMIX_OBJ_STATIC_INIT(BASE_CLASS) { PMIX_CLASS(BASE_CLASS), 1 }
#endif

/**
 * Base object.
 *
 * This is special and does not follow the pattern for other classes.
 */
struct pmix_object_t {
#if PMIX_ENABLE_DEBUG
    /** Magic ID -- want this to be the very first item in the
        struct's memory */
    uint64_t obj_magic_id;
#endif
    pmix_class_t *obj_class;            /**< class descriptor */
    pmix_atomic_int32_t obj_reference_count;   /**< reference count */
#if PMIX_ENABLE_DEBUG
   const char* cls_init_file_name;        /**< In debug mode store the file where the object get contructed */
   int   cls_init_lineno;           /**< In debug mode store the line number where the object get contructed */
#endif  /* PMIX_ENABLE_DEBUG */
};

/* macros ************************************************************/

/**
 * Return a pointer to the class descriptor associated with a
 * class type.
 *
 * @param NAME          Name of class
 * @return              Pointer to class descriptor
 */
#define PMIX_CLASS(NAME)     (&(NAME ## _class))


/**
 * Static initializer for a class descriptor
 *
 * @param NAME          Name of class
 * @param PARENT        Name of parent class
 * @param CONSTRUCTOR   Pointer to constructor
 * @param DESTRUCTOR    Pointer to destructor
 *
 * Put this in NAME.c
 */
#define PMIX_CLASS_INSTANCE(NAME, PARENT, CONSTRUCTOR, DESTRUCTOR)       \
    pmix_class_t NAME ## _class = {                                     \
        # NAME,                                                         \
        PMIX_CLASS(PARENT),                                              \
        (pmix_construct_t) CONSTRUCTOR,                                 \
        (pmix_destruct_t) DESTRUCTOR,                                   \
        0, 0, NULL, NULL,                                               \
        sizeof(NAME)                                                    \
    }


/**
 * Declaration for class descriptor
 *
 * @param NAME          Name of class
 *
 * Put this in NAME.h
 */
#define PMIX_CLASS_DECLARATION(NAME)             \
    extern pmix_class_t NAME ## _class


/**
 * Create an object: dynamically allocate storage and run the class
 * constructor.
 *
 * @param type          Type (class) of the object
 * @return              Pointer to the object
 */
static inline pmix_object_t *pmix_obj_new(pmix_class_t * cls);
#if PMIX_ENABLE_DEBUG
static inline pmix_object_t *pmix_obj_new_debug(pmix_class_t* type, const char* file, int line)
{
    pmix_object_t* object = pmix_obj_new(type);
    object->obj_magic_id = PMIX_OBJ_MAGIC_ID;
    object->cls_init_file_name = file;
    object->cls_init_lineno = line;
    return object;
}
#define PMIX_NEW(type)                                   \
    ((type *)pmix_obj_new_debug(PMIX_CLASS(type), __FILE__, __LINE__))
#else
#define PMIX_NEW(type)                                   \
    ((type *) pmix_obj_new(PMIX_CLASS(type)))
#endif  /* PMIX_ENABLE_DEBUG */

/**
 * Retain an object (by incrementing its reference count)
 *
 * @param object        Pointer to the object
 */
#if PMIX_ENABLE_DEBUG
#define PMIX_RETAIN(object)                                              \
    do {                                                                \
        assert(NULL != ((pmix_object_t *) (object))->obj_class);        \
        assert(PMIX_OBJ_MAGIC_ID == ((pmix_object_t *) (object))->obj_magic_id); \
        pmix_obj_update((pmix_object_t *) (object), 1);                 \
        assert(((pmix_object_t *) (object))->obj_reference_count >= 0); \
    } while (0)
#else
#define PMIX_RETAIN(object)  pmix_obj_update((pmix_object_t *) (object), 1);
#endif

/**
 * Helper macro for the debug mode to store the locations where the status of
 * an object change.
 */
#if PMIX_ENABLE_DEBUG
#define PMIX_REMEMBER_FILE_AND_LINENO( OBJECT, FILE, LINENO )    \
    do {                                                        \
        ((pmix_object_t*)(OBJECT))->cls_init_file_name = FILE;  \
        ((pmix_object_t*)(OBJECT))->cls_init_lineno = LINENO;   \
    } while (0)
#define PMIX_SET_MAGIC_ID( OBJECT, VALUE )                       \
    do {                                                        \
        ((pmix_object_t*)(OBJECT))->obj_magic_id = (VALUE);     \
    } while (0)
#else
#define PMIX_REMEMBER_FILE_AND_LINENO( OBJECT, FILE, LINENO )
#define PMIX_SET_MAGIC_ID( OBJECT, VALUE )
#endif  /* PMIX_ENABLE_DEBUG */

/**
 * Release an object (by decrementing its reference count).  If the
 * reference count reaches zero, destruct (finalize) the object and
 * free its storage.
 *
 * Note: If the object is freed, then the value of the pointer is set
 * to NULL.
 *
 * @param object        Pointer to the object
 */
#if PMIX_ENABLE_DEBUG
#define PMIX_RELEASE(object)                                             \
    do {                                                                \
        assert(NULL != ((pmix_object_t *) (object))->obj_class);        \
        assert(PMIX_OBJ_MAGIC_ID == ((pmix_object_t *) (object))->obj_magic_id); \
        if (0 == pmix_obj_update((pmix_object_t *) (object), -1)) {     \
            PMIX_SET_MAGIC_ID((object), 0);                              \
            pmix_obj_run_destructors((pmix_object_t *) (object));       \
            PMIX_REMEMBER_FILE_AND_LINENO( object, __FILE__, __LINE__ ); \
            free(object);                                               \
            object = NULL;                                              \
        }                                                               \
    } while (0)
#else
#define PMIX_RELEASE(object)                                             \
    do {                                                                \
        if (0 == pmix_obj_update((pmix_object_t *) (object), -1)) {     \
            pmix_obj_run_destructors((pmix_object_t *) (object));       \
            free(object);                                               \
            object = NULL;                                              \
        }                                                               \
    } while (0)
#endif


/**
 * Construct (initialize) objects that are not dynamically allocated.
 *
 * @param object        Pointer to the object
 * @param type          The object type
 */

#define PMIX_CONSTRUCT(object, type)                             \
do {                                                            \
    PMIX_CONSTRUCT_INTERNAL((object), PMIX_CLASS(type));          \
} while (0)

#define PMIX_CONSTRUCT_INTERNAL(object, type)                        \
do {                                                                \
    PMIX_SET_MAGIC_ID((object), PMIX_OBJ_MAGIC_ID);              \
    if (pmix_class_init_epoch != (type)->cls_initialized) {                             \
        pmix_class_initialize((type));                              \
    }                                                               \
    ((pmix_object_t *) (object))->obj_class = (type);               \
    ((pmix_object_t *) (object))->obj_reference_count = 1;          \
    pmix_obj_run_constructors((pmix_object_t *) (object));          \
    PMIX_REMEMBER_FILE_AND_LINENO( object, __FILE__, __LINE__ ); \
} while (0)


/**
 * Destruct (finalize) an object that is not dynamically allocated.
 *
 * @param object        Pointer to the object
 */
#if PMIX_ENABLE_DEBUG
#define PMIX_DESTRUCT(object)                                    \
do {                                                            \
    assert(PMIX_OBJ_MAGIC_ID == ((pmix_object_t *) (object))->obj_magic_id); \
    PMIX_SET_MAGIC_ID((object), 0);                              \
    pmix_obj_run_destructors((pmix_object_t *) (object));       \
    PMIX_REMEMBER_FILE_AND_LINENO( object, __FILE__, __LINE__ ); \
} while (0)
#else
#define PMIX_DESTRUCT(object)                                    \
do {                                                            \
    pmix_obj_run_destructors((pmix_object_t *) (object));       \
    PMIX_REMEMBER_FILE_AND_LINENO( object, __FILE__, __LINE__ ); \
} while (0)
#endif

PMIX_CLASS_DECLARATION(pmix_object_t);

/* declarations *******************************************************/

/**
 * Lazy initialization of class descriptor.
 *
 * Specifically cache arrays of function pointers for the constructor
 * and destructor hierarchies for this class.
 *
 * @param class    Pointer to class descriptor
 */
PMIX_EXPORT void pmix_class_initialize(pmix_class_t *);

/**
 * Shut down the class system and release all memory
 *
 * This function should be invoked as the ABSOLUTE LAST function to
 * use the class subsystem.  It frees all associated memory with ALL
 * classes, rendering all of them inoperable.  It is here so that
 * tools like valgrind and purify don't report still-reachable memory
 * upon process termination.
 */
PMIX_EXPORT int pmix_class_finalize(void);

/**
 * Run the hierarchy of class constructors for this object, in a
 * parent-first order.
 *
 * Do not use this function directly: use PMIX_CONSTRUCT() instead.
 *
 * WARNING: This implementation relies on a hardwired maximum depth of
 * the inheritance tree!!!
 *
 * Hardwired for fairly shallow inheritance trees
 * @param size          Pointer to the object.
 */
static inline void pmix_obj_run_constructors(pmix_object_t * object)
{
    pmix_construct_t* cls_construct;

    assert(NULL != object->obj_class);

    cls_construct = object->obj_class->cls_construct_array;
    while( NULL != *cls_construct ) {
        (*cls_construct)(object);
        cls_construct++;
    }
}


/**
 * Run the hierarchy of class destructors for this object, in a
 * parent-last order.
 *
 * Do not use this function directly: use PMIX_DESTRUCT() instead.
 *
 * @param size          Pointer to the object.
 */
static inline void pmix_obj_run_destructors(pmix_object_t * object)
{
    pmix_destruct_t* cls_destruct;

    assert(NULL != object->obj_class);

    cls_destruct = object->obj_class->cls_destruct_array;
    while( NULL != *cls_destruct ) {
        (*cls_destruct)(object);
        cls_destruct++;
    }
}


/**
 * Create new object: dynamically allocate storage and run the class
 * constructor.
 *
 * Do not use this function directly: use PMIX_NEW() instead.
 *
 * @param size          Size of the object
 * @param cls           Pointer to the class descriptor of this object
 * @return              Pointer to the object
 */
static inline pmix_object_t *pmix_obj_new(pmix_class_t * cls)
{
    pmix_object_t *object;
    assert(cls->cls_sizeof >= sizeof(pmix_object_t));

    object = (pmix_object_t *) malloc(cls->cls_sizeof);
    if (pmix_class_init_epoch != cls->cls_initialized) {
        pmix_class_initialize(cls);
    }
    if (NULL != object) {
        object->obj_class = cls;
        object->obj_reference_count = 1;
        pmix_obj_run_constructors(object);
    }
    return object;
}


/**
 * Atomically update the object's reference count by some increment.
 *
 * This function should not be used directly: it is called via the
 * macros PMIX_RETAIN and PMIX_RELEASE
 *
 * @param object        Pointer to the object
 * @param inc           Increment by which to update reference count
 * @return              New value of the reference count
 */
static inline int pmix_obj_update(pmix_object_t *object, int inc) __pmix_attribute_always_inline__;
static inline int pmix_obj_update(pmix_object_t *object, int inc)
{
    return PMIX_THREAD_ADD_FETCH32(&object->obj_reference_count, inc);
}

END_C_DECLS

#endif
