/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017 IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file mca_base_component_repository.h
 *
 * This file provide the external interface to our base component
 * module.  Most of the components that depend on it, will use the
 * retain_component() function to increase the reference count on a
 * particular component (as opposed to the retain() function, which is
 * internal to the opal/mca/base).  But it's convenient to have all
 * the functions exported from one header file rather than to separate
 * retain_component() and retain() into two separate header files
 * (i.e., have a separate header file just for retain()).
 */

#ifndef MCA_BASE_COMPONENT_REPOSITORY_H
#define MCA_BASE_COMPONENT_REPOSITORY_H

#include "opal_config.h"

#include "opal/mca/dl/dl.h"
#include "opal/mca/dl/base/base.h"

BEGIN_C_DECLS
struct mca_base_component_repository_item_t {
    opal_list_item_t super;

    char ri_type[MCA_BASE_MAX_TYPE_NAME_LEN + 1];
    char ri_name[MCA_BASE_MAX_COMPONENT_NAME_LEN + 1];

    char *ri_path;
    char *ri_base;

    opal_dl_handle_t *ri_dlhandle;
    const mca_base_component_t *ri_component_struct;

    int ri_refcnt;
};
typedef struct mca_base_component_repository_item_t mca_base_component_repository_item_t;

OBJ_CLASS_DECLARATION(mca_base_component_repository_item_t);

/*
 * Structure to track information about why a component failed to load.
 */
struct mca_base_failed_component_t {
    opal_list_item_t super;
    mca_base_component_repository_item_t *comp;
    char *error_msg;
};
typedef struct mca_base_failed_component_t mca_base_failed_component_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_base_failed_component_t);

/**
 * @brief initialize the component repository
 *
 * This function must be called before any frameworks are registered or
 * opened. It is responsible for setting up the repository of dynamically
 * loaded components. The initial search path is taken from the
 * mca_base_component_path MCA parameter. mca_base_open () is a
 * prerequisite call as it registers the mca_base_component_path parameter.
 */
OPAL_DECLSPEC int mca_base_component_repository_init(void);

/**
 * @brief add search path for dynamically loaded components
 *
 * @param[in] path        delimited list of search paths to add
 */
OPAL_DECLSPEC int mca_base_component_repository_add (const char *path);


/**
 * @brief return the list of components that match a given framework
 *
 * @param[in]  framework  framework to match
 * @param[out] framework_components components that match this framework
 *
 * The list returned in {framework_components} is owned by the component
 * repository and CAN NOT be modified by the caller.
 */
OPAL_DECLSPEC int mca_base_component_repository_get_components (mca_base_framework_t *framework,
                                                                opal_list_t **framework_components);

/**
 * @brief finalize the mca component repository
 */
OPAL_DECLSPEC void mca_base_component_repository_finalize(void);

/**
 * @brief open the repository item and add it to the framework's component
 * list
 *
 * @param[in] framework   framework that matches the component
 * @param[in] ri          dynamic component to open
 */
int mca_base_component_repository_open (mca_base_framework_t *framework,
                                        mca_base_component_repository_item_t *ri);


/**
 * @brief Reduce the reference count of a component and dlclose it if necessary
 */
void mca_base_component_repository_release (const mca_base_component_t *component);

/**
 * @brief Increase the reference count of a component
 *
 * Each component repository item starts with a reference count of 0. This ensures that
 * when a framework closes it's components the repository items are all correctly
 * dlclosed. This function can be used to prevent the dlclose if a component is needed
 * after its framework has closed the associated component. Users of this function
 * should call mca_base_component_repository_release() once they are finished with the
 * component.
 *
 * @note all components are automatically unloaded by the
 * mca_base_component_repository_finalize() call.
 */
int mca_base_component_repository_retain_component (const char *type, const char *name);

END_C_DECLS

#endif /* MCA_BASE_COMPONENT_REPOSITORY_H */
