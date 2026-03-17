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
 * Copyright (c) 2006-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_WIN_H
#define OMPI_WIN_H

#include "ompi_config.h"
#include "mpi.h"

#include "opal/class/opal_object.h"
#include "opal/class/opal_hash_table.h"
#include "opal/util/info_subscriber.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/info/info.h"
#include "ompi/communicator/communicator.h"
#include "ompi/group/group.h"
#include "ompi/mca/osc/osc.h"

BEGIN_C_DECLS

/* flags */
#define OMPI_WIN_FREED        0x00000001
#define OMPI_WIN_INVALID      0x00000002
#define OMPI_WIN_NO_LOCKS     0x00000004
#define OMPI_WIN_SAME_DISP    0x00000008
#define OMPI_WIN_SAME_SIZE    0x00000010

enum ompi_win_accumulate_ops_t {
    OMPI_WIN_ACCUMULATE_OPS_SAME_OP_NO_OP,
    OMPI_WIN_ACCUMULATE_OPS_SAME_OP,
};
typedef enum ompi_win_accumulate_ops_t ompi_win_accumulate_ops_t;

/**
 * Accumulate ordering flags. The default accumulate ordering in
 * MPI-3.1 is rar,war,raw,waw.
 */
enum ompi_win_accumulate_order_flags_t {
    /** no accumulate ordering (may valid with any other flag) */
    OMPI_WIN_ACC_ORDER_NONE = 0x01,
    /** read-after-read ordering */
    OMPI_WIN_ACC_ORDER_RAR  = 0x02,
    /** write-after-read ordering */
    OMPI_WIN_ACC_ORDER_WAR  = 0x04,
    /** read-after-write ordering */
    OMPI_WIN_ACC_ORDER_RAW  = 0x08,
    /** write-after-write ordering */
    OMPI_WIN_ACC_ORDER_WAW  = 0x10,
};

OMPI_DECLSPEC extern mca_base_var_enum_t *ompi_win_accumulate_ops;
OMPI_DECLSPEC extern mca_base_var_enum_flag_t *ompi_win_accumulate_order;

OMPI_DECLSPEC extern opal_pointer_array_t ompi_mpi_windows;

struct ompi_win_t {
    opal_infosubscriber_t super;

    opal_mutex_t  w_lock;

    char w_name[MPI_MAX_OBJECT_NAME];
  
    /* Group associated with this window. */
    ompi_group_t *w_group;

    /* Information about the state of the window.  */
    uint16_t w_flags;

    /** Window flavor */
    uint16_t w_flavor;

    /** Accumulate ops */
    ompi_win_accumulate_ops_t w_acc_ops;

    /* Attributes */
    opal_hash_table_t *w_keyhash;

    /* index in Fortran <-> C translation array */
    int w_f_to_c_index;

    /* Error handling.  This field does not have the "w_" prefix so
       that the OMPI_ERRHDL_* macros can find it, regardless of
       whether it's a comm, window, or file. */
    ompi_errhandler_t                    *error_handler;
    ompi_errhandler_type_t               errhandler_type;

    /* one sided interface */
    ompi_osc_base_module_t *w_osc_module;

    /** Accumulate ordering (see ompi_win_accumulate_order_flags_t above) */
    int32_t w_acc_order;
};
typedef struct ompi_win_t ompi_win_t;
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_win_t);

/**
 * Padded struct to maintain back compatibiltiy.
 * See ompi/communicator/communicator.h comments with struct ompi_communicator_t
 * for full explanation why we chose the following padding construct for predefines.
 */
#define PREDEFINED_WIN_PAD 512

struct ompi_predefined_win_t {
    struct ompi_win_t win;
    char padding[PREDEFINED_WIN_PAD - sizeof(ompi_win_t)];
};
typedef struct ompi_predefined_win_t ompi_predefined_win_t;

OMPI_DECLSPEC extern ompi_predefined_win_t ompi_mpi_win_null;
OMPI_DECLSPEC extern ompi_predefined_win_t *ompi_mpi_win_null_addr;

int ompi_win_init(void);
int ompi_win_finalize(void);

int ompi_win_create(void *base, size_t size, int disp_unit,
                    ompi_communicator_t *comm, opal_info_t *info,
                    ompi_win_t **newwin);
int ompi_win_allocate(size_t size, int disp_unit, opal_info_t *info,
                      ompi_communicator_t *comm, void *baseptr, ompi_win_t **newwin);
int ompi_win_allocate_shared(size_t size, int disp_unit, opal_info_t *info,
                      ompi_communicator_t *comm, void *baseptr, ompi_win_t **newwin);
int ompi_win_create_dynamic(opal_info_t *info, ompi_communicator_t *comm, ompi_win_t **newwin);

int ompi_win_free(ompi_win_t *win);

OMPI_DECLSPEC int ompi_win_set_name(ompi_win_t *win, const char *win_name);
OMPI_DECLSPEC int ompi_win_get_name(ompi_win_t *win, char *win_name, int *length);

OMPI_DECLSPEC int ompi_win_group(ompi_win_t *win, ompi_group_t **group);

/* Note that the defintion of an "invalid" window is closely related
   to the defintion of an "invalid" communicator.  See a big comment
   in ompi/communicator/communicator.h about this. */
static inline int ompi_win_invalid(ompi_win_t *win) {
    if (NULL == win ||
        MPI_WIN_NULL == win ||
        (OMPI_WIN_INVALID & win->w_flags) ||
        (OMPI_WIN_FREED & win->w_flags)) {
        return true;
    } else {
        return false;
    }
}

static inline int ompi_win_peer_invalid(ompi_win_t *win, int peer) {
    if (win->w_group->grp_proc_count <= peer || peer < 0) return true;
    return false;
}

static inline int ompi_win_rank(ompi_win_t *win) {
    return win->w_group->grp_my_rank;
}

static inline bool ompi_win_allow_locks(ompi_win_t *win) {
    return (0 == (win->w_flags & OMPI_WIN_NO_LOCKS));
}

END_C_DECLS
#endif
