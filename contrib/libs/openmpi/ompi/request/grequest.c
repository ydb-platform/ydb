/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/communicator/communicator.h"
#include "ompi/request/grequest.h"
#include "ompi/mpi/fortran/base/fint_2_int.h"


/*
 * See the comment in the grequest destructor for the weird semantics
 * here.  If the request has been marked complete via a call to
 * MPI_GREQUEST_COMPLETE, actually release the object.  OTherwise,
 * just mark this object as "freed" so that a later call to
 * MPI_GREQUEST_COMPLETE will release it (!).
 *
 * Note that TEST* and WAIT* will call this function when a request
 * has been completed.
 */
static int ompi_grequest_free(ompi_request_t** req)
{
    OBJ_RELEASE(*req);
    *req = MPI_REQUEST_NULL;
    return OMPI_SUCCESS;
}

static int ompi_grequest_cancel(ompi_request_t* req, int flag)
{
    int rc = OMPI_SUCCESS;
    MPI_Fint ierr;
    ompi_fortran_logical_t fflag;
    ompi_grequest_t* greq = (ompi_grequest_t*)req;

    if (greq->greq_cancel.c_cancel != NULL) {
        if (greq->greq_funcs_are_c) {
            rc = greq->greq_cancel.c_cancel(greq->greq_state,
                                            REQUEST_COMPLETE(&greq->greq_base));
        } else {
            fflag = (ompi_fortran_logical_t) REQUEST_COMPLETE(&greq->greq_base);
            greq->greq_cancel.f_cancel((MPI_Aint*)greq->greq_state, &fflag, &ierr);
            rc = OMPI_FINT_2_INT(ierr);
        }
    }
    return rc;
}

static void ompi_grequest_construct(ompi_grequest_t* greq)
{
    greq->greq_base.req_free     = ompi_grequest_free;
    greq->greq_base.req_cancel   = ompi_grequest_cancel;
    greq->greq_base.req_type = OMPI_REQUEST_GEN;
    greq->greq_base.req_mpi_object.comm = &(ompi_mpi_comm_world.comm);
    /* Set the function pointers to C here; the F77 MPI API will
       override this value if the gen request was created from
       Fortran */
    greq->greq_funcs_are_c = true;
}

/*
 * MPI has some weird semantics with respect to generalized requests
 * -- different than all other MPI object types.  So we move some
 * cleanup stuff here to the destructor rather than in
 * greqeust_request_free -- mainly because the cleanup may be required
 * in two different places.
 *
 * Specifically, generalized requests can be completed (and therefore
 * released) the following ways:
 *
 * 1. Call to MPI_GREQUEST_COMPLETE and then a corresponding call to
 * some flavor of MPI_TEST* or MPI_WAIT*.  This will both complete the
 * requests and destroy the coresponding MPI generalized request
 * object.
 *
 * 2. Call MPI_REQUEST_FREE and then (!) -- with some other
 * still-valid copy of the handler -- call MPI_GREQUEST_COMPLETE.
 *
 * 3. Reverse the order of #2 -- call MPI_GREQUEST_COMPLETE and then
 * MPI_REQUEST_FREE.
 *
 * So any one of these functions may actually be the one that
 * de-allocates the back-end request object.  Hence, this is perfect
 * for our reference counting system -- so the call to the gen request
 * free_fn() is back here in the destructor, whenever the object is
 * actually freed.
 *
 * Hence, the following must occur before a grequest is freed:
 *
 * - ompi_grequest_complete() (i.e., GREQUEST_COMPLETE) is invoked
 * - ompi_grequest_free() is invoked
 *
 * Remember that ompi_grequest_free() is invoked by MPI_TEST* and
 * MPI_WAIT* when the request was previously marked as complete and
 * TEST* / WAIT* notified the user as such, and this function is also
 * invoked by REQUEST_FREE).  Hence, these two functions will *always*
 * be invoked, but the order in which they are invoked is up to the
 * user.  So this is a perfect opprotunity for the OBJ_* reference
 * count system.  When we create an ompi_grequest_t in
 * ompi_grequest_start(), we both OBJ_NEW and OBJ_RETAIN it so that
 * its reference count goes to 0.  Then in ompi_grequest_complete()
 * and ompi_grequest_free(), we OBJ_RELEASE it.  Hence, when both of
 * them have RELEASEd -- regardless of the order in which the
 * functions were invoked, then the destructor is invoked and
 * everything is cleaned up (and we invoked the grequest free_fn).
 */
static void ompi_grequest_destruct(ompi_grequest_t* greq)
{
    MPI_Fint ierr;

    if (greq->greq_free.c_free != NULL) {
        if (greq->greq_funcs_are_c) {
            greq->greq_free.c_free(greq->greq_state);
        } else {
            greq->greq_free.f_free((MPI_Aint*)greq->greq_state, &ierr);
        }
    }

    OMPI_REQUEST_FINI(&greq->greq_base);
}


OBJ_CLASS_INSTANCE(
    ompi_grequest_t,
    ompi_request_t,
    ompi_grequest_construct,
    ompi_grequest_destruct);


int ompi_grequest_start(
    MPI_Grequest_query_function *gquery_fn,
    MPI_Grequest_free_function *gfree_fn,
    MPI_Grequest_cancel_function *gcancel_fn,
    void* gstate,
    ompi_request_t** request)
{
    ompi_grequest_t *greq = OBJ_NEW(ompi_grequest_t);
    if(greq == NULL) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    /* We call RETAIN here specifically to increase the refcount to 2.
       See comment before the destructor for an explanation. */
    OBJ_RETAIN(greq);

    greq->greq_base.req_state = OMPI_REQUEST_ACTIVE;
    greq->greq_state = gstate;
    greq->greq_query.c_query = gquery_fn;
    greq->greq_free.c_free = gfree_fn;
    greq->greq_cancel.c_cancel = gcancel_fn;
    greq->greq_base.req_status = ompi_status_empty;

    *request = &greq->greq_base;
    return OMPI_SUCCESS;
}


/*
 * Beware the odd semantics listed in MPI-2:8.2...  See the comment in
 * the grequest destructor.
 *
 * First do the normal stuff to complete the request (i.e., call
 * ompi_request_complete()).  Then, if this request object was
 * previously freed via MPI_REQUEST_FREE, release it.
 */
int ompi_grequest_complete(ompi_request_t *req)
{
    int rc;

    rc = ompi_request_complete(req, true);
    OBJ_RELEASE(req);
    return rc;
}


/*
 * Grequest queries are invoked in two places:
 *
 * 1. MPI_TEST* / MPI_WAIT*, when requests have completed.
 *
 * 2. MPI_REQUEST_GET_STATUS, when requests may or may not have
 * completed.
 *
 */
int ompi_grequest_invoke_query(ompi_request_t *request,
                               ompi_status_public_t *status)
{
    int rc = OMPI_SUCCESS;
    ompi_grequest_t *g = (ompi_grequest_t*) request;

    /* MPI-2:8.2 does not say what to do with the return value from
       the query function (i.e., the int return value from the C
       function or the ierr argument from the Fortran function).
       Making the command decision here to ignore it.  If the handler
       wants to pass an error back, it should set it in the MPI_ERROR
       field in the status (which is always kept, regardless if the
       top-level function was invoked with MPI_STATUS[ES]_IGNORE or
       not).  */
    if (NULL != g->greq_query.c_query) {
        if (g->greq_funcs_are_c) {
            rc = g->greq_query.c_query(g->greq_state, status);
        } else {
            MPI_Fint ierr;
            MPI_Fint fstatus[sizeof(MPI_Status) / sizeof(int)];
            g->greq_query.f_query((MPI_Aint*)g->greq_state, fstatus, &ierr);
            MPI_Status_f2c(fstatus, status);
            rc = OMPI_FINT_2_INT(ierr);
        }
    }

    return rc;
}

