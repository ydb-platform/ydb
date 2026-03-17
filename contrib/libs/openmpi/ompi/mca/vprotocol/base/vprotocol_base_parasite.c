/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "base.h"
#include "vprotocol_base_request.h"

int mca_vprotocol_base_parasite(void) {
    if(mca_vprotocol.add_procs)
        mca_pml.pml_add_procs = mca_vprotocol.add_procs;
    if(mca_vprotocol.del_procs)
        mca_pml.pml_del_procs = mca_vprotocol.del_procs;
    if(mca_vprotocol.progress)
        mca_pml.pml_progress = mca_vprotocol.progress;
    if(mca_vprotocol.add_comm)
        mca_pml.pml_add_comm = mca_vprotocol.add_comm;
    if(mca_vprotocol.del_comm)
        mca_pml.pml_del_comm = mca_vprotocol.del_comm;
    if(mca_vprotocol.irecv_init)
        mca_pml.pml_irecv_init = mca_vprotocol.irecv_init;
    if(mca_vprotocol.irecv)
        mca_pml.pml_irecv = mca_vprotocol.irecv;
    if(mca_vprotocol.recv)
        mca_pml.pml_recv = mca_vprotocol.recv;
    if(mca_vprotocol.isend_init)
        mca_pml.pml_isend_init = mca_vprotocol.isend_init;
    if(mca_vprotocol.isend)
        mca_pml.pml_isend = mca_vprotocol.isend;
    if(mca_vprotocol.send)
        mca_pml.pml_send = mca_vprotocol.send;
    if(mca_vprotocol.iprobe)
        mca_pml.pml_iprobe = mca_vprotocol.iprobe;
    if(mca_vprotocol.probe)
        mca_pml.pml_probe = mca_vprotocol.probe;
    if(mca_vprotocol.start)
        mca_pml.pml_start = mca_vprotocol.start;
    if(mca_vprotocol.dump)
        mca_pml.pml_dump = mca_vprotocol.dump;
    if(mca_vprotocol.wait)
        ompi_request_functions.req_wait = mca_vprotocol.wait;
    if(mca_vprotocol.wait_all)
        ompi_request_functions.req_wait_all = mca_vprotocol.wait_all;
    if(mca_vprotocol.wait_any)
        ompi_request_functions.req_wait_any = mca_vprotocol.wait_any;
    if(mca_vprotocol.wait_some)
        ompi_request_functions.req_wait_some = mca_vprotocol.wait_some;
    if(mca_vprotocol.test)
        ompi_request_functions.req_test = mca_vprotocol.test;
    if(mca_vprotocol.test_all)
        ompi_request_functions.req_test_all = mca_vprotocol.test_all;
    if(mca_vprotocol.test_any)
        ompi_request_functions.req_test_any = mca_vprotocol.test_any;
    if(mca_vprotocol.test_some)
        ompi_request_functions.req_test_some = mca_vprotocol.test_some;
    return mca_vprotocol_base_request_parasite();
}
