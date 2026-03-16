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
#include "vprotocol_pessimist.h"

int mca_vprotocol_pessimist_add_procs(struct ompi_proc_t **procs, size_t nprocs)
{
  /* TODO: for each proc, retrieve post send of sender based request, post recieve of list
      block any other communications until we are up. To be determined how i manage to send (or resend) data to failed nodes
  */
  return mca_pml_v.host_pml.pml_add_procs(procs, nprocs);
}

int mca_vprotocol_pessimist_del_procs(struct ompi_proc_t **procs, size_t nprocs)
{
  /* TODO: I don't know, now... */
  return mca_pml_v.host_pml.pml_del_procs(procs, nprocs);
}
