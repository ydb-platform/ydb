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
#include "ompi/request/request.h"
#include "ompi/mca/pml/base/pml_base_recvreq.h"
#include "vprotocol_pessimist.h"





int mca_vprotocol_pessimist_irecv(void *addr,
                     size_t count,
                     ompi_datatype_t * datatype,
                     int src,
                     int tag,
                     struct ompi_communicator_t *comm,
                     struct ompi_request_t **request)
{
  int ret;

  V_OUTPUT_VERBOSE(50, "pessimist:\tirecv\trequest\t%"PRIpclock"\tfrom %d\ttag %d\tsize %lu",
                        mca_vprotocol_pessimist.clock, src, tag, (unsigned long) count);

  /* first, see if we have to enforce matching order */
  VPROTOCOL_PESSIMIST_MATCHING_REPLAY(src);
  /* now just let the host pml do its job */
  ret = mca_pml_v.host_pml.pml_irecv(addr, count, datatype, src, tag, comm, request);
  VPESSIMIST_FTREQ_INIT(*request);
  vprotocol_pessimist_matching_log_prepare(*request);
  return ret;
}





int mca_vprotocol_pessimist_recv(void *addr,
                      size_t count,
                      ompi_datatype_t * datatype,
                      int src,
                      int tag,
                      struct ompi_communicator_t *comm,
                      ompi_status_public_t * status )
{
  ompi_request_t *request = MPI_REQUEST_NULL;
  int ret;

  V_OUTPUT_VERBOSE(50, "pessimist:\trecv\tposted\t%"PRIpclock"\tfrom %d\ttag %d\tsize %lu",
                       mca_vprotocol_pessimist.clock, src, tag, (unsigned long) count);
  /* first, see if we have to enforce matching order */
  VPROTOCOL_PESSIMIST_MATCHING_REPLAY(src);
  /* now just let the pml do its job */
  ret = mca_pml_v.host_pml.pml_irecv(addr, count, datatype, src, tag, comm, &request);
  VPESSIMIST_FTREQ_INIT(request);
  vprotocol_pessimist_matching_log_prepare(request);
  /* block until the request is completed */
  VPROTOCOL_PESSIMIST_WAIT(&request, status, ret);
  return ret;
}
