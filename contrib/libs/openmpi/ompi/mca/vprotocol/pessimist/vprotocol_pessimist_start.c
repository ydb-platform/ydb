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

OMPI_DECLSPEC int mca_vprotocol_pessimist_start(size_t count, ompi_request_t **requests)
{
  int ret;
  size_t i;

  for(i = 0; i < count; i++)
  {
    mca_pml_base_request_t *pml_request = (mca_pml_base_request_t *) requests[i];
    if(NULL == pml_request) continue;

    switch(pml_request->req_type)
    {
      case MCA_PML_REQUEST_RECV :
        V_OUTPUT_VERBOSE(50, "pessimist:\tstart\trecv\t%"PRIpclock"\tfrom %d\ttag %d\tsize %lu", mca_vprotocol_pessimist.clock, pml_request->req_peer, pml_request->req_tag, (long) pml_request->req_count);
        /* It's a persistent recv request, first, see if we have to enforce matching order */
        VPROTOCOL_PESSIMIST_MATCHING_REPLAY(pml_request->req_peer);
        break;

      case MCA_PML_REQUEST_SEND :
        V_OUTPUT_VERBOSE(50, "pessimist:\tstart\tsend\t%"PRIpclock"\tto %d\ttag %d\tsize %lu", mca_vprotocol_pessimist.clock, pml_request->req_peer, pml_request->req_tag, (long) pml_request->req_count);
        /* It's a persistent send request, first, check if we are waiting ack
         * for some older events */
        break;

      default:
        V_OUTPUT_VERBOSE(50, "pessimist:\tstart\twrong %d\t%"PRIpclock"\tfrom %d\ttag %d\tsize %lu", pml_request->req_type, mca_vprotocol_pessimist.clock, pml_request->req_peer, pml_request->req_tag, (long) pml_request->req_count);
        return OMPI_ERR_REQUEST;
    }
  }
  ret = mca_pml_v.host_pml.pml_start(count, requests);

  /* restore requests status */
  return ret;
}
