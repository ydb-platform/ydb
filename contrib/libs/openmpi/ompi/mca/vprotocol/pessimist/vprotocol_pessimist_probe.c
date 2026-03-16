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

static inline int replay_iprobe(int src, int tag,
                                  struct ompi_communicator_t *comm,
                                  int *matched, ompi_status_public_t * status );
static inline void log_iprobe(int ret, int src, int tag,
                               struct ompi_communicator_t *comm,
                               int *matched, ompi_status_public_t * status);
static inline int replay_probe(int src, int tag,
                                 struct ompi_communicator_t *comm,
                                 ompi_status_public_t * status);
static inline void log_probe(int ret, int src, int tag,
                               struct ompi_communicator_t *comm,
                               ompi_status_public_t * status);

/*******************************************************************************
 * MPI level functions
 */
int mca_vprotocol_pessimist_iprobe( int src, int tag,
                        struct ompi_communicator_t *comm,
                        int *matched, ompi_status_public_t * status )
{
  int ret;

  if(mca_vprotocol_pessimist.replay)
  {
    ret = replay_iprobe(src, tag, comm, matched, status);
  }
  else
  {
    ret = mca_pml_v.host_pml.pml_iprobe(src, tag, comm, matched, status);
    log_iprobe(ret, src, tag, comm, matched, status);
  }
  return ret;
}

int mca_vprotocol_pessimist_probe( int src, int tag,
                       struct ompi_communicator_t *comm,
                       ompi_status_public_t * status )
{
  int ret;

  if(mca_vprotocol_pessimist.replay)
  {
    ret = replay_probe(src, tag, comm, status);
  }
  else
  {
    ret = mca_pml_v.host_pml.pml_probe(src, tag, comm, status);
    log_probe(ret, src, tag, comm, status);
  }
  return ret;
}

/*******************************************************************************
 * message logging internals
 */
static inline int replay_iprobe(int src, int tag,
                                  struct ompi_communicator_t *comm,
                                  int *matched, ompi_status_public_t * status )
{
  return OMPI_ERROR;
}

static inline void log_iprobe(int ret, int src, int tag,
                               struct ompi_communicator_t *comm,
                               int *matched, ompi_status_public_t * status)
{
  return;
}

static inline int replay_probe(int src, int tag,
                                 struct ompi_communicator_t *comm,
                                 ompi_status_public_t * status)
{
  return OMPI_ERROR;
}

static inline void log_probe(int ret, int src, int tag,
                               struct ompi_communicator_t *comm,
                               ompi_status_public_t * status)
{
  return;
}
