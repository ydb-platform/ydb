// -*- c++ -*-
//
// Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
//                         University Research and Technology
//                         Corporation.  All rights reserved.
// Copyright (c) 2004-2005 The University of Tennessee and The University
//                         of Tennessee Research Foundation.  All rights
//                         reserved.
// Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
//                         University of Stuttgart.  All rights reserved.
// Copyright (c) 2004-2005 The Regents of the University of California.
//                         All rights reserved.
// Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
// Copyright (c) 2011      FUJITSU LIMITED.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//


class Intercomm : public Comm {
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  //  friend class PMPI::Intercomm;
#endif
public:

  // construction
  Intercomm() : Comm(MPI_COMM_NULL) { }
  // copy
  Intercomm(const Comm_Null& data) : Comm(data) { }
  // inter-language operability
  Intercomm(MPI_Comm data) : Comm(data) { }

#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  // copy
  Intercomm(const Intercomm& data) : Comm(data), pmpi_comm(data.pmpi_comm) { }
  Intercomm(const PMPI::Intercomm& d) :
    Comm((const PMPI::Comm&)d), pmpi_comm(d) { }

  // assignment
  Intercomm& operator=(const Intercomm& data) {
    Comm::operator=(data);
    pmpi_comm = data.pmpi_comm; return *this; }
  Intercomm& operator=(const Comm_Null& data) {
    Comm::operator=(data);
    Intercomm& ic = (Intercomm&)data;
    pmpi_comm = ic.pmpi_comm; return *this; }
  // inter-language operability
  Intercomm& operator=(const MPI_Comm& data) {
    Comm::operator=(data);
    pmpi_comm = PMPI::Intercomm(data); return *this; }
#else
  // copy
  Intercomm(const Intercomm& data) : Comm(data.mpi_comm) { }
  // assignment
  Intercomm& operator=(const Intercomm& data) {
    mpi_comm = data.mpi_comm; return *this; }
  Intercomm& operator=(const Comm_Null& data) {
    mpi_comm = data; return *this; }
  // inter-language operability
  Intercomm& operator=(const MPI_Comm& data) {
    mpi_comm = data; return *this; }

#endif


  //
  // Groups, Contexts, and Communicators
  //

  Intercomm Dup() const;

  virtual Intercomm& Clone() const;

  virtual int Get_remote_size() const;

  virtual Group Get_remote_group() const;

  virtual Intracomm Merge(bool high) const;

  virtual Intercomm Create(const Group& group) const;

  virtual Intercomm Split(int color, int key) const;

};
