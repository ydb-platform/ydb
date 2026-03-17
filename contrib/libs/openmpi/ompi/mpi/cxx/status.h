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
// Copyright (c) 2006-2008 Cisco Systems, Inc.  All rights reserved.
// Copyright (c) 2017      Research Organization for Information Science
//                         and Technology (RIST). All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//


class Status {
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  //  friend class PMPI::Status;
#endif
  friend class MPI::Comm; //so I can access pmpi_status data member in comm.cc
  friend class MPI::Request; //and also from request.cc
  friend class MPI::File;

public:
#if 0 /* OMPI_ENABLE_MPI_PROFILING */

  // construction / destruction
  Status() { }
  virtual ~Status() {}

  // copy / assignment
  Status(const Status& data) : pmpi_status(data.pmpi_status) { }

  Status(const MPI_Status &i) : pmpi_status(i) { }

  Status& operator=(const Status& data) {
    pmpi_status = data.pmpi_status; return *this; }

  // comparison, don't need for status

  // inter-language operability
  Status& operator= (const MPI_Status &i) {
    pmpi_status = i; return *this; }
  operator MPI_Status () const { return pmpi_status; }
  //  operator MPI_Status* () const { return pmpi_status; }
  operator const PMPI::Status&() const { return pmpi_status; }

#else

  Status() : mpi_status() { }
  // copy
  Status(const Status& data) : mpi_status(data.mpi_status) { }

  Status(const MPI_Status &i) : mpi_status(i) { }

  virtual ~Status() {}

  Status& operator=(const Status& data) {
    mpi_status = data.mpi_status; return *this; }

  // comparison, don't need for status

  // inter-language operability
  Status& operator= (const MPI_Status &i) {
    mpi_status = i; return *this; }
  operator MPI_Status () const { return mpi_status; }
  //  operator MPI_Status* () const { return (MPI_Status*)&mpi_status; }

#endif

  //
  // Point-to-Point Communication
  //

  virtual int Get_count(const Datatype& datatype) const;

  virtual bool Is_cancelled() const;

  virtual int Get_elements(const Datatype& datatype) const;

  //
  // Status Access
  //
  virtual int Get_source() const;

  virtual void Set_source(int source);

  virtual int Get_tag() const;

  virtual void Set_tag(int tag);

  virtual int Get_error() const;

  virtual void Set_error(int error);

  virtual void Set_elements(const MPI::Datatype& datatype, int count);

  virtual void Set_cancelled(bool flag);

protected:
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  PMPI::Status pmpi_status;
#else
  MPI_Status mpi_status;
#endif

};
