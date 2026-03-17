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
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

//
// Point-to-Point Communication
//

inline int
MPI::Status::Get_count(const MPI::Datatype& datatype) const
{
  int count;
  (void)MPI_Get_count(const_cast<MPI_Status*>(&mpi_status), datatype, &count);
  return count;
}

inline bool
MPI::Status::Is_cancelled() const
{
  int t;
  (void)MPI_Test_cancelled(const_cast<MPI_Status*>(&mpi_status), &t);
  return OPAL_INT_TO_BOOL(t);
}

inline int
MPI::Status::Get_elements(const MPI::Datatype& datatype) const
{
  int count;
  (void)MPI_Get_elements(const_cast<MPI_Status*>(&mpi_status), datatype, &count);
  return count;
}

//
// Status Access
//
inline int
MPI::Status::Get_source() const
{
  int source;
  source = mpi_status.MPI_SOURCE;
  return source;
}

inline void
MPI::Status::Set_source(int source)
{
  mpi_status.MPI_SOURCE = source;
}

inline int
MPI::Status::Get_tag() const
{
  int tag;
  tag = mpi_status.MPI_TAG;
  return tag;
}

inline void
MPI::Status::Set_tag(int tag)
{
  mpi_status.MPI_TAG = tag;
}

inline int
MPI::Status::Get_error() const
{
  int error;
  error = mpi_status.MPI_ERROR;
  return error;
}

inline void
MPI::Status::Set_error(int error)
{
  mpi_status.MPI_ERROR = error;
}

inline void
MPI::Status::Set_elements(const MPI::Datatype& datatype, int count)
{
    MPI_Status_set_elements(&mpi_status, datatype, count);
}

inline void
MPI::Status::Set_cancelled(bool flag)
{
    MPI_Status_set_cancelled(&mpi_status, (int) flag);
}

