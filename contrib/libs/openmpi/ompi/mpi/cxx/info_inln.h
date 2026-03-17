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
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//


inline MPI::Info
MPI::Info::Create()
{
  MPI_Info newinfo;
  (void) MPI_Info_create(&newinfo);
  return newinfo;
}

inline void
MPI::Info::Delete(const char* key)
{
   (void)MPI_Info_delete(mpi_info, const_cast<char *>(key));
}

inline MPI::Info
MPI::Info::Dup() const
{
  MPI_Info newinfo;
  (void)MPI_Info_dup(mpi_info, &newinfo);
  return newinfo;
}

inline void
MPI::Info::Free()
{
  (void) MPI_Info_free(&mpi_info);
}

inline bool
MPI::Info::Get(const char* key, int valuelen, char* value) const
{
  int flag;
  (void)MPI_Info_get(mpi_info, const_cast<char *>(key), valuelen, value, &flag);
  return OPAL_INT_TO_BOOL(flag);
}

inline int
MPI::Info::Get_nkeys() const
{
  int nkeys;
  MPI_Info_get_nkeys(mpi_info, &nkeys);
  return nkeys;
}

inline void
MPI::Info::Get_nthkey(int n, char* key) const
{
  (void) MPI_Info_get_nthkey(mpi_info, n, key);
}

inline bool
MPI::Info::Get_valuelen(const char* key, int& valuelen) const
{
  int flag;
  (void) MPI_Info_get_valuelen(mpi_info, const_cast<char *>(key), &valuelen, &flag);
  return OPAL_INT_TO_BOOL(flag);
}

inline void
MPI::Info::Set(const char* key, const char* value)
{
  (void) MPI_Info_set(mpi_info, const_cast<char *>(key), const_cast<char *>(value));
}
