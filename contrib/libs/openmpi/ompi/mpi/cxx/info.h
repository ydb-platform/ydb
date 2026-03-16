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
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//


class Info {
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  //  friend class PMPI::Info;
#endif
  friend class MPI::Comm; //so I can access pmpi_info data member in comm.cc
  friend class MPI::Request; //and also from request.cc

public:
#if 0 /* OMPI_ENABLE_MPI_PROFILING */

  // construction / destruction
  Info() { }
  virtual ~Info() {}


  // copy / assignment
  Info(const Info& data) : pmpi_info(data.pmpi_info) { }

  Info(MPI_Info i) : pmpi_info(i) { }

  Info& operator=(const Info& data) {
    pmpi_info = data.pmpi_info; return *this; }

  // comparison, don't need for info

  // inter-language operability
  Info& operator= (const MPI_Info &i) {
    pmpi_info = i; return *this; }
  operator MPI_Info () const { return pmpi_info; }
  //  operator MPI_Info* () const { return pmpi_info; }
  operator const PMPI::Info&() const { return pmpi_info; }


#else

  Info() : mpi_info(MPI_INFO_NULL) { }
  // copy
  Info(const Info& data) : mpi_info(data.mpi_info) { }

  Info(MPI_Info i) : mpi_info(i) { }

  virtual ~Info() {}

  Info& operator=(const Info& data) {
    mpi_info = data.mpi_info; return *this; }

  // comparison, don't need for info

  // inter-language operability
  Info& operator= (const MPI_Info &i) {
    mpi_info = i; return *this; }
  operator MPI_Info () const { return mpi_info; }
  //  operator MPI_Info* () const { return (MPI_Info*)&mpi_info; }

#endif

  static Info Create();

  virtual void Delete(const char* key);

  virtual Info Dup() const;

  virtual void Free();

  virtual bool Get(const char* key, int valuelen, char* value) const;

  virtual int Get_nkeys() const;

  virtual void Get_nthkey(int n, char* key) const;

  virtual bool Get_valuelen(const char* key, int& valuelen) const;

  virtual void Set(const char* key, const char* value);

protected:
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  PMPI::Info pmpi_info;
#else
  MPI_Info mpi_info;
#endif

};
