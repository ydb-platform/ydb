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
// Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
// Copyright (c) 2011      FUJITSU LIMITED.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

#include <string.h>

//
// Point-to-Point Communication
//

inline void
MPI::Attach_buffer(void* buffer, int size)
{
  (void)MPI_Buffer_attach(buffer, size);
}

inline int
MPI::Detach_buffer(void*& buffer)
{
  int size;
  (void)MPI_Buffer_detach(&buffer, &size);
  return size;
}

//
// Process Topologies
//

inline void
MPI::Compute_dims(int nnodes, int ndims, int dims[])
{
  (void)MPI_Dims_create(nnodes, ndims, dims);
}


//
// Environmental Inquiry
//

inline int
MPI::Add_error_class()
{
    int errcls;
    (void)MPI_Add_error_class(&errcls);
    return errcls;
}

inline int
MPI::Add_error_code(int errorclass)
{
    int errcode;
    (void)MPI_Add_error_code(errorclass, &errcode);
    return errcode;
}

inline void
MPI::Add_error_string(int errorcode, const char* string)
{
    (void)MPI_Add_error_string(errorcode, const_cast<char *>(string));
}

inline void
MPI::Get_processor_name(char* name, int& resultlen)
{
  (void)MPI_Get_processor_name(name, &resultlen);
}

inline void
MPI::Get_error_string(int errorcode, char* string, int& resultlen)
{
  (void)MPI_Error_string(errorcode, string, &resultlen);
}

inline int
MPI::Get_error_class(int errorcode)
{
  int errorclass;
  (void)MPI_Error_class(errorcode, &errorclass);
  return errorclass;
}

inline double
MPI::Wtime()
{
  return (MPI_Wtime());
}

inline double
MPI::Wtick()
{
  return (MPI_Wtick());
}

inline void
MPI::Real_init()
{
    MPI::InitializeIntercepts();
}


inline void
MPI::Init(int& argc, char**& argv)
{
  (void)MPI_Init(&argc, &argv);
  Real_init();
}

inline void
MPI::Init()
{
  (void)MPI_Init(0, 0);
  Real_init();
}

inline void
MPI::Finalize()
{
  (void)MPI_Finalize();
}

inline bool
MPI::Is_initialized()
{
  int t;
  (void)MPI_Initialized(&t);
  return OPAL_INT_TO_BOOL(t);
}

inline bool
MPI::Is_finalized()
{
  int t;
  (void)MPI_Finalized(&t);
  return OPAL_INT_TO_BOOL(t);
}


//
// External Interfaces
//

inline int
MPI::Init_thread(int required)
{
  int provided;
  (void) MPI_Init_thread(0, NULL, required, &provided);
  Real_init();
  return provided;
}


inline int
MPI::Init_thread(int& argc, char**& argv, int required)
{
  int provided;
  (void) MPI_Init_thread(&argc, &argv, required, &provided);
  Real_init();
  return provided;
}


inline bool
MPI::Is_thread_main()
{
  int flag;
  (void) MPI_Is_thread_main(&flag);
  return OPAL_INT_TO_BOOL(flag == 1);
}


inline int
MPI::Query_thread()
{
  int provided;
  (void) MPI_Query_thread(&provided);
  return provided;
}


//
// Miscellany
//


inline void*
MPI::Alloc_mem(MPI::Aint size, const MPI::Info& info)
{
  void* baseptr;
  (void) MPI_Alloc_mem(size, info, &baseptr);
  return baseptr;
}


inline void
MPI::Free_mem(void* base)
{
  (void) MPI_Free_mem(base);
}


//
// Process Creation
//


inline void
MPI::Close_port(const char* port_name)
{
  (void) MPI_Close_port(const_cast<char *>(port_name));
}


inline void
MPI::Lookup_name(const char * service_name,
			const MPI::Info& info,
			char* port_name)
{
  (void) MPI_Lookup_name(const_cast<char *>(service_name), info, port_name);
}


inline void
MPI::Open_port(const MPI::Info& info, char* port_name)
{
  (void) MPI_Open_port(info, port_name);
}


inline void
MPI::Publish_name(const char* service_name,
			 const MPI::Info& info,
			 const char* port_name)
{
  (void) MPI_Publish_name(const_cast<char *>(service_name), info,
                          const_cast<char *>(port_name));
}


inline void
MPI::Unpublish_name(const char* service_name,
			   const MPI::Info& info,
			   const char* port_name)
{
  (void)MPI_Unpublish_name(const_cast<char *>(service_name), info,
                           const_cast<char *>(port_name));
}



//
// Profiling
//

inline void
MPI::Pcontrol(const int level, ...)
{
  va_list ap;
  va_start(ap, level);

  (void)MPI_Pcontrol(level, ap);
  va_end(ap);
}


inline void
MPI::Get_version(int& version, int& subversion)
{
  (void)MPI_Get_version(&version, &subversion);
}


inline MPI::Aint
MPI::Get_address(void* location)
{
  MPI::Aint ret;
  MPI_Get_address(location, &ret);
  return ret;
}
