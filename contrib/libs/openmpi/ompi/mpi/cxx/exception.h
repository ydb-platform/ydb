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

class Exception {
public:

#if 0 /* OMPI_ENABLE_MPI_PROFILING */

  inline Exception(int ec) : pmpi_exception(ec) { }

  int Get_error_code() const;

  int Get_error_class() const;

  const char* Get_error_string() const;

#else

  inline Exception(int ec) : error_code(ec), error_string(0), error_class(-1) {
    (void)MPI_Error_class(error_code, &error_class);
    int resultlen;
    error_string = new char[MAX_ERROR_STRING];
    (void)MPI_Error_string(error_code, error_string, &resultlen);
  }
  inline ~Exception() {
    delete[] error_string;
  }
  // Better put in a copy constructor here since we have a string;
  // copy by value (from the default copy constructor) would be
  // disasterous.
  inline Exception(const Exception& a)
    : error_code(a.error_code), error_class(a.error_class)
  {
    error_string = new char[MAX_ERROR_STRING];
    // Rather that force an include of <string.h>, especially this
    // late in the game (recall that this file is included deep in
    // other .h files), we'll just do the copy ourselves.
    for (int i = 0; i < MAX_ERROR_STRING; i++)
      error_string[i] = a.error_string[i];
  }

  inline int Get_error_code() const { return error_code; }

  inline int Get_error_class() const { return error_class; }

  inline const char* Get_error_string() const { return error_string; }

#endif

protected:
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  PMPI::Exception pmpi_exception;
#else
  int error_code;
  char* error_string;
  int error_class;
#endif
};
