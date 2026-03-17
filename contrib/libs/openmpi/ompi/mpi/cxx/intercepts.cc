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
// Copyright (c) 2006-2009 Cisco Systems, Inc.  All rights reserved.
// Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
// Copyright (c) 2016      Los Alamos National Security, LLC. All rights
//                         reserved.
// Copyright (c) 2017      Research Organization for Information Science
//                         and Technology (RIST). All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//


#include "mpicxx.h"
#include <cstdio>

#include "ompi_config.h"
#include "cxx_glue.h"

extern "C"
void ompi_mpi_cxx_throw_exception(int *errcode)
{
#if OMPI_HAVE_CXX_EXCEPTION_SUPPORT
    throw(MPI::Exception(*errcode));
#else
  // Ick.  This is really ugly, but necesary if someone uses a C compiler
  // and -lmpi++ (which can legally happen in the LAM MPI implementation,
  // and probably in MPICH and others who include -lmpi++ by default in their
  // wrapper compilers)
  fprintf(stderr, "MPI 2 C++ exception throwing is disabled, MPI::mpi_errno has the error code\n");
  MPI::mpi_errno = *errcode;
#endif
}

extern "C"
void ompi_mpi_cxx_comm_throw_excptn_fctn(MPI_Comm *, int *errcode, ...)
{
    /* Portland compiler raises a warning if va_start is not used in a
     * variable argument function */
    va_list ap;
    va_start(ap, errcode);
    ompi_mpi_cxx_throw_exception(errcode);
    va_end(ap);
}

extern "C"
void ompi_mpi_cxx_file_throw_excptn_fctn(MPI_File *, int *errcode, ...)
{
    va_list ap;
    va_start(ap, errcode);
    ompi_mpi_cxx_throw_exception(errcode);
    va_end(ap);
}

extern "C"
void ompi_mpi_cxx_win_throw_excptn_fctn(MPI_Win *, int *errcode, ...)
{
    va_list ap;
    va_start(ap, errcode);
    ompi_mpi_cxx_throw_exception(errcode);
    va_end(ap);
}


void
MPI::InitializeIntercepts()
{
    ompi_cxx_errhandler_set_callbacks ((struct ompi_errhandler_t *) &ompi_mpi_errors_throw_exceptions,
                                       ompi_mpi_cxx_comm_throw_excptn_fctn,
                                       ompi_mpi_cxx_file_throw_excptn_fctn,
                                       ompi_mpi_cxx_win_throw_excptn_fctn);
}


// This function uses OMPI types, and is invoked with C linkage for
// the express purpose of having a C++ entity call back the C++
// function (so that types can be converted, etc.).
extern "C"
void ompi_mpi_cxx_comm_errhandler_invoke(MPI_Comm *c_comm, int *err,
                                         const char *message, void *comm_fn)
{
    // MPI::Comm is an abstract base class; can't instantiate one of
    // those.  So fake it by instantiating an MPI::Intracomm and then
    // casting it down to an (MPI::Comm&) when invoking the callback.
    MPI::Intracomm cxx_comm(*c_comm);
    MPI::Comm::Errhandler_function *cxx_fn =
        (MPI::Comm::Errhandler_function*) comm_fn;

    cxx_fn((MPI::Comm&) cxx_comm, err, message);
}

// This function uses OMPI types, and is invoked with C linkage for
// the express purpose of having a C++ entity call back the C++
// function (so that types can be converted, etc.).
extern "C"
void ompi_mpi_cxx_file_errhandler_invoke(MPI_File *c_file, int *err,
                                         const char *message, void *file_fn)
{
    MPI::File cxx_file(*c_file);
    MPI::File::Errhandler_function *cxx_fn =
        (MPI::File::Errhandler_function*) file_fn;

    cxx_fn(cxx_file, err, message);
}

// This function uses OMPI types, and is invoked with C linkage for
// the express purpose of having a C++ entity call back the C++
// function (so that types can be converted, etc.).
extern "C"
void ompi_mpi_cxx_win_errhandler_invoke(MPI_Win *c_win, int *err,
                                        const char *message, void *win_fn)
{
    MPI::Win cxx_win(*c_win);
    MPI::Win::Errhandler_function *cxx_fn =
        (MPI::Win::Errhandler_function*) win_fn;

    cxx_fn(cxx_win, err, message);
}

// This is a bit weird; bear with me.  The user-supplied function for
// MPI::Op contains a C++ object reference.  So it must be called from
// a C++-compiled function.  However, libmpi does not contain any C++
// code because there are portability and bootstrapping issues
// involved if someone tries to make a 100% C application link against
// a libmpi that contains C++ code.  At a minimum, the user will have
// to use the C++ compiler to link.  LA-MPI has shown that users don't
// want to do this (there are other problems, but this one is easy to
// cite).
//
// Hence, there are two problems when trying to invoke the user's
// callback funcion from an MPI::Op:
//
// 1. The MPI_Datatype that the C library has must be converted to an
// (MPI::Datatype)
// 2. The C++ callback function must then be called with a
// (MPI::Datatype&)
//
// Some relevant facts for the discussion:
//
// - The main engine for invoking Op callback functions is in libmpi
// (i.e., in C code).
//
// - The C++ bindings are a thin layer on top of the C bindings.
//
// - The C++ bindings are a separate library from the C bindings
// (libmpi_cxx.la).
//
// - As a direct result, the mpiCC wrapper compiler must generate a
// link order thus: "... -lmpi_cxx -lmpi ...", meaning that we cannot
// have a direct function call from the libmpi to libmpi_cxx.  We can
// only do it by function pointer.
//
// So the problem remains -- how to invoke a C++ MPI::Op callback
// function (which only occurrs for user-defined datatypes, BTW) from
// within the C Op callback engine in libmpi?
//
// It is easy to cache a function pointer to the
// ompi_mpi_cxx_op_intercept() function on the MPI_Op (that is located
// in the libmpi_cxx library, and is therefore compiled with a C++
// compiler).  But the normal C callback MPI_User_function type
// signature is (void*, void*, int*, MPI_Datatype*) -- so if
// ompi_mpi_cxx_op_intercept() is invoked with these arguments, it has
// no way to deduce what the user-specified callback function is that
// is associated with the MPI::Op.
//
// One can easily imagine a scenario of caching the callback pointer
// of the current MPI::Op in a global variable somewhere, and when
// ompi_mpi_cxx_op_intercept() is invoked, simply use that global
// variable.  This is unfortunately not thread safe.
//
// So what we do is as follows:
//
// 1. The C++ dispatch function ompi_mpi_cxx_op_intercept() is *not*
// of type (MPI_User_function*).  More specifically, it takes an
// additional argument: a function pointer.  its signature is (void*,
// void*, int*, MPI_Datatype*, MPI_Op*, MPI::User_function*).  This
// last argument is the function pointer of the user callback function
// to be invoked.
//
// The careful reader will notice that it is impossible for the C Op
// dispatch code in libmpi to call this function properly because the
// last argument is of a type that is not defined in libmpi (i.e.,
// it's only in libmpi_cxx).  Keep reading -- this is explained below.
//
// 2. When the MPI::Op is created (in MPI::Op::Init()), we call the
// back-end C MPI_Op_create() function as normal (just like the F77
// bindings, in fact), and pass it the ompi_mpi_cxx_op_intercept()
// function (casting it to (MPI_User_function*) -- it's a function
// pointer, so its size is guaranteed to be the same, even if the
// signature of the real function is different).
//
// 3. The function pointer to ompi_mpi_cxx_op_intercept() will be
// cached in the MPI_Op in op->o_func[0].cxx_intercept_fn.
//
// Recall that MPI_Op is implemented to have an array of function
// pointers so that optimized versions of reduction operations can be
// invoked based on the corresponding datatype.  But when an MPI_Op
// represents a user-defined function operation, there is only one
// function, so it is always stored in function pointer array index 0.
//
// 4. When MPI_Op_create() returns, the C++ MPI::Op::Init function
// manually sets OMPI_OP_FLAGS_CXX_FUNC flag on the resulting MPI_Op
// (again, very similar to the F77 MPI_OP_CREATE wrapper).  It also
// caches the user's C++ callback function in op->o_func[1].c_fn
// (recall that the array of function pointers is actually a union of
// multiple different function pointer types -- it doesn't matter
// which type the user's callback function pointer is stored in; since
// all the types in the union are function pointers, it's guaranteed
// to be large enough to hold what we need.
//
// Note that we don't have a member of the union for the C++ callback
// function because its signature includes a (MPI::Datatype&), which
// we can't put in the C library libmpi.
//
// 5. When the user invokes an function that uses the MPI::Op (or,
// more specifically, when the Op dispatch engine in ompi/op/op.c [in
// libmpi] tries to dispatch off to it), it will see the
// OMPI_OP_FLAGS_CXX_FUNC flag and know to use the
// op->o_func[0].cxx_intercept_fn and also pass as the 4th argument,
// op->o_func[1].c_fn.
//
// 6. ompi_mpi_cxx_op_intercept() is therefore invoked and receives
// both the (MPI_Datatype*) (which is easy to convert to
// (MPI::Datatype&)) and a pointer to the user's C++ callback function
// (albiet cast as the wrong type).  So it casts the callback function
// pointer to (MPI::User_function*) and invokes it.
//
// Wasn't that simple?
//
extern "C" void
ompi_mpi_cxx_op_intercept(void *invec, void *outvec, int *len,
                          MPI_Datatype *datatype, MPI_User_function *c_fn)
{
    MPI::Datatype cxx_datatype = *datatype;
    MPI::User_function *cxx_callback = (MPI::User_function*) c_fn;
    cxx_callback(invec, outvec, *len, cxx_datatype);
}

//
// Attribute copy functions -- comm, type, and win
//
extern "C" int
ompi_mpi_cxx_comm_copy_attr_intercept(MPI_Comm comm, int keyval,
                                      void *extra_state,
                                      void *attribute_val_in,
                                      void *attribute_val_out, int *flag,
                                      MPI_Comm newcomm)
{
  int ret = 0;
  MPI::Comm::keyval_intercept_data_t *kid =
      (MPI::Comm::keyval_intercept_data_t*) extra_state;

  // The callback may be in C or C++.  If it's in C, it's easy - just
  // call it with no extra C++ machinery.

  if (NULL != kid->c_copy_fn) {
      return kid->c_copy_fn(comm, keyval, kid->extra_state, attribute_val_in,
                            attribute_val_out, flag);
  }

  // If the callback was C++, we have to do a little more work

  MPI::Intracomm intracomm;
  MPI::Intercomm intercomm;
  MPI::Graphcomm graphcomm;
  MPI::Cartcomm cartcomm;

  bool bflag = OPAL_INT_TO_BOOL(*flag);

  if (NULL != kid->cxx_copy_fn) {
      ompi_cxx_communicator_type_t comm_type =
          ompi_cxx_comm_get_type (comm);
      switch (comm_type) {
      case OMPI_CXX_COMM_TYPE_GRAPH:
          graphcomm = MPI::Graphcomm(comm);
          ret = kid->cxx_copy_fn(graphcomm, keyval, kid->extra_state,
                                 attribute_val_in, attribute_val_out,
                                 bflag);
          break;
      case OMPI_CXX_COMM_TYPE_CART:
          cartcomm = MPI::Cartcomm(comm);
          ret = kid->cxx_copy_fn(cartcomm, keyval, kid->extra_state,
                                 attribute_val_in, attribute_val_out,
                                 bflag);
          break;
      case OMPI_CXX_COMM_TYPE_INTRACOMM:
          intracomm = MPI::Intracomm(comm);
          ret = kid->cxx_copy_fn(intracomm, keyval, kid->extra_state,
                                 attribute_val_in, attribute_val_out,
                                 bflag);
          break;
      case OMPI_CXX_COMM_TYPE_INTERCOMM:
          intercomm = MPI::Intercomm(comm);
          ret = kid->cxx_copy_fn(intercomm, keyval, kid->extra_state,
                                 attribute_val_in, attribute_val_out,
                                 bflag);
          break;
      default:
          ret = MPI::ERR_COMM;
      }
  } else {
      ret = MPI::ERR_OTHER;
  }

  *flag = (int)bflag;
  return ret;
}

extern "C" int
ompi_mpi_cxx_comm_delete_attr_intercept(MPI_Comm comm, int keyval,
                                        void *attribute_val, void *extra_state)
{
  int ret = 0;
  MPI::Comm::keyval_intercept_data_t *kid =
      (MPI::Comm::keyval_intercept_data_t*) extra_state;

  // The callback may be in C or C++.  If it's in C, it's easy - just
  // call it with no extra C++ machinery.

  if (NULL != kid->c_delete_fn) {
      return kid->c_delete_fn(comm, keyval, attribute_val, kid->extra_state);
  }

  // If the callback was C++, we have to do a little more work

  MPI::Intracomm intracomm;
  MPI::Intercomm intercomm;
  MPI::Graphcomm graphcomm;
  MPI::Cartcomm cartcomm;

  if (NULL != kid->cxx_delete_fn) {
      ompi_cxx_communicator_type_t comm_type =
          ompi_cxx_comm_get_type (comm);
      switch (comm_type) {
      case OMPI_CXX_COMM_TYPE_GRAPH:
          graphcomm = MPI::Graphcomm(comm);
          ret = kid->cxx_delete_fn(graphcomm, keyval, attribute_val,
                                   kid->extra_state);
          break;
      case OMPI_CXX_COMM_TYPE_CART:
          cartcomm = MPI::Cartcomm(comm);
          ret = kid->cxx_delete_fn(cartcomm, keyval, attribute_val,
                                   kid->extra_state);
          break;
      case OMPI_CXX_COMM_TYPE_INTRACOMM:
          intracomm = MPI::Intracomm(comm);
          ret = kid->cxx_delete_fn(intracomm, keyval, attribute_val,
                                   kid->extra_state);
          break;
      case OMPI_CXX_COMM_TYPE_INTERCOMM:
          intercomm = MPI::Intercomm(comm);
          ret = kid->cxx_delete_fn(intercomm, keyval, attribute_val,
                                   kid->extra_state);
          break;
      default:
          ret = MPI::ERR_COMM;
      }
  } else {
      ret = MPI::ERR_OTHER;
  }

  return ret;
}

extern "C" int
ompi_mpi_cxx_type_copy_attr_intercept(MPI_Datatype oldtype, int keyval,
                                      void *extra_state, void *attribute_val_in,
                                      void *attribute_val_out, int *flag)
{
  int ret = 0;
  MPI::Datatype::keyval_intercept_data_t *kid =
      (MPI::Datatype::keyval_intercept_data_t*) extra_state;


  if (NULL != kid->c_copy_fn) {
      // The callback may be in C or C++.  If it's in C, it's easy - just
      // call it with no extra C++ machinery.
      ret = kid->c_copy_fn(oldtype, keyval, kid->extra_state, attribute_val_in,
                           attribute_val_out, flag);
  } else if (NULL != kid->cxx_copy_fn) {
      // If the callback was C++, we have to do a little more work
      bool bflag = OPAL_INT_TO_BOOL(*flag);
      MPI::Datatype cxx_datatype(oldtype);
      ret = kid->cxx_copy_fn(cxx_datatype, keyval, kid->extra_state,
                             attribute_val_in, attribute_val_out, bflag);
      *flag = (int)bflag;
  } else {
    ret = MPI::ERR_TYPE;
  }

  return ret;
}

extern "C" int
ompi_mpi_cxx_type_delete_attr_intercept(MPI_Datatype type, int keyval,
                                        void *attribute_val, void *extra_state)
{
  int ret = 0;
  MPI::Datatype::keyval_intercept_data_t *kid =
      (MPI::Datatype::keyval_intercept_data_t*) extra_state;

  if (NULL != kid->c_delete_fn) {
      return kid->c_delete_fn(type, keyval, attribute_val, kid->extra_state);
  } else if (NULL != kid->cxx_delete_fn) {
      MPI::Datatype cxx_datatype(type);
      return kid->cxx_delete_fn(cxx_datatype, keyval, attribute_val,
                                kid->extra_state);
  } else {
    ret = MPI::ERR_TYPE;
  }

  return ret;
}

extern "C" int
ompi_mpi_cxx_win_copy_attr_intercept(MPI_Win oldwin, int keyval,
                                      void *extra_state, void *attribute_val_in,
                                      void *attribute_val_out, int *flag)
{
  int ret = 0;
  MPI::Win::keyval_intercept_data_t *kid =
    (MPI::Win::keyval_intercept_data_t*) extra_state;

  if (NULL != kid->c_copy_fn) {
      // The callback may be in C or C++.  If it's in C, it's easy - just
      // call it with no extra C++ machinery.
      ret = kid->c_copy_fn(oldwin, keyval, kid->extra_state, attribute_val_in,
                           attribute_val_out, flag);
  } else if (NULL != kid->cxx_copy_fn) {
      // If the callback was C++, we have to do a little more work
      bool bflag = OPAL_INT_TO_BOOL(*flag);
      MPI::Win cxx_win(oldwin);
      ret = kid->cxx_copy_fn(cxx_win, keyval, kid->extra_state,
                             attribute_val_in, attribute_val_out, bflag);
      *flag = (int)bflag;
  } else {
      ret = MPI::ERR_WIN;
  }

  return ret;
}

extern "C" int
ompi_mpi_cxx_win_delete_attr_intercept(MPI_Win win, int keyval,
                                        void *attribute_val, void *extra_state)
{
  int ret = 0;
  MPI::Win::keyval_intercept_data_t *kid =
      (MPI::Win::keyval_intercept_data_t*) extra_state;

  if (NULL != kid->c_delete_fn) {
      return kid->c_delete_fn(win, keyval, attribute_val, kid->extra_state);
  } else if (NULL != kid->cxx_delete_fn) {
      MPI::Win cxx_win(win);
      return kid->cxx_delete_fn(cxx_win, keyval, attribute_val,
                                kid->extra_state);
  } else {
      ret = MPI::ERR_WIN;
  }

  return ret;
}

// For similar reasons as above, we need to intercept calls for the 3
// generalized request callbacks (convert arguments to C++ types and
// invoke the C++ callback signature).

extern "C" int
ompi_mpi_cxx_grequest_query_fn_intercept(void *state, MPI_Status *status)
{
    MPI::Grequest::Intercept_data_t *data =
        (MPI::Grequest::Intercept_data_t *) state;

    MPI::Status s(*status);
    int ret = data->id_cxx_query_fn(data->id_extra, s);
    *status = s;
    return ret;
}

extern "C" int
ompi_mpi_cxx_grequest_free_fn_intercept(void *state)
{
    MPI::Grequest::Intercept_data_t *data =
        (MPI::Grequest::Intercept_data_t *) state;
    int ret = data->id_cxx_free_fn(data->id_extra);
    // Delete the struct that was "new"ed in MPI::Grequest::Start()
    delete data;
    return ret;
}

extern "C" int
ompi_mpi_cxx_grequest_cancel_fn_intercept(void *state, int cancelled)
{
    MPI::Grequest::Intercept_data_t *data =
        (MPI::Grequest::Intercept_data_t *) state;
    return data->id_cxx_cancel_fn(data->id_extra,
                                  (0 != cancelled ? true : false));
}
