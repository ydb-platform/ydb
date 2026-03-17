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
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

#if 0 /* OMPI_ENABLE_MPI_PROFILING */

inline
MPI::Op::Op() { }

inline
MPI::Op::Op(const MPI::Op& o) : pmpi_op(o.pmpi_op) { }

inline
MPI::Op::Op(MPI_Op o) : pmpi_op(o) { }

inline
MPI::Op::~Op() { }

inline
MPI::Op& MPI::Op::operator=(const MPI::Op& op) {
    pmpi_op = op.pmpi_op; return *this;
}

// comparison
inline bool
MPI::Op::operator== (const MPI::Op &a) {
    return (bool)(pmpi_op == a.pmpi_op);
}

inline bool
MPI::Op::operator!= (const MPI::Op &a) {
    return (bool)!(*this == a);
}

// inter-language operability
inline MPI::Op&
MPI::Op::operator= (const MPI_Op &i) { pmpi_op = i; return *this; }

inline
MPI::Op::operator MPI_Op () const { return pmpi_op; }

//inline
//MPI::Op::operator MPI_Op* () { return pmpi_op; }


#else  // ============= NO PROFILING ===================================

// construction
inline
MPI::Op::Op() : mpi_op(MPI_OP_NULL) { }

inline
MPI::Op::Op(MPI_Op i) : mpi_op(i) { }

inline
MPI::Op::Op(const MPI::Op& op)
  : mpi_op(op.mpi_op) { }

inline
MPI::Op::~Op()
{
#if 0
    mpi_op = MPI_OP_NULL;
    op_user_function = 0;
#endif
}

inline MPI::Op&
MPI::Op::operator=(const MPI::Op& op) {
    mpi_op = op.mpi_op;
    return *this;
}

// comparison
inline bool
MPI::Op::operator== (const MPI::Op &a) { return (bool)(mpi_op == a.mpi_op); }

inline bool
MPI::Op::operator!= (const MPI::Op &a) { return (bool)!(*this == a); }

// inter-language operability
inline MPI::Op&
MPI::Op::operator= (const MPI_Op &i) { mpi_op = i; return *this; }

inline
MPI::Op::operator MPI_Op () const { return mpi_op; }

//inline
//MPI::Op::operator MPI_Op* () { return &mpi_op; }

#endif

// Extern this function here rather than include an internal Open MPI
// header file (and therefore force installing the internal Open MPI
// header file so that user apps can #include it)

extern "C" void ompi_op_set_cxx_callback(MPI_Op op, MPI_User_function*);

// There is a lengthy comment in ompi/mpi/cxx/intercepts.cc explaining
// what this function is doing.  Please read it before modifying this
// function.
inline void
MPI::Op::Init(MPI::User_function *func, bool commute)
{
    (void)MPI_Op_create((MPI_User_function*) ompi_mpi_cxx_op_intercept,
                        (int) commute, &mpi_op);
    ompi_op_set_cxx_callback(mpi_op, (MPI_User_function*) func);
}


inline void
MPI::Op::Free()
{
    (void)MPI_Op_free(&mpi_op);
}


inline void
MPI::Op::Reduce_local(const void *inbuf, void *inoutbuf, int count,
                      const MPI::Datatype& datatype) const
{
    (void)MPI_Reduce_local(const_cast<void*>(inbuf), inoutbuf, count,
                           datatype, mpi_op);
}


inline bool
MPI::Op::Is_commutative(void) const
{
    int commute;
    (void)MPI_Op_commutative(mpi_op, &commute);
    return (bool) commute;
}
