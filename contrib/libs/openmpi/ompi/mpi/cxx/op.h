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

class Op {
public:

  // construction
  Op();
  Op(MPI_Op i);
  Op(const Op& op);
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  Op(const PMPI::Op& op) : pmpi_op(op) { }
#endif
  // destruction
  virtual ~Op();
  // assignment
  Op& operator=(const Op& op);
  Op& operator= (const MPI_Op &i);
  // comparison
  inline bool operator== (const Op &a);
  inline bool operator!= (const Op &a);
  // conversion functions for inter-language operability
  inline operator MPI_Op () const;
  //  inline operator MPI_Op* (); //JGS const
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  inline operator const PMPI::Op&() const { return pmpi_op; }
#endif
  // Collective Communication
  //JGS took const out
  virtual void Init(User_function *func, bool commute);
  virtual void Free();

  virtual void Reduce_local(const void *inbuf, void *inoutbuf, int count,
                            const MPI::Datatype& datatype) const;
  virtual bool Is_commutative(void) const;

#if ! 0 /* OMPI_ENABLE_MPI_PROFILING */
protected:
  MPI_Op mpi_op;
#endif

#if 0 /* OMPI_ENABLE_MPI_PROFILING */
private:
  PMPI::Op pmpi_op;
#endif

};

