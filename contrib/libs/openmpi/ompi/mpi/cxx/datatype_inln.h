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
// Copyright (c) 2006-2008 Sun Microsystems, Inc.  All rights reserved.
// Copyright (c) 2011      FUJITSU LIMITED.  All rights reserved.

// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//


//
// Point-to-Point Communication
//

inline MPI::Datatype
MPI::Datatype::Create_contiguous(int count) const
{
  MPI_Datatype newtype;
  (void)MPI_Type_contiguous(count, mpi_datatype, &newtype);
  return newtype;
}

inline MPI::Datatype
MPI::Datatype::Create_vector(int count, int blocklength,
			     int stride) const
{
  MPI_Datatype newtype;
  (void)MPI_Type_vector(count, blocklength, stride, mpi_datatype, &newtype);
  return newtype;
}

inline MPI::Datatype
MPI::Datatype::Create_indexed(int count,
				     const int array_of_blocklengths[],
				     const int array_of_displacements[]) const
{
  MPI_Datatype newtype;
  (void)MPI_Type_indexed(count, const_cast<int *>(array_of_blocklengths),
			 const_cast<int *>(array_of_displacements), mpi_datatype, &newtype);
  return newtype;
}

inline MPI::Datatype
MPI::Datatype::Create_struct(int count, const int array_of_blocklengths[],
				    const MPI::Aint array_of_displacements[],
				    const MPI::Datatype array_of_types[])
{
  MPI_Datatype newtype;
  int i;
  MPI_Datatype* type_array = new MPI_Datatype[count];
  for (i=0; i < count; i++)
    type_array[i] = array_of_types[i];

  (void)MPI_Type_create_struct(count, const_cast<int *>(array_of_blocklengths),
                               const_cast<MPI_Aint*>(array_of_displacements),
                               type_array, &newtype);
  delete[] type_array;
  return newtype;
}

inline MPI::Datatype
MPI::Datatype::Create_hindexed(int count, const int array_of_blocklengths[],
				      const MPI::Aint array_of_displacements[]) const
{
  MPI_Datatype newtype;
  (void)MPI_Type_create_hindexed(count, const_cast<int *>(array_of_blocklengths),
                                 const_cast<MPI_Aint*>(array_of_displacements),
                                 mpi_datatype, &newtype) ;
  return newtype;
}

inline MPI::Datatype
MPI::Datatype::Create_hvector(int count, int blocklength,
				     MPI::Aint stride) const
{
  MPI_Datatype newtype;
  (void)MPI_Type_create_hvector(count, blocklength, (MPI_Aint)stride,
                                mpi_datatype, &newtype);

  return newtype;
}

inline MPI::Datatype
MPI::Datatype::Create_indexed_block(int count, int blocklength,
				    const int array_of_displacements[]) const
{
  MPI_Datatype newtype;
  (void)MPI_Type_create_indexed_block(count, blocklength, const_cast<int *>(array_of_displacements),
                                      mpi_datatype, &newtype);
  return newtype;
}

inline MPI::Datatype
MPI::Datatype::Create_resized(const MPI::Aint lb, const MPI::Aint extent) const
{
    MPI_Datatype newtype;

    (void) MPI_Type_create_resized(mpi_datatype, lb, extent, &newtype);
    return(newtype);
}

inline int
MPI::Datatype::Get_size() const
{
  int size;
  (void)MPI_Type_size(mpi_datatype, &size);
  return size;
}

inline void
MPI::Datatype::Get_extent(MPI::Aint& lb, MPI::Aint& extent) const
{
  (void)MPI_Type_get_extent(mpi_datatype, &lb, &extent);
}

inline void
MPI::Datatype::Get_true_extent(MPI::Aint& lb, MPI::Aint& extent) const
{
    (void) MPI_Type_get_true_extent(mpi_datatype, &lb, &extent);
}

inline void
MPI::Datatype::Commit()
{
  (void)MPI_Type_commit(&mpi_datatype);
}

inline void
MPI::Datatype::Pack(const void* inbuf, int incount,
			   void *outbuf, int outsize,
			   int& position, const MPI::Comm &comm) const
{
  (void)MPI_Pack(const_cast<void *>(inbuf), incount,  mpi_datatype, outbuf,
		 outsize, &position, comm);
}

inline void
MPI::Datatype::Unpack(const void* inbuf, int insize,
			     void *outbuf, int outcount, int& position,
			     const MPI::Comm& comm) const
{
  (void)MPI_Unpack(const_cast<void *>(inbuf), insize, &position,
		   outbuf, outcount, mpi_datatype, comm);
}

inline int
MPI::Datatype::Pack_size(int incount, const MPI::Comm& comm) const
{
  int size;
  (void)MPI_Pack_size(incount, mpi_datatype, comm, &size);
  return size;
}

inline void
MPI::Datatype::Pack_external(const char* datarep, const void* inbuf, int incount,
            void* outbuf, MPI::Aint outsize, MPI::Aint& position) const
{
    (void)MPI_Pack_external(const_cast<char *>(datarep), const_cast<void *>(inbuf),
                             incount, mpi_datatype, outbuf, outsize, &position);
}

inline MPI::Aint
MPI::Datatype::Pack_external_size(const char* datarep, int incount) const
{
    MPI_Aint addr;
    (void)MPI_Pack_external_size(const_cast<char *>(datarep), incount, mpi_datatype, &addr);
    return addr;
}

inline void
MPI::Datatype::Unpack_external(const char* datarep, const void* inbuf,
            MPI::Aint insize, MPI::Aint& position, void* outbuf, int outcount) const
{
    (void)MPI_Unpack_external(const_cast<char *>(datarep), const_cast<void *>(inbuf),
                               insize, &position, outbuf, outcount, mpi_datatype);
}

//
// Miscellany
//

inline MPI::Datatype
MPI::Datatype::Create_subarray(int ndims, const int array_of_sizes[],
				      const int array_of_subsizes[],
				      const int array_of_starts[], int order)
  const
{
  MPI_Datatype type;
  (void) MPI_Type_create_subarray(ndims, const_cast<int *>(array_of_sizes),
                                  const_cast<int *>(array_of_subsizes),
                                  const_cast<int *>(array_of_starts),
                                  order, mpi_datatype, &type);
  return type;
}

inline MPI::Datatype
MPI::Datatype::Create_darray(int size, int rank, int ndims,
                   const int array_of_gsizes[], const int array_of_distribs[],
                   const int array_of_dargs[],  const int array_of_psizes[],
                   int order) const
{
    MPI_Datatype type;
    (void) MPI_Type_create_darray(size, rank, ndims,
                   const_cast<int *>(array_of_gsizes),
                   const_cast<int *>(array_of_distribs),
                   const_cast<int *>(array_of_dargs),
                   const_cast<int *>(array_of_psizes),
                   order, mpi_datatype, &type);
    return type;
}

inline MPI::Datatype
MPI::Datatype::Create_f90_complex(int p, int r)
{
    MPI_Datatype type;
    (void) MPI_Type_create_f90_complex(p, r, &type);
    return type;
}

inline MPI::Datatype
MPI::Datatype::Create_f90_integer(int r)
{
    MPI_Datatype type;
    (void) MPI_Type_create_f90_integer(r, &type);
    return type;
}

inline MPI::Datatype
MPI::Datatype::Create_f90_real(int p, int r)
{
    MPI_Datatype type;
    (void) MPI_Type_create_f90_real(p, r, &type);
    return type;
}

inline MPI::Datatype
MPI::Datatype::Match_size(int typeclass, int size)
{
    MPI_Datatype type;
    (void) MPI_Type_match_size(typeclass, size, &type);
    return type;
}

//
// External Interfaces
//


inline MPI::Datatype
MPI::Datatype::Dup() const
{
  MPI_Datatype type;
  (void) MPI_Type_dup(mpi_datatype, &type);
  return type;
}


// 1) original Create_keyval that takes the first 2 arguments as C++
//    functions
inline int
MPI::Datatype::Create_keyval(MPI::Datatype::Copy_attr_function* type_copy_attr_fn,
                             MPI::Datatype::Delete_attr_function* type_delete_attr_fn,
                             void* extra_state)
{
    // Back-end function does the heavy lifting
    int ret, keyval;
    ret = do_create_keyval(NULL, NULL,
                           type_copy_attr_fn, type_delete_attr_fn,
                           extra_state, keyval);
    return (MPI_SUCCESS == ret) ? keyval : ret;
}

// 2) overload Create_keyval to take the first 2 arguments as C
//    functions
inline int
MPI::Datatype::Create_keyval(MPI_Type_copy_attr_function* type_copy_attr_fn,
                             MPI_Type_delete_attr_function* type_delete_attr_fn,
                             void* extra_state)
{
    // Back-end function does the heavy lifting
    int ret, keyval;
    ret = do_create_keyval(type_copy_attr_fn, type_delete_attr_fn,
                           NULL, NULL,
                           extra_state, keyval);
    return (MPI_SUCCESS == ret) ? keyval : ret;
}

// 3) overload Create_keyval to take the first 2 arguments as C++ & C
//    functions
inline int
MPI::Datatype::Create_keyval(MPI::Datatype::Copy_attr_function* type_copy_attr_fn,
                             MPI_Type_delete_attr_function* type_delete_attr_fn,
                             void* extra_state)
{
    // Back-end function does the heavy lifting
    int ret, keyval;
    ret = do_create_keyval(NULL, type_delete_attr_fn,
                           type_copy_attr_fn, NULL,
                           extra_state, keyval);
    return (MPI_SUCCESS == ret) ? keyval : ret;
}

// 4) overload Create_keyval to take the first 2 arguments as C & C++
//    functions
inline int
MPI::Datatype::Create_keyval(MPI_Type_copy_attr_function* type_copy_attr_fn,
                             MPI::Datatype::Delete_attr_function* type_delete_attr_fn,
                             void* extra_state)
{
    // Back-end function does the heavy lifting
    int ret, keyval;
    ret = do_create_keyval(type_copy_attr_fn, NULL,
                           NULL, type_delete_attr_fn,
                           extra_state, keyval);
    return (MPI_SUCCESS == ret) ? keyval : ret;
}

inline void
MPI::Datatype::Delete_attr(int type_keyval)
{
  (void) MPI_Type_delete_attr(mpi_datatype, type_keyval);
}

inline void
MPI::Datatype::Free_keyval(int& type_keyval)
{
    (void) MPI_Type_free_keyval(&type_keyval);
}

inline bool
MPI::Datatype::Get_attr(int type_keyval,
                          void* attribute_val) const
{
  int ret;
  (void) MPI_Type_get_attr(mpi_datatype, type_keyval, attribute_val, &ret);
  return OPAL_INT_TO_BOOL(ret);
}


inline void
MPI::Datatype::Get_contents(int max_integers, int max_addresses,
                            int max_datatypes, int array_of_integers[],
                            MPI::Aint array_of_addresses[],
                            MPI::Datatype array_of_datatypes[]) const
{
    int i;
    MPI_Datatype *c_datatypes = new MPI_Datatype[max_datatypes];

    (void) MPI_Type_get_contents(mpi_datatype, max_integers, max_addresses,
                                 max_datatypes,
                                 const_cast<int *>(array_of_integers),
                                 const_cast<MPI_Aint*>(array_of_addresses),
                                 c_datatypes);
    // Convert the C MPI_Datatypes to the user's OUT MPI::Datatype
    // array parameter
    for (i = 0; i < max_datatypes; ++i) {
        array_of_datatypes[i] = c_datatypes[i];
    }
    delete[] c_datatypes;
}

inline void
MPI::Datatype::Get_envelope(int& num_integers, int& num_addresses,
			  int& num_datatypes, int& combiner) const
{
  (void) MPI_Type_get_envelope(mpi_datatype, &num_integers, &num_addresses,
				&num_datatypes, &combiner);
}

inline void
MPI::Datatype::Get_name(char* type_name, int& resultlen) const
{
  (void) MPI_Type_get_name(mpi_datatype, type_name, &resultlen);
}

inline void
MPI::Datatype::Set_attr(int type_keyval, const void* attribute_val)
{
  (void) MPI_Type_set_attr(mpi_datatype, type_keyval, const_cast<void *>(attribute_val));
}

inline void
MPI::Datatype::Set_name(const char* type_name)
{
  (void) MPI_Type_set_name(mpi_datatype, const_cast<char *>(type_name));
}


#if 0
//
// User Defined Functions
//

typedef int MPI::Datatype::Copy_attr_function(const Datatype& oldtype,
						     int type_keyval,
						     void* extra_state,
						     void* attribute_val_in,
						     void* attribute_val_out,
						     bool& flag);

typedef int MPI::Datatype::Delete_attr_function(Datatype& type,
						       int type_keyval,
						       void* attribute_val,
						       void* extra_state);
#endif
