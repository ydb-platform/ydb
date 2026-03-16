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
// Copyright (c) 2006-2007 Cisco Systems, Inc.  All rights reserved.
// Copyright (c) 2011      FUJITSU LIMITED.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//


class Datatype {
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  //  friend class PMPI::Datatype;
#endif
public:

#if 0 /* OMPI_ENABLE_MPI_PROFILING */

  // construction
  inline Datatype() { }

  // inter-language operability
  inline Datatype(MPI_Datatype i) : pmpi_datatype(i) { }

  // copy / assignment
  inline Datatype(const Datatype& dt) : pmpi_datatype(dt.pmpi_datatype) { }

  inline Datatype(const PMPI::Datatype& dt) : pmpi_datatype(dt) { }

  inline virtual ~Datatype() {}

  inline Datatype& operator=(const Datatype& dt) {
    pmpi_datatype = dt.pmpi_datatype; return *this; }

  // comparison
  inline bool operator== (const Datatype &a) const
    { return (bool) (pmpi_datatype == a.pmpi_datatype); }

  inline bool operator!= (const Datatype &a) const
    { return (bool) !(*this == a); }

  // inter-language operability
  inline Datatype& operator= (const MPI_Datatype &i)
    { pmpi_datatype = i; return *this; }

  inline operator MPI_Datatype() const { return (MPI_Datatype)pmpi_datatype; }
  //  inline operator MPI_Datatype* ()/* JGS const */ { return pmpi_datatype; }

  inline operator const PMPI::Datatype&() const { return pmpi_datatype; }

  inline const PMPI::Datatype& pmpi() const { return pmpi_datatype; }

#else

  // construction / destruction
  inline Datatype() : mpi_datatype(MPI_DATATYPE_NULL) { }
  inline virtual ~Datatype() {}
  // inter-language operability
  inline Datatype(MPI_Datatype i) : mpi_datatype(i) { }

  // copy / assignment
  inline Datatype(const Datatype& dt) : mpi_datatype(dt.mpi_datatype) { }
  inline Datatype& operator=(const Datatype& dt) {
    mpi_datatype = dt.mpi_datatype; return *this; }

  // comparison
  inline bool operator== (const Datatype &a) const
    { return (bool) (mpi_datatype == a.mpi_datatype); }

  inline bool operator!= (const Datatype &a) const
    { return (bool) !(*this == a); }

  // inter-language operability
  inline Datatype& operator= (const MPI_Datatype &i)
    { mpi_datatype = i; return *this; }

  inline operator MPI_Datatype () const { return mpi_datatype; }
  // inline operator MPI_Datatype* ()/* JGS const */ { return &mpi_datatype; }

#endif

  //
  // User Defined Functions
  //
  typedef int Copy_attr_function(const Datatype& oldtype,
						int type_keyval,
						void* extra_state,
						const void* attribute_val_in,
						void* attribute_val_out,
						bool& flag);

  typedef int Delete_attr_function(Datatype& type, int type_keyval,
				   void* attribute_val, void* extra_state);

  //
  // Point-to-Point Communication
  //

  virtual Datatype Create_contiguous(int count) const;

  virtual Datatype Create_vector(int count, int blocklength,
				 int stride) const;

  virtual Datatype Create_indexed(int count,
				  const int array_of_blocklengths[],
				  const int array_of_displacements[]) const;

  static Datatype Create_struct(int count, const int array_of_blocklengths[],
				const Aint array_of_displacements[],
				const Datatype array_if_types[]);

  virtual Datatype Create_hindexed(int count, const int array_of_blocklengths[],
				   const Aint array_of_displacements[]) const;

  virtual Datatype Create_hvector(int count, int blocklength, Aint stride) const;

  virtual Datatype Create_indexed_block(int count, int blocklength,
					const int array_of_blocklengths[]) const;
  virtual Datatype Create_resized(const Aint lb, const Aint extent) const;

  virtual int Get_size() const;

  virtual void Get_extent(Aint& lb, Aint& extent) const;

  virtual void Get_true_extent(Aint&, Aint&) const;

  virtual void Commit();

  virtual void Free();

  virtual void Pack(const void* inbuf, int incount, void *outbuf,
		    int outsize, int& position, const Comm &comm) const;

  virtual void Unpack(const void* inbuf, int insize, void *outbuf, int outcount,
		      int& position, const Comm& comm) const;

  virtual int Pack_size(int incount, const Comm& comm) const;

  virtual void Pack_external(const char* datarep, const void* inbuf, int incount,
              void* outbuf, Aint outsize, Aint& position) const;

  virtual Aint Pack_external_size(const char* datarep, int incount) const;

  virtual void Unpack_external(const char* datarep, const void* inbuf,
              Aint insize, Aint& position, void* outbuf, int outcount) const;

  //
  // Miscellany
  //
  virtual Datatype Create_subarray(int ndims, const int array_of_sizes[],
				   const int array_of_subsizes[],
				   const int array_of_starts[], int order)
    const;

  virtual Datatype Create_darray(int size, int rank, int ndims,
                   const int array_of_gsizes[], const int array_of_distribs[],
                   const int array_of_dargs[],  const int array_of_psizes[],
                   int order) const;

  // Language Binding
  static Datatype Create_f90_complex(int p, int r);

  static Datatype Create_f90_integer(int r);

  static Datatype Create_f90_real(int p, int r);

  static Datatype Match_size(int typeclass, int size);

  //
  // External Interfaces
  //

  virtual Datatype Dup() const;

  // Need 4 overloaded versions of this function because per the
  // MPI-2 spec, you can mix-n-match the C predefined functions with
  // C++ functions.
  static int Create_keyval(Copy_attr_function* type_copy_attr_fn,
                           Delete_attr_function* type_delete_attr_fn,
                           void* extra_state);
  static int Create_keyval(MPI_Type_copy_attr_function* type_copy_attr_fn,
                           MPI_Type_delete_attr_function* type_delete_attr_fn,
                           void* extra_state);
  static int Create_keyval(Copy_attr_function* type_copy_attr_fn,
                           MPI_Type_delete_attr_function* type_delete_attr_fn,
                           void* extra_state);
  static int Create_keyval(MPI_Type_copy_attr_function* type_copy_attr_fn,
                           Delete_attr_function* type_delete_attr_fn,
                           void* extra_state);

protected:
  // Back-end function to do the heavy lifting for creating the
  // keyval
  static int do_create_keyval(MPI_Type_copy_attr_function* c_copy_fn,
                              MPI_Type_delete_attr_function* c_delete_fn,
                              Copy_attr_function* cxx_copy_fn,
                              Delete_attr_function* cxx_delete_fn,
                              void* extra_state, int &keyval);

public:

  virtual void Delete_attr(int type_keyval);

  static void Free_keyval(int& type_keyval);

  virtual bool Get_attr(int type_keyval, void* attribute_val) const;

  virtual void Get_contents(int max_integers, int max_addresses,
                            int max_datatypes, int array_of_integers[],
                            Aint array_of_addresses[],
                            Datatype array_of_datatypes[]) const;

  virtual void Get_envelope(int& num_integers, int& num_addresses,
                            int& num_datatypes, int& combiner) const;

  virtual void Get_name(char* type_name, int& resultlen) const;

  virtual void Set_attr(int type_keyval, const void* attribute_val);

  virtual void Set_name(const char* type_name);



#if 0 /* OMPI_ENABLE_MPI_PROFILING */
private:
  PMPI::Datatype pmpi_datatype;
#else
protected:
  MPI_Datatype mpi_datatype;
#endif

public:
    // Data that is passed through keyval create when C++ callback
    // functions are used
    struct keyval_intercept_data_t {
        MPI_Type_copy_attr_function *c_copy_fn;
        MPI_Type_delete_attr_function *c_delete_fn;
        Copy_attr_function* cxx_copy_fn;
        Delete_attr_function* cxx_delete_fn;
        void *extra_state;
    };

    // Protect the global list from multiple thread access
    static opal_mutex_t cxx_extra_states_lock;
};
