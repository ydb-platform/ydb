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
// Copyright (c) 2011      FUJITSU LIMITED.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

class Comm_Null {
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  //  friend class PMPI::Comm_Null;
#endif
public:

#if 0 /* OMPI_ENABLE_MPI_PROFILING */

  // construction
  inline Comm_Null() { }
  // copy
  inline Comm_Null(const Comm_Null& data) : pmpi_comm(data.pmpi_comm) { }
  // inter-language operability
  inline Comm_Null(MPI_Comm data) : pmpi_comm(data) { }

  inline Comm_Null(const PMPI::Comm_Null& data) : pmpi_comm(data) { }

  // destruction
  virtual inline ~Comm_Null() { }

  inline Comm_Null& operator=(const Comm_Null& data) {
    pmpi_comm = data.pmpi_comm;
    return *this;
  }

  // comparison
  inline bool operator==(const Comm_Null& data) const {
    return (bool) (pmpi_comm == data.pmpi_comm); }

  inline bool operator!=(const Comm_Null& data) const {
    return (bool) (pmpi_comm != data.pmpi_comm);}

  // inter-language operability (conversion operators)
  inline operator MPI_Comm() const { return pmpi_comm; }
  //  inline operator MPI_Comm*() /*const JGS*/ { return pmpi_comm; }
  inline operator const PMPI::Comm_Null&() const { return pmpi_comm; }

#else

  // construction
  inline Comm_Null() : mpi_comm(MPI_COMM_NULL) { }
  // copy
  inline Comm_Null(const Comm_Null& data) : mpi_comm(data.mpi_comm) { }
  // inter-language operability
  inline Comm_Null(MPI_Comm data) : mpi_comm(data) { }

  // destruction
  virtual inline ~Comm_Null() { }

 // comparison
  // JGS make sure this is right (in other classes too)
  inline bool operator==(const Comm_Null& data) const {
    return (bool) (mpi_comm == data.mpi_comm); }

  inline bool operator!=(const Comm_Null& data) const {
    return (bool) !(*this == data);}

  // inter-language operability (conversion operators)
  inline operator MPI_Comm() const { return mpi_comm; }

#endif


protected:

#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  PMPI::Comm_Null pmpi_comm;
#else
  MPI_Comm mpi_comm;
#endif


};


class Comm : public Comm_Null {
public:

  typedef void Errhandler_function(Comm&, int*, ...);
  typedef Errhandler_function Errhandler_fn
        __mpi_interface_deprecated__("MPI::Comm::Errhandler_fn was deprecated in MPI-2.2; use MPI::Comm::Errhandler_function instead");
  typedef int Copy_attr_function(const Comm& oldcomm, int comm_keyval,
				 void* extra_state, void* attribute_val_in,
				 void* attribute_val_out,
				 bool& flag);
  typedef int Delete_attr_function(Comm& comm, int comm_keyval,
				   void* attribute_val,
				   void* extra_state);
#if !0 /* OMPI_ENABLE_MPI_PROFILING */
#define _MPI2CPP_ERRHANDLERFN_ Errhandler_function
#define _MPI2CPP_COPYATTRFN_ Copy_attr_function
#define _MPI2CPP_DELETEATTRFN_ Delete_attr_function
#endif

  // construction
  Comm();

  // copy
  Comm(const Comm_Null& data);

#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  Comm(const Comm& data) :
    Comm_Null(data),
    pmpi_comm((const PMPI::Comm&) data) { }

  // inter-language operability
  Comm(MPI_Comm data) : Comm_Null(data), pmpi_comm(data) { }

  Comm(const PMPI::Comm& data) :
    Comm_Null((const PMPI::Comm_Null&)data),
    pmpi_comm(data) { }

  operator const PMPI::Comm&() const { return pmpi_comm; }

  // assignment
  Comm& operator=(const Comm& data) {
    this->Comm_Null::operator=(data);
    pmpi_comm = data.pmpi_comm;
    return *this;
  }
  Comm& operator=(const Comm_Null& data) {
    this->Comm_Null::operator=(data);
    MPI_Comm tmp = data;
    pmpi_comm = tmp;
    return *this;
  }
  // inter-language operability
  Comm& operator=(const MPI_Comm& data) {
    this->Comm_Null::operator=(data);
    pmpi_comm = data;
    return *this;
  }

#else
  Comm(const Comm& data) : Comm_Null(data.mpi_comm) { }
  // inter-language operability
  Comm(MPI_Comm data) : Comm_Null(data) { }
#endif


  //
  // Point-to-Point
  //

  virtual void Send(const void *buf, int count,
		    const Datatype & datatype, int dest, int tag) const;

  virtual void Recv(void *buf, int count, const Datatype & datatype,
		    int source, int tag, Status & status) const;


  virtual void Recv(void *buf, int count, const Datatype & datatype,
		    int source, int tag) const;

  virtual void Bsend(const void *buf, int count,
		     const Datatype & datatype, int dest, int tag) const;

  virtual void Ssend(const void *buf, int count,
		     const Datatype & datatype, int dest, int tag) const ;

  virtual void Rsend(const void *buf, int count,
		     const Datatype & datatype, int dest, int tag) const;

  virtual Request Isend(const void *buf, int count,
			const Datatype & datatype, int dest, int tag) const;

  virtual Request Ibsend(const void *buf, int count, const
			 Datatype & datatype, int dest, int tag) const;

  virtual Request Issend(const void *buf, int count,
			 const Datatype & datatype, int dest, int tag) const;

  virtual Request Irsend(const void *buf, int count,
			 const Datatype & datatype, int dest, int tag) const;

  virtual Request Irecv(void *buf, int count,
			const Datatype & datatype, int source, int tag) const;

  virtual bool Iprobe(int source, int tag, Status & status) const;

  virtual bool Iprobe(int source, int tag) const;

  virtual void Probe(int source, int tag, Status & status) const;

  virtual void Probe(int source, int tag) const;

  virtual Prequest Send_init(const void *buf, int count,
			     const Datatype & datatype, int dest,
			     int tag) const;

  virtual Prequest Bsend_init(const void *buf, int count,
			      const Datatype & datatype, int dest,
			      int tag) const;

  virtual Prequest Ssend_init(const void *buf, int count,
			      const Datatype & datatype, int dest,
			      int tag) const;

  virtual Prequest Rsend_init(const void *buf, int count,
			      const Datatype & datatype, int dest,
			      int tag) const;

  virtual Prequest Recv_init(void *buf, int count,
			     const Datatype & datatype, int source,
			     int tag) const;

  virtual void Sendrecv(const void *sendbuf, int sendcount,
			const Datatype & sendtype, int dest, int sendtag,
			void *recvbuf, int recvcount,
			const Datatype & recvtype, int source,
			int recvtag, Status & status) const;

  virtual void Sendrecv(const void *sendbuf, int sendcount,
			const Datatype & sendtype, int dest, int sendtag,
			void *recvbuf, int recvcount,
			const Datatype & recvtype, int source,
			int recvtag) const;

  virtual void Sendrecv_replace(void *buf, int count,
				const Datatype & datatype, int dest,
				int sendtag, int source,
				int recvtag, Status & status) const;

  virtual void Sendrecv_replace(void *buf, int count,
				const Datatype & datatype, int dest,
				int sendtag, int source,
				int recvtag) const;

  //
  // Groups, Contexts, and Communicators
  //

  virtual Group Get_group() const;

  virtual int Get_size() const;

  virtual int Get_rank() const;

  static int Compare(const Comm & comm1, const Comm & comm2);

  virtual Comm& Clone() const = 0;

  virtual void Free(void);

  virtual bool Is_inter() const;


  //
  // Collective Communication
  //
  // Up in Comm because as of MPI-2, they are common to intracomm and
  // intercomm -- with the exception of Scan and Exscan, which are not
  // defined on intercomms.
  //

  virtual void
  Barrier() const;

  virtual void
  Bcast(void *buffer, int count,
	const Datatype& datatype, int root) const;

  virtual void
  Gather(const void *sendbuf, int sendcount,
	 const Datatype & sendtype,
	 void *recvbuf, int recvcount,
	 const Datatype & recvtype, int root) const;

  virtual void
  Gatherv(const void *sendbuf, int sendcount,
	  const Datatype & sendtype, void *recvbuf,
	  const int recvcounts[], const int displs[],
	  const Datatype & recvtype, int root) const;

  virtual void
  Scatter(const void *sendbuf, int sendcount,
	  const Datatype & sendtype,
	  void *recvbuf, int recvcount,
	  const Datatype & recvtype, int root) const;

  virtual void
  Scatterv(const void *sendbuf, const int sendcounts[],
	   const int displs[], const Datatype & sendtype,
	   void *recvbuf, int recvcount,
	   const Datatype & recvtype, int root) const;

  virtual void
  Allgather(const void *sendbuf, int sendcount,
	    const Datatype & sendtype, void *recvbuf,
	    int recvcount, const Datatype & recvtype) const;

  virtual void
  Allgatherv(const void *sendbuf, int sendcount,
	     const Datatype & sendtype, void *recvbuf,
	     const int recvcounts[], const int displs[],
	     const Datatype & recvtype) const;

  virtual void
  Alltoall(const void *sendbuf, int sendcount,
	   const Datatype & sendtype, void *recvbuf,
	   int recvcount, const Datatype & recvtype) const;

  virtual void
  Alltoallv(const void *sendbuf, const int sendcounts[],
	    const int sdispls[], const Datatype & sendtype,
	    void *recvbuf, const int recvcounts[],
	    const int rdispls[], const Datatype & recvtype) const;

  virtual void
  Alltoallw(const void *sendbuf, const int sendcounts[],
            const int sdispls[], const Datatype sendtypes[],
            void *recvbuf, const int recvcounts[],
            const int rdispls[], const Datatype recvtypes[]) const;

  virtual void
  Reduce(const void *sendbuf, void *recvbuf, int count,
	 const Datatype & datatype, const Op & op,
	 int root) const;


  virtual void
  Allreduce(const void *sendbuf, void *recvbuf, int count,
	    const Datatype & datatype, const Op & op) const;

  virtual void
  Reduce_scatter(const void *sendbuf, void *recvbuf,
		 int recvcounts[],
		 const Datatype & datatype,
		 const Op & op) const;

  //
  // Process Creation
  //

  virtual void Disconnect();

  static Intercomm Get_parent();

  static Intercomm Join(const int fd);

  //
  // External Interfaces
  //

  virtual void Get_name(char * comm_name, int& resultlen) const;

  virtual void Set_name(const char* comm_name);

  //
  // Process Topologies
  //

  virtual int Get_topology() const;

  //
  // Environmental Inquiry
  //

  virtual void Abort(int errorcode);

  //
  // Errhandler
  //

  static Errhandler Create_errhandler(Comm::Errhandler_function* function);

  virtual void Set_errhandler(const Errhandler& errhandler);

  virtual Errhandler Get_errhandler() const;

  void Call_errhandler(int errorcode) const;

  //
  // Keys and Attributes
  //

  // Need 4 overloaded versions of this function because per the
  // MPI-2 spec, you can mix-n-match the C predefined functions with
  // C++ functions.
  static int Create_keyval(Copy_attr_function* comm_copy_attr_fn,
                           Delete_attr_function* comm_delete_attr_fn,
                           void* extra_state);
  static int Create_keyval(MPI_Comm_copy_attr_function* comm_copy_attr_fn,
                           MPI_Comm_delete_attr_function* comm_delete_attr_fn,
                           void* extra_state);
  static int Create_keyval(Copy_attr_function* comm_copy_attr_fn,
                           MPI_Comm_delete_attr_function* comm_delete_attr_fn,
                           void* extra_state);
  static int Create_keyval(MPI_Comm_copy_attr_function* comm_copy_attr_fn,
                           Delete_attr_function* comm_delete_attr_fn,
                           void* extra_state);

protected:
  static int do_create_keyval(MPI_Comm_copy_attr_function* c_copy_fn,
                              MPI_Comm_delete_attr_function* c_delete_fn,
                              Copy_attr_function* cxx_copy_fn,
                              Delete_attr_function* cxx_delete_fn,
                              void* extra_state, int &keyval);

public:

  static void Free_keyval(int& comm_keyval);

  virtual void Set_attr(int comm_keyval, const void* attribute_val) const;

  virtual bool Get_attr(int comm_keyval, void* attribute_val) const;

  virtual void Delete_attr(int comm_keyval);

  static int NULL_COPY_FN(const Comm& oldcomm, int comm_keyval,
			  void* extra_state, void* attribute_val_in,
			  void* attribute_val_out, bool& flag);

  static int DUP_FN(const Comm& oldcomm, int comm_keyval,
		    void* extra_state, void* attribute_val_in,
		    void* attribute_val_out, bool& flag);

  static int NULL_DELETE_FN(Comm& comm, int comm_keyval, void* attribute_val,
			    void* extra_state);


private:
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  PMPI::Comm pmpi_comm;
#endif

#if ! 0 /* OMPI_ENABLE_MPI_PROFILING */
public:

    // Data that is passed through keyval create when C++ callback
    // functions are used
    struct keyval_intercept_data_t {
        MPI_Comm_copy_attr_function *c_copy_fn;
        MPI_Comm_delete_attr_function *c_delete_fn;
        Copy_attr_function* cxx_copy_fn;
        Delete_attr_function* cxx_delete_fn;
        void *extra_state;
    };

    // Protect the global list from multiple thread access
    static opal_mutex_t cxx_extra_states_lock;
#endif

};
