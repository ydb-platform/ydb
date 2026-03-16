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


class Request {
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  //    friend class PMPI::Request;
#endif
public:
#if 0 /* OMPI_ENABLE_MPI_PROFILING */

  // construction
  Request() { }
  Request(MPI_Request i) : pmpi_request(i) { }

  // copy / assignment
  Request(const Request& r) : pmpi_request(r.pmpi_request) { }

  Request(const PMPI::Request& r) : pmpi_request(r) { }

  virtual ~Request() {}

  Request& operator=(const Request& r) {
    pmpi_request = r.pmpi_request; return *this; }

  // comparison
  bool operator== (const Request &a)
  { return (bool)(pmpi_request == a.pmpi_request); }
  bool operator!= (const Request &a)
  { return (bool)!(*this == a); }

  // inter-language operability
  Request& operator= (const MPI_Request &i) {
    pmpi_request = i; return *this;  }

  operator MPI_Request () const { return pmpi_request; }
  //  operator MPI_Request* () const { return pmpi_request; }
  operator const PMPI::Request&() const { return pmpi_request; }

#else

  // construction / destruction
  Request() : mpi_request(MPI_REQUEST_NULL) { }
  virtual ~Request() {}
  Request(MPI_Request i) : mpi_request(i) { }

  // copy / assignment
  Request(const Request& r) : mpi_request(r.mpi_request) { }

  Request& operator=(const Request& r) {
    mpi_request = r.mpi_request; return *this; }

  // comparison
  bool operator== (const Request &a)
  { return (bool)(mpi_request == a.mpi_request); }
  bool operator!= (const Request &a)
  { return (bool)!(*this == a); }

  // inter-language operability
  Request& operator= (const MPI_Request &i) {
    mpi_request = i; return *this; }
  operator MPI_Request () const { return mpi_request; }
  //  operator MPI_Request* () const { return (MPI_Request*)&mpi_request; }

#endif

  //
  // Point-to-Point Communication
  //

  virtual void Wait(Status &status);

  virtual void Wait();

  virtual bool Test(Status &status);

  virtual bool Test();

  virtual void Free(void);

  static int Waitany(int count, Request array[], Status& status);

  static int Waitany(int count, Request array[]);

  static bool Testany(int count, Request array[], int& index, Status& status);

  static bool Testany(int count, Request array[], int& index);

  static void Waitall(int count, Request req_array[], Status stat_array[]);

  static void Waitall(int count, Request req_array[]);

  static bool Testall(int count, Request req_array[], Status stat_array[]);

  static bool Testall(int count, Request req_array[]);

  static int Waitsome(int incount, Request req_array[],
			     int array_of_indices[], Status stat_array[]) ;

  static int Waitsome(int incount, Request req_array[],
			     int array_of_indices[]);

  static int Testsome(int incount, Request req_array[],
			     int array_of_indices[], Status stat_array[]);

  static int Testsome(int incount, Request req_array[],
			     int array_of_indices[]);

  virtual void Cancel(void) const;

  virtual bool Get_status(Status& status) const;

  virtual bool Get_status() const;

protected:
#if ! 0 /* OMPI_ENABLE_MPI_PROFILING */
  MPI_Request mpi_request;
#endif

private:

#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  PMPI::Request pmpi_request;
#endif

};


class Prequest : public Request {
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  //  friend class PMPI::Prequest;
#endif
public:

  Prequest() { }

#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  Prequest(const Request& p) : Request(p), pmpi_request(p) { }

  Prequest(const PMPI::Prequest& r) :
    Request((const PMPI::Request&)r),
    pmpi_request(r) { }

  Prequest(const MPI_Request &i) : Request(i), pmpi_request(i) { }

  virtual ~Prequest() { }

  Prequest& operator=(const Request& r) {
    Request::operator=(r);
    pmpi_request = (PMPI::Prequest)r; return *this; }

  Prequest& operator=(const Prequest& r) {
    Request::operator=(r);
    pmpi_request = r.pmpi_request; return *this; }
#else
  Prequest(const Request& p) : Request(p) { }

  Prequest(const MPI_Request &i) : Request(i) { }

  virtual ~Prequest() { }

  Prequest& operator=(const Request& r) {
    mpi_request = r; return *this; }

  Prequest& operator=(const Prequest& r) {
    mpi_request = r.mpi_request; return *this; }
#endif

  virtual void Start();

  static void Startall(int count, Prequest array_of_requests[]);

#if 0 /* OMPI_ENABLE_MPI_PROFILING */
private:
  PMPI::Prequest pmpi_request;
#endif
};


//
// Generalized requests
//
class Grequest : public MPI::Request {
  public:
    typedef int Query_function(void *, Status&);
    typedef int Free_function(void *);
    typedef int Cancel_function(void *, bool);

    Grequest() {}
    Grequest(const Request& req) : Request(req) {}
    Grequest(const MPI_Request &req) : Request(req) {}
    virtual ~Grequest() {}

    Grequest& operator=(const Request& req) {
	mpi_request = req; return(*this);
    }

    Grequest& operator=(const Grequest& req) {
	mpi_request = req.mpi_request; return(*this);
    }

    static Grequest Start(Query_function *, Free_function *,
	    Cancel_function *, void *);

    virtual void Complete();

    //
    // Type used for intercepting Generalized requests in the C++ layer so
    // that the type can be converted to C++ types before invoking the
    // user-specified C++ callbacks.
    //
    struct Intercept_data_t {
        void *id_extra;
        Grequest::Query_function *id_cxx_query_fn;
        Grequest::Free_function *id_cxx_free_fn;
        Grequest::Cancel_function *id_cxx_cancel_fn;
    };
};
