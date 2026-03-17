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
// Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

//
// Point-to-Point Communication
//

inline void
MPI::Request::Wait(MPI::Status &status)
{
  (void)MPI_Wait(&mpi_request, &status.mpi_status);
}

inline void
MPI::Request::Wait()
{
  (void)MPI_Wait(&mpi_request, MPI_STATUS_IGNORE);
}

inline void
MPI::Request::Free()
{
  (void)MPI_Request_free(&mpi_request);
}

inline bool
MPI::Request::Test(MPI::Status &status)
{
  int t;
  (void)MPI_Test(&mpi_request, &t, &status.mpi_status);
  return OPAL_INT_TO_BOOL(t);
}

inline bool
MPI::Request::Test()
{
  int t;
  (void)MPI_Test(&mpi_request, &t, MPI_STATUS_IGNORE);
  return OPAL_INT_TO_BOOL(t);
}

inline int
MPI::Request::Waitany(int count, MPI::Request array[],
			     MPI::Status& status)
{
  int index, i;
  MPI_Request* array_of_requests = new MPI_Request[count];
  for (i=0; i < count; i++) {
    array_of_requests[i] = array[i];
  }
  (void)MPI_Waitany(count, array_of_requests, &index, &status.mpi_status);
  for (i=0; i < count; i++) {
    array[i] = array_of_requests[i];
  }
  delete [] array_of_requests;
  return index;
}

inline int
MPI::Request::Waitany(int count, MPI::Request array[])
{
  int index, i;
  MPI_Request* array_of_requests = new MPI_Request[count];
  for (i=0; i < count; i++) {
    array_of_requests[i] = array[i];
  }
  (void)MPI_Waitany(count, array_of_requests, &index, MPI_STATUS_IGNORE);
  for (i=0; i < count; i++) {
    array[i] = array_of_requests[i];
  }
  delete [] array_of_requests;
  return index; //JGS, Waitany return value
}

inline bool
MPI::Request::Testany(int count, MPI::Request array[],
			     int& index, MPI::Status& status)
{
  int i, flag;
  MPI_Request* array_of_requests = new MPI_Request[count];
  for (i=0; i < count; i++) {
    array_of_requests[i] = array[i];
  }
  (void)MPI_Testany(count, array_of_requests, &index, &flag, &status.mpi_status);
  for (i=0; i < count; i++) {
    array[i] = array_of_requests[i];
  }
  delete [] array_of_requests;
  return (bool)(flag != 0 ? true : false);
}

inline bool
MPI::Request::Testany(int count, MPI::Request array[], int& index)
{
  int i, flag;
  MPI_Request* array_of_requests = new MPI_Request[count];
  for (i=0; i < count; i++) {
    array_of_requests[i] = array[i];
  }
  (void)MPI_Testany(count, array_of_requests, &index, &flag,
		    MPI_STATUS_IGNORE);
  for (i=0; i < count; i++) {
    array[i] = array_of_requests[i];
  }
  delete [] array_of_requests;
  return OPAL_INT_TO_BOOL(flag);
}

inline void
MPI::Request::Waitall(int count, MPI::Request req_array[],
			     MPI::Status stat_array[])
{
  int i;
  MPI_Request* array_of_requests = new MPI_Request[count];
  MPI_Status* array_of_statuses = new MPI_Status[count];
  for (i=0; i < count; i++) {
    array_of_requests[i] = req_array[i];
  }
  (void)MPI_Waitall(count, array_of_requests, array_of_statuses);
  for (i=0; i < count; i++) {
    req_array[i] = array_of_requests[i];
    stat_array[i] = array_of_statuses[i];
  }
  delete [] array_of_requests;
  delete [] array_of_statuses;
}

inline void
MPI::Request::Waitall(int count, MPI::Request req_array[])
{
  int i;
  MPI_Request* array_of_requests = new MPI_Request[count];

  for (i=0; i < count; i++) {
    array_of_requests[i] = req_array[i];
  }
  (void)MPI_Waitall(count, array_of_requests, MPI_STATUSES_IGNORE);

  for (i=0; i < count; i++) {
    req_array[i] = array_of_requests[i];
  }

  delete [] array_of_requests;
}

inline bool
MPI::Request::Testall(int count, MPI::Request req_array[],
			     MPI::Status stat_array[])
{
  int i, flag;
  MPI_Request* array_of_requests = new MPI_Request[count];
  MPI_Status* array_of_statuses = new MPI_Status[count];
  for (i=0; i < count; i++) {
    array_of_requests[i] = req_array[i];
  }
  (void)MPI_Testall(count, array_of_requests, &flag, array_of_statuses);
  for (i=0; i < count; i++) {
    req_array[i] = array_of_requests[i];
    stat_array[i] = array_of_statuses[i];
  }
  delete [] array_of_requests;
  delete [] array_of_statuses;
  return OPAL_INT_TO_BOOL(flag);
}

inline bool
MPI::Request::Testall(int count, MPI::Request req_array[])
{
  int i, flag;
  MPI_Request* array_of_requests = new MPI_Request[count];

  for (i=0; i < count; i++) {
    array_of_requests[i] = req_array[i];
  }
  (void)MPI_Testall(count, array_of_requests, &flag, MPI_STATUSES_IGNORE);

  for (i=0; i < count; i++) {
    req_array[i] = array_of_requests[i];
  }
  delete [] array_of_requests;

  return OPAL_INT_TO_BOOL(flag);
}

inline int
MPI::Request::Waitsome(int incount, MPI::Request req_array[],
			      int array_of_indices[], MPI::Status stat_array[])
{
  int i, outcount;
  MPI_Request* array_of_requests = new MPI_Request[incount];
  MPI_Status* array_of_statuses = new MPI_Status[incount];
  for (i=0; i < incount; i++) {
    array_of_requests[i] = req_array[i];
  }
  (void)MPI_Waitsome(incount, array_of_requests, &outcount,
		     array_of_indices, array_of_statuses);
  for (i=0; i < incount; i++) {
    req_array[i] = array_of_requests[i];
    stat_array[i] = array_of_statuses[i];
  }
  delete [] array_of_requests;
  delete [] array_of_statuses;
  return outcount;
}

inline int
MPI::Request::Waitsome(int incount, MPI::Request req_array[],
			      int array_of_indices[])
{
  int i, outcount;
  MPI_Request* array_of_requests = new MPI_Request[incount];

  for (i=0; i < incount; i++) {
    array_of_requests[i] = req_array[i];
  }
  (void)MPI_Waitsome(incount, array_of_requests, &outcount,
		     array_of_indices, MPI_STATUSES_IGNORE);

  for (i=0; i < incount; i++) {
    req_array[i] = array_of_requests[i];
  }
  delete [] array_of_requests;

  return outcount;
}

inline int
MPI::Request::Testsome(int incount, MPI::Request req_array[],
			      int array_of_indices[], MPI::Status stat_array[])
{
  int i, outcount;
  MPI_Request* array_of_requests = new MPI_Request[incount];
  MPI_Status* array_of_statuses = new MPI_Status[incount];
  for (i=0; i < incount; i++) {
    array_of_requests[i] = req_array[i];
  }
  (void)MPI_Testsome(incount, array_of_requests, &outcount,
		     array_of_indices, array_of_statuses);
  for (i=0; i < incount; i++) {
    req_array[i] = array_of_requests[i];
    stat_array[i] = array_of_statuses[i];
  }
  delete [] array_of_requests;
  delete [] array_of_statuses;
  return outcount;
}

inline int
MPI::Request::Testsome(int incount, MPI::Request req_array[],
			      int array_of_indices[])
{
  int i, outcount;
  MPI_Request* array_of_requests = new MPI_Request[incount];

  for (i=0; i < incount; i++) {
    array_of_requests[i] = req_array[i];
  }
  (void)MPI_Testsome(incount, array_of_requests, &outcount,
		     array_of_indices, MPI_STATUSES_IGNORE);

  for (i=0; i < incount; i++) {
    req_array[i] = array_of_requests[i];
  }
  delete [] array_of_requests;

  return outcount;
}

inline void
MPI::Request::Cancel(void) const
{
  (void)MPI_Cancel(const_cast<MPI_Request *>(&mpi_request));
}

inline void
MPI::Prequest::Start()
{
  (void)MPI_Start(&mpi_request);
}

inline void
MPI::Prequest::Startall(int count, MPI:: Prequest array_of_requests[])
{
  //convert the array of Prequests to an array of MPI_requests
  MPI_Request* mpi_requests = new MPI_Request[count];
  int i;
  for (i=0; i < count; i++) {
    mpi_requests[i] = array_of_requests[i];
  }
  (void)MPI_Startall(count, mpi_requests);
  for (i=0; i < count; i++) {
    array_of_requests[i].mpi_request = mpi_requests[i] ;
  }
  delete [] mpi_requests;
}

inline bool MPI::Request::Get_status(MPI::Status& status) const
{
    int flag = 0;
    MPI_Status c_status;

    // Call the underlying MPI function rather than simply returning
    // status.mpi_status because we may have to invoke the generalized
    // request query function
    (void)MPI_Request_get_status(mpi_request, &flag, &c_status);
    if (flag) {
        status = c_status;
    }
    return OPAL_INT_TO_BOOL(flag);
}

inline bool MPI::Request::Get_status() const
{
    int flag;

    // Call the underlying MPI function rather than simply returning
    // status.mpi_status because we may have to invoke the generalized
    // request query function
    (void)MPI_Request_get_status(mpi_request, &flag, MPI_STATUS_IGNORE);
    return OPAL_INT_TO_BOOL(flag);
}

inline MPI::Grequest
MPI::Grequest::Start(Query_function *query_fn, Free_function *free_fn,
	Cancel_function *cancel_fn, void *extra)
{
    MPI_Request grequest = 0;
    Intercept_data_t *new_extra =
        new MPI::Grequest::Intercept_data_t;

    new_extra->id_extra = extra;
    new_extra->id_cxx_query_fn = query_fn;
    new_extra->id_cxx_free_fn = free_fn;
    new_extra->id_cxx_cancel_fn = cancel_fn;
    (void) MPI_Grequest_start(ompi_mpi_cxx_grequest_query_fn_intercept,
                              ompi_mpi_cxx_grequest_free_fn_intercept,
                              ompi_mpi_cxx_grequest_cancel_fn_intercept,
                              new_extra, &grequest);

    return(grequest);
}

inline void
MPI::Grequest::Complete()
{
    (void) MPI_Grequest_complete(mpi_request);
}

