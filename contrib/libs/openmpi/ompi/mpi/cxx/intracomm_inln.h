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
// Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
// Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

inline
MPI::Intracomm::Intracomm(MPI_Comm data) {
  int flag = 0;
  if (MPI::Is_initialized() && (data != MPI_COMM_NULL)) {
    (void)MPI_Comm_test_inter(data, &flag);
    if (flag) {
      mpi_comm = MPI_COMM_NULL;
    } else {
      mpi_comm = data;
    }
  }
  else {
    mpi_comm = data;
  }
}

inline void
MPI::Intracomm::Scan(const void *sendbuf, void *recvbuf, int count,
     const MPI::Datatype & datatype, const MPI::Op& op) const
{
  (void)MPI_Scan(const_cast<void *>(sendbuf), recvbuf, count, datatype, op, mpi_comm);
}

inline void
MPI::Intracomm::Exscan(const void *sendbuf, void *recvbuf, int count,
			      const MPI::Datatype & datatype,
			      const MPI::Op& op) const
{
  (void)MPI_Exscan(const_cast<void *>(sendbuf), recvbuf, count, datatype, op, mpi_comm);
}

inline MPI::Intracomm
MPI::Intracomm::Dup() const
{
  MPI_Comm newcomm;
  (void)MPI_Comm_dup(mpi_comm, &newcomm);
  return newcomm;
}

inline MPI::Intracomm&
MPI::Intracomm::Clone() const
{
  MPI_Comm newcomm;
  (void)MPI_Comm_dup(mpi_comm, &newcomm);
  MPI::Intracomm* dup = new MPI::Intracomm(newcomm);
  return *dup;
}

inline MPI::Intracomm
MPI::Intracomm::Create(const MPI::Group& group) const
{
  MPI_Comm newcomm;
  (void)MPI_Comm_create(mpi_comm, group, &newcomm);
  return newcomm;
}

inline MPI::Intracomm
MPI::Intracomm::Split(int color, int key) const
{
  MPI_Comm newcomm;
  (void)MPI_Comm_split(mpi_comm, color, key, &newcomm);
  return newcomm;
}



inline MPI::Intercomm
MPI::Intracomm::Create_intercomm(int local_leader,
					const MPI::Comm& peer_comm,
					int remote_leader, int tag) const
{
  MPI_Comm newintercomm;
  (void)MPI_Intercomm_create(mpi_comm, local_leader, peer_comm,
			     remote_leader, tag, &newintercomm);
  return newintercomm;
}

inline MPI::Cartcomm
MPI::Intracomm::Create_cart(int ndims, const int dims[],
				   const bool periods[], bool reorder) const
{
  int *int_periods = new int [ndims];
  for (int i=0; i<ndims; i++)
    int_periods[i] = (int) periods[i];

  MPI_Comm newcomm;
  (void)MPI_Cart_create(mpi_comm, ndims, const_cast<int *>(dims),
		      int_periods, (int)reorder, &newcomm);
  delete [] int_periods;
  return newcomm;
}

inline MPI::Graphcomm
MPI::Intracomm::Create_graph(int nnodes, const int index[],
				    const int edges[], bool reorder) const
{
  MPI_Comm newcomm;
  (void)MPI_Graph_create(mpi_comm, nnodes, const_cast<int *>(index),
                         const_cast<int *>(edges), (int)reorder, &newcomm);
  return newcomm;
}


//
// Process Creation and Management
//

inline MPI::Intercomm
MPI::Intracomm::Accept(const char* port_name,
			      const MPI::Info& info,
			      int root) const
{
  MPI_Comm newcomm;
  (void) MPI_Comm_accept(const_cast<char *>(port_name), info, root, mpi_comm,
			 &newcomm);
  return newcomm;
}


inline MPI::Intercomm
MPI::Intracomm::Connect(const char* port_name,
			       const MPI::Info& info,
			       int root) const
{
  MPI_Comm newcomm;
  (void) MPI_Comm_connect(const_cast<char *>(port_name), info, root, mpi_comm,
			  &newcomm);
  return newcomm;
}


inline MPI::Intercomm
MPI::Intracomm::Spawn(const char* command, const char* argv[],
			     int maxprocs, const MPI::Info& info,
			     int root) const
{
  MPI_Comm newcomm;
  (void) MPI_Comm_spawn(const_cast<char *>(command), const_cast<char **>(argv), maxprocs,
			info, root, mpi_comm, &newcomm,
			(int *)MPI_ERRCODES_IGNORE);
  return newcomm;
}


inline MPI::Intercomm
MPI::Intracomm::Spawn(const char* command, const char* argv[],
                             int maxprocs, const MPI::Info& info,
                             int root, int array_of_errcodes[]) const
{
  MPI_Comm newcomm;
  (void) MPI_Comm_spawn(const_cast<char *>(command), const_cast<char **>(argv), maxprocs,
                        info, root, mpi_comm, &newcomm,
			array_of_errcodes);
  return newcomm;
}


inline MPI::Intercomm
MPI::Intracomm::Spawn_multiple(int count,
				      const char* array_of_commands[],
				      const char** array_of_argv[],
				      const int array_of_maxprocs[],
				      const Info array_of_info[], int root)
{
  MPI_Comm newcomm;
  MPI_Info *const array_of_mpi_info =
      convert_info_to_mpi_info(count, array_of_info);

  MPI_Comm_spawn_multiple(count, const_cast<char **>(array_of_commands),
			  const_cast<char ***>(array_of_argv),
                          const_cast<int *>(array_of_maxprocs),
			  array_of_mpi_info, root,
			  mpi_comm, &newcomm, (int *)MPI_ERRCODES_IGNORE);
  delete[] array_of_mpi_info;
  return newcomm;
}

inline MPI_Info *
MPI::Intracomm::convert_info_to_mpi_info(int p_nbr, const Info p_info_tbl[])
{
   MPI_Info *const mpi_info_tbl = new MPI_Info [p_nbr];

   for (int i_tbl=0; i_tbl < p_nbr; i_tbl++) {
       mpi_info_tbl[i_tbl] = p_info_tbl[i_tbl];
   }

   return mpi_info_tbl;
}

inline MPI::Intercomm
MPI::Intracomm::Spawn_multiple(int count,
                                      const char* array_of_commands[],
                                      const char** array_of_argv[],
                                      const int array_of_maxprocs[],
                                      const Info array_of_info[], int root,
				      int array_of_errcodes[])
{
  MPI_Comm newcomm;
  MPI_Info *const array_of_mpi_info =
      convert_info_to_mpi_info(count, array_of_info);

  MPI_Comm_spawn_multiple(count, const_cast<char **>(array_of_commands),
                          const_cast<char ***>(array_of_argv),
                          const_cast<int *>(array_of_maxprocs),
                          array_of_mpi_info, root,
                          mpi_comm, &newcomm, array_of_errcodes);
  delete[] array_of_mpi_info;
  return newcomm;
}










