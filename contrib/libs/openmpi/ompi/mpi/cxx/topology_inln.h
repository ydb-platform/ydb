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
// Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
// Copyright (c) 2011      FUJITSU LIMITED.  All rights reserved.
// Copyright (c) 2016 Cisco Systems, Inc.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

//
//   ========   Cartcomm member functions  ========
//

inline
MPI::Cartcomm::Cartcomm(const MPI_Comm& data) {
  int status = 0;
  if (MPI::Is_initialized() && (data != MPI_COMM_NULL)) {
    (void)MPI_Topo_test(data, &status) ;
    if (status == MPI_CART)
      mpi_comm = data;
    else
      mpi_comm = MPI_COMM_NULL;
  }
  else {
    mpi_comm = data;
  }
}

//
// Groups, Contexts, and Communicators
//

inline MPI::Cartcomm
MPI::Cartcomm::Dup() const
{
  MPI_Comm newcomm;
  (void)MPI_Comm_dup(mpi_comm, &newcomm);
  return newcomm;
}

//
//  Process Topologies
//

inline int
MPI::Cartcomm::Get_dim() const
{
  int ndims;
  (void)MPI_Cartdim_get(mpi_comm, &ndims);
  return ndims;
}

inline void
MPI::Cartcomm::Get_topo(int maxdims, int dims[], bool periods[],
			       int coords[]) const
{
  int *int_periods = new int [maxdims];
  int i;
  for (i=0; i<maxdims; i++) {
    int_periods[i] = (int)periods[i];
  }
  (void)MPI_Cart_get(mpi_comm, maxdims, dims, int_periods, coords);
  for (i=0; i<maxdims; i++) {
    periods[i] = OPAL_INT_TO_BOOL(int_periods[i]);
  }
  delete [] int_periods;
}

inline int
MPI::Cartcomm::Get_cart_rank(const int coords[]) const
{
  int myrank;
  (void)MPI_Cart_rank(mpi_comm, const_cast<int *>(coords), &myrank);
  return myrank;
}

inline void
MPI::Cartcomm::Get_coords(int rank, int maxdims, int coords[]) const
{
  (void)MPI_Cart_coords(mpi_comm, rank, maxdims, coords);
}

inline void
MPI::Cartcomm::Shift(int direction, int disp,
			    int &rank_source, int &rank_dest) const
{
  (void)MPI_Cart_shift(mpi_comm, direction, disp, &rank_source, &rank_dest);
}

inline MPI::Cartcomm
MPI::Cartcomm::Sub(const bool remain_dims[]) const
{
  int ndims;
  MPI_Cartdim_get(mpi_comm, &ndims);
  int* int_remain_dims = new int[ndims];
  for (int i=0; i<ndims; i++) {
    int_remain_dims[i] = (int)remain_dims[i];
  }
  MPI_Comm newcomm;
  (void)MPI_Cart_sub(mpi_comm, int_remain_dims, &newcomm);
  delete [] int_remain_dims;
  return newcomm;
}

inline int
MPI::Cartcomm::Map(int ndims, const int dims[], const bool periods[]) const
{
  int *int_periods = new int [ndims];
  for (int i=0; i<ndims; i++) {
    int_periods[i] = (int) periods[i];
  }
  int newrank;
  (void)MPI_Cart_map(mpi_comm, ndims, const_cast<int *>(dims), int_periods, &newrank);
  delete [] int_periods;
  return newrank;
}


inline MPI::Cartcomm&
MPI::Cartcomm::Clone() const
{
  MPI_Comm newcomm;
  (void)MPI_Comm_dup(mpi_comm, &newcomm);
  MPI::Cartcomm* dup = new MPI::Cartcomm(newcomm);
  return *dup;
}

//
//   ========   Graphcomm member functions  ========
//

inline
MPI::Graphcomm::Graphcomm(const MPI_Comm& data) {
  int status = 0;
  if (MPI::Is_initialized() && (data != MPI_COMM_NULL)) {
    (void)MPI_Topo_test(data, &status) ;
    if (status == MPI_GRAPH)
      mpi_comm = data;
    else
      mpi_comm = MPI_COMM_NULL;
  }
  else {
    mpi_comm = data;
  }
}

//
// Groups, Contexts, and Communicators
//

inline MPI::Graphcomm
MPI::Graphcomm::Dup() const
{
  MPI_Comm newcomm;
  (void)MPI_Comm_dup(mpi_comm, &newcomm);
  return newcomm;
}

//
//  Process Topologies
//

inline void
MPI::Graphcomm::Get_dims(int nnodes[], int nedges[]) const
{
  (void)MPI_Graphdims_get(mpi_comm, nnodes, nedges);
}

inline void
MPI::Graphcomm::Get_topo(int maxindex, int maxedges, int index[],
	 int edges[]) const
{
  (void)MPI_Graph_get(mpi_comm, maxindex, maxedges, index, edges);
}

inline int
MPI::Graphcomm::Get_neighbors_count(int rank) const
{
  int nneighbors;
  (void)MPI_Graph_neighbors_count(mpi_comm, rank, &nneighbors);
  return nneighbors;
}

inline void
MPI::Graphcomm::Get_neighbors(int rank, int maxneighbors,
	      int neighbors[]) const
{
  (void)MPI_Graph_neighbors(mpi_comm, rank, maxneighbors, neighbors);
}

inline int
MPI::Graphcomm::Map(int nnodes, const int index[],
    const int edges[]) const
{
  int newrank;
  (void)MPI_Graph_map(mpi_comm, nnodes, const_cast<int *>(index), const_cast<int *>(edges), &newrank);
  return newrank;
}

inline MPI::Graphcomm&
MPI::Graphcomm::Clone() const
{
  MPI_Comm newcomm;
  (void)MPI_Comm_dup(mpi_comm, &newcomm);
  MPI::Graphcomm* dup = new MPI::Graphcomm(newcomm);
  return *dup;
}
