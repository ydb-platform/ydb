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
// Copyright (c) 2011      FUJITSU LIMITED.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//


class Cartcomm : public Intracomm {
public:

  // construction
  Cartcomm() { }
  // copy
  Cartcomm(const Comm_Null& data) : Intracomm(data) { }
  // inter-language operability
  inline Cartcomm(const MPI_Comm& data);
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  Cartcomm(const Cartcomm& data) : Intracomm(data), pmpi_comm(data) { }
  Cartcomm(const PMPI::Cartcomm& d) :
    Intracomm((const PMPI::Intracomm&)d),
    pmpi_comm(d) { }

  // assignment
  Cartcomm& operator=(const Cartcomm& data) {
    Intracomm::operator=(data);
    pmpi_comm = data.pmpi_comm; return *this; }
  Cartcomm& operator=(const Comm_Null& data) {
    Intracomm::operator=(data);
    pmpi_comm = (PMPI::Cartcomm)data; return *this; }
  // inter-language operability
  Cartcomm& operator=(const MPI_Comm& data) {
    Intracomm::operator=(data);
    pmpi_comm = data; return *this; }
#else
  Cartcomm(const Cartcomm& data) : Intracomm(data.mpi_comm) { }
  // assignment
  Cartcomm& operator=(const Cartcomm& data) {
    mpi_comm = data.mpi_comm; return *this; }
  Cartcomm& operator=(const Comm_Null& data) {
    mpi_comm = data; return *this; }
  // inter-language operability
  Cartcomm& operator=(const MPI_Comm& data) {
    mpi_comm = data; return *this; }
#endif
  //
  // Groups, Contexts, and Communicators
  //

  Cartcomm Dup() const;

  virtual Cartcomm& Clone() const;


  //
  // Groups, Contexts, and Communicators
  //

  virtual int Get_dim() const;

  virtual void Get_topo(int maxdims, int dims[], bool periods[],
			int coords[]) const;

  virtual int Get_cart_rank(const int coords[]) const;

  virtual void Get_coords(int rank, int maxdims, int coords[]) const;

  virtual void Shift(int direction, int disp,
		     int &rank_source, int &rank_dest) const;

  virtual Cartcomm Sub(const bool remain_dims[]) const;

  virtual int Map(int ndims, const int dims[], const bool periods[]) const;

#if 0 /* OMPI_ENABLE_MPI_PROFILING */
private:
  PMPI::Cartcomm pmpi_comm;
#endif
};


//===================================================================
//                    Class Graphcomm
//===================================================================

class Graphcomm : public Intracomm {
public:

  // construction
  Graphcomm() { }
  // copy
  Graphcomm(const Comm_Null& data) : Intracomm(data) { }
  // inter-language operability
  inline Graphcomm(const MPI_Comm& data);
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  Graphcomm(const Graphcomm& data) : Intracomm(data), pmpi_comm(data) { }
  Graphcomm(const PMPI::Graphcomm& d) :
    Intracomm((const PMPI::Intracomm&)d), pmpi_comm(d) { }

  // assignment
  Graphcomm& operator=(const Graphcomm& data) {
    Intracomm::operator=(data);
    pmpi_comm = data.pmpi_comm; return *this; }
  Graphcomm& operator=(const Comm_Null& data) {
    Intracomm::operator=(data);
    pmpi_comm = (PMPI::Graphcomm)data; return *this; }
  // inter-language operability
  Graphcomm& operator=(const MPI_Comm& data) {
    Intracomm::operator=(data);
    pmpi_comm = data; return *this; }

#else
  Graphcomm(const Graphcomm& data) : Intracomm(data.mpi_comm) { }
  // assignment
  Graphcomm& operator=(const Graphcomm& data) {
    mpi_comm = data.mpi_comm; return *this; }
  Graphcomm& operator=(const Comm_Null& data) {
    mpi_comm = data; return *this; }
  // inter-language operability
  Graphcomm& operator=(const MPI_Comm& data) {
    mpi_comm = data; return *this; }
#endif

  //
  // Groups, Contexts, and Communicators
  //

  Graphcomm Dup() const;

  virtual Graphcomm& Clone() const;

  //
  //  Process Topologies
  //

  virtual void Get_dims(int nnodes[], int nedges[]) const;

  virtual void Get_topo(int maxindex, int maxedges, int index[],
			int edges[]) const;

  virtual int Get_neighbors_count(int rank) const;

  virtual void Get_neighbors(int rank, int maxneighbors,
			     int neighbors[]) const;

  virtual int Map(int nnodes, const int index[],
		  const int edges[]) const;

#if 0 /* OMPI_ENABLE_MPI_PROFILING */
private:
  PMPI::Graphcomm pmpi_comm;
#endif
};

