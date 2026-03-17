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
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

class Group {
#if 0 /* OMPI_ENABLE_MPI_PROFILING */
  //  friend class PMPI::Group;
#endif
public:

#if 0 /* OMPI_ENABLE_MPI_PROFILING */

  // construction
  inline Group() { }
  inline Group(MPI_Group i) : pmpi_group(i) { }
  // copy
  inline Group(const Group& g) : pmpi_group(g.pmpi_group) { }

  inline Group(const PMPI::Group& g) : pmpi_group(g) { }

  inline virtual ~Group() {}

  Group& operator=(const Group& g) {
    pmpi_group = g.pmpi_group; return *this;
  }

  // comparison
  inline bool operator== (const Group &a) {
    return (bool)(pmpi_group == a.pmpi_group);
  }
  inline bool operator!= (const Group &a) {
    return (bool)!(*this == a);
  }

  // inter-language operability
  Group& operator= (const MPI_Group &i) { pmpi_group = i; return *this; }
  inline operator MPI_Group () const { return pmpi_group.mpi(); }
  //  inline operator MPI_Group* () const { return pmpi_group; }
  inline operator const PMPI::Group&() const { return pmpi_group; }

  const PMPI::Group& pmpi() { return pmpi_group; }
#else

  // construction
  inline Group() : mpi_group(MPI_GROUP_NULL) { }
  inline Group(MPI_Group i) : mpi_group(i) { }

  // copy
  inline Group(const Group& g) : mpi_group(g.mpi_group) { }

  inline virtual ~Group() {}

  inline Group& operator=(const Group& g) { mpi_group = g.mpi_group; return *this; }

  // comparison
  inline bool operator== (const Group &a) { return (bool)(mpi_group == a.mpi_group); }
  inline bool operator!= (const Group &a) { return (bool)!(*this == a); }

  // inter-language operability
  inline Group& operator= (const MPI_Group &i) { mpi_group = i; return *this; }
  inline operator MPI_Group () const { return mpi_group; }
  //  inline operator MPI_Group* () const { return (MPI_Group*)&mpi_group; }

  inline MPI_Group mpi() const { return mpi_group; }

#endif

  //
  // Groups, Contexts, and Communicators
  //

  virtual int Get_size() const;

  virtual int Get_rank() const;

  static void Translate_ranks (const Group& group1, int n, const int ranks1[],
			       const Group& group2, int ranks2[]);

  static int Compare(const Group& group1, const Group& group2);

  static Group Union(const Group &group1, const Group &group2);

  static Group Intersect(const Group &group1, const Group &group2);

  static Group Difference(const Group &group1, const Group &group2);

  virtual Group Incl(int n, const int ranks[]) const;

  virtual Group Excl(int n, const int ranks[]) const;

  virtual Group Range_incl(int n, const int ranges[][3]) const;

  virtual Group Range_excl(int n, const int ranges[][3]) const;

  virtual void Free();

protected:
#if ! 0 /* OMPI_ENABLE_MPI_PROFILING */
  MPI_Group mpi_group;
#endif

#if 0 /* OMPI_ENABLE_MPI_PROFILING */
private:
  PMPI::Group pmpi_group;
#endif

};

