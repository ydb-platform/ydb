#ifndef PyMPI_FALLBACK_H
#define PyMPI_FALLBACK_H

/* ---------------------------------------------------------------- */

#include <stdlib.h>
#ifndef PyMPI_MALLOC
  #define PyMPI_MALLOC malloc
#endif
#ifndef PyMPI_FREE
  #define PyMPI_FREE free
#endif

/* ---------------------------------------------------------------- */

/* Version Number */

#ifndef PyMPI_HAVE_MPI_VERSION
#if !defined(MPI_VERSION)
#define MPI_VERSION 1
#endif
#endif

#ifndef PyMPI_HAVE_MPI_SUBVERSION
#if !defined(MPI_SUBVERSION)
#define MPI_SUBVERSION 0
#endif
#endif

#ifndef PyMPI_HAVE_MPI_Get_version
static int PyMPI_Get_version(int *version, int* subversion)
{
  if (!version)    return MPI_ERR_ARG;
  if (!subversion) return MPI_ERR_ARG;
  *version    = MPI_VERSION;
  *subversion = MPI_SUBVERSION;
  return MPI_SUCCESS;
}
#undef  MPI_Get_version
#define MPI_Get_version PyMPI_Get_version
#endif

#ifndef PyMPI_HAVE_MPI_Get_library_version
#define PyMPI_MAX_LIBRARY_VERSION_STRING 8
static int PyMPI_Get_library_version(char version[], int *rlen)
{
  if (!version) return MPI_ERR_ARG;
  if (!rlen)    return MPI_ERR_ARG;
  version[0] = 'M';
  version[1] = 'P';
  version[2] = 'I';
  version[3] = ' ';
  version[4] = '0' + (char) MPI_VERSION;
  version[5] = '.';
  version[6] = '0' + (char) MPI_SUBVERSION;
  version[7] = 0;
  *rlen = 7;
  return MPI_SUCCESS;
}
#undef  MPI_MAX_LIBRARY_VERSION_STRING
#define MPI_MAX_LIBRARY_VERSION_STRING \
        PyMPI_MAX_LIBRARY_VERSION_STRING
#undef  MPI_Get_library_version
#define MPI_Get_library_version \
        PyMPI_Get_library_version
#endif

/* ---------------------------------------------------------------- */

/* Threading Support */

#ifndef PyMPI_HAVE_MPI_Init_thread
static int PyMPI_Init_thread(int *argc, char ***argv,
                             int required, int *provided)
{
  int ierr = MPI_SUCCESS;
  if (!provided) return MPI_ERR_ARG;
  ierr = MPI_Init(argc, argv);
  if (ierr != MPI_SUCCESS) return ierr;
  *provided = MPI_THREAD_SINGLE;
  return MPI_SUCCESS;
}
#undef  MPI_Init_thread
#define MPI_Init_thread PyMPI_Init_thread
#endif

#ifndef PyMPI_HAVE_MPI_Query_thread
static int PyMPI_Query_thread(int *provided)
{
  if (!provided) return MPI_ERR_ARG;
  provided = MPI_THREAD_SINGLE;
  return MPI_SUCCESS;
}
#undef  MPI_Query_thread
#define MPI_Query_thread PyMPI_Query_thread
#endif

#ifndef PyMPI_HAVE_MPI_Is_thread_main
static int PyMPI_Is_thread_main(int *flag)
{
  if (!flag) return MPI_ERR_ARG;
  *flag = 1; /* XXX this is completely broken !! */
  return MPI_SUCCESS;
}
#undef  MPI_Is_thread_main
#define MPI_Is_thread_main PyMPI_Is_thread_main
#endif

/* ---------------------------------------------------------------- */

/* Status */

#ifndef PyMPI_HAVE_MPI_STATUS_IGNORE
static MPI_Status PyMPI_STATUS_IGNORE;
#undef  MPI_STATUS_IGNORE
#define MPI_STATUS_IGNORE ((MPI_Status*)(&PyMPI_STATUS_IGNORE))
#endif

#ifndef PyMPI_HAVE_MPI_STATUSES_IGNORE
#ifndef PyMPI_MPI_STATUSES_IGNORE_SIZE
#define PyMPI_MPI_STATUSES_IGNORE_SIZE 4096
#endif
static MPI_Status PyMPI_STATUSES_IGNORE[PyMPI_MPI_STATUSES_IGNORE_SIZE];
#undef  MPI_STATUSES_IGNORE
#define MPI_STATUSES_IGNORE ((MPI_Status*)(PyMPI_STATUSES_IGNORE))
#endif

/* ---------------------------------------------------------------- */

/* Datatypes */

#ifndef PyMPI_HAVE_MPI_LONG_LONG
#undef  MPI_LONG_LONG
#define MPI_LONG_LONG MPI_LONG_LONG_INT
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_extent
static int PyMPI_Type_get_extent(MPI_Datatype datatype,
                                 MPI_Aint *lb, MPI_Aint *extent)
{
  int ierr = MPI_SUCCESS;
  ierr = MPI_Type_lb(datatype, lb);
  if (ierr != MPI_SUCCESS) return ierr;
  ierr = MPI_Type_extent(datatype, extent);
  if (ierr != MPI_SUCCESS) return ierr;
  return MPI_SUCCESS;
}
#undef  MPI_Type_get_extent
#define MPI_Type_get_extent PyMPI_Type_get_extent
#endif

#ifndef PyMPI_HAVE_MPI_Type_dup
static int PyMPI_Type_dup(MPI_Datatype datatype, MPI_Datatype *newtype)
{
  int ierr = MPI_SUCCESS;
  ierr = MPI_Type_contiguous(1, datatype, newtype);
  if (ierr != MPI_SUCCESS) return ierr;
  ierr = MPI_Type_commit(newtype); /* the safe way  ... */
  if (ierr != MPI_SUCCESS) return ierr;
  return MPI_SUCCESS;
}
#undef  MPI_Type_dup
#define MPI_Type_dup PyMPI_Type_dup
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_indexed_block
static int PyMPI_Type_create_indexed_block(int count,
                                           int blocklength,
                                           int displacements[],
                                           MPI_Datatype oldtype,
                                           MPI_Datatype *newtype)
{
  int i, *blocklengths = 0, ierr = MPI_SUCCESS;
  if (count > 0) {
    blocklengths = (int *) PyMPI_MALLOC((size_t)count*sizeof(int));
    if (!blocklengths) return MPI_ERR_INTERN;
  }
  for (i=0; i<count; i++) blocklengths[i] = blocklength;
  ierr = MPI_Type_indexed(count,blocklengths,displacements,oldtype,newtype);
  if (blocklengths) PyMPI_FREE(blocklengths);
  return ierr;
}
#undef  MPI_Type_create_indexed_block
#define MPI_Type_create_indexed_block PyMPI_Type_create_indexed_block
#undef  MPI_COMBINER_INDEXED_BLOCK
#define MPI_COMBINER_INDEXED_BLOCK MPI_COMBINER_INDEXED
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_hindexed_block
static int PyMPI_Type_create_hindexed_block(int count,
                                            int blocklength,
                                            MPI_Aint displacements[],
                                            MPI_Datatype oldtype,
                                            MPI_Datatype *newtype)
{
  int i, *blocklengths = 0, ierr = MPI_SUCCESS;
  if (count > 0) {
    blocklengths = (int *) PyMPI_MALLOC((size_t)count*sizeof(int));
    if (!blocklengths) return MPI_ERR_INTERN;
  }
  for (i=0; i<count; i++) blocklengths[i] = blocklength;
  ierr = MPI_Type_create_hindexed(count,blocklengths,displacements,oldtype,newtype);
  if (blocklengths) PyMPI_FREE(blocklengths);
  return ierr;
}
#undef  MPI_Type_create_hindexed_block
#define MPI_Type_create_hindexed_block PyMPI_Type_create_hindexed_block
#undef  MPI_COMBINER_HINDEXED_BLOCK
#define MPI_COMBINER_HINDEXED_BLOCK MPI_COMBINER_HINDEXED
#endif

/*
 * Adapted from the implementation in MPICH2 sources,
 * mpich2-1.0.7/src/mpi/datatype/type_create_subarray.c
 *
 */
#ifndef PyMPI_HAVE_MPI_Type_create_subarray

#undef  PyMPI_CHKARG
#define PyMPI_CHKARG(expr) if (!(expr)) return MPI_ERR_ARG

static int PyMPI_Type_create_subarray(int ndims,
                                      int sizes[],
                                      int subsizes[],
                                      int starts[],
                                      int order,
                                      MPI_Datatype oldtype,
                                      MPI_Datatype *newtype)
{
  int i = 0;
  MPI_Aint size = 0, extent = 0, disps[3];
  int blklens[3];
  MPI_Datatype tmp1, tmp2, types[3];
  int ierr = MPI_SUCCESS;
  tmp1 = tmp2 = types[0] = types[1] = types[2] = MPI_DATATYPE_NULL;

  PyMPI_CHKARG(ndims > 0);
  PyMPI_CHKARG(sizes);
  PyMPI_CHKARG(subsizes);
  PyMPI_CHKARG(starts);
  PyMPI_CHKARG(newtype);
  for (i=0; i<ndims; i++) {
    PyMPI_CHKARG(sizes[i] >  0);
    PyMPI_CHKARG(subsizes[i] >  0);
    PyMPI_CHKARG(starts[i] >= 0);
    PyMPI_CHKARG(sizes[i] >= subsizes[i]);
    PyMPI_CHKARG(starts[i] <= (sizes[i] - subsizes[i]));
  }
  PyMPI_CHKARG((order==MPI_ORDER_C) || (order==MPI_ORDER_FORTRAN));

  ierr = MPI_Type_extent(oldtype, &extent);
  if (ierr != MPI_SUCCESS) return ierr;

  if (order == MPI_ORDER_FORTRAN) {
    if (ndims == 1) {
      ierr = MPI_Type_contiguous(subsizes[0], oldtype, &tmp1);
      if (ierr != MPI_SUCCESS) return ierr;
    } else {
      ierr = MPI_Type_vector(subsizes[1], subsizes[0],
                             sizes[0], oldtype, &tmp1);
      if (ierr != MPI_SUCCESS) return ierr;
      size = sizes[0]*extent;
      for (i=2; i<ndims; i++) {
        size *= sizes[i-1];
        ierr = MPI_Type_hvector(subsizes[i], 1, size, tmp1, &tmp2);
        if (ierr != MPI_SUCCESS) return ierr;
        ierr = MPI_Type_free(&tmp1);
        if (ierr != MPI_SUCCESS) return ierr;
        tmp1 = tmp2;
      }
    }
    /* add displacement and UB */
    disps[1] = starts[0];
    size = 1;
    for (i=1; i<ndims; i++) {
      size *= sizes[i-1];
      disps[1] += size*starts[i];
    }
  } else /* MPI_ORDER_C */ {
    /* dimension ndims-1 changes fastest */
    if (ndims == 1) {
      ierr = MPI_Type_contiguous(subsizes[0], oldtype, &tmp1);
      if (ierr != MPI_SUCCESS) return ierr;
    } else {
      ierr = MPI_Type_vector(subsizes[ndims-2], subsizes[ndims-1],
                             sizes[ndims-1], oldtype, &tmp1);
      if (ierr != MPI_SUCCESS) return ierr;
      size = sizes[ndims-1]*extent;
      for (i=ndims-3; i>=0; i--) {
        size *= sizes[i+1];
        ierr = MPI_Type_hvector(subsizes[i], 1, size, tmp1, &tmp2);
        if (ierr != MPI_SUCCESS) return ierr;
        ierr = MPI_Type_free(&tmp1);
        if (ierr != MPI_SUCCESS) return ierr;
        tmp1 = tmp2;
      }
    }
    /* add displacement and UB */
    disps[1] = starts[ndims-1];
    size = 1;
    for (i=ndims-2; i>=0; i--) {
      size *= sizes[i+1];
      disps[1] += size*starts[i];
    }
  }

  disps[1] *= extent;
  disps[2] = extent;
  for (i=0; i<ndims; i++) disps[2] *= sizes[i];
  disps[0] = 0;
  blklens[0] = blklens[1] = blklens[2] = 1;
  types[0] = MPI_LB;
  types[1] = tmp1;
  types[2] = MPI_UB;

  ierr = MPI_Type_struct(3, blklens, disps, types, newtype);
  if (ierr != MPI_SUCCESS) return ierr;
  ierr = MPI_Type_free(&tmp1);
  if (ierr != MPI_SUCCESS) return ierr;

  return MPI_SUCCESS;
}

#undef PyMPI_CHKARG

#undef  MPI_Type_create_subarray
#define MPI_Type_create_subarray PyMPI_Type_create_subarray
#endif

/*
 * Adapted from the implementation in MPICH2 sources,
 * mpich2-1.0.7/src/mpi/datatype/type_create_darray.c
 *
 */
#ifndef PyMPI_HAVE_MPI_Type_create_darray

#undef  PyMPI_CHKARG
#define PyMPI_CHKARG(expr) if (!(expr)) return MPI_ERR_ARG

static int PyMPI_Type_block(int *gsizes,
                            int dim,
                            int ndims,
                            int nprocs,
                            int rank,
                            int darg,
                            int order,
                            MPI_Aint orig_extent,
                            MPI_Datatype type_old,
                            MPI_Datatype *type_new,
                            MPI_Aint *offset)
{
  int ierr, blksize, global_size, mysize, i, j;
  MPI_Aint stride;

  global_size = gsizes[dim];

  if (darg == MPI_DISTRIBUTE_DFLT_DARG)
    blksize = (global_size + nprocs - 1)/nprocs;
  else {
    blksize = darg;
    PyMPI_CHKARG(blksize > 0);
    PyMPI_CHKARG(blksize * nprocs >= global_size);
  }

  j = global_size - blksize*rank;
  mysize = (blksize < j) ? blksize : j;
  if (mysize < 0) mysize = 0;
  stride = orig_extent;
  if (order == MPI_ORDER_FORTRAN) {
    if (dim == 0) {
      ierr = MPI_Type_contiguous(mysize, type_old, type_new);
      if (ierr != MPI_SUCCESS) goto fn_exit;
    } else {
      for (i=0; i<dim; i++) stride *= gsizes[i];
      ierr = MPI_Type_hvector(mysize, 1, stride, type_old, type_new);
      if (ierr != MPI_SUCCESS) goto fn_exit;
    }
  } else { /* order == MPI_ORDER_C */
    if (dim == ndims-1) {
      ierr = MPI_Type_contiguous(mysize, type_old, type_new);
      if (ierr != MPI_SUCCESS) goto fn_exit;
    } else {
      for (i=ndims-1; i>dim; i--) stride *= gsizes[i];
      ierr = MPI_Type_hvector(mysize, 1, stride, type_old, type_new);
      if (ierr != MPI_SUCCESS) goto fn_exit;
    }
  }

  *offset = blksize * rank;
  if (mysize == 0) *offset = 0;

  ierr = MPI_SUCCESS;
 fn_exit:
  return ierr;
}
static int PyMPI_Type_cyclic(int *gsizes,
                             int dim,
                             int ndims,
                             int nprocs,
                             int rank,
                             int darg,
                             int order,
                             MPI_Aint orig_extent,
                             MPI_Datatype type_old,
                             MPI_Datatype *type_new,
                             MPI_Aint *offset)
{
  int ierr, blksize, i, blklens[3], st_index, end_index,
    local_size, rem, count;
  MPI_Aint stride, disps[3];
  MPI_Datatype type_tmp, types[3];

  type_tmp = MPI_DATATYPE_NULL;
  types[0] = types[1] = types[2] = MPI_DATATYPE_NULL;

  if (darg == MPI_DISTRIBUTE_DFLT_DARG)
    blksize = 1;
  else
    blksize = darg;
  PyMPI_CHKARG(blksize > 0);

  st_index = rank*blksize;
  end_index = gsizes[dim] - 1;

  if (end_index < st_index)
    local_size = 0;
  else {
    local_size = ((end_index - st_index + 1)/(nprocs*blksize))*blksize;
    rem = (end_index - st_index + 1) % (nprocs*blksize);
    local_size += (rem < blksize) ? rem : blksize;
  }

  count = local_size/blksize;
  rem = local_size % blksize;

  stride = nprocs*blksize*orig_extent;
  if (order == MPI_ORDER_FORTRAN)
    for (i=0; i<dim; i++) stride *= gsizes[i];
  else
    for (i=ndims-1; i>dim; i--) stride *= gsizes[i];

  ierr = MPI_Type_hvector(count, blksize, stride, type_old, type_new);
  if (ierr != MPI_SUCCESS) goto fn_exit;
  /* if the last block is of size less than blksize,
     include it separately using MPI_Type_struct */
  if (rem) {
    types[0] = *type_new;
    types[1] = type_old;
    disps[0] = 0;
    disps[1] = count*stride;
    blklens[0] = 1;
    blklens[1] = rem;
    ierr = MPI_Type_struct(2, blklens, disps, types, &type_tmp);
    if (ierr != MPI_SUCCESS) goto fn_exit;
    ierr = MPI_Type_free(type_new);
    if (ierr != MPI_SUCCESS) goto fn_exit;
    *type_new = type_tmp;
  }
  /* In the first iteration, we need to set the
     displacement in that dimension correctly. */
  if ( ((order == MPI_ORDER_FORTRAN) && (dim == 0)) ||
       ((order == MPI_ORDER_C) && (dim == ndims-1)) )
    {
      types[0] = MPI_LB;
      disps[0] = 0;
      types[1] = *type_new;
      disps[1] = rank * blksize * orig_extent;
      types[2] = MPI_UB;
      disps[2] = orig_extent * gsizes[dim];
      blklens[0] = blklens[1] = blklens[2] = 1;
      ierr = MPI_Type_struct(3, blklens, disps, types, &type_tmp);
      if (ierr != MPI_SUCCESS) goto fn_exit;
      ierr = MPI_Type_free(type_new);
      if (ierr != MPI_SUCCESS) goto fn_exit;
      *type_new = type_tmp;
      *offset = 0;
    } else {
    *offset = rank * blksize;
  }

  if (local_size == 0) *offset = 0;

  ierr = MPI_SUCCESS;
 fn_exit:
  return ierr;
}
static int PyMPI_Type_create_darray(int size,
                                    int rank,
                                    int ndims,
                                    int gsizes[],
                                    int distribs[],
                                    int dargs[],
                                    int psizes[],
                                    int order,
                                    MPI_Datatype oldtype,
                                    MPI_Datatype *newtype)
{
  int ierr = MPI_SUCCESS, i;
  int procs, tmp_rank, tmp_size, blklens[3];
  MPI_Aint orig_extent, disps[3];
  MPI_Datatype type_old, type_new, types[3];

  int      *coords  = 0;
  MPI_Aint *offsets = 0;

  orig_extent=0;
  type_old = type_new = MPI_DATATYPE_NULL;
  types[0] = types[1] = types[2] = MPI_DATATYPE_NULL;

  ierr = MPI_Type_extent(oldtype, &orig_extent);
  if (ierr != MPI_SUCCESS) goto fn_exit;

  PyMPI_CHKARG(rank >= 0);
  PyMPI_CHKARG(size > 0);
  PyMPI_CHKARG(ndims > 0);
  PyMPI_CHKARG(gsizes);
  PyMPI_CHKARG(distribs);
  PyMPI_CHKARG(dargs);
  PyMPI_CHKARG(psizes);
  PyMPI_CHKARG((order==MPI_ORDER_C) ||
               (order==MPI_ORDER_FORTRAN) );
  for (i=0; i < ndims; i++) {
    PyMPI_CHKARG(gsizes[1] > 0);
    PyMPI_CHKARG(psizes[1] > 0);
    PyMPI_CHKARG((distribs[i] == MPI_DISTRIBUTE_NONE)  ||
                 (distribs[i] == MPI_DISTRIBUTE_BLOCK) ||
                 (distribs[i] == MPI_DISTRIBUTE_CYCLIC));
    PyMPI_CHKARG((dargs[i] == MPI_DISTRIBUTE_DFLT_DARG) ||
                 (dargs[i] > 0));
    PyMPI_CHKARG(!((distribs[i] == MPI_DISTRIBUTE_NONE) &&
                   (psizes[i] != 1)));
  }

  /* calculate position in Cartesian grid
     as MPI would (row-major ordering) */
  coords  = (int *) PyMPI_MALLOC((size_t)ndims*sizeof(int));
  if (!coords)  { ierr = MPI_ERR_INTERN; goto fn_exit; }
  offsets = (MPI_Aint *) PyMPI_MALLOC((size_t)ndims*sizeof(MPI_Aint));
  if (!offsets) { ierr = MPI_ERR_INTERN; goto fn_exit; }

  procs = size;
  tmp_rank = rank;
  for (i=0; i<ndims; i++) {
    procs = procs/psizes[i];
    coords[i] = tmp_rank/procs;
    tmp_rank = tmp_rank % procs;
  }

  type_old = oldtype;

  if (order == MPI_ORDER_FORTRAN) {
    /* dimension 0 changes fastest */
    for (i=0; i<ndims; i++) {
      if (distribs[i] == MPI_DISTRIBUTE_BLOCK) {
        ierr = PyMPI_Type_block(gsizes, i, ndims,
                                psizes[i], coords[i], dargs[i],
                                order, orig_extent,
                                type_old,  &type_new,
                                offsets+i);
        if (ierr != MPI_SUCCESS) goto fn_exit;
      } else if (distribs[i] == MPI_DISTRIBUTE_CYCLIC) {
        ierr = PyMPI_Type_cyclic(gsizes, i, ndims,
                                 psizes[i], coords[i], dargs[i],
                                 order, orig_extent,
                                 type_old, &type_new,
                                 offsets+i);
        if (ierr != MPI_SUCCESS) goto fn_exit;
      } else if (distribs[i] == MPI_DISTRIBUTE_NONE) {
        /* treat it as a block distribution on 1 process */
        ierr = PyMPI_Type_block(gsizes, i, ndims,
                                1, 0, MPI_DISTRIBUTE_DFLT_DARG,
                                order, orig_extent,
                                type_old, &type_new,
                                offsets+i);
        if (ierr != MPI_SUCCESS) goto fn_exit;
      }
      if (i != 0) {
        ierr = MPI_Type_free(&type_old);
        if (ierr != MPI_SUCCESS) goto fn_exit;
      }
      type_old = type_new;
    }
    /* add displacement and UB */
    disps[1] = offsets[0];
    tmp_size = 1;
    for (i=1; i<ndims; i++) {
      tmp_size *= gsizes[i-1];
      disps[1] += tmp_size*offsets[i];
    }
    /* rest done below for both Fortran and C order */
  } else /* order == MPI_ORDER_C */ {
    /* dimension ndims-1 changes fastest */
    for (i=ndims-1; i>=0; i--) {
      if (distribs[i] == MPI_DISTRIBUTE_BLOCK) {
        ierr = PyMPI_Type_block(gsizes, i, ndims,
                                psizes[i], coords[i], dargs[i],
                                order, orig_extent,
                                type_old, &type_new,
                                offsets+i);
        if (ierr != MPI_SUCCESS) goto fn_exit;
      } else if (distribs[i] == MPI_DISTRIBUTE_CYCLIC) {
        ierr = PyMPI_Type_cyclic(gsizes, i, ndims,
                                 psizes[i], coords[i], dargs[i],
                                 order,  orig_extent,
                                 type_old, &type_new,
                                 offsets+i);
        if (ierr != MPI_SUCCESS) goto fn_exit;
      } else if (distribs[i] == MPI_DISTRIBUTE_NONE) {
        /* treat it as a block distribution on 1 process */
        ierr = PyMPI_Type_block(gsizes, i, ndims,
                                psizes[i], coords[i],
                                MPI_DISTRIBUTE_DFLT_DARG,
                                order, orig_extent,
                                type_old, &type_new,
                                offsets+i);
        if (ierr != MPI_SUCCESS) goto fn_exit;
      }
      if (i != ndims-1) {
        ierr = MPI_Type_free(&type_old);
        if (ierr != MPI_SUCCESS) goto fn_exit;
      }
      type_old = type_new;
    }
    /* add displacement and UB */
    disps[1] = offsets[ndims-1];
    tmp_size = 1;
    for (i=ndims-2; i>=0; i--) {
      tmp_size *= gsizes[i+1];
      disps[1] += tmp_size*offsets[i];
    }
    /* rest done below for both Fortran and C order */
  }

  disps[0] = 0;
  disps[1] *= orig_extent;
  disps[2] = orig_extent;
  for (i=0; i<ndims; i++) disps[2] *= gsizes[i];
  blklens[0] = blklens[1] = blklens[2] = 1;
  types[0] = MPI_LB;
  types[1] = type_new;
  types[2] = MPI_UB;
  ierr = MPI_Type_struct(3, blklens, disps, types, newtype);
  if (ierr != MPI_SUCCESS) goto fn_exit;
  ierr = MPI_Type_free(&type_new);
  if (ierr != MPI_SUCCESS) goto fn_exit;

  ierr = MPI_SUCCESS;
 fn_exit:
  if (coords)  PyMPI_FREE(coords);
  if (offsets) PyMPI_FREE(offsets);
  return ierr;
}

#undef PyMPI_CHKARG

#undef  MPI_Type_create_darray
#define MPI_Type_create_darray PyMPI_Type_create_darray
#endif

#ifndef PyMPI_HAVE_MPI_Type_size_x
static int PyMPI_Type_size_x(MPI_Datatype datatype,
                             MPI_Count *size)
{
  int ierr = MPI_SUCCESS;
  int size_ = MPI_UNDEFINED;
  ierr = MPI_Type_size(datatype, &size_);
  if (ierr != MPI_SUCCESS) return ierr;
  *size = (MPI_Count) size_;
  return MPI_SUCCESS;
}
#undef  MPI_Type_size_x
#define MPI_Type_size_x PyMPI_Type_size_x
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_extent_x
static int PyMPI_Type_get_extent_x(MPI_Datatype datatype,
                                   MPI_Count *lb,
                                   MPI_Count *extent)
{
  int ierr = MPI_SUCCESS;
  MPI_Aint lb_ = MPI_UNDEFINED, extent_ = MPI_UNDEFINED;
  ierr = MPI_Type_get_extent(datatype, &lb_, &extent_);
  if (ierr != MPI_SUCCESS) return ierr;
  *lb     = (MPI_Count) lb_;
  *extent = (MPI_Count) extent_;
  return MPI_SUCCESS;
}
#undef  MPI_Type_get_extent_x
#define MPI_Type_get_extent_x PyMPI_Type_get_extent_x
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_true_extent_x
static int PyMPI_Type_get_true_extent_x(MPI_Datatype datatype,
                                        MPI_Count *lb,
                                        MPI_Count *extent)
{
  int ierr = MPI_SUCCESS;
  MPI_Aint lb_ = MPI_UNDEFINED, extent_ = MPI_UNDEFINED;
  ierr = MPI_Type_get_true_extent(datatype, &lb_, &extent_);
  if (ierr != MPI_SUCCESS) return ierr;
  *lb     = (MPI_Count) lb_;
  *extent = (MPI_Count) extent_;
  return MPI_SUCCESS;
}
#undef  MPI_Type_get_true_extent_x
#define MPI_Type_get_true_extent_x PyMPI_Type_get_true_extent_x
#endif

#ifndef PyMPI_HAVE_MPI_Get_elements_x
static int PyMPI_Get_elements_x(MPI_Status *status,
                                MPI_Datatype datatype,
                                MPI_Count *elements)
{
  int ierr = MPI_SUCCESS;
  int elements_ = MPI_UNDEFINED;
  ierr = MPI_Get_elements(status, datatype, &elements_);
  if (ierr != MPI_SUCCESS) return ierr;
  *elements = (MPI_Count) elements_;
  return MPI_SUCCESS;
}
#undef  MPI_Get_elements_x
#define MPI_Get_elements_x PyMPI_Get_elements_x
#endif

#ifndef PyMPI_HAVE_MPI_Status_set_elements_x
static int PyMPI_Status_set_elements_x(MPI_Status *status,
                                       MPI_Datatype datatype,
                                       MPI_Count elements)
{
  int elements_ = (int) elements;
  if (elements != (MPI_Count) elements_) return MPI_ERR_ARG; /* XXX */
  return MPI_Status_set_elements(status, datatype, elements_);
}
#undef  MPI_Status_set_elements_x
#define MPI_Status_set_elements_x PyMPI_Status_set_elements_x
#endif

#ifndef PyMPI_HAVE_MPI_Aint_add
static MPI_Aint PyMPI_Aint_add(MPI_Aint base, MPI_Aint disp)
{
  return (MPI_Aint) ((char*)base + disp);
}
#undef  MPI_Aint_add
#define MPI_Aint_add PyMPI_Aint_add
#endif

#ifndef PyMPI_HAVE_MPI_Aint_diff
static MPI_Aint PyMPI_Aint_diff(MPI_Aint addr1, MPI_Aint addr2)
{
  return (MPI_Aint) ((char*)addr1 - (char*)addr2);
}
#undef  MPI_Aint_diff
#define MPI_Aint_diff PyMPI_Aint_diff
#endif

/* ---------------------------------------------------------------- */

#ifdef PyMPI_HAVE_MPI_Request_get_status
#if ((10 * MPI_VERSION + MPI_SUBVERSION) < 22)
static int PyMPI_Request_get_status(MPI_Request request,
                                    int *flag, MPI_Status *status)
{
  if (request != MPI_REQUEST_NULL || !flag)
    return MPI_Request_get_status(request, flag, status);
  *flag = 1;
  if (status &&
      status != MPI_STATUS_IGNORE &&
      status != MPI_STATUSES_IGNORE) {
    #if !defined(PyMPI_HAVE_MPI_Status_set_cancelled) || \
        !defined(PyMPI_HAVE_MPI_Status_set_elements)
    int n = (int) sizeof(MPI_Status);
    unsigned char *p = (unsigned char *)status;
    while (n-- > 0) p[n] = 0;
    #endif
    status->MPI_SOURCE = MPI_ANY_SOURCE;
    status->MPI_TAG    = MPI_ANY_TAG;
    status->MPI_ERROR  = MPI_SUCCESS;
    #ifdef PyMPI_HAVE_MPI_Status_set_elements
    (void)MPI_Status_set_elements(status, MPI_BYTE, 0);
    #endif
    #ifdef PyMPI_HAVE_MPI_Status_set_cancelled
    (void)MPI_Status_set_cancelled(status, 0);
    #endif
  }
  return MPI_SUCCESS;
}
#undef  MPI_Request_get_status
#define MPI_Request_get_status PyMPI_Request_get_status
#endif
#endif

/* ---------------------------------------------------------------- */

#ifndef PyMPI_HAVE_MPI_Reduce_scatter_block
static int PyMPI_Reduce_scatter_block(void *sendbuf, void *recvbuf,
                                      int recvcount, MPI_Datatype datatype,
                                      MPI_Op op, MPI_Comm comm)
{
  int ierr = MPI_SUCCESS;
  int n = 1, *recvcounts = 0;
  ierr = MPI_Comm_size(comm, &n);
  if (ierr != MPI_SUCCESS) return ierr;
  recvcounts = (int *) PyMPI_MALLOC((size_t)n*sizeof(int));
  if (!recvcounts) return MPI_ERR_INTERN;
  while (n-- > 0) recvcounts[n] = recvcount;
  ierr = MPI_Reduce_scatter(sendbuf, recvbuf,
                            recvcounts, datatype,
                            op, comm);
  PyMPI_FREE(recvcounts);
  return ierr;
}
#undef  MPI_Reduce_scatter_block
#define MPI_Reduce_scatter_block PyMPI_Reduce_scatter_block
#endif

/* ---------------------------------------------------------------- */

/* Communicator Info */

#ifndef PyMPI_HAVE_MPI_Comm_dup_with_info
static int PyMPI_Comm_dup_with_info(MPI_Comm comm, MPI_Info info,
                                    MPI_Comm *newcomm)
{
  int dummy, ierr;
  if (info != MPI_INFO_NULL) {
    ierr = MPI_Info_get_nkeys(info, &dummy);
    if (ierr != MPI_SUCCESS) return ierr;
  }
  return MPI_Comm_dup(comm, newcomm);
}
#undef  MPI_Comm_dup_with_info
#define MPI_Comm_dup_with_info PyMPI_Comm_dup_with_info
#endif

#ifndef PyMPI_HAVE_MPI_Comm_set_info
static int PyMPI_Comm_set_info(MPI_Comm comm, MPI_Info info)
{
  int dummy, ierr;
  ierr = MPI_Comm_size(comm, &dummy);
  if (ierr != MPI_SUCCESS) return ierr;
  if (info != MPI_INFO_NULL) {
    ierr = MPI_Info_get_nkeys(info, &dummy);
    if (ierr != MPI_SUCCESS) return ierr;
  }
  return MPI_SUCCESS;
}
#undef  MPI_Comm_set_info
#define MPI_Comm_set_info PyMPI_Comm_set_info
#endif

#ifndef PyMPI_HAVE_MPI_Comm_get_info
static int PyMPI_Comm_get_info(MPI_Comm comm, MPI_Info *info)
{
  int dummy, ierr;
  ierr = MPI_Comm_size(comm, &dummy);
  if (ierr != MPI_SUCCESS) return ierr;
  return MPI_Info_create(info);
}
#undef  MPI_Comm_get_info
#define MPI_Comm_get_info PyMPI_Comm_get_info
#endif

/* ---------------------------------------------------------------- */

#if !defined(PyMPI_HAVE_MPI_WEIGHTS_EMPTY)
static const int PyMPI_WEIGHTS_EMPTY_ARRAY[1] = {MPI_UNDEFINED};
static int * const PyMPI_WEIGHTS_EMPTY = (int*)PyMPI_WEIGHTS_EMPTY_ARRAY;
#undef  MPI_WEIGHTS_EMPTY
#define MPI_WEIGHTS_EMPTY PyMPI_WEIGHTS_EMPTY
#endif

/* ---------------------------------------------------------------- */

/* Memory Allocation */

#if !defined(PyMPI_HAVE_MPI_Alloc_mem) || \
    !defined(PyMPI_HAVE_MPI_Free_mem)

static int PyMPI_Alloc_mem(MPI_Aint size, MPI_Info info, void *baseptr)
{
  char *buf = 0, **basebuf = 0;
  if (size < 0) return MPI_ERR_ARG;
  if (!baseptr) return MPI_ERR_ARG;
  if (size == 0) size = 1;
  buf = (char *) PyMPI_MALLOC((size_t)size);
  if (!buf) return MPI_ERR_NO_MEM;
  basebuf = (char **) baseptr;
  *basebuf = buf;
  return MPI_SUCCESS;
}
#undef  MPI_Alloc_mem
#define MPI_Alloc_mem PyMPI_Alloc_mem

static int PyMPI_Free_mem(void *baseptr)
{
  if (!baseptr) return MPI_ERR_ARG;
  PyMPI_FREE(baseptr);
  return MPI_SUCCESS;
}
#undef  MPI_Free_mem
#define MPI_Free_mem PyMPI_Free_mem

#endif

/* ---------------------------------------------------------------- */

#ifndef PyMPI_HAVE_MPI_Win_allocate
#ifdef  PyMPI_HAVE_MPI_Win_create

static int PyMPI_WIN_KEYVAL_MPIMEM = MPI_KEYVAL_INVALID;

static int MPIAPI
PyMPI_win_free_mpimem(MPI_Win win, int k, void *v, void *xs)
{
  (void)win; (void)k; (void)xs; /* unused */
  return MPI_Free_mem(v);
}

static int MPIAPI
PyMPI_win_free_keyval(MPI_Comm comm, int k, void *v, void *xs)
{
  int ierr = MPI_SUCCESS;
  (void)comm; (void)xs; /* unused */
  ierr = MPI_Win_free_keyval((int *)v);
  if (ierr != MPI_SUCCESS) return ierr;
  ierr = MPI_Comm_free_keyval(&k);
  if (ierr != MPI_SUCCESS) return ierr;
  return MPI_SUCCESS;
}

static int PyMPI_Win_allocate(MPI_Aint size, int disp_unit,
                              MPI_Info info, MPI_Comm comm,
                              void *baseptr_, MPI_Win *win_)
{
  int ierr = MPI_SUCCESS;
  void *baseptr = MPI_BOTTOM;
  MPI_Win win = MPI_WIN_NULL;
  if (!baseptr_) return MPI_ERR_ARG;
  if (!win_)     return MPI_ERR_ARG;
  ierr = MPI_Alloc_mem(size?size:1, info, &baseptr);
  if (ierr != MPI_SUCCESS) goto error;
  ierr = MPI_Win_create(baseptr, size, disp_unit, info, comm, &win);
  if (ierr != MPI_SUCCESS) goto error;
#if defined(PyMPI_HAVE_MPI_Win_create_keyval) && \
    defined(PyMPI_HAVE_MPI_Win_set_attr)
  if (PyMPI_WIN_KEYVAL_MPIMEM == MPI_KEYVAL_INVALID) {
    int comm_keyval = MPI_KEYVAL_INVALID;
    ierr = MPI_Win_create_keyval(MPI_WIN_NULL_COPY_FN,
                                 PyMPI_win_free_mpimem,
                                 &PyMPI_WIN_KEYVAL_MPIMEM, NULL);
    if (ierr != MPI_SUCCESS) goto error;
    ierr = MPI_Comm_create_keyval(MPI_COMM_NULL_COPY_FN,
                                  PyMPI_win_free_keyval,
                                  &comm_keyval, NULL);
    if (ierr == MPI_SUCCESS)
      (void)MPI_Comm_set_attr(MPI_COMM_SELF, comm_keyval,
                              &PyMPI_WIN_KEYVAL_MPIMEM);
  }
  ierr = MPI_Win_set_attr(win, PyMPI_WIN_KEYVAL_MPIMEM, baseptr);
  if (ierr != MPI_SUCCESS) goto error;
#endif
  *((void**)baseptr_) = baseptr;
  *win_ = win;
  return MPI_SUCCESS;
 error:
  if (baseptr != MPI_BOTTOM) (void)MPI_Free_mem(baseptr);
  if (win != MPI_WIN_NULL)   (void)MPI_Win_free(&win);
  return ierr;
}
#undef  MPI_Win_allocate
#define MPI_Win_allocate PyMPI_Win_allocate

#endif
#endif

#ifndef PyMPI_HAVE_MPI_Win_set_info
static int PyMPI_Win_set_info(MPI_Win win, MPI_Info info)
{
  int dummy, ierr;
  if (win == MPI_WIN_NULL) return MPI_ERR_WIN;
  if (info != MPI_INFO_NULL) {
    ierr = MPI_Info_get_nkeys(info, &dummy);
    if (ierr != MPI_SUCCESS) return ierr;
  }
  return MPI_SUCCESS;
}
#undef  MPI_Win_set_info
#define MPI_Win_set_info PyMPI_Win_set_info
#endif

#ifndef PyMPI_HAVE_MPI_Win_get_info
static int PyMPI_Win_get_info(MPI_Win win, MPI_Info *info)
{
  if (win == MPI_WIN_NULL) return MPI_ERR_WIN;
  return MPI_Info_create(info);
}
#undef  MPI_Win_get_info
#define MPI_Win_get_info PyMPI_Win_get_info
#endif

/* ---------------------------------------------------------------- */

#endif /* !PyMPI_FALLBACK_H */

/*
  Local variables:
  c-basic-offset: 2
  indent-tabs-mode: nil
  End:
*/
