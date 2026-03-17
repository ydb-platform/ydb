/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stddef.h>

#include "ompi/datatype/ompi_datatype.h"

static int
block(const int *gsize_array, int dim, int ndims, int nprocs,
      int rank, int darg, int order, ptrdiff_t orig_extent,
      ompi_datatype_t *type_old, ompi_datatype_t **type_new,
      ptrdiff_t *st_offset)
{
    int blksize, global_size, mysize, i, j, rc, start_loop, step;
    ptrdiff_t stride, disps[2];

    global_size = gsize_array[dim];

    if (darg == MPI_DISTRIBUTE_DFLT_DARG)
        blksize = (global_size + nprocs - 1) / nprocs;
    else {
        blksize = darg;
    }

    j = global_size - blksize*rank;
    mysize = blksize < j ? blksize : j;
    if (mysize < 0) mysize = 0;

    if (MPI_ORDER_C == order) {
        start_loop = ndims - 1 ; step = -1;
    } else {
        start_loop = 0 ; step = 1;
    }

    stride = orig_extent;
    if (dim == start_loop) {
        rc = ompi_datatype_create_contiguous(mysize, type_old, type_new);
        if (OMPI_SUCCESS != rc) return rc;
    } else {
        for (i = start_loop ; i != dim ; i += step) {
            stride *= gsize_array[i];
        }
        rc = ompi_datatype_create_hvector(mysize, 1, stride, type_old, type_new);
        if (OMPI_SUCCESS != rc) return rc;
    }

    *st_offset = blksize * rank;
    /* in terms of no. of elements of type oldtype in this dimension */
    if (mysize == 0) *st_offset = 0;

    /* need to set the UB for block-cyclic to work */
    disps[0] = 0;         disps[1] = orig_extent;
    if (order == MPI_ORDER_FORTRAN) {
        for(i=0; i<=dim; i++) {
            disps[1] *= gsize_array[i];
        }
    } else {
        for(i=ndims-1; i>=dim; i--) {
            disps[1] *= gsize_array[i];
        }
    }
    rc = opal_datatype_resize( &(*type_new)->super, disps[0], disps[1] );
    if (OMPI_SUCCESS != rc) return rc;

    return OMPI_SUCCESS;
}


static int
cyclic(const int *gsize_array, int dim, int ndims, int nprocs,
       int rank, int darg, int order, ptrdiff_t orig_extent,
       ompi_datatype_t* type_old, ompi_datatype_t **type_new,
       ptrdiff_t *st_offset)
{
    int blksize, i, blklens[2], st_index, end_index, local_size, rem, count, rc;
    ptrdiff_t stride, disps[2];
    ompi_datatype_t *type_tmp, *types[2];

    if (darg == MPI_DISTRIBUTE_DFLT_DARG) {
        blksize = 1;
    } else {
        blksize = darg;
    }

    st_index = rank * blksize;
    end_index = gsize_array[dim] - 1;

    if (end_index < st_index) {
        local_size = 0;
    } else {
        local_size = ((end_index - st_index + 1)/(nprocs*blksize))*blksize;
        rem = (end_index - st_index + 1) % (nprocs*blksize);
        local_size += rem < blksize ? rem : blksize;
    }

    count = local_size / blksize;
    rem = local_size % blksize;

    stride = nprocs*blksize*orig_extent;
    if (order == MPI_ORDER_FORTRAN) {
        for (i=0; i<dim; i++) {
            stride *= gsize_array[i];
        }
    } else {
        for (i=ndims-1; i>dim; i--) {
            stride *= gsize_array[i];
        }
    }

    rc = ompi_datatype_create_hvector(count, blksize, stride, type_old, type_new);
    if (OMPI_SUCCESS != rc) return rc;

    if (rem) {
        /* if the last block is of size less than blksize, include
           it separately using MPI_Type_struct */

        types  [0] = *type_new; types  [1] = type_old;
        disps  [0] = 0;         disps  [1] = count*stride;
        blklens[0] = 1;         blklens[1] = rem;

        rc = ompi_datatype_create_struct(2, blklens, disps, types, &type_tmp);
        ompi_datatype_destroy(type_new);
        /* even in error condition, need to destroy type_new, so check
           for error after destroy. */
        if (OMPI_SUCCESS != rc) return rc;
        *type_new = type_tmp;
    }

    /* need to set the UB for block-cyclic to work */
    disps[0] = 0;         disps[1] = orig_extent;
    if (order == MPI_ORDER_FORTRAN) {
        for(i=0; i<=dim; i++) {
            disps[1] *= gsize_array[i];
        }
    } else {
        for(i=ndims-1; i>=dim; i--) {
            disps[1] *= gsize_array[i];
        }
    }
    rc = opal_datatype_resize( &(*type_new)->super, disps[0], disps[1] );
    if (OMPI_SUCCESS != rc) return rc;

    *st_offset = rank * blksize;
    /* in terms of no. of elements of type oldtype in this dimension */
    if (local_size == 0) *st_offset = 0;

    return OMPI_SUCCESS;
}

int32_t ompi_datatype_create_darray(int size,
                                    int rank,
                                    int ndims,
                                    int const* gsize_array,
                                    int const* distrib_array,
                                    int const* darg_array,
                                    int const* psize_array,
                                    int order,
                                    const ompi_datatype_t* oldtype,
                                    ompi_datatype_t** newtype)
{
    ompi_datatype_t *lastType;
    ptrdiff_t orig_extent, *st_offsets = NULL;
    int i, start_loop, end_loop, step;
    int *coords = NULL, rc = OMPI_SUCCESS;
    ptrdiff_t displs[2], tmp_size = 1;

    /* speedy corner case */
    if (ndims < 1) {
        /* Don't just return MPI_DATATYPE_NULL as that can't be
           MPI_TYPE_FREE()ed, and that seems bad */
        *newtype = ompi_datatype_create(0);
        ompi_datatype_add(*newtype, &ompi_mpi_datatype_null.dt, 0, 0, 0);
        return MPI_SUCCESS;
    }

    rc = ompi_datatype_type_extent(oldtype, &orig_extent);
    if (MPI_SUCCESS != rc) goto cleanup;

    /* calculate position in grid using row-major ordering */
    {
        int tmp_rank = rank, procs = size;

        coords = (int *) malloc(ndims * sizeof(int));
        displs[1] = orig_extent;
        for (i = 0 ; i < ndims ; i++) {
            procs = procs / psize_array[i];
            coords[i] = tmp_rank / procs;
            tmp_rank = tmp_rank % procs;
            /* compute the upper bound of the datatype, including all dimensions */
            displs[1] *= gsize_array[i];
        }
    }

    st_offsets = (ptrdiff_t *) malloc(ndims * sizeof(ptrdiff_t));

    /* duplicate type to here to 1) deal with constness without
       casting and 2) eliminate need to for conditional destroy below.
       Lame, yes.  But cleaner code all around. */
    rc = ompi_datatype_duplicate(oldtype, &lastType);
    if (OMPI_SUCCESS != rc) goto cleanup;

    /* figure out ordering issues */
    if (MPI_ORDER_C == order) {
        start_loop = ndims - 1 ; step = -1; end_loop = -1;
    } else {
        start_loop = 0 ; step = 1; end_loop = ndims;
    }

    /* Build up array */
    for (i = start_loop; i != end_loop; i += step) {
        int nprocs, tmp_rank;

        switch(distrib_array[i]) {
        case MPI_DISTRIBUTE_BLOCK:
            rc = block(gsize_array, i, ndims, psize_array[i], coords[i],
                       darg_array[i], order, orig_extent,
                       lastType, newtype, st_offsets+i);
            break;
        case MPI_DISTRIBUTE_CYCLIC:
            rc = cyclic(gsize_array, i, ndims, psize_array[i], coords[i],
                        darg_array[i], order, orig_extent,
                        lastType, newtype, st_offsets+i);
            break;
        case MPI_DISTRIBUTE_NONE:
            /* treat it as a block distribution on 1 process */
            if (order == MPI_ORDER_C) {
                nprocs = psize_array[i]; tmp_rank = coords[i];
            } else {
                nprocs = 1; tmp_rank = 0;
            }

            rc = block(gsize_array, i, ndims, nprocs, tmp_rank,
                       MPI_DISTRIBUTE_DFLT_DARG, order, orig_extent,
                       lastType, newtype, st_offsets+i);
            break;
        default:
            rc = MPI_ERR_ARG;
        }
        ompi_datatype_destroy(&lastType);
        /* need to destroy the old type even in error condition, so
           don't check return code from above until after cleanup. */
        if (MPI_SUCCESS != rc) goto cleanup;
        lastType = *newtype;
    }

    /**
     * We need to shift the content (useful data) of the datatype, so
     * we need to force the displacement to be moved. Therefore, we
     * cannot use resize as it will only set the soft lb and ub
     * markers without moving the data. Instead, we have to create a
     * new data, and insert the last_Type with the correct
     * displacement.
     */
    displs[0] = st_offsets[start_loop];
    for (i = start_loop + step; i != end_loop; i += step) {
        tmp_size *= gsize_array[i - step];
        displs[0] += tmp_size * st_offsets[i];
    }
    displs[0] *= orig_extent;

    *newtype = ompi_datatype_create(lastType->super.desc.used);
    rc = ompi_datatype_add(*newtype, lastType, 1, displs[0], displs[1]);
    ompi_datatype_destroy(&lastType);
    /* need to destroy the old type even in error condition, so
       don't check return code from above until after cleanup. */
    if (MPI_SUCCESS != rc) {
        ompi_datatype_destroy (newtype);
    } else {
        (void) opal_datatype_resize( &(*newtype)->super, 0, displs[1]);
    }

 cleanup:
    free(st_offsets);
    free(coords);
    return rc;
}
