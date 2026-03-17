/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2014 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <math.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Dims_create = PMPI_Dims_create
#endif
#define MPI_Dims_create PMPI_Dims_create
#endif

static const char FUNC_NAME[] = "MPI_Dims_create";

/* static functions */
static int assignnodes(int ndim, int nfactor, int *pfacts,int **pdims);
static int getfactors(int num, int *nfators, int **factors);


/*
 * This is a utility function, no need to have anything in the lower
 * layer for this at all
 */
int MPI_Dims_create(int nnodes, int ndims, int dims[])
{
    int i;
    int freeprocs;
    int freedims;
    int nfactors;
    int *factors;
    int *procs;
    int *p;
    int err;

    OPAL_CR_NOOP_PROGRESS();

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (0 > ndims) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD,
                                           MPI_ERR_DIMS, FUNC_NAME);
        }

        if ((0 != ndims) && (NULL == dims)) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD,
                                           MPI_ERR_ARG, FUNC_NAME);
        }

        if (1 > nnodes) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD,
                                           MPI_ERR_DIMS, FUNC_NAME);
        }
    }

    /* Get # of free-to-be-assigned processes and # of free dimensions */
    freeprocs = nnodes;
    freedims = 0;
    for (i = 0, p = dims; i < ndims; ++i,++p) {
        if (*p == 0) {
            ++freedims;
        } else if ((*p < 0) || ((nnodes % *p) != 0)) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD, MPI_ERR_DIMS,
                                           FUNC_NAME);
        } else {
            freeprocs /= *p;
        }
    }

    if (freedims == 0) {
       if (freeprocs == 1) {
          return MPI_SUCCESS;
       }
       return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_DIMS,
                                     FUNC_NAME);
    }

    if (freeprocs == 1) {
        for (i = 0; i < ndims; ++i, ++dims) {
            if (*dims == 0) {
               *dims = 1;
            }
        }
        return MPI_SUCCESS;
    }

    /* Factor the number of free processes */
    if (MPI_SUCCESS != (err = getfactors(freeprocs, &nfactors, &factors))) {
       return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, err,
                                     FUNC_NAME);
    }

    /* Assign free processes to free dimensions */
    if (MPI_SUCCESS != (err = assignnodes(freedims, nfactors, factors, &procs))) {
       free(factors);
       return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, err,
                                     FUNC_NAME);
    }

    /* Return assignment results */
    p = procs;
    for (i = 0; i < ndims; ++i, ++dims) {
        if (*dims == 0) {
           *dims = *p++;
        }
    }

    free((char *) factors);
    free((char *) procs);

    /* all done */
    return MPI_SUCCESS;
}

/*
 *  assignnodes
 *
 *  Function:   - assign processes to dimensions
 *          - get "best-balanced" grid
 *          - greedy bin-packing algorithm used
 *          - sort dimensions in decreasing order
 *          - dimensions array dynamically allocated
 *  Accepts:    - # of dimensions
 *          - # of prime factors
 *          - array of prime factors
 *          - ptr to array of dimensions (returned value)
 *  Returns:    - 0 or ERROR
 */
static int
assignnodes(int ndim, int nfactor, int *pfacts, int **pdims)
{
    int *bins;
    int i, j;
    int n;
    int f;
    int *p;
    int *pmin;

    if (0 >= ndim) {
       return MPI_ERR_DIMS;
    }

    /* Allocate and initialize the bins */
    bins = (int *) malloc((unsigned) ndim * sizeof(int));
    if (NULL == bins) {
       return MPI_ERR_NO_MEM;
    }
    *pdims = bins;

    for (i = 0, p = bins; i < ndim; ++i, ++p) {
        *p = 1;
     }

    /* Loop assigning factors from the highest to the lowest */
    for (j = nfactor - 1; j >= 0; --j) {
        f = pfacts[j];
        /* Assign a factor to the smallest bin */
        pmin = bins;
        for (i = 1, p = pmin + 1; i < ndim; ++i, ++p) {
            if (*p < *pmin) {
                pmin = p;
            }
        }
        *pmin *= f;
     }

     /* Sort dimensions in decreasing order (O(n^2) for now) */
     for (i = 0, pmin = bins; i < ndim - 1; ++i, ++pmin) {
         for (j = i + 1, p = pmin + 1; j < ndim; ++j, ++p) {
             if (*p > *pmin) {
                n = *p;
                *p = *pmin;
                *pmin = n;
             }
         }
     }

     return MPI_SUCCESS;
}

/*
 *  getfactors
 *
 *  Function:   - factorize a number
 *  Accepts:    - number
 *          - # prime factors
 *          - array of prime factors
 *  Returns:    - MPI_SUCCESS or ERROR
 */
static int
getfactors(int num, int *nfactors, int **factors) {
    int size;
    int d;
    int i;
    int sqrtnum;

    if(num  < 2) {
        (*nfactors) = 0;
        (*factors) = NULL;
        return MPI_SUCCESS;
    }
    /* Allocate the array of prime factors which cannot exceed log_2(num) entries */
    sqrtnum = ceil(sqrt(num));
    size = ceil(log(num) / log(2));
    *factors = (int *) malloc((unsigned) size * sizeof(int));

    i = 0;
    /* determine all occurences of factor 2 */
    while((num % 2) == 0) {
        num /= 2;
        (*factors)[i++] = 2;
    }
    /* determine all occurences of uneven prime numbers up to sqrt(num) */
    d = 3;
    for(d = 3; (num > 1) && (d < sqrtnum); d += 2) {
        while((num % d) == 0) {
            num /= d;
            (*factors)[i++] = d;
        }
    }
    /* as we looped only up to sqrt(num) one factor > sqrt(num) may be left over */
    if(num != 1) {
        (*factors)[i++] = num;
    }
    (*nfactors) = i;
    return MPI_SUCCESS;
}
