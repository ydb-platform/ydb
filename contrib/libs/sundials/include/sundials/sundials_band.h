/* -----------------------------------------------------------------
 * Programmer(s): Alan C. Hindmarsh and Radu Serban @ LLNL
 * -----------------------------------------------------------------
 * SUNDIALS Copyright Start
 * Copyright (c) 2002-2019, Lawrence Livermore National Security
 * and Southern Methodist University.
 * All rights reserved.
 *
 * See the top-level LICENSE and NOTICE files for details.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * SUNDIALS Copyright End
 * -----------------------------------------------------------------
 * This is the header file for a generic BAND linear solver
 * package, based on the DlsMat type defined in sundials_direct.h.
 *
 * There are two sets of band solver routines listed in
 * this file: one set uses type DlsMat defined below and the
 * other set uses the type realtype ** for band matrix arguments.
 * Routines that work with the type DlsMat begin with "Band".
 * Routines that work with realtype ** begin with "band".
 * -----------------------------------------------------------------*/

#ifndef _SUNDIALS_BAND_H
#define _SUNDIALS_BAND_H

#include <sundials/sundials_direct.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/*
 * -----------------------------------------------------------------
 * Function : BandGBTRF
 * -----------------------------------------------------------------
 * Usage : ier = BandGBTRF(A, p);
 *         if (ier != 0) ... A is singular
 * -----------------------------------------------------------------
 * BandGBTRF performs the LU factorization of the N by N band
 * matrix A. This is done using standard Gaussian elimination
 * with partial pivoting.
 *
 * A successful LU factorization leaves the "matrix" A and the
 * pivot array p with the following information:
 *
 * (1) p[k] contains the row number of the pivot element chosen
 *     at the beginning of elimination step k, k=0, 1, ..., N-1.
 *
 * (2) If the unique LU factorization of A is given by PA = LU,
 *     where P is a permutation matrix, L is a lower triangular
 *     matrix with all 1's on the diagonal, and U is an upper
 *     triangular matrix, then the upper triangular part of A
 *     (including its diagonal) contains U and the strictly lower
 *     triangular part of A contains the multipliers, I-L.
 *
 * BandGBTRF returns 0 if successful. Otherwise it encountered
 * a zero diagonal element during the factorization. In this case
 * it returns the column index (numbered from one) at which
 * it encountered the zero.
 *
 * Important Note: A must be allocated to accommodate the increase
 * in upper bandwidth that occurs during factorization. If
 * mathematically, A is a band matrix with upper bandwidth mu and
 * lower bandwidth ml, then the upper triangular factor U can
 * have upper bandwidth as big as smu = MIN(n-1,mu+ml). The lower
 * triangular factor L has lower bandwidth ml. Allocate A with
 * call A = BandAllocMat(N,mu,ml,smu), where mu, ml, and smu are
 * as defined above. The user does not have to zero the "extra"
 * storage allocated for the purpose of factorization. This will
 * handled by the BandGBTRF routine.
 *
 * BandGBTRF is only a wrapper around bandGBTRF. All work is done
 * in bandGBTRF, which works directly on the data in the DlsMat A
 * (i.e. in the field A->cols).
 * -----------------------------------------------------------------
 */

SUNDIALS_EXPORT sunindextype BandGBTRF(DlsMat A, sunindextype *p);
SUNDIALS_EXPORT sunindextype bandGBTRF(realtype **a, sunindextype n,
                                       sunindextype mu, sunindextype ml,
                                       sunindextype smu, sunindextype *p);

/*
 * -----------------------------------------------------------------
 * Function : BandGBTRS
 * -----------------------------------------------------------------
 * Usage : BandGBTRS(A, p, b);
 * -----------------------------------------------------------------
 * BandGBTRS solves the N-dimensional system A x = b using
 * the LU factorization in A and the pivot information in p
 * computed in BandGBTRF. The solution x is returned in b. This
 * routine cannot fail if the corresponding call to BandGBTRF
 * did not fail.
 *
 * BandGBTRS is only a wrapper around bandGBTRS which does all the
 * work directly on the data in the DlsMat A (i.e. in A->cols).
 * -----------------------------------------------------------------
 */

SUNDIALS_EXPORT void BandGBTRS(DlsMat A, sunindextype *p, realtype *b);
SUNDIALS_EXPORT void bandGBTRS(realtype **a, sunindextype n, sunindextype smu,
                               sunindextype ml, sunindextype *p, realtype *b);

/*
 * -----------------------------------------------------------------
 * Function : BandCopy
 * -----------------------------------------------------------------
 * Usage : BandCopy(A, B, copymu, copyml);
 * -----------------------------------------------------------------
 * BandCopy copies the submatrix with upper and lower bandwidths
 * copymu, copyml of the N by N band matrix A into the N by N
 * band matrix B.
 *
 * BandCopy is a wrapper around bandCopy which accesses the data
 * in the DlsMat A and DlsMat B (i.e. the fields cols).
 * -----------------------------------------------------------------
 */

SUNDIALS_EXPORT void BandCopy(DlsMat A, DlsMat B, sunindextype copymu,
                              sunindextype copyml);
SUNDIALS_EXPORT void bandCopy(realtype **a, realtype **b, sunindextype n,
                              sunindextype a_smu, sunindextype b_smu,
                              sunindextype copymu, sunindextype copyml);

/*
 * -----------------------------------------------------------------
 * Function: BandScale
 * -----------------------------------------------------------------
 * Usage : BandScale(c, A);
 * -----------------------------------------------------------------
 * A(i,j) <- c*A(i,j),   j-(A->mu) <= i <= j+(A->ml).
 *
 * BandScale is a wrapper around bandScale which performs the actual
 * scaling by accessing the data in the DlsMat A (i.e. the field
 * A->cols).
 * -----------------------------------------------------------------
 */

SUNDIALS_EXPORT void BandScale(realtype c, DlsMat A);
SUNDIALS_EXPORT void bandScale(realtype c, realtype **a, sunindextype n,
                               sunindextype mu, sunindextype ml,
                               sunindextype smu);

/*
 * -----------------------------------------------------------------
 * Function: bandAddIdentity
 * -----------------------------------------------------------------
 * bandAddIdentity adds the identity matrix to the n-by-n matrix
 * stored in the realtype** arrays.
 * -----------------------------------------------------------------
 */

SUNDIALS_EXPORT void bandAddIdentity(realtype **a, sunindextype n,
                                     sunindextype smu);


/*
 * -----------------------------------------------------------------
 * Function: BandMatvec
 * -----------------------------------------------------------------
 * BandMatvec computes the matrix-vector product y = A*x, where A
 * is an M-by-N band matrix, x is a vector of length N, and y is a
 * vector of length M.  No error checking is performed on the length
 * of the arrays x and y.  Only y is modified in this routine.
 *
 * BandMatvec is a wrapper around bandMatvec which performs the
 * actual product by accessing the data in the DlsMat A.
 * -----------------------------------------------------------------
 */

SUNDIALS_EXPORT void BandMatvec(DlsMat A, realtype *x, realtype *y);
SUNDIALS_EXPORT void bandMatvec(realtype **a, realtype *x, realtype *y,
                                sunindextype n, sunindextype mu,
                                sunindextype ml, sunindextype smu);

#ifdef __cplusplus
}
#endif

#endif
