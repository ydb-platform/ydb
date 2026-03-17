/*
 * -----------------------------------------------------------------
 * Programmer(s): Daniel Reynolds @ SMU
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
 * This is the implementation file for the band implementation of 
 * the SUNLINSOL package.
 * -----------------------------------------------------------------
 */ 

#include <stdio.h>
#include <stdlib.h>

#include <sunlinsol/sunlinsol_band.h>
#include <sundials/sundials_math.h>

#define ZERO  RCONST(0.0)
#define ONE   RCONST(1.0)
#define ROW(i,j,smu) (i-j+smu)


/* Private function prototypes */
sunindextype GlobalVectorLength_BandLS(N_Vector y);

/*
 * -----------------------------------------------------------------
 * Band solver structure accessibility macros: 
 * -----------------------------------------------------------------
 */

#define BAND_CONTENT(S)   ( (SUNLinearSolverContent_Band)(S->content) )
#define PIVOTS(S)         ( BAND_CONTENT(S)->pivots )
#define LASTFLAG(S)       ( BAND_CONTENT(S)->last_flag )

/*
 * -----------------------------------------------------------------
 * deprecated wrapper functions
 * -----------------------------------------------------------------
 */

SUNLinearSolver SUNBandLinearSolver(N_Vector y, SUNMatrix A)
{ return(SUNLinSol_Band(y, A)); }

/*
 * -----------------------------------------------------------------
 * exported functions
 * -----------------------------------------------------------------
 */

/* ----------------------------------------------------------------------------
 * Function to create a new band linear solver
 */

SUNLinearSolver SUNLinSol_Band(N_Vector y, SUNMatrix A)
{
  SUNLinearSolver S;
  SUNLinearSolver_Ops ops;
  SUNLinearSolverContent_Band content;
  sunindextype MatrixRows, VecLength;
  
  /* Check compatibility with supplied SUNMatrix and N_Vector */
  if (SUNMatGetID(A) != SUNMATRIX_BAND)
    return(NULL);
  if (SUNBandMatrix_Rows(A) != SUNBandMatrix_Columns(A))
    return(NULL);
  MatrixRows = SUNBandMatrix_Rows(A);
  if ( (N_VGetVectorID(y) != SUNDIALS_NVEC_SERIAL) &&
       (N_VGetVectorID(y) != SUNDIALS_NVEC_OPENMP) &&
       (N_VGetVectorID(y) != SUNDIALS_NVEC_PTHREADS) )
    return(NULL);

  /* Check that A has appropriate storage upper bandwidth for factorization */
  if (SUNBandMatrix_StoredUpperBandwidth(A) <
      SUNMIN(MatrixRows-1, SUNBandMatrix_LowerBandwidth(A)+SUNBandMatrix_UpperBandwidth(A)))
    return(NULL);

  /* optimally this function would be replaced with a generic N_Vector routine */
  VecLength = GlobalVectorLength_BandLS(y);
  if (MatrixRows != VecLength)
    return(NULL);
  
  /* Create linear solver */
  S = NULL;
  S = (SUNLinearSolver) malloc(sizeof *S);
  if (S == NULL) return(NULL);
  
  /* Create linear solver operation structure */
  ops = NULL;
  ops = (SUNLinearSolver_Ops) malloc(sizeof(struct _generic_SUNLinearSolver_Ops));
  if (ops == NULL) { free(S); return(NULL); }

  /* Attach operations */
  ops->gettype           = SUNLinSolGetType_Band;
  ops->initialize        = SUNLinSolInitialize_Band;
  ops->setup             = SUNLinSolSetup_Band;
  ops->solve             = SUNLinSolSolve_Band;
  ops->lastflag          = SUNLinSolLastFlag_Band;
  ops->space             = SUNLinSolSpace_Band;
  ops->free              = SUNLinSolFree_Band;
  ops->setatimes         = NULL;
  ops->setpreconditioner = NULL;
  ops->setscalingvectors = NULL;
  ops->numiters          = NULL;
  ops->resnorm           = NULL;
  ops->resid             = NULL;

  /* Create content */
  content = NULL;
  content = (SUNLinearSolverContent_Band) malloc(sizeof(struct _SUNLinearSolverContent_Band));
  if (content == NULL) { free(ops); free(S); return(NULL); }

  /* Fill content */
  content->N = MatrixRows;
  content->last_flag = 0;
  content->pivots = NULL;
  content->pivots = (sunindextype *) malloc(MatrixRows * sizeof(sunindextype));
  if (content->pivots == NULL) {
    free(content); free(ops); free(S); return(NULL);
  }
  
  /* Attach content and ops */
  S->content = content;
  S->ops     = ops;

  return(S);
}

/*
 * -----------------------------------------------------------------
 * implementation of linear solver operations
 * -----------------------------------------------------------------
 */

SUNLinearSolver_Type SUNLinSolGetType_Band(SUNLinearSolver S)
{
  return(SUNLINEARSOLVER_DIRECT);
}

int SUNLinSolInitialize_Band(SUNLinearSolver S)
{
  /* all solver-specific memory has already been allocated */
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}

int SUNLinSolSetup_Band(SUNLinearSolver S, SUNMatrix A)
{
  realtype **A_cols;
  sunindextype *pivots;

  /* check for valid inputs */
  if ( (A == NULL) || (S == NULL) ) 
    return(SUNLS_MEM_NULL);
  
  /* Ensure that A is a band matrix */
  if (SUNMatGetID(A) != SUNMATRIX_BAND) {
    LASTFLAG(S) = SUNLS_ILL_INPUT;
    return(LASTFLAG(S));
  }
  
  /* access data pointers (return with failure on NULL) */
  A_cols = NULL;
  pivots = NULL;
  A_cols = SM_COLS_B(A);
  pivots = PIVOTS(S);
  if ( (A_cols == NULL) || (pivots == NULL) ) {
    LASTFLAG(S) = SUNLS_MEM_FAIL;
    return(LASTFLAG(S));
  }

  /* ensure that storage upper bandwidth is sufficient for fill-in */
  if (SM_SUBAND_B(A) < SUNMIN(SM_COLUMNS_B(A)-1, SM_UBAND_B(A) + SM_LBAND_B(A))) {
    LASTFLAG(S) = SUNLS_MEM_FAIL;
    return(LASTFLAG(S));
  }
  
  /* perform LU factorization of input matrix */
  LASTFLAG(S) = bandGBTRF(A_cols, SM_COLUMNS_B(A), SM_UBAND_B(A),
			  SM_LBAND_B(A), SM_SUBAND_B(A), pivots);
  
  /* store error flag (if nonzero, that row encountered zero-valued pivod) */
  if (LASTFLAG(S) > 0)
    return(SUNLS_LUFACT_FAIL);
  return(SUNLS_SUCCESS);
}

int SUNLinSolSolve_Band(SUNLinearSolver S, SUNMatrix A, N_Vector x, 
                        N_Vector b, realtype tol)
{
  realtype **A_cols, *xdata;
  sunindextype *pivots;
  
  /* check for valid inputs */
  if ( (A == NULL) || (S == NULL) || (x == NULL) || (b == NULL) ) 
    return(SUNLS_MEM_NULL);
  
  /* copy b into x */
  N_VScale(ONE, b, x);

  /* access data pointers (return with failure on NULL) */
  A_cols = NULL;
  xdata = NULL;
  pivots = NULL;
  A_cols = SUNBandMatrix_Cols(A);
  xdata = N_VGetArrayPointer(x);
  pivots = PIVOTS(S);
  if ( (A_cols == NULL) || (xdata == NULL)  || (pivots == NULL) ) {
    LASTFLAG(S) = SUNLS_MEM_FAIL;
    return(LASTFLAG(S));
  }

  /* solve using LU factors */
  bandGBTRS(A_cols, SM_COLUMNS_B(A), SM_SUBAND_B(A), 
            SM_LBAND_B(A), pivots, xdata);
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}

long int SUNLinSolLastFlag_Band(SUNLinearSolver S)
{
  /* return the stored 'last_flag' value */
  return(LASTFLAG(S));
}

int SUNLinSolSpace_Band(SUNLinearSolver S, 
                        long int *lenrwLS, 
                        long int *leniwLS)
{
  *leniwLS = 2 + BAND_CONTENT(S)->N;
  *lenrwLS = 0;
  return(SUNLS_SUCCESS);
}

int SUNLinSolFree_Band(SUNLinearSolver S)
{
  /* return if S is already free */
  if (S == NULL)
    return(SUNLS_SUCCESS);

  /* delete items from contents, then delete generic structure */
  if (S->content) {
    if (PIVOTS(S)) {
      free(PIVOTS(S));
      PIVOTS(S) = NULL;
    }
    free(S->content);  
    S->content = NULL;
  }
  if (S->ops) {
    free(S->ops);  
    S->ops = NULL;
  }
  free(S); S = NULL;
  return(SUNLS_SUCCESS);
}

/*
 * -----------------------------------------------------------------
 * private functions
 * -----------------------------------------------------------------
 */

/* Inefficient kludge for determining the number of entries in a N_Vector 
   object (replace if such a routine is ever added to the N_Vector API).

   Returns "-1" on an error. */
sunindextype GlobalVectorLength_BandLS(N_Vector y)
{
  realtype len;
  N_Vector tmp = NULL;
  tmp = N_VClone(y);
  if (tmp == NULL)  return(-1);
  N_VConst(ONE, tmp);
  len = N_VDotProd(tmp, tmp);
  N_VDestroy(tmp);
  return( (sunindextype) len );
}
