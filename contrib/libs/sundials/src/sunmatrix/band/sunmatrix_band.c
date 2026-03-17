/*
 * -----------------------------------------------------------------
 * Programmer(s): Daniel Reynolds @ SMU
 *                David Gardner @ LLNL
 * Based on code sundials_band.c by: Alan C. Hindmarsh and 
 *    Radu Serban @ LLNL
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
 * the SUNMATRIX package.
 * -----------------------------------------------------------------
 */ 

#include <stdio.h>
#include <stdlib.h>

#include <sunmatrix/sunmatrix_band.h>
#include <sundials/sundials_math.h>

#define ZERO RCONST(0.0)
#define ONE  RCONST(1.0)


/* Private function prototypes */
static booleantype SMCompatible_Band(SUNMatrix A, SUNMatrix B);
static booleantype SMCompatible2_Band(SUNMatrix A, N_Vector x, N_Vector y);
static int SMScaleAddNew_Band(realtype c, SUNMatrix A, SUNMatrix B);


/*
 * -----------------------------------------------------------------
 * exported functions
 * -----------------------------------------------------------------
 */

/* ----------------------------------------------------------------------------
 * Function to create a new band matrix with default storage upper bandwidth 
 */

SUNMatrix SUNBandMatrix(sunindextype N, sunindextype mu, sunindextype ml)
{
  return (SUNBandMatrixStorage(N, mu, ml, mu+ml));
}

/* ----------------------------------------------------------------------------
 * Function to create a new band matrix with specified storage upper bandwidth
 */

SUNMatrix SUNBandMatrixStorage(sunindextype N, sunindextype mu,
                               sunindextype ml, sunindextype smu)
{
  SUNMatrix A;
  SUNMatrix_Ops ops;
  SUNMatrixContent_Band content;
  sunindextype j, colSize;

  /* return with NULL matrix on illegal dimension input */
  if ( (N <= 0) || (smu < 0) || (ml < 0) ) return(NULL);

  /* Create matrix */
  A = NULL;
  A = (SUNMatrix) malloc(sizeof *A);
  if (A == NULL) return(NULL);
  
  /* Create matrix operation structure */
  ops = NULL;
  ops = (SUNMatrix_Ops) malloc(sizeof(struct _generic_SUNMatrix_Ops));
  if (ops == NULL) { free(A); return(NULL); }

  /* Attach operations */
  ops->getid       = SUNMatGetID_Band;
  ops->clone       = SUNMatClone_Band;
  ops->destroy     = SUNMatDestroy_Band;
  ops->zero        = SUNMatZero_Band;
  ops->copy        = SUNMatCopy_Band;
  ops->scaleadd    = SUNMatScaleAdd_Band;
  ops->scaleaddi   = SUNMatScaleAddI_Band;
  ops->matvec      = SUNMatMatvec_Band;
  ops->space       = SUNMatSpace_Band;

  /* Create content */
  content = NULL;
  content = (SUNMatrixContent_Band) malloc(sizeof(struct _SUNMatrixContent_Band));
  if (content == NULL) { free(ops); free(A); return(NULL); }

  /* Fill content */
  colSize = smu + ml + 1;
  content->M = N;
  content->N = N;
  content->mu = mu;
  content->ml = ml;
  content->s_mu = smu;
  content->ldim = colSize;
  content->ldata = N * colSize;
  content->data = NULL;
  content->data = (realtype *) calloc(N * colSize, sizeof(realtype));
  if (content->data == NULL) {
    free(content); free(ops); free(A); return(NULL);
  }
  content->cols = NULL;
  content->cols = (realtype **) malloc(N * sizeof(realtype *));
  if (content->cols == NULL) {
    free(content->data); free(content); free(ops); free(A); return(NULL);
  }
  for (j=0; j<N; j++) content->cols[j] = content->data + j * colSize;

  /* Attach content and ops */
  A->content = content;
  A->ops     = ops;

  return(A);
}

/* ----------------------------------------------------------------------------
 * Function to print the band matrix 
 */
 
void SUNBandMatrix_Print(SUNMatrix A, FILE* outfile)
{
  sunindextype i, j, start, finish;

  /* should not be called unless A is a band matrix; 
     otherwise return immediately */
  if (SUNMatGetID(A) != SUNMATRIX_BAND)
    return;

  /* perform operation */
  fprintf(outfile,"\n");
  for (i=0; i<SM_ROWS_B(A); i++) {
    start = SUNMAX(0, i-SM_LBAND_B(A));
    finish = SUNMIN(SM_COLUMNS_B(A)-1, i+SM_UBAND_B(A));
    for (j=0; j<start; j++)
      fprintf(outfile,"%12s  ","");
    for (j=start; j<=finish; j++) {
#if defined(SUNDIALS_EXTENDED_PRECISION)
      fprintf(outfile,"%12Lg  ", SM_ELEMENT_B(A,i,j));
#elif defined(SUNDIALS_DOUBLE_PRECISION)
      fprintf(outfile,"%12g  ", SM_ELEMENT_B(A,i,j));
#else
      fprintf(outfile,"%12g  ", SM_ELEMENT_B(A,i,j));
#endif
    }
    fprintf(outfile,"\n");
  }
  fprintf(outfile,"\n");
  return;
}

/* ----------------------------------------------------------------------------
 * Functions to access the contents of the band matrix structure
 */

sunindextype SUNBandMatrix_Rows(SUNMatrix A)
{
  if (SUNMatGetID(A) == SUNMATRIX_BAND)
    return SM_ROWS_B(A);
  else
    return -1;
}

sunindextype SUNBandMatrix_Columns(SUNMatrix A)
{
  if (SUNMatGetID(A) == SUNMATRIX_BAND)
    return SM_COLUMNS_B(A);
  else
    return -1;
}

sunindextype SUNBandMatrix_LowerBandwidth(SUNMatrix A)
{
  if (SUNMatGetID(A) == SUNMATRIX_BAND)
    return SM_LBAND_B(A);
  else
    return -1;
}

sunindextype SUNBandMatrix_UpperBandwidth(SUNMatrix A)
{
  if (SUNMatGetID(A) == SUNMATRIX_BAND)
    return SM_UBAND_B(A);
  else
    return -1;
}

sunindextype SUNBandMatrix_StoredUpperBandwidth(SUNMatrix A)
{
  if (SUNMatGetID(A) == SUNMATRIX_BAND)
    return SM_SUBAND_B(A);
  else
    return -1;
}

sunindextype SUNBandMatrix_LDim(SUNMatrix A)
{
  if (SUNMatGetID(A) == SUNMATRIX_BAND)
    return SM_LDIM_B(A);
  else
    return -1;
}

realtype* SUNBandMatrix_Data(SUNMatrix A)
{
  if (SUNMatGetID(A) == SUNMATRIX_BAND)
    return SM_DATA_B(A);
  else
    return NULL;
}

realtype** SUNBandMatrix_Cols(SUNMatrix A)
{
  if (SUNMatGetID(A) == SUNMATRIX_BAND)
    return SM_COLS_B(A);
  else
    return NULL;
}

realtype* SUNBandMatrix_Column(SUNMatrix A, sunindextype j)
{
  if (SUNMatGetID(A) == SUNMATRIX_BAND)
    return SM_COLUMN_B(A,j);
  else
    return NULL;
}

/*
 * -----------------------------------------------------------------
 * implementation of matrix operations
 * -----------------------------------------------------------------
 */

SUNMatrix_ID SUNMatGetID_Band(SUNMatrix A)
{
  return SUNMATRIX_BAND;
}

SUNMatrix SUNMatClone_Band(SUNMatrix A)
{
  SUNMatrix B = SUNBandMatrixStorage(SM_COLUMNS_B(A), SM_UBAND_B(A),
                                     SM_LBAND_B(A), SM_SUBAND_B(A));
  return(B);
}

void SUNMatDestroy_Band(SUNMatrix A)
{
  if (A == NULL)  return;
  if (A->ops)  free(A->ops);
  A->ops = NULL;
  if (A->content == NULL) {
    free(A); A = NULL;
    return;
  }
  if (SM_DATA_B(A))  free(SM_DATA_B(A));
  SM_DATA_B(A) = NULL;
  if (SM_COLS_B(A))  free(SM_COLS_B(A));
  SM_COLS_B(A) = NULL;
  if (A->content)  free(A->content);
  A->content = NULL;
  free(A); A = NULL;
  return;
}

int SUNMatZero_Band(SUNMatrix A)
{
  sunindextype i;
  realtype *Adata;

  /* Verify that A is a band matrix */
  if (SUNMatGetID(A) != SUNMATRIX_BAND)
    return 1;

  /* Perform operation */
  Adata = SM_DATA_B(A);
  for (i=0; i<SM_LDATA_B(A); i++)
    Adata[i] = ZERO;
  return 0;
}

int SUNMatCopy_Band(SUNMatrix A, SUNMatrix B)
{
  sunindextype i, j, colSize, ml, mu, smu;
  realtype *A_colj, *B_colj;

  /* Verify that A and B have compatible dimensions */
  if (!SMCompatible_Band(A, B))
    return 1;

  /* Grow B if A's bandwidth is larger */
  if ( (SM_UBAND_B(A) > SM_UBAND_B(B)) ||
       (SM_LBAND_B(A) > SM_LBAND_B(B)) ) {
    ml  = SUNMAX(SM_LBAND_B(B),SM_LBAND_B(A));
    mu  = SUNMAX(SM_UBAND_B(B),SM_UBAND_B(A));
    smu = SUNMAX(SM_SUBAND_B(B),SM_SUBAND_B(A));
    colSize = smu + ml + 1;
    SM_CONTENT_B(B)->mu = mu;
    SM_CONTENT_B(B)->ml = ml;
    SM_CONTENT_B(B)->s_mu = smu;
    SM_CONTENT_B(B)->ldim = colSize;
    SM_CONTENT_B(B)->ldata = SM_COLUMNS_B(B) * colSize;
    SM_CONTENT_B(B)->data = (realtype *)
      realloc(SM_CONTENT_B(B)->data, SM_COLUMNS_B(B) * colSize*sizeof(realtype));
    for (j=0; j<SM_COLUMNS_B(B); j++)
      SM_CONTENT_B(B)->cols[j] = SM_CONTENT_B(B)->data + j * colSize;   
  }
  
  /* Perform operation */
  if (SUNMatZero_Band(B) != 0)
    return 1;
  for (j=0; j<SM_COLUMNS_B(B); j++) {
    B_colj = SM_COLUMN_B(B,j);
    A_colj = SM_COLUMN_B(A,j);
    for (i=-SM_UBAND_B(A); i<=SM_LBAND_B(A); i++)
      B_colj[i] = A_colj[i];
  }
  return 0;
}

int SUNMatScaleAddI_Band(realtype c, SUNMatrix A)
{
  sunindextype i, j;
  realtype *A_colj;
  
  /* Verify that A is a band matrix */
  if (SUNMatGetID(A) != SUNMATRIX_BAND)
    return 1;

  /* Perform operation */
  for (j=0; j<SM_COLUMNS_B(A); j++) {
    A_colj = SM_COLUMN_B(A,j);
    for (i=-SM_UBAND_B(A); i<=SM_LBAND_B(A); i++)
      A_colj[i] *= c;
    SM_ELEMENT_B(A,j,j) += ONE;
  }
  return 0;
}

int SUNMatScaleAdd_Band(realtype c, SUNMatrix A, SUNMatrix B)
{
  sunindextype i, j;
  realtype *A_colj, *B_colj;

  /* Verify that A and B are compatible */
  if (!SMCompatible_Band(A, B))
    return 1;

  /* Call separate routine in B has larger bandwidth(s) than A */
  if ( (SM_UBAND_B(B) > SM_UBAND_B(A)) ||
       (SM_LBAND_B(B) > SM_LBAND_B(A)) ) {
    return SMScaleAddNew_Band(c,A,B);
  }
  
  /* Otherwise, perform operation in-place */
  for (j=0; j<SM_COLUMNS_B(A); j++) {
    A_colj = SM_COLUMN_B(A,j);
    B_colj = SM_COLUMN_B(B,j);
    for (i=-SM_UBAND_B(B); i<=SM_LBAND_B(B); i++)
      A_colj[i] = c*A_colj[i] + B_colj[i];
  }
  return 0;
}

int SUNMatMatvec_Band(SUNMatrix A, N_Vector x, N_Vector y)
{
  sunindextype i, j, is, ie;
  realtype *col_j, *xd, *yd;
  
  /* Verify that A, x and y are compatible */
  if (!SMCompatible2_Band(A, x, y))
    return 1;

  /* access vector data (return if failure) */
  xd = N_VGetArrayPointer(x);
  yd = N_VGetArrayPointer(y);
  if ((xd == NULL) || (yd == NULL) || (xd == yd))
    return 1;

  /* Perform operation */
  for (i=0; i<SM_ROWS_B(A); i++)
    yd[i] = ZERO;
  for(j=0; j<SM_COLUMNS_B(A); j++) {
    col_j = SM_COLUMN_B(A,j);
    is = SUNMAX(0, j-SM_UBAND_B(A));
    ie = SUNMIN(SM_ROWS_B(A)-1, j+SM_LBAND_B(A));
    for (i=is; i<=ie; i++)
      yd[i] += col_j[i-j]*xd[j];
  }
  return 0;
}

int SUNMatSpace_Band(SUNMatrix A, long int *lenrw, long int *leniw)
{
  *lenrw = SM_COLUMNS_B(A) * (SM_SUBAND_B(A) + SM_LBAND_B(A) + 1);
  *leniw = 7 + SM_COLUMNS_B(A);
  return 0;
}


/*
 * -----------------------------------------------------------------
 * private functions
 * -----------------------------------------------------------------
 */

static booleantype SMCompatible_Band(SUNMatrix A, SUNMatrix B)
{
  /* both matrices must be SUNMATRIX_BAND */
  if (SUNMatGetID(A) != SUNMATRIX_BAND)
    return SUNFALSE;
  if (SUNMatGetID(B) != SUNMATRIX_BAND)
    return SUNFALSE;

  /* both matrices must have the same number of columns
     (note that we do not check for identical bandwidth) */
  if (SM_ROWS_B(A) != SM_ROWS_B(B))
    return SUNFALSE;
  if (SM_COLUMNS_B(A) != SM_COLUMNS_B(B))
    return SUNFALSE;

  return SUNTRUE;
}


static booleantype SMCompatible2_Band(SUNMatrix A, N_Vector x, N_Vector y)
{
  /*   matrix must be SUNMATRIX_BAND */
  if (SUNMatGetID(A) != SUNMATRIX_BAND)
    return SUNFALSE;

  /*   vectors must be one of {SERIAL, OPENMP, PTHREADS} */ 
  if ( (N_VGetVectorID(x) != SUNDIALS_NVEC_SERIAL) &&
       (N_VGetVectorID(x) != SUNDIALS_NVEC_OPENMP) &&
       (N_VGetVectorID(x) != SUNDIALS_NVEC_PTHREADS) )
    return SUNFALSE;

  /* Optimally we would verify that the dimensions of A, x and y agree, 
   but since there is no generic 'length' routine for N_Vectors we cannot */

  return SUNTRUE;
}


int SMScaleAddNew_Band(realtype c, SUNMatrix A, SUNMatrix B)
{
  sunindextype i, j, ml, mu, smu;
  realtype *A_colj, *B_colj, *C_colj;
  SUNMatrix C;

  /* create new matrix large enough to hold both A and B */
  ml  = SUNMAX(SM_LBAND_B(A),SM_LBAND_B(B));
  mu  = SUNMAX(SM_UBAND_B(A),SM_UBAND_B(B));
  smu = SUNMIN(SM_COLUMNS_B(A)-1, mu + ml);
  C = SUNBandMatrixStorage(SM_COLUMNS_B(A), mu, ml, smu);

  /* scale/add c*A into new matrix */
  for (j=0; j<SM_COLUMNS_B(A); j++) {
    A_colj = SM_COLUMN_B(A,j);
    C_colj = SM_COLUMN_B(C,j);
    for (i=-SM_UBAND_B(A); i<=SM_LBAND_B(A); i++)
      C_colj[i] = c*A_colj[i];
  }
  
  /* add B into new matrix */
  for (j=0; j<SM_COLUMNS_B(B); j++) {
    B_colj = SM_COLUMN_B(B,j);
    C_colj = SM_COLUMN_B(C,j);
    for (i=-SM_UBAND_B(B); i<=SM_LBAND_B(B); i++)
      C_colj[i] += B_colj[i];
  }
  
  /* replace A contents with C contents, nullify C content pointer, destroy C */
  free(SM_DATA_B(A));  SM_DATA_B(A) = NULL;
  free(SM_COLS_B(A));  SM_COLS_B(A) = NULL;
  free(A->content);    A->content = NULL;
  A->content = C->content;
  C->content = NULL;
  SUNMatDestroy_Band(C);
  
  return 0;
}

