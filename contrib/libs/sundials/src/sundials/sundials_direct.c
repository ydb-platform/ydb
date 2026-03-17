/* -----------------------------------------------------------------
 * Programmer: Radu Serban @ LLNL
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
 * This is the implementation file for operations to be used by a
 * generic direct linear solver.
 * -----------------------------------------------------------------*/ 

#include <stdio.h>
#include <stdlib.h>

#include <sundials/sundials_direct.h>
#include <sundials/sundials_math.h>

#define ZERO RCONST(0.0)
#define ONE  RCONST(1.0)

DlsMat NewDenseMat(sunindextype M, sunindextype N)
{
  DlsMat A;
  sunindextype j;

  if ( (M <= 0) || (N <= 0) ) return(NULL);

  A = NULL;
  A = (DlsMat) malloc(sizeof *A);
  if (A==NULL) return (NULL);
  
  A->data = (realtype *) malloc(M * N * sizeof(realtype));
  if (A->data == NULL) {
    free(A); A = NULL;
    return(NULL);
  }
  A->cols = (realtype **) malloc(N * sizeof(realtype *));
  if (A->cols == NULL) {
    free(A->data); A->data = NULL;
    free(A); A = NULL;
    return(NULL);
  }

  for (j=0; j < N; j++) A->cols[j] = A->data + j * M;

  A->M = M;
  A->N = N;
  A->ldim = M;
  A->ldata = M*N;

  A->type = SUNDIALS_DENSE;

  return(A);
}

realtype **newDenseMat(sunindextype m, sunindextype n)
{
  sunindextype j;
  realtype **a;

  if ( (n <= 0) || (m <= 0) ) return(NULL);

  a = NULL;
  a = (realtype **) malloc(n * sizeof(realtype *));
  if (a == NULL) return(NULL);

  a[0] = NULL;
  a[0] = (realtype *) malloc(m * n * sizeof(realtype));
  if (a[0] == NULL) {
    free(a); a = NULL;
    return(NULL);
  }

  for (j=1; j < n; j++) a[j] = a[0] + j * m;

  return(a);
}


DlsMat NewBandMat(sunindextype N, sunindextype mu, sunindextype ml, sunindextype smu)
{
  DlsMat A;
  sunindextype j, colSize;

  if (N <= 0) return(NULL);
  
  A = NULL;
  A = (DlsMat) malloc(sizeof *A);
  if (A == NULL) return (NULL);

  colSize = smu + ml + 1;
  A->data = NULL;
  A->data = (realtype *) malloc(N * colSize * sizeof(realtype));
  if (A->data == NULL) {
    free(A); A = NULL;
    return(NULL);
  }

  A->cols = NULL;
  A->cols = (realtype **) malloc(N * sizeof(realtype *));
  if (A->cols == NULL) {
    free(A->data);
    free(A); A = NULL;
    return(NULL);
  }

  for (j=0; j < N; j++) A->cols[j] = A->data + j * colSize;

  A->M = N;
  A->N = N;
  A->mu = mu;
  A->ml = ml;
  A->s_mu = smu;
  A->ldim =  colSize;
  A->ldata = N * colSize;

  A->type = SUNDIALS_BAND;

  return(A);
}

realtype **newBandMat(sunindextype n, sunindextype smu, sunindextype ml)
{
  realtype **a;
  sunindextype j, colSize;

  if (n <= 0) return(NULL);

  a = NULL;
  a = (realtype **) malloc(n * sizeof(realtype *));
  if (a == NULL) return(NULL);

  colSize = smu + ml + 1;
  a[0] = NULL;
  a[0] = (realtype *) malloc(n * colSize * sizeof(realtype));
  if (a[0] == NULL) {
    free(a); a = NULL;
    return(NULL);
  }

  for (j=1; j < n; j++) a[j] = a[0] + j * colSize;

  return(a);
}

void DestroyMat(DlsMat A)
{
  free(A->data);  A->data = NULL;
  free(A->cols);
  free(A); A = NULL;
}

void destroyMat(realtype **a)
{
  free(a[0]); a[0] = NULL;
  free(a); a = NULL;
}

int *NewIntArray(int N)
{
  int *vec;

  if (N <= 0) return(NULL);

  vec = NULL;
  vec = (int *) malloc(N * sizeof(int));

  return(vec);
}

int *newIntArray(int n)
{
  int *v;

  if (n <= 0) return(NULL);

  v = NULL;
  v = (int *) malloc(n * sizeof(int));

  return(v);
}

sunindextype *NewIndexArray(sunindextype N)
{
  sunindextype *vec;

  if (N <= 0) return(NULL);

  vec = NULL;
  vec = (sunindextype *) malloc(N * sizeof(sunindextype));

  return(vec);
}

sunindextype *newIndexArray(sunindextype n)
{
  sunindextype *v;

  if (n <= 0) return(NULL);

  v = NULL;
  v = (sunindextype *) malloc(n * sizeof(sunindextype));

  return(v);
}

realtype *NewRealArray(sunindextype N)
{
  realtype *vec;

  if (N <= 0) return(NULL);

  vec = NULL;
  vec = (realtype *) malloc(N * sizeof(realtype));

  return(vec);
}

realtype *newRealArray(sunindextype m)
{
  realtype *v;

  if (m <= 0) return(NULL);

  v = NULL;
  v = (realtype *) malloc(m * sizeof(realtype));

  return(v);
}

void DestroyArray(void *V)
{ 
  free(V); 
  V = NULL;
}

void destroyArray(void *v)
{
  free(v); 
  v = NULL;
}


void AddIdentity(DlsMat A)
{
  sunindextype i;

  switch (A->type) {

  case SUNDIALS_DENSE:
    for (i=0; i<A->N; i++) A->cols[i][i] += ONE;
    break;

  case SUNDIALS_BAND:
    for (i=0; i<A->M; i++) A->cols[i][A->s_mu] += ONE;
    break;

  }

}


void SetToZero(DlsMat A)
{
  sunindextype i, j, colSize;
  realtype *col_j;

  switch (A->type) {

  case SUNDIALS_DENSE:
    
    for (j=0; j<A->N; j++) {
      col_j = A->cols[j];
      for (i=0; i<A->M; i++)
        col_j[i] = ZERO;
    }

    break;

  case SUNDIALS_BAND:

    colSize = A->mu + A->ml + 1;
    for (j=0; j<A->M; j++) {
      col_j = A->cols[j] + A->s_mu - A->mu;
      for (i=0; i<colSize; i++)
        col_j[i] = ZERO;
    }

    break;

  }

}


void PrintMat(DlsMat A, FILE *outfile)
{
  sunindextype i, j, start, finish;
  realtype **a;

  switch (A->type) {

  case SUNDIALS_DENSE:

    fprintf(outfile, "\n");
    for (i=0; i < A->M; i++) {
      for (j=0; j < A->N; j++) {
#if defined(SUNDIALS_EXTENDED_PRECISION)
        fprintf(outfile, "%12Lg  ", DENSE_ELEM(A,i,j));
#elif defined(SUNDIALS_DOUBLE_PRECISION)
        fprintf(outfile, "%12g  ", DENSE_ELEM(A,i,j));
#else
        fprintf(outfile, "%12g  ", DENSE_ELEM(A,i,j));
#endif
      }
      fprintf(outfile, "\n");
    }
    fprintf(outfile, "\n");
    
    break;

  case SUNDIALS_BAND:

    a = A->cols;
    fprintf(outfile, "\n");
    for (i=0; i < A->N; i++) {
      start = SUNMAX(0,i-A->ml);
      finish = SUNMIN(A->N-1,i+A->mu);
      for (j=0; j < start; j++) fprintf(outfile, "%12s  ","");
      for (j=start; j <= finish; j++) {
#if defined(SUNDIALS_EXTENDED_PRECISION)
        fprintf(outfile, "%12Lg  ", a[j][i-j+A->s_mu]);
#elif defined(SUNDIALS_DOUBLE_PRECISION)
        fprintf(outfile, "%12g  ", a[j][i-j+A->s_mu]);
#else
        fprintf(outfile, "%12g  ", a[j][i-j+A->s_mu]);
#endif
      }
      fprintf(outfile, "\n");
    }
    fprintf(outfile, "\n");
    
    break;

  }

}


