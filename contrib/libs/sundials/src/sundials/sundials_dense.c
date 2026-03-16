/*
 * -----------------------------------------------------------------
 * $Revision$
 * $Date$
 * -----------------------------------------------------------------
 * Programmer(s): Scott D. Cohen, Alan C. Hindmarsh and
 *                Radu Serban @ LLNL
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
 * This is the implementation file for a generic package of dense
 * matrix operations.
 * -----------------------------------------------------------------
 */ 

#include <stdio.h>
#include <stdlib.h>

#include <sundials/sundials_dense.h>
#include <sundials/sundials_math.h>

#define ZERO RCONST(0.0)
#define ONE  RCONST(1.0)
#define TWO  RCONST(2.0)

/*
 * -----------------------------------------------------
 * Functions working on DlsMat
 * -----------------------------------------------------
 */

sunindextype DenseGETRF(DlsMat A, sunindextype *p)
{
  return(denseGETRF(A->cols, A->M, A->N, p));
}

void DenseGETRS(DlsMat A, sunindextype *p, realtype *b)
{
  denseGETRS(A->cols, A->N, p, b);
}

sunindextype DensePOTRF(DlsMat A)
{
  return(densePOTRF(A->cols, A->M));
}

void DensePOTRS(DlsMat A, realtype *b)
{
  densePOTRS(A->cols, A->M, b);
}

int DenseGEQRF(DlsMat A, realtype *beta, realtype *wrk)
{
  return(denseGEQRF(A->cols, A->M, A->N, beta, wrk));
}

int DenseORMQR(DlsMat A, realtype *beta, realtype *vn, realtype *vm, realtype *wrk)
{
  return(denseORMQR(A->cols, A->M, A->N, beta, vn, vm, wrk));
}

void DenseCopy(DlsMat A, DlsMat B)
{
  denseCopy(A->cols, B->cols, A->M, A->N);
}

void DenseScale(realtype c, DlsMat A)
{
  denseScale(c, A->cols, A->M, A->N);
}

void DenseMatvec(DlsMat A, realtype *x, realtype *y)
{
  denseMatvec(A->cols, x, y, A->M, A->N);
}

sunindextype denseGETRF(realtype **a, sunindextype m, sunindextype n, sunindextype *p)
{
  sunindextype i, j, k, l;
  realtype *col_j, *col_k;
  realtype temp, mult, a_kj;

  /* k-th elimination step number */
  for (k=0; k < n; k++) {

    col_k  = a[k];

    /* find l = pivot row number */
    l=k;
    for (i=k+1; i < m; i++)
      if (SUNRabs(col_k[i]) > SUNRabs(col_k[l])) l=i;
    p[k] = l;

    /* check for zero pivot element */
    if (col_k[l] == ZERO) return(k+1);
    
    /* swap a(k,1:n) and a(l,1:n) if necessary */    
    if ( l!= k ) {
      for (i=0; i<n; i++) {
        temp = a[i][l];
        a[i][l] = a[i][k];
        a[i][k] = temp;
      }
    }

    /* Scale the elements below the diagonal in
     * column k by 1.0/a(k,k). After the above swap
     * a(k,k) holds the pivot element. This scaling
     * stores the pivot row multipliers a(i,k)/a(k,k)
     * in a(i,k), i=k+1, ..., m-1.                      
     */
    mult = ONE/col_k[k];
    for(i=k+1; i < m; i++) col_k[i] *= mult;

    /* row_i = row_i - [a(i,k)/a(k,k)] row_k, i=k+1, ..., m-1 */
    /* row k is the pivot row after swapping with row l.      */
    /* The computation is done one column at a time,          */
    /* column j=k+1, ..., n-1.                                */

    for (j=k+1; j < n; j++) {

      col_j = a[j];
      a_kj = col_j[k];

      /* a(i,j) = a(i,j) - [a(i,k)/a(k,k)]*a(k,j)  */
      /* a_kj = a(k,j), col_k[i] = - a(i,k)/a(k,k) */

      if (a_kj != ZERO) {
	for (i=k+1; i < m; i++)
	  col_j[i] -= a_kj * col_k[i];
      }
    }
  }

  /* return 0 to indicate success */

  return(0);
}

void denseGETRS(realtype **a, sunindextype n, sunindextype *p, realtype *b)
{
  sunindextype i, k, pk;
  realtype *col_k, tmp;

  /* Permute b, based on pivot information in p */
  for (k=0; k<n; k++) {
    pk = p[k];
    if(pk != k) {
      tmp = b[k];
      b[k] = b[pk];
      b[pk] = tmp;
    }
  }

  /* Solve Ly = b, store solution y in b */
  for (k=0; k<n-1; k++) {
    col_k = a[k];
    for (i=k+1; i<n; i++) b[i] -= col_k[i]*b[k];
  }

  /* Solve Ux = y, store solution x in b */
  for (k = n-1; k > 0; k--) {
    col_k = a[k];
    b[k] /= col_k[k];
    for (i=0; i<k; i++) b[i] -= col_k[i]*b[k];
  }
  b[0] /= a[0][0];

}

/*
 * Cholesky decomposition of a symmetric positive-definite matrix
 * A = C^T*C: gaxpy version.
 * Only the lower triangle of A is accessed and it is overwritten with
 * the lower triangle of C.
 */
sunindextype densePOTRF(realtype **a, sunindextype m)
{
  realtype *a_col_j, *a_col_k;
  realtype a_diag;
  sunindextype i, j, k;

  for (j=0; j<m; j++) {

    a_col_j = a[j];
   
    if (j>0) {
      for(i=j; i<m; i++) {
        for(k=0;k<j;k++) {
          a_col_k = a[k];
          a_col_j[i] -= a_col_k[i]*a_col_k[j];
        }
      }
    }

    a_diag = a_col_j[j];
    if (a_diag <= ZERO) return(j+1);
    a_diag = SUNRsqrt(a_diag);

    for(i=j; i<m; i++) a_col_j[i] /= a_diag;
    
  }

  return(0);
}

/*
 * Solution of Ax=b, with A s.p.d., based on the Cholesky decomposition
 * obtained with denPOTRF.; A = C*C^T, C lower triangular
 *
 */
void densePOTRS(realtype **a, sunindextype m, realtype *b)
{
  realtype *col_j, *col_i;
  sunindextype i, j;

  /* Solve C y = b, forward substitution - column version.
     Store solution y in b */
  for (j=0; j < m-1; j++) {
    col_j = a[j];
    b[j] /= col_j[j];
    for (i=j+1; i < m; i++)
      b[i] -= b[j]*col_j[i];
  }
  col_j = a[m-1];
  b[m-1] /= col_j[m-1];

  /* Solve C^T x = y, backward substitution - row version.
     Store solution x in b */
  col_j = a[m-1];
  b[m-1] /= col_j[m-1];
  for (i=m-2; i>=0; i--) {
    col_i = a[i];
    for (j=i+1; j<m; j++) 
      b[i] -= col_i[j]*b[j];
    b[i] /= col_i[i];
  }

}

/*
 * QR factorization of a rectangular matrix A of size m by n (m >= n)
 * using Householder reflections.
 *
 * On exit, the elements on and above the diagonal of A contain the n by n 
 * upper triangular matrix R; the elements below the diagonal, with the array beta, 
 * represent the orthogonal matrix Q as a product of elementary reflectors .
 *
 * v (of length m) must be provided as workspace.
 *
 */

int denseGEQRF(realtype **a, sunindextype m, sunindextype n, realtype *beta, realtype *v)
{
  realtype ajj, s, mu, v1, v1_2;
  realtype *col_j, *col_k;
  sunindextype i, j, k;

  /* For each column...*/
  for(j=0; j<n; j++) {

    col_j = a[j];

    ajj = col_j[j];
    
    /* Compute the j-th Householder vector (of length m-j) */
    v[0] = ONE;
    s = ZERO;
    for(i=1; i<m-j; i++) {
      v[i] = col_j[i+j];
      s += v[i]*v[i];
    }

    if(s != ZERO) {
      mu = SUNRsqrt(ajj*ajj+s);
      v1 = (ajj <= ZERO) ? ajj-mu : -s/(ajj+mu);
      v1_2 = v1*v1;
      beta[j] = TWO * v1_2 / (s + v1_2);
      for(i=1; i<m-j; i++) v[i] /= v1;
    } else {
      beta[j] = ZERO;      
    }

    /* Update upper triangle of A (load R) */
    for(k=j; k<n; k++) {
      col_k = a[k];
      s = ZERO;
      for(i=0; i<m-j; i++) s += col_k[i+j]*v[i];
      s *= beta[j];
      for(i=0; i<m-j; i++) col_k[i+j] -= s*v[i];
    }

    /* Update A (load Householder vector) */
    if(j<m-1) {
      for(i=1; i<m-j; i++) col_j[i+j] = v[i];
    }

  }


  return(0);
}

/*
 * Computes vm = Q * vn, where the orthogonal matrix Q is stored as
 * elementary reflectors in the m by n matrix A and in the vector beta.
 * (NOTE: It is assumed that an QR factorization has been previously 
 * computed with denGEQRF).
 *
 * vn (IN) has length n, vm (OUT) has length m, and it's assumed that m >= n.
 *
 * v (of length m) must be provided as workspace.
 */
int denseORMQR(realtype **a, sunindextype m, sunindextype n, realtype *beta,
               realtype *vn, realtype *vm, realtype *v)
{
  realtype *col_j, s;
  sunindextype i, j;

  /* Initialize vm */
  for(i=0; i<n; i++) vm[i] = vn[i];
  for(i=n; i<m; i++) vm[i] = ZERO;

  /* Accumulate (backwards) corrections into vm */
  for(j=n-1; j>=0; j--) {

    col_j = a[j];

    v[0] = ONE;
    s = vm[j];
    for(i=1; i<m-j; i++) {
      v[i] = col_j[i+j];
      s += v[i]*vm[i+j];
    }
    s *= beta[j];

    for(i=0; i<m-j; i++) vm[i+j] -= s * v[i];

  }

  return(0);
}

void denseCopy(realtype **a, realtype **b, sunindextype m, sunindextype n)
{
  sunindextype i, j;
  realtype *a_col_j, *b_col_j;

  for (j=0; j < n; j++) {
    a_col_j = a[j];
    b_col_j = b[j];
    for (i=0; i < m; i++)
      b_col_j[i] = a_col_j[i];
  }

}

void denseScale(realtype c, realtype **a, sunindextype m, sunindextype n)
{
  sunindextype i, j;
  realtype *col_j;

  for (j=0; j < n; j++) {
    col_j = a[j];
    for (i=0; i < m; i++)
      col_j[i] *= c;
  }
}

void denseAddIdentity(realtype **a, sunindextype n)
{
  sunindextype i;
  
  for (i=0; i < n; i++) a[i][i] += ONE;
}

void denseMatvec(realtype **a, realtype *x, realtype *y, sunindextype m, sunindextype n)
{
  sunindextype i, j;
  realtype *col_j;

  for (i=0; i<m; i++) {
    y[i] = 0.0;
  }

  for (j=0; j<n; j++) {
    col_j = a[j];
    for (i=0; i<m; i++)
      y[i] += col_j[i]*x[j];
  }
}

