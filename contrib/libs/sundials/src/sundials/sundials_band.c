/*
 * -----------------------------------------------------------------
 * $Revision$
 * $Date$
 * -----------------------------------------------------------------
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
 * This is the implementation file for a generic BAND linear
 * solver package.
 * -----------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>

#include <sundials/sundials_band.h>
#include <sundials/sundials_math.h>

#define ZERO RCONST(0.0)
#define ONE  RCONST(1.0)

#define ROW(i,j,smu) (i-j+smu)

/*
 * -----------------------------------------------------
 * Functions working on DlsMat
 * -----------------------------------------------------
 */

sunindextype BandGBTRF(DlsMat A, sunindextype *p)
{
  return(bandGBTRF(A->cols, A->M, A->mu, A->ml, A->s_mu, p));
}

void BandGBTRS(DlsMat A, sunindextype *p, realtype *b)
{
  bandGBTRS(A->cols, A->M, A->s_mu, A->ml, p, b);
}

void BandCopy(DlsMat A, DlsMat B, sunindextype copymu, sunindextype copyml)
{
  bandCopy(A->cols, B->cols, A->M, A->s_mu, B->s_mu, copymu, copyml);
}

void BandScale(realtype c, DlsMat A)
{
  bandScale(c, A->cols, A->M, A->mu, A->ml, A->s_mu);
}

void BandMatvec(DlsMat A, realtype *x, realtype *y)
{
  bandMatvec(A->cols, x, y, A->M, A->mu, A->ml, A->s_mu);
}

/*
 * -----------------------------------------------------
 * Functions working on realtype**
 * -----------------------------------------------------
 */

sunindextype bandGBTRF(realtype **a, sunindextype n, sunindextype mu, sunindextype ml, sunindextype smu, sunindextype *p)
{
  sunindextype c, r, num_rows;
  sunindextype i, j, k, l, storage_l, storage_k, last_col_k, last_row_k;
  realtype *a_c, *col_k, *diag_k, *sub_diag_k, *col_j, *kptr, *jptr;
  realtype max, temp, mult, a_kj;
  booleantype swap;

  /* zero out the first smu - mu rows of the rectangular array a */

  num_rows = smu - mu;
  if (num_rows > 0) {
    for (c=0; c < n; c++) {
      a_c = a[c];
      for (r=0; r < num_rows; r++) {
	a_c[r] = ZERO;
      }
    }
  }

  /* k = elimination step number */

  for (k=0; k < n-1; k++, p++) {
    
    col_k     = a[k];
    diag_k    = col_k + smu;
    sub_diag_k = diag_k + 1;
    last_row_k = SUNMIN(n-1,k+ml);

    /* find l = pivot row number */

    l=k;
    max = SUNRabs(*diag_k);
    for (i=k+1, kptr=sub_diag_k; i <= last_row_k; i++, kptr++) { 
      if (SUNRabs(*kptr) > max) {
	l=i;
	max = SUNRabs(*kptr);
      }
    }
    storage_l = ROW(l, k, smu);
    *p = l;
    
    /* check for zero pivot element */

    if (col_k[storage_l] == ZERO) return(k+1);
    
    /* swap a(l,k) and a(k,k) if necessary */
    
    if ( (swap = (l != k) )) {
      temp = col_k[storage_l];
      col_k[storage_l] = *diag_k;
      *diag_k = temp;
    }

    /* Scale the elements below the diagonal in         */
    /* column k by -1.0 / a(k,k). After the above swap, */
    /* a(k,k) holds the pivot element. This scaling     */
    /* stores the pivot row multipliers -a(i,k)/a(k,k)  */
    /* in a(i,k), i=k+1, ..., SUNMIN(n-1,k+ml).            */
    
    mult = -ONE / (*diag_k);
    for (i=k+1, kptr = sub_diag_k; i <= last_row_k; i++, kptr++)
      (*kptr) *= mult;

    /* row_i = row_i - [a(i,k)/a(k,k)] row_k, i=k+1, ..., SUNMIN(n-1,k+ml) */
    /* row k is the pivot row after swapping with row l.                */
    /* The computation is done one column at a time,                    */
    /* column j=k+1, ..., SUNMIN(k+smu,n-1).                               */
    
    last_col_k = SUNMIN(k+smu,n-1);
    for (j=k+1; j <= last_col_k; j++) {
      
      col_j = a[j];
      storage_l = ROW(l,j,smu); 
      storage_k = ROW(k,j,smu); 
      a_kj = col_j[storage_l];

      /* Swap the elements a(k,j) and a(k,l) if l!=k. */
      
      if (swap) {
	col_j[storage_l] = col_j[storage_k];
	col_j[storage_k] = a_kj;
      }

      /* a(i,j) = a(i,j) - [a(i,k)/a(k,k)]*a(k,j) */
      /* a_kj = a(k,j), *kptr = - a(i,k)/a(k,k), *jptr = a(i,j) */

      if (a_kj != ZERO) {
	for (i=k+1, kptr=sub_diag_k, jptr=col_j+ROW(k+1,j,smu);
	     i <= last_row_k;
	     i++, kptr++, jptr++)
	  (*jptr) += a_kj * (*kptr);
      }
    }    
  }
  
  /* set the last pivot row to be n-1 and check for a zero pivot */

  *p = n-1; 
  if (a[n-1][smu] == ZERO) return(n);

  /* return 0 to indicate success */

  return(0);
}

void bandGBTRS(realtype **a, sunindextype n, sunindextype smu, sunindextype ml, sunindextype *p, realtype *b)
{
  sunindextype k, l, i, first_row_k, last_row_k;
  realtype mult, *diag_k;
  
  /* Solve Ly = Pb, store solution y in b */
  
  for (k=0; k < n-1; k++) {
    l = p[k];
    mult = b[l];
    if (l != k) {
      b[l] = b[k];
      b[k] = mult;
    }
    diag_k = a[k]+smu;
    last_row_k = SUNMIN(n-1,k+ml);
    for (i=k+1; i <= last_row_k; i++)
      b[i] += mult * diag_k[i-k];
  }
  
  /* Solve Ux = y, store solution x in b */
  
  for (k=n-1; k >= 0; k--) {
    diag_k = a[k]+smu;
    first_row_k = SUNMAX(0,k-smu);
    b[k] /= (*diag_k);
    mult = -b[k];
    for (i=first_row_k; i <= k-1; i++)
      b[i] += mult*diag_k[i-k];
  }
}

void bandCopy(realtype **a, realtype **b, sunindextype n, sunindextype a_smu, sunindextype b_smu, 
              sunindextype copymu, sunindextype copyml)
{
  sunindextype i, j, copySize;
  realtype *a_col_j, *b_col_j;

  copySize = copymu + copyml + 1;
 
  for (j=0; j < n; j++) {
    a_col_j = a[j]+a_smu-copymu;
    b_col_j = b[j]+b_smu-copymu;
    for (i=0; i < copySize; i++)
      b_col_j[i] = a_col_j[i];
  }
}

void bandScale(realtype c, realtype **a, sunindextype n, sunindextype mu, sunindextype ml, sunindextype smu)
{
  sunindextype i, j, colSize;
  realtype *col_j;

  colSize = mu + ml + 1;

  for(j=0; j < n; j++) {
    col_j = a[j]+smu-mu;
    for (i=0; i < colSize; i++)
      col_j[i] *= c;
  }
}

void bandAddIdentity(realtype **a, sunindextype n, sunindextype smu)
{
  sunindextype j;
 
  for(j=0; j < n; j++)
    a[j][smu] += ONE;
}

void bandMatvec(realtype **a, realtype *x, realtype *y, sunindextype n, 
		sunindextype mu, sunindextype ml, sunindextype smu)
{
  sunindextype i, j, is, ie;
  realtype *col_j;

  for (i=0; i<n; i++)
    y[i] = 0.0;

  for(j=0; j<n; j++) {
    col_j = a[j]+smu-mu;
    is = (0 > j-mu) ? 0 : j-mu;
    ie = (n-1 < j+ml) ? n-1 : j+ml;
    for (i=is; i<=ie; i++)
      y[i] += col_j[i-j+mu]*x[j];
  }
}

