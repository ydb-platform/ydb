/*
 * ----------------------------------------------------------------- 
 * Programmer(s): Daniel Reynolds @ SMU
 *                David Gardner, Carol Woodward, Slaven Peles @ LLNL
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
 * This is the implementation file for a generic SUNMATRIX package.
 * It contains the implementation of the SUNMatrix operations listed
 * in sundials_matrix.h
 * -----------------------------------------------------------------
 */

#include <stdlib.h>
#include <sundials/sundials_matrix.h>
#include <sundials/sundials_nvector.h>

/*
 * -----------------------------------------------------------------
 * Functions in the 'ops' structure
 * -----------------------------------------------------------------
 */

SUNMatrix_ID SUNMatGetID(SUNMatrix A)
{
  SUNMatrix_ID id;
  id = A->ops->getid(A);
  return(id);
}

SUNMatrix SUNMatClone(SUNMatrix A)
{
  SUNMatrix B = NULL;
  B = A->ops->clone(A);
  return(B);
}

void SUNMatDestroy(SUNMatrix A)
{
  if (A==NULL) return;
  A->ops->destroy(A);
  return;
}

int SUNMatZero(SUNMatrix A)
{
  return((int) A->ops->zero(A));
}

int SUNMatCopy(SUNMatrix A, SUNMatrix B)
{
  return((int) A->ops->copy(A, B));
}

int SUNMatScaleAdd(realtype c, SUNMatrix A, SUNMatrix B)
{
  return((int) A->ops->scaleadd(c, A, B));
}

int SUNMatScaleAddI(realtype c, SUNMatrix A)
{
  return((int) A->ops->scaleaddi(c, A));
}

int SUNMatMatvec(SUNMatrix A, N_Vector x, N_Vector y)
{
  return((int) A->ops->matvec(A, x, y));
}

int SUNMatSpace(SUNMatrix A, long int *lenrw, long int *leniw)
{
  return((int) A->ops->space(A, lenrw, leniw));
}

