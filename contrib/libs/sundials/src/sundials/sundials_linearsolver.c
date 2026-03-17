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
 * This is the implementation file for a generic SUNLINEARSOLVER 
 * package.  It contains the implementation of the SUNLinearSolver
 * operations listed in sundials_linearsolver.h
 * -----------------------------------------------------------------
 */

#include <stdlib.h>
#include <sundials/sundials_linearsolver.h>

/*
 * -----------------------------------------------------------------
 * Functions in the 'ops' structure
 * -----------------------------------------------------------------
 */

SUNLinearSolver_Type SUNLinSolGetType(SUNLinearSolver S)
{
  SUNLinearSolver_Type type;
  type = S->ops->gettype(S);
  return(type);
}

int SUNLinSolSetATimes(SUNLinearSolver S, void* A_data,
                       ATimesFn ATimes)
{
  if (S->ops->setatimes)
    return ((int) S->ops->setatimes(S, A_data, ATimes));
  else
    return SUNLS_SUCCESS;
}

  
int SUNLinSolSetPreconditioner(SUNLinearSolver S, void* P_data,
                               PSetupFn Pset, PSolveFn Psol)
{
  if (S->ops->setpreconditioner)
    return ((int) S->ops->setpreconditioner(S, P_data, Pset, Psol));
  else
    return SUNLS_SUCCESS;
}
  
int SUNLinSolSetScalingVectors(SUNLinearSolver S,
                               N_Vector s1, N_Vector s2)
{
  if (S->ops->setscalingvectors)
    return ((int) S->ops->setscalingvectors(S, s1, s2));
  else
    return SUNLS_SUCCESS;
}
  
int SUNLinSolInitialize(SUNLinearSolver S)
{
  return ((int) S->ops->initialize(S));
}
  
int SUNLinSolSetup(SUNLinearSolver S, SUNMatrix A)
{
  return ((int) S->ops->setup(S, A));
}

int SUNLinSolSolve(SUNLinearSolver S, SUNMatrix A, N_Vector x,
                   N_Vector b, realtype tol)
{
  return ((int) S->ops->solve(S, A, x, b, tol));
}
  
int SUNLinSolNumIters(SUNLinearSolver S)
{
  if (S->ops->numiters)
    return ((int) S->ops->numiters(S));
  else
    return 0;
}

realtype SUNLinSolResNorm(SUNLinearSolver S)
{
  if (S->ops->resnorm)
    return ((realtype) S->ops->resnorm(S));
  else
    return RCONST(0.0);
}

N_Vector SUNLinSolResid(SUNLinearSolver S)
{
  if (S->ops->resid)
    return ((N_Vector) S->ops->resid(S));
  else
    return NULL;
}

long int SUNLinSolLastFlag(SUNLinearSolver S)
{
  if (S->ops->lastflag)
    return ((long int) S->ops->lastflag(S));
  else
    return SUNLS_SUCCESS;
}

int SUNLinSolSpace(SUNLinearSolver S, long int *lenrwLS,
                   long int *leniwLS)
{
  if (S->ops->space)
    return ((int) S->ops->space(S, lenrwLS, leniwLS));
  else {
    *lenrwLS = 0;
    *leniwLS = 0;
    return SUNLS_SUCCESS;
  }
}

int SUNLinSolFree(SUNLinearSolver S)
{
  if (S==NULL) return 0;
  S->ops->free(S);
  return 0;
}

