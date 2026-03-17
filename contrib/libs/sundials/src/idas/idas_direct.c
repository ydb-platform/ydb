/*-----------------------------------------------------------------
 * Programmer(s): Daniel R. Reynolds @ SMU
 *-----------------------------------------------------------------
 * SUNDIALS Copyright Start
 * Copyright (c) 2002-2019, Lawrence Livermore National Security
 * and Southern Methodist University.
 * All rights reserved.
 *
 * See the top-level LICENSE and NOTICE files for details.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * SUNDIALS Copyright End
 *-----------------------------------------------------------------
 * Implementation file for the deprecated direct linear solver interface in 
 * IDA; these routines now just wrap the updated IDA generic
 * linear solver interface in idas_ls.h.
 *-----------------------------------------------------------------*/

#include <idas/idas_ls.h>
#include <idas/idas_direct.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/*=================================================================
  Exported Functions (wrappers for equivalent routines in idas_ls.h)
  =================================================================*/

int IDADlsSetLinearSolver(void *ida_mem, SUNLinearSolver LS,
                          SUNMatrix A)
{ return(IDASetLinearSolver(ida_mem, LS, A)); }
  
int IDADlsSetJacFn(void *ida_mem, IDADlsJacFn jac)
{ return(IDASetJacFn(ida_mem, jac)); }
  
int IDADlsGetWorkSpace(void *ida_mem, long int *lenrwLS,
                       long int *leniwLS)
{ return(IDAGetLinWorkSpace(ida_mem, lenrwLS, leniwLS)); }
  
int IDADlsGetNumJacEvals(void *ida_mem, long int *njevals)
{ return(IDAGetNumJacEvals(ida_mem, njevals)); }
  
int IDADlsGetNumResEvals(void *ida_mem, long int *nrevalsLS)
{ return(IDAGetNumLinResEvals(ida_mem, nrevalsLS)); }
  
int IDADlsGetLastFlag(void *ida_mem, long int *flag)
{ return(IDAGetLastLinFlag(ida_mem, flag)); }
  
char *IDADlsGetReturnFlagName(long int flag)
{ return(IDAGetLinReturnFlagName(flag)); }
  
int IDADlsSetLinearSolverB(void *ida_mem, int which,
                           SUNLinearSolver LS, SUNMatrix A)
{ return(IDASetLinearSolverB(ida_mem, which, LS, A)); }
  
int IDADlsSetJacFnB(void *ida_mem, int which, IDADlsJacFnB jacB)
{ return(IDASetJacFnB(ida_mem, which, jacB)); }
  
int IDADlsSetJacFnBS(void *ida_mem, int which, IDADlsJacFnBS jacBS)
{ return(IDASetJacFnBS(ida_mem, which, jacBS)); }

#ifdef __cplusplus
}
#endif

