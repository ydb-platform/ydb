/*-----------------------------------------------------------------
 * Programmer(s): Daniel R. Reynolds @ SMU
 *                Alan C. Hindmarsh and Radu Serban @ LLNL
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
 * Implementation header file for IDAS's linear solver interface.
 *-----------------------------------------------------------------*/

#ifndef _IDASLS_IMPL_H
#define _IDASLS_IMPL_H

#include <idas/idas_ls.h>
#include "idas_impl.h"

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/*-----------------------------------------------------------------
  Types : struct IDALsMemRec, struct *IDALsMem

  The type IDALsMem is a pointer to a IDALsMemRec, which is a
  structure containing fields that must be accessible by LS module
  routines.
  -----------------------------------------------------------------*/
typedef struct IDALsMemRec {

  /* Jacobian construction & storage */
  booleantype jacDQ;    /* SUNTRUE if using internal DQ Jacobian approx. */
  IDALsJacFn jac;       /* Jacobian routine to be called                 */
  void *J_data;         /* J_data is passed to jac                       */

  /* Linear solver, matrix and vector objects/pointers */
  SUNLinearSolver LS;   /* generic linear solver object                  */
  SUNMatrix J;          /* J = dF/dy + cj*dF/dy'                         */
  N_Vector ytemp;       /* temp vector used by IDAAtimesDQ               */
  N_Vector yptemp;      /* temp vector used by IDAAtimesDQ               */
  N_Vector x;           /* temp vector used by the solve function        */
  N_Vector ycur;        /* current y vector in Newton iteration          */
  N_Vector ypcur;       /* current yp vector in Newton iteration         */
  N_Vector rcur;        /* rcur = F(tn, ycur, ypcur)                     */

  /* Iterative solver tolerance */
  realtype sqrtN;     /* sqrt(N)                                      */
  realtype eplifac;   /* eplifac = linear convergence factor          */

  /* Statistics and associated parameters */
  realtype dqincfac;  /* dqincfac = optional increment factor in Jv   */
  long int nje;       /* nje = no. of calls to jac                    */
  long int npe;       /* npe = total number of precond calls          */
  long int nli;       /* nli = total number of linear iterations      */
  long int nps;       /* nps = total number of psolve calls           */
  long int ncfl;      /* ncfl = total number of convergence failures  */
  long int nreDQ;     /* nreDQ = total number of calls to res         */
  long int njtsetup;  /* njtsetup = total number of calls to jtsetup  */
  long int njtimes;   /* njtimes = total number of calls to jtimes    */
  long int nst0;      /* nst0 = saved nst (for performance monitor)   */
  long int nni0;      /* nni0 = saved nni (for performance monitor)   */
  long int ncfn0;     /* ncfn0 = saved ncfn (for performance monitor) */
  long int ncfl0;     /* ncfl0 = saved ncfl (for performance monitor) */
  long int nwarn;     /* nwarn = no. of warnings (for perf. monitor)  */

  long int last_flag; /* last error return flag                       */

  /* Preconditioner computation
     (a) user-provided:
         - pdata == user_data
         - pfree == NULL (the user dealocates memory)
     (b) internal preconditioner module
         - pdata == ida_mem
         - pfree == set by the prec. module and called in idaLsFree */
  IDALsPrecSetupFn pset;
  IDALsPrecSolveFn psolve;
  int (*pfree)(IDAMem IDA_mem);
  void *pdata;

  /* Jacobian times vector compuation
     (a) jtimes function provided by the user:
         - jt_data == user_data
         - jtimesDQ == SUNFALSE
     (b) internal jtimes
         - jt_data == ida_mem
         - jtimesDQ == SUNTRUE */
  booleantype jtimesDQ;
  IDALsJacTimesSetupFn jtsetup;
  IDALsJacTimesVecFn jtimes;
  void *jt_data;

} *IDALsMem;


/*-----------------------------------------------------------------
  Prototypes of internal functions
  -----------------------------------------------------------------*/

/* Interface routines called by system SUNLinearSolver */
int idaLsATimes(void *ida_mem, N_Vector v, N_Vector z);
int idaLsPSetup(void *ida_mem);
int idaLsPSolve(void *ida_mem, N_Vector r, N_Vector z,
                realtype tol, int lr);

/* Difference quotient approximation for Jac times vector */
int idaLsDQJtimes(realtype tt, N_Vector yy, N_Vector yp,
                  N_Vector rr, N_Vector v, N_Vector Jv,
                  realtype c_j, void *data,
                  N_Vector work1, N_Vector work2);

/* Difference-quotient Jacobian approximation routines */
int idaLsDQJac(realtype tt, realtype c_j, N_Vector yy, N_Vector yp,
               N_Vector rr, SUNMatrix Jac, void *data,
               N_Vector tmp1, N_Vector tmp2, N_Vector tmp3);
int idaLsDenseDQJac(realtype tt, realtype c_j, N_Vector yy,
                    N_Vector yp, N_Vector rr, SUNMatrix Jac,
                    IDAMem IDA_mem, N_Vector tmp1);
int idaLsBandDQJac(realtype tt, realtype c_j,  N_Vector yy,
                   N_Vector yp, N_Vector rr, SUNMatrix Jac,
                   IDAMem IDA_mem, N_Vector tmp1,
                   N_Vector tmp2, N_Vector tmp3);

/* Generic linit/lsetup/lsolve/lperf/lfree interface routines for IDA to call */
int idaLsInitialize(IDAMem IDA_mem);
int idaLsSetup(IDAMem IDA_mem, N_Vector y, N_Vector yp, N_Vector r,
               N_Vector vt1, N_Vector vt2, N_Vector vt3);
int idaLsSolve(IDAMem IDA_mem, N_Vector b, N_Vector weight,
               N_Vector ycur, N_Vector ypcur, N_Vector rescur);
int idaLsPerf(IDAMem IDA_mem, int perftask);
int idaLsFree(IDAMem IDA_mem);


/* Auxilliary functions */
int idaLsInitializeCounters(IDALsMem idals_mem);
int idaLs_AccessLMem(void* ida_mem, const char* fname,
                     IDAMem* IDA_mem, IDALsMem* idals_mem);


/*---------------------------------------------------------------
  Error and Warning Messages
  ---------------------------------------------------------------*/

#if defined(SUNDIALS_EXTENDED_PRECISION)
#define MSG_LS_TIME "at t = %Lg, "
#define MSG_LS_FRMT "%Le."
#elif defined(SUNDIALS_DOUBLE_PRECISION)
#define MSG_LS_TIME "at t = %lg, "
#define MSG_LS_FRMT "%le."
#else
#define MSG_LS_TIME "at t = %g, "
#define MSG_LS_FRMT "%e."
#endif

/* Error Messages */
#define MSG_LS_IDAMEM_NULL    "Integrator memory is NULL."
#define MSG_LS_MEM_FAIL       "A memory request failed."
#define MSG_LS_BAD_NVECTOR    "A required vector operation is not implemented."
#define MSG_LS_BAD_SIZES      "Illegal bandwidth parameter(s). Must have 0 <=  ml, mu <= N-1."
#define MSG_LS_BAD_LSTYPE     "Incompatible linear solver type."
#define MSG_LS_LMEM_NULL      "Linear solver memory is NULL."
#define MSG_LS_BAD_GSTYPE     "gstype has an illegal value."
#define MSG_LS_NEG_MAXRS      "maxrs < 0 illegal."
#define MSG_LS_NEG_EPLIFAC    "eplifac < 0.0 illegal."
#define MSG_LS_NEG_DQINCFAC   "dqincfac < 0.0 illegal."
#define MSG_LS_PSET_FAILED    "The preconditioner setup routine failed in an unrecoverable manner."
#define MSG_LS_PSOLVE_FAILED  "The preconditioner solve routine failed in an unrecoverable manner."
#define MSG_LS_JTSETUP_FAILED "The Jacobian x vector setup routine failed in an unrecoverable manner."
#define MSG_LS_JTIMES_FAILED  "The Jacobian x vector routine failed in an unrecoverable manner."
#define MSG_LS_JACFUNC_FAILED "The Jacobian routine failed in an unrecoverable manner."
#define MSG_LS_MATZERO_FAILED "The SUNMatZero routine failed in an unrecoverable manner."

/* Warning Messages */
#define MSG_LS_WARN  "Warning: " MSG_LS_TIME "poor iterative algorithm performance. "
#define MSG_LS_CFN_WARN  MSG_LS_WARN "Nonlinear convergence failure rate is " MSG_LS_FRMT
#define MSG_LS_CFL_WARN  MSG_LS_WARN "Linear convergence failure rate is " MSG_LS_FRMT


/*-----------------------------------------------------------------
  PART II - backward problems
  -----------------------------------------------------------------*/

/*-----------------------------------------------------------------
  Types : IDALsMemRecB, IDALsMemB

  IDASetLinearSolverB attaches such a structure to the lmemB
  field of IDAadjMem
  -----------------------------------------------------------------*/
typedef struct IDALsMemRecB {

  IDALsJacFnB            jacB;
  IDALsJacFnBS           jacBS;
  IDALsJacTimesSetupFnB  jtsetupB;
  IDALsJacTimesSetupFnBS jtsetupBS;
  IDALsJacTimesVecFnB    jtimesB;
  IDALsJacTimesVecFnBS   jtimesBS;
  IDALsPrecSetupFnB      psetB;
  IDALsPrecSetupFnBS     psetBS;
  IDALsPrecSolveFnB      psolveB;
  IDALsPrecSolveFnBS     psolveBS;
  void                  *P_dataB;

} *IDALsMemB;


/*-----------------------------------------------------------------
  Prototypes of internal functions
  -----------------------------------------------------------------*/

int idaLsFreeB(IDABMem IDAB_mem);
int idaLs_AccessLMemB(void *ida_mem, int which, const char *fname,
                      IDAMem *IDA_mem, IDAadjMem *IDAADJ_mem,
                      IDABMem *IDAB_mem, IDALsMemB *idalsB_mem);
int idaLs_AccessLMemBCur(void *ida_mem, const char *fname,
                         IDAMem *IDA_mem, IDAadjMem *IDAADJ_mem,
                         IDABMem *IDAB_mem, IDALsMemB *idalsB_mem);


/*-----------------------------------------------------------------
  Error Messages
  -----------------------------------------------------------------*/
#define MSG_LS_CAMEM_NULL  "idaadj_mem = NULL illegal."
#define MSG_LS_LMEMB_NULL  "Linear solver memory is NULL for the backward integration."
#define MSG_LS_BAD_T       "Bad t for interpolation."
#define MSG_LS_BAD_WHICH   "Illegal value for which."
#define MSG_LS_NO_ADJ      "Illegal attempt to call before calling IDAAdjInit."


#ifdef __cplusplus
}
#endif

#endif
