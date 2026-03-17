/*-----------------------------------------------------------------
 * Programmer(s): Daniel R. Reynolds @ SMU
 *                Radu Serban @ LLNL
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
 * Implementation header file for the scaled, preconditioned
 * linear solver interface.
 *-----------------------------------------------------------------*/

#ifndef _CVSLS_IMPL_H
#define _CVSLS_IMPL_H

#include <cvodes/cvodes_ls.h>
#include "cvodes_impl.h"

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/*-----------------------------------------------------------------
  CVSLS solver constants

  CVLS_MSBJ   maximum number of steps between Jacobian and/or
              preconditioner evaluations
  CVLS_DGMAX  maximum change in gamma between Jacobian and/or
              preconditioner evaluations
  CVLS_EPLIN  default value for factor by which the tolerance on
              the nonlinear iteration is multiplied to get a
              tolerance on the linear iteration
  -----------------------------------------------------------------*/
#define CVLS_MSBJ   50
#define CVLS_DGMAX  RCONST(0.2)
#define CVLS_EPLIN  RCONST(0.05)


/*=================================================================
  PART I:  Forward Problems
  =================================================================*/

/*-----------------------------------------------------------------
  Types : CVLsMemRec, CVLsMem

  The type CVLsMem is pointer to a CVLsMemRec.
  -----------------------------------------------------------------*/
typedef struct CVLsMemRec {

  /* Jacobian construction & storage */
  booleantype jacDQ;  /* SUNTRUE if using internal DQ Jac approx.     */
  CVLsJacFn jac;      /* Jacobian routine to be called                */
  void *J_data;       /* user data is passed to jac                   */
  booleantype jbad;   /* heuristic suggestion for pset                */

  /* Iterative solver tolerance */
  realtype sqrtN;     /* sqrt(N)                                      */
  realtype eplifac;   /* eplifac = user specified or EPLIN_DEFAULT    */

  /* Linear solver, matrix and vector objects/pointers */
  SUNLinearSolver LS; /* generic linear solver object                 */
  SUNMatrix A;        /* A = I - gamma * df/dy                        */
  SUNMatrix savedJ;   /* savedJ = old Jacobian                        */
  N_Vector ytemp;     /* temp vector passed to jtimes and psolve      */
  N_Vector x;         /* temp vector used by CVLsSolve                */
  N_Vector ycur;      /* CVODE current y vector in Newton Iteration   */
  N_Vector fcur;      /* fcur = f(tn, ycur)                           */

  /* Statistics and associated parameters */
  long int msbj;      /* max num steps between jac/pset calls         */
  long int nje;       /* nje = no. of calls to jac                    */
  long int nfeDQ;     /* no. of calls to f due to DQ Jacobian or J*v
                         approximations                               */
  long int nstlj;     /* nstlj = nst at last jac/pset call            */
  long int npe;       /* npe = total number of pset calls             */
  long int nli;       /* nli = total number of linear iterations      */
  long int nps;       /* nps = total number of psolve calls           */
  long int ncfl;      /* ncfl = total number of convergence failures  */
  long int njtsetup;  /* njtsetup = total number of calls to jtsetup  */
  long int njtimes;   /* njtimes = total number of calls to jtimes    */

  /* Preconditioner computation
   * (a) user-provided:
   *     - P_data == user_data
   *     - pfree == NULL (the user dealocates memory for user_data)
   * (b) internal preconditioner module
   *     - P_data == cvode_mem
   *     - pfree == set by the prec. module and called in CVodeFree */
  CVLsPrecSetupFn pset;
  CVLsPrecSolveFn psolve;
  int (*pfree)(CVodeMem cv_mem);
  void *P_data;

  /* Jacobian times vector compuation
   * (a) jtimes function provided by the user:
   *     - jt_data == user_data
   *     - jtimesDQ == SUNFALSE
   * (b) internal jtimes
   *     - jt_data == cvode_mem
   *     - jtimesDQ == SUNTRUE */
  booleantype jtimesDQ;
  CVLsJacTimesSetupFn jtsetup;
  CVLsJacTimesVecFn jtimes;
  void *jt_data;

  long int last_flag; /* last error flag returned by any function */

} *CVLsMem;

/*-----------------------------------------------------------------
  Prototypes of internal functions
  -----------------------------------------------------------------*/

/* Interface routines called by system SUNLinearSolver */
int cvLsATimes(void* cvode_mem, N_Vector v, N_Vector z);
int cvLsPSetup(void* cvode_mem);
int cvLsPSolve(void* cvode_mem, N_Vector r, N_Vector z,
               realtype tol, int lr);

/* Difference quotient approximation for Jac times vector */
int cvLsDQJtimes(N_Vector v, N_Vector Jv, realtype t,
                 N_Vector y, N_Vector fy, void *data,
                 N_Vector work);

/* Difference-quotient Jacobian approximation routines */
int cvLsDQJac(realtype t, N_Vector y, N_Vector fy, SUNMatrix Jac,
              void *data, N_Vector tmp1, N_Vector tmp2, N_Vector tmp3);
int cvLsDenseDQJac(realtype t, N_Vector y, N_Vector fy,
                   SUNMatrix Jac, CVodeMem cv_mem, N_Vector tmp1);
int cvLsBandDQJac(realtype t, N_Vector y, N_Vector fy,
                  SUNMatrix Jac, CVodeMem cv_mem, N_Vector tmp1,
                  N_Vector tmp2);

/* Generic linit/lsetup/lsolve/lfree interface routines for CVode to call */
int cvLsInitialize(CVodeMem cv_mem);
int cvLsSetup(CVodeMem cv_mem, int convfail, N_Vector ypred,
              N_Vector fpred, booleantype *jcurPtr,
              N_Vector vtemp1, N_Vector vtemp2, N_Vector vtemp3);
int cvLsSolve(CVodeMem cv_mem, N_Vector b, N_Vector weight,
              N_Vector ycur, N_Vector fcur);
int cvLsFree(CVodeMem cv_mem);

/* Auxilliary functions */
int cvLsInitializeCounters(CVLsMem cvls_mem);
int cvLs_AccessLMem(void* cvode_mem, const char* fname,
                    CVodeMem* cv_mem, CVLsMem* cvls_mem);

/*=================================================================
  PART II:  Backward Problems
  =================================================================*/

/*-----------------------------------------------------------------
  Types : CVLsMemRecB, CVLsMemB

  CVodeSetLinearSolverB attaches such a structure to the lmemB
  field of CVodeBMem
  -----------------------------------------------------------------*/
typedef struct CVLsMemRecB {

  CVLsJacFnB            jacB;
  CVLsJacFnBS           jacBS;
  CVLsJacTimesSetupFnB  jtsetupB;
  CVLsJacTimesSetupFnBS jtsetupBS;
  CVLsJacTimesVecFnB    jtimesB;
  CVLsJacTimesVecFnBS   jtimesBS;
  CVLsPrecSetupFnB      psetB;
  CVLsPrecSetupFnBS     psetBS;
  CVLsPrecSolveFnB      psolveB;
  CVLsPrecSolveFnBS     psolveBS;
  void                 *P_dataB;

} *CVLsMemB;


/*-----------------------------------------------------------------
  Prototypes of internal functions
  -----------------------------------------------------------------*/

int cvLsFreeB(CVodeBMem cvb_mem);
int cvLs_AccessLMemB(void *cvode_mem, int which, const char *fname,
                     CVodeMem *cv_mem, CVadjMem *ca_mem,
                     CVodeBMem *cvB_mem, CVLsMemB *cvlsB_mem);
int cvLs_AccessLMemBCur(void *cvode_mem, const char *fname,
                        CVodeMem *cv_mem, CVadjMem *ca_mem,
                        CVodeBMem *cvB_mem, CVLsMemB *cvlsB_mem);


/*=================================================================
  Error Messages
  =================================================================*/

#define MSG_LS_CVMEM_NULL     "Integrator memory is NULL."
#define MSG_LS_MEM_FAIL       "A memory request failed."
#define MSG_LS_BAD_NVECTOR    "A required vector operation is not implemented."
#define MSG_LS_BAD_SIZES      "Illegal bandwidth parameter(s). Must have 0 <=  ml, mu <= N-1."
#define MSG_LS_BAD_LSTYPE     "Incompatible linear solver type."
#define MSG_LS_BAD_PRETYPE    "Illegal value for pretype. Legal values are PREC_NONE, PREC_LEFT, PREC_RIGHT, and PREC_BOTH."
#define MSG_LS_PSOLVE_REQ     "pretype != PREC_NONE, but PSOLVE = NULL is illegal."
#define MSG_LS_LMEM_NULL      "Linear solver memory is NULL."
#define MSG_LS_BAD_GSTYPE     "Illegal value for gstype. Legal values are MODIFIED_GS and CLASSICAL_GS."
#define MSG_LS_BAD_EPLIN      "eplifac < 0 illegal."

#define MSG_LS_PSET_FAILED    "The preconditioner setup routine failed in an unrecoverable manner."
#define MSG_LS_PSOLVE_FAILED  "The preconditioner solve routine failed in an unrecoverable manner."
#define MSG_LS_JTSETUP_FAILED "The Jacobian x vector setup routine failed in an unrecoverable manner."
#define MSG_LS_JTIMES_FAILED  "The Jacobian x vector routine failed in an unrecoverable manner."
#define MSG_LS_JACFUNC_FAILED "The Jacobian routine failed in an unrecoverable manner."
#define MSG_LS_SUNMAT_FAILED  "A SUNMatrix routine failed in an unrecoverable manner."

#define MSG_LS_NO_ADJ         "Illegal attempt to call before calling CVodeAdjMalloc."
#define MSG_LS_BAD_WHICH      "Illegal value for which."
#define MSG_LS_LMEMB_NULL     "Linear solver memory is NULL for the backward integration."
#define MSG_LS_BAD_TINTERP    "Bad t for interpolation."


#ifdef __cplusplus
}
#endif

#endif
