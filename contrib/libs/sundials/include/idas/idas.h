/* -----------------------------------------------------------------
 * Programmer(s): Radu Serban @ LLNL
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
 * This is the header file for the main IDAS solver.
 * -----------------------------------------------------------------*/

#ifndef _IDAS_H
#define _IDAS_H

#include <stdio.h>
#include <sundials/sundials_nvector.h>
#include <sundials/sundials_nonlinearsolver.h>
#include <idas/idas_ls.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/* -----------------
 * IDAS Constants
 * ----------------- */

/* itask */
#define IDA_NORMAL           1
#define IDA_ONE_STEP         2

/* icopt */
#define IDA_YA_YDP_INIT      1
#define IDA_Y_INIT           2

/* ism */
#define IDA_SIMULTANEOUS     1
#define IDA_STAGGERED        2

/* DQtype */
#define IDA_CENTERED         1
#define IDA_FORWARD          2

/* interp */
#define IDA_HERMITE          1
#define IDA_POLYNOMIAL       2

/* return values */

#define IDA_SUCCESS          0
#define IDA_TSTOP_RETURN     1
#define IDA_ROOT_RETURN      2

#define IDA_WARNING          99

#define IDA_TOO_MUCH_WORK   -1
#define IDA_TOO_MUCH_ACC    -2
#define IDA_ERR_FAIL        -3
#define IDA_CONV_FAIL       -4

#define IDA_LINIT_FAIL      -5
#define IDA_LSETUP_FAIL     -6
#define IDA_LSOLVE_FAIL     -7
#define IDA_RES_FAIL        -8
#define IDA_REP_RES_ERR     -9
#define IDA_RTFUNC_FAIL     -10
#define IDA_CONSTR_FAIL     -11

#define IDA_FIRST_RES_FAIL  -12
#define IDA_LINESEARCH_FAIL -13
#define IDA_NO_RECOVERY     -14
#define IDA_NLS_INIT_FAIL   -15
#define IDA_NLS_SETUP_FAIL  -16

#define IDA_MEM_NULL        -20
#define IDA_MEM_FAIL        -21
#define IDA_ILL_INPUT       -22
#define IDA_NO_MALLOC       -23
#define IDA_BAD_EWT         -24
#define IDA_BAD_K           -25
#define IDA_BAD_T           -26
#define IDA_BAD_DKY         -27
#define IDA_VECTOROP_ERR    -28

#define IDA_NO_QUAD         -30
#define IDA_QRHS_FAIL       -31
#define IDA_FIRST_QRHS_ERR  -32
#define IDA_REP_QRHS_ERR    -33

#define IDA_NO_SENS         -40
#define IDA_SRES_FAIL       -41
#define IDA_REP_SRES_ERR    -42
#define IDA_BAD_IS          -43

#define IDA_NO_QUADSENS     -50
#define IDA_QSRHS_FAIL      -51
#define IDA_FIRST_QSRHS_ERR -52
#define IDA_REP_QSRHS_ERR   -53

#define IDA_UNRECOGNIZED_ERROR -99

/* adjoint return values */

#define IDA_NO_ADJ          -101
#define IDA_NO_FWD          -102
#define IDA_NO_BCK          -103
#define IDA_BAD_TB0         -104
#define IDA_REIFWD_FAIL     -105
#define IDA_FWD_FAIL        -106
#define IDA_GETY_BADT       -107

/* ------------------------------
 * User-Supplied Function Types
 * ------------------------------ */

typedef int (*IDAResFn)(realtype tt, N_Vector yy, N_Vector yp,
                        N_Vector rr, void *user_data);

typedef int (*IDARootFn)(realtype t, N_Vector y, N_Vector yp,
                         realtype *gout, void *user_data);

typedef int (*IDAEwtFn)(N_Vector y, N_Vector ewt, void *user_data);

typedef void (*IDAErrHandlerFn)(int error_code,
                                const char *module, const char *function,
                                char *msg, void *user_data);

typedef int (*IDAQuadRhsFn)(realtype tres, N_Vector yy, N_Vector yp,
                            N_Vector rrQ, void *user_data);

typedef int (*IDASensResFn)(int Ns, realtype t,
                            N_Vector yy, N_Vector yp, N_Vector resval,
                            N_Vector *yyS, N_Vector *ypS,
                            N_Vector *resvalS, void *user_data,
                            N_Vector tmp1, N_Vector tmp2, N_Vector tmp3);

typedef int (*IDAQuadSensRhsFn)(int Ns, realtype t,
                               N_Vector yy, N_Vector yp,
                               N_Vector *yyS, N_Vector *ypS,
                               N_Vector rrQ, N_Vector *rhsvalQS,
                               void *user_data,
                               N_Vector yytmp, N_Vector yptmp, N_Vector tmpQS);

typedef int (*IDAResFnB)(realtype tt,
                         N_Vector yy, N_Vector yp,
                         N_Vector yyB, N_Vector ypB,
                         N_Vector rrB, void *user_dataB);

typedef int (*IDAResFnBS)(realtype t,
                          N_Vector yy, N_Vector yp,
                          N_Vector *yyS, N_Vector *ypS,
                          N_Vector yyB, N_Vector ypB,
                          N_Vector rrBS, void *user_dataB);

typedef int (*IDAQuadRhsFnB)(realtype tt,
                             N_Vector yy, N_Vector yp,
                             N_Vector yyB, N_Vector ypB,
                             N_Vector rhsvalBQ, void *user_dataB);

typedef int (*IDAQuadRhsFnBS)(realtype t,
                              N_Vector yy, N_Vector yp,
                              N_Vector *yyS, N_Vector *ypS,
                              N_Vector yyB, N_Vector ypB,
                              N_Vector rhsvalBQS, void *user_dataB);


/* ---------------------------------------
 * Exported Functions -- Forward Problems
 * --------------------------------------- */

/* Initialization functions */
SUNDIALS_EXPORT void *IDACreate(void);

SUNDIALS_EXPORT int IDAInit(void *ida_mem, IDAResFn res, realtype t0,
                            N_Vector yy0, N_Vector yp0);
SUNDIALS_EXPORT int IDAReInit(void *ida_mem, realtype t0, N_Vector yy0,
                              N_Vector yp0);

/* Tolerance input functions */
SUNDIALS_EXPORT int IDASStolerances(void *ida_mem, realtype reltol,
                                    realtype abstol);
SUNDIALS_EXPORT int IDASVtolerances(void *ida_mem, realtype reltol,
                                    N_Vector abstol);
SUNDIALS_EXPORT int IDAWFtolerances(void *ida_mem, IDAEwtFn efun);

/* Initial condition calculation function */
SUNDIALS_EXPORT int IDACalcIC(void *ida_mem, int icopt, realtype tout1);

/* Initial condition calculation optional input functions */
SUNDIALS_EXPORT int IDASetNonlinConvCoefIC(void *ida_mem, realtype epiccon);
SUNDIALS_EXPORT int IDASetMaxNumStepsIC(void *ida_mem, int maxnh);
SUNDIALS_EXPORT int IDASetMaxNumJacsIC(void *ida_mem, int maxnj);
SUNDIALS_EXPORT int IDASetMaxNumItersIC(void *ida_mem, int maxnit);
SUNDIALS_EXPORT int IDASetLineSearchOffIC(void *ida_mem, booleantype lsoff);
SUNDIALS_EXPORT int IDASetStepToleranceIC(void *ida_mem, realtype steptol);
SUNDIALS_EXPORT int IDASetMaxBacksIC(void *ida_mem, int maxbacks);

/* Optional input functions */
SUNDIALS_EXPORT int IDASetErrHandlerFn(void *ida_mem, IDAErrHandlerFn ehfun,
                                       void *eh_data);
SUNDIALS_EXPORT int IDASetErrFile(void *ida_mem, FILE *errfp);
SUNDIALS_EXPORT int IDASetUserData(void *ida_mem, void *user_data);
SUNDIALS_EXPORT int IDASetMaxOrd(void *ida_mem, int maxord);
SUNDIALS_EXPORT int IDASetMaxNumSteps(void *ida_mem, long int mxsteps);
SUNDIALS_EXPORT int IDASetInitStep(void *ida_mem, realtype hin);
SUNDIALS_EXPORT int IDASetMaxStep(void *ida_mem, realtype hmax);
SUNDIALS_EXPORT int IDASetStopTime(void *ida_mem, realtype tstop);
SUNDIALS_EXPORT int IDASetNonlinConvCoef(void *ida_mem, realtype epcon);
SUNDIALS_EXPORT int IDASetMaxErrTestFails(void *ida_mem, int maxnef);
SUNDIALS_EXPORT int IDASetMaxNonlinIters(void *ida_mem, int maxcor);
SUNDIALS_EXPORT int IDASetMaxConvFails(void *ida_mem, int maxncf);
SUNDIALS_EXPORT int IDASetSuppressAlg(void *ida_mem, booleantype suppressalg);
SUNDIALS_EXPORT int IDASetId(void *ida_mem, N_Vector id);
SUNDIALS_EXPORT int IDASetConstraints(void *ida_mem, N_Vector constraints);

SUNDIALS_EXPORT int IDASetNonlinearSolver(void *ida_mem,
                                          SUNNonlinearSolver NLS);

/* Rootfinding initialization function */
SUNDIALS_EXPORT int IDARootInit(void *ida_mem, int nrtfn, IDARootFn g);

/* Rootfinding optional input functions */
SUNDIALS_EXPORT int IDASetRootDirection(void *ida_mem, int *rootdir);
SUNDIALS_EXPORT int IDASetNoInactiveRootWarn(void *ida_mem);

/* Solver function */
SUNDIALS_EXPORT int IDASolve(void *ida_mem, realtype tout, realtype *tret,
                             N_Vector yret, N_Vector ypret, int itask);

/* Dense output function */
SUNDIALS_EXPORT int IDAGetDky(void *ida_mem, realtype t, int k, N_Vector dky);

/* Optional output functions */
SUNDIALS_EXPORT int IDAGetWorkSpace(void *ida_mem, long int *lenrw,
                                    long int *leniw);
SUNDIALS_EXPORT int IDAGetNumSteps(void *ida_mem, long int *nsteps);
SUNDIALS_EXPORT int IDAGetNumResEvals(void *ida_mem, long int *nrevals);
SUNDIALS_EXPORT int IDAGetNumLinSolvSetups(void *ida_mem, long int *nlinsetups);
SUNDIALS_EXPORT int IDAGetNumErrTestFails(void *ida_mem, long int *netfails);
SUNDIALS_EXPORT int IDAGetNumBacktrackOps(void *ida_mem, long int *nbacktr);
SUNDIALS_EXPORT int IDAGetConsistentIC(void *ida_mem, N_Vector yy0_mod,
                                       N_Vector yp0_mod);
SUNDIALS_EXPORT int IDAGetLastOrder(void *ida_mem, int *klast);
SUNDIALS_EXPORT int IDAGetCurrentOrder(void *ida_mem, int *kcur);
SUNDIALS_EXPORT int IDAGetActualInitStep(void *ida_mem, realtype *hinused);
SUNDIALS_EXPORT int IDAGetLastStep(void *ida_mem, realtype *hlast);
SUNDIALS_EXPORT int IDAGetCurrentStep(void *ida_mem, realtype *hcur);
SUNDIALS_EXPORT int IDAGetCurrentTime(void *ida_mem, realtype *tcur);
SUNDIALS_EXPORT int IDAGetTolScaleFactor(void *ida_mem, realtype *tolsfact);
SUNDIALS_EXPORT int IDAGetErrWeights(void *ida_mem, N_Vector eweight);
SUNDIALS_EXPORT int IDAGetEstLocalErrors(void *ida_mem, N_Vector ele);
SUNDIALS_EXPORT int IDAGetNumGEvals(void *ida_mem, long int *ngevals);
SUNDIALS_EXPORT int IDAGetRootInfo(void *ida_mem, int *rootsfound);
SUNDIALS_EXPORT int IDAGetIntegratorStats(void *ida_mem, long int *nsteps,
                                          long int *nrevals,
                                          long int *nlinsetups,
                                          long int *netfails,
                                          int *qlast, int *qcur,
                                          realtype *hinused, realtype *hlast,
                                          realtype *hcur, realtype *tcur);
SUNDIALS_EXPORT int IDAGetNumNonlinSolvIters(void *ida_mem, long int *nniters);
SUNDIALS_EXPORT int IDAGetNumNonlinSolvConvFails(void *ida_mem,
                                                 long int *nncfails);
SUNDIALS_EXPORT int IDAGetNonlinSolvStats(void *ida_mem, long int *nniters,
                                          long int *nncfails);
SUNDIALS_EXPORT char *IDAGetReturnFlagName(long int flag);

/* Free function */
SUNDIALS_EXPORT void IDAFree(void **ida_mem);


/* ---------------------------------
 * Exported Functions -- Quadrature
 * --------------------------------- */

/* Initialization functions */
SUNDIALS_EXPORT int IDAQuadInit(void *ida_mem, IDAQuadRhsFn rhsQ, N_Vector yQ0);
SUNDIALS_EXPORT int IDAQuadReInit(void *ida_mem, N_Vector yQ0);

/* Tolerance input functions */
SUNDIALS_EXPORT int IDAQuadSStolerances(void *ida_mem, realtype reltolQ,
                                        realtype abstolQ);
SUNDIALS_EXPORT int IDAQuadSVtolerances(void *ida_mem, realtype reltolQ,
                                        N_Vector abstolQ);

/* Optional input specification functions */
SUNDIALS_EXPORT int IDASetQuadErrCon(void *ida_mem, booleantype errconQ);

/* Extraction and dense output functions */
SUNDIALS_EXPORT int IDAGetQuad(void *ida_mem, realtype *t, N_Vector yQout);
SUNDIALS_EXPORT int IDAGetQuadDky(void *ida_mem, realtype t, int k,
                                  N_Vector dky);

/* Optional output specification functions */
SUNDIALS_EXPORT int IDAGetQuadNumRhsEvals(void *ida_mem, long int *nrhsQevals);
SUNDIALS_EXPORT int IDAGetQuadNumErrTestFails(void *ida_mem,
                                              long int *nQetfails);
SUNDIALS_EXPORT int IDAGetQuadErrWeights(void *ida_mem, N_Vector eQweight);
SUNDIALS_EXPORT int IDAGetQuadStats(void *ida_mem, long int *nrhsQevals,
                                    long int *nQetfails);

/* Free function */
SUNDIALS_EXPORT void IDAQuadFree(void *ida_mem);


/* ------------------------------------
 * Exported Functions -- Sensitivities
 * ------------------------------------ */

/* Initialization functions */
SUNDIALS_EXPORT int IDASensInit(void *ida_mem, int Ns, int ism,
                                IDASensResFn resS, N_Vector *yS0,
                                N_Vector *ypS0);
SUNDIALS_EXPORT int IDASensReInit(void *ida_mem, int ism, N_Vector *yS0,
                                  N_Vector *ypS0);

/* Tolerance input functions */
SUNDIALS_EXPORT int IDASensSStolerances(void *ida_mem, realtype reltolS,
                                        realtype *abstolS);
SUNDIALS_EXPORT int IDASensSVtolerances(void *ida_mem, realtype reltolS,
                                        N_Vector *abstolS);
SUNDIALS_EXPORT int IDASensEEtolerances(void *ida_mem);

/* Initial condition calculation function */
SUNDIALS_EXPORT int IDAGetSensConsistentIC(void *ida_mem, N_Vector *yyS0,
                                           N_Vector *ypS0);

/* Optional input specification functions */
SUNDIALS_EXPORT int IDASetSensDQMethod(void *ida_mem, int DQtype,
                                       realtype DQrhomax);
SUNDIALS_EXPORT int IDASetSensErrCon(void *ida_mem, booleantype errconS);
SUNDIALS_EXPORT int IDASetSensMaxNonlinIters(void *ida_mem, int maxcorS);
SUNDIALS_EXPORT int IDASetSensParams(void *ida_mem, realtype *p, realtype *pbar,
                                     int *plist);

/* Integrator nonlinear solver specification functions */
SUNDIALS_EXPORT int IDASetNonlinearSolverSensSim(void *ida_mem,
                                                 SUNNonlinearSolver NLS);
SUNDIALS_EXPORT int IDASetNonlinearSolverSensStg(void *ida_mem,
                                                 SUNNonlinearSolver NLS);

/* Enable/disable sensitivities */
SUNDIALS_EXPORT int IDASensToggleOff(void *ida_mem);

/* Extraction and dense output functions */
SUNDIALS_EXPORT int IDAGetSens(void *ida_mem, realtype *tret, N_Vector *yySout);
SUNDIALS_EXPORT int IDAGetSens1(void *ida_mem, realtype *tret, int is,
                                N_Vector yySret);

SUNDIALS_EXPORT int IDAGetSensDky(void *ida_mem, realtype t, int k,
                                  N_Vector *dkyS);
SUNDIALS_EXPORT int IDAGetSensDky1(void *ida_mem, realtype t, int k, int is,
                                   N_Vector dkyS);

/* Optional output specification functions */
SUNDIALS_EXPORT int IDAGetSensNumResEvals(void *ida_mem, long int *nresSevals);
SUNDIALS_EXPORT int IDAGetNumResEvalsSens(void *ida_mem, long int *nresevalsS);
SUNDIALS_EXPORT int IDAGetSensNumErrTestFails(void *ida_mem,
                                              long int *nSetfails);
SUNDIALS_EXPORT int IDAGetSensNumLinSolvSetups(void *ida_mem,
                                               long int *nlinsetupsS);
SUNDIALS_EXPORT int IDAGetSensErrWeights(void *ida_mem, N_Vector_S eSweight);
SUNDIALS_EXPORT int IDAGetSensStats(void *ida_mem, long int *nresSevals,
                                    long int *nresevalsS, long int *nSetfails,
                                    long int *nlinsetupsS);
SUNDIALS_EXPORT int IDAGetSensNumNonlinSolvIters(void *ida_mem,
                                                 long int *nSniters);
SUNDIALS_EXPORT int IDAGetSensNumNonlinSolvConvFails(void *ida_mem,
                                                     long int *nSncfails);
SUNDIALS_EXPORT int IDAGetSensNonlinSolvStats(void *ida_mem,
                                              long int *nSniters,
                                              long int *nSncfails);

/* Free function */
SUNDIALS_EXPORT void IDASensFree(void *ida_mem);


/* -------------------------------------------------------
 * Exported Functions -- Sensitivity dependent quadrature
 * ------------------------------------------------------- */

/* Initialization functions */
SUNDIALS_EXPORT int IDAQuadSensInit(void *ida_mem, IDAQuadSensRhsFn resQS,
                                    N_Vector *yQS0);
SUNDIALS_EXPORT int IDAQuadSensReInit(void *ida_mem, N_Vector *yQS0);

/* Tolerance input functions */
SUNDIALS_EXPORT int IDAQuadSensSStolerances(void *ida_mem, realtype reltolQS,
                                            realtype *abstolQS);
SUNDIALS_EXPORT int IDAQuadSensSVtolerances(void *ida_mem, realtype reltolQS,
                                            N_Vector *abstolQS);
SUNDIALS_EXPORT int IDAQuadSensEEtolerances(void *ida_mem);

/* Optional input specification functions */
SUNDIALS_EXPORT int IDASetQuadSensErrCon(void *ida_mem, booleantype errconQS);

/* Extraction and dense output functions */
SUNDIALS_EXPORT int IDAGetQuadSens(void *ida_mem, realtype *tret,
                                   N_Vector *yyQSout);
SUNDIALS_EXPORT int IDAGetQuadSens1(void *ida_mem, realtype *tret, int is,
                                    N_Vector yyQSret);
SUNDIALS_EXPORT int IDAGetQuadSensDky(void *ida_mem, realtype t, int k,
                                      N_Vector *dkyQS);
SUNDIALS_EXPORT int IDAGetQuadSensDky1(void *ida_mem, realtype t, int k, int is,
                                       N_Vector dkyQS);

/* Optional output specification functions */
SUNDIALS_EXPORT int IDAGetQuadSensNumRhsEvals(void *ida_mem,
                                              long int *nrhsQSevals);
SUNDIALS_EXPORT int IDAGetQuadSensNumErrTestFails(void *ida_mem,
                                                  long int *nQSetfails);
SUNDIALS_EXPORT int IDAGetQuadSensErrWeights(void *ida_mem,
                                             N_Vector *eQSweight);
SUNDIALS_EXPORT int IDAGetQuadSensStats(void *ida_mem,
                                          long int *nrhsQSevals,
                                          long int *nQSetfails);

/* Free function */
SUNDIALS_EXPORT void IDAQuadSensFree(void* ida_mem);


/* ----------------------------------------
 * Exported Functions -- Backward Problems
 * ---------------------------------------- */

/* Initialization functions */

SUNDIALS_EXPORT int IDAAdjInit(void *ida_mem, long int steps, int interp);

SUNDIALS_EXPORT int IDAAdjReInit(void *ida_mem);

SUNDIALS_EXPORT void IDAAdjFree(void *ida_mem);

/* Backward Problem Setup Functions */

SUNDIALS_EXPORT int IDACreateB(void *ida_mem, int *which);

SUNDIALS_EXPORT int IDAInitB(void *ida_mem, int which, IDAResFnB resB,
                             realtype tB0, N_Vector yyB0, N_Vector ypB0);

SUNDIALS_EXPORT int IDAInitBS(void *ida_mem, int which, IDAResFnBS resS,
                              realtype tB0, N_Vector yyB0, N_Vector ypB0);

SUNDIALS_EXPORT int IDAReInitB(void *ida_mem, int which,
                               realtype tB0, N_Vector yyB0, N_Vector ypB0);

SUNDIALS_EXPORT int IDASStolerancesB(void *ida_mem, int which,
                                     realtype relTolB, realtype absTolB);
SUNDIALS_EXPORT int IDASVtolerancesB(void *ida_mem, int which,
                                     realtype relTolB, N_Vector absTolB);

SUNDIALS_EXPORT int IDAQuadInitB(void *ida_mem, int which,
                                 IDAQuadRhsFnB rhsQB, N_Vector yQB0);

SUNDIALS_EXPORT int IDAQuadInitBS(void *ida_mem, int which,
                                  IDAQuadRhsFnBS rhsQS, N_Vector yQB0);

SUNDIALS_EXPORT int IDAQuadReInitB(void *ida_mem, int which, N_Vector yQB0);

SUNDIALS_EXPORT int IDAQuadSStolerancesB(void *ida_mem, int which,
                                         realtype reltolQB, realtype abstolQB);
SUNDIALS_EXPORT int IDAQuadSVtolerancesB(void *ida_mem, int which,
                                         realtype reltolQB, N_Vector abstolQB);

/* Consistent IC calculation functions */

SUNDIALS_EXPORT int IDACalcICB (void *ida_mem, int which, realtype tout1,
                                N_Vector yy0, N_Vector yp0);

SUNDIALS_EXPORT int IDACalcICBS(void *ida_mem, int which, realtype tout1,
                                N_Vector yy0, N_Vector yp0,
                                N_Vector *yyS0, N_Vector *ypS0);

/* Solver Function For Forward Problems */

SUNDIALS_EXPORT int IDASolveF(void *ida_mem, realtype tout,
                              realtype *tret,
                              N_Vector yret, N_Vector ypret,
                              int itask, int *ncheckPtr);

/* Solver Function For Backward Problems */

SUNDIALS_EXPORT int IDASolveB(void *ida_mem, realtype tBout, int itaskB);

/* Optional Input Functions For Adjoint Problems */

SUNDIALS_EXPORT int IDAAdjSetNoSensi(void *ida_mem);

SUNDIALS_EXPORT int IDASetUserDataB(void *ida_mem, int which, void *user_dataB);
SUNDIALS_EXPORT int IDASetMaxOrdB(void *ida_mem, int which, int maxordB);
SUNDIALS_EXPORT int IDASetMaxNumStepsB(void *ida_mem, int which,
                                       long int mxstepsB);
SUNDIALS_EXPORT int IDASetInitStepB(void *ida_mem, int which, realtype hinB);
SUNDIALS_EXPORT int IDASetMaxStepB(void *ida_mem, int which, realtype hmaxB);
SUNDIALS_EXPORT int IDASetSuppressAlgB(void *ida_mem, int which,
                                       booleantype suppressalgB);
SUNDIALS_EXPORT int IDASetIdB(void *ida_mem, int which, N_Vector idB);
SUNDIALS_EXPORT int IDASetConstraintsB(void *ida_mem, int which,
                                       N_Vector constraintsB);
SUNDIALS_EXPORT int IDASetQuadErrConB(void *ida_mem, int which, int errconQB);

SUNDIALS_EXPORT int IDASetNonlinearSolverB(void *ida_mem, int which,
                                           SUNNonlinearSolver NLS);

/* Extraction And Dense Output Functions For Backward Problems */

SUNDIALS_EXPORT int IDAGetB(void* ida_mem, int which, realtype *tret,
                            N_Vector yy, N_Vector yp);
SUNDIALS_EXPORT int IDAGetQuadB(void *ida_mem, int which,
                                realtype *tret, N_Vector qB);

/* Optional Output Functions For Backward Problems */

SUNDIALS_EXPORT void *IDAGetAdjIDABmem(void *ida_mem, int which);

SUNDIALS_EXPORT int IDAGetConsistentICB(void *ida_mem, int which,
                                        N_Vector yyB0, N_Vector ypB0);

SUNDIALS_EXPORT int IDAGetAdjY(void *ida_mem, realtype t,
                               N_Vector yy, N_Vector yp);

typedef struct {
  void *my_addr;
  void *next_addr;
  realtype t0;
  realtype t1;
  long int nstep;
  int order;
  realtype step;
} IDAadjCheckPointRec;

SUNDIALS_EXPORT int IDAGetAdjCheckPointsInfo(void *ida_mem,
                                             IDAadjCheckPointRec *ckpnt);


/* Undocumented Optional Output Functions For Backward Problems */

/* -----------------------------------------------------------------
 * IDAGetAdjDataPointHermite
 * -----------------------------------------------------------------
 *    Returns the 2 vectors stored for cubic Hermite interpolation
 *    at the data point 'which'. The user must allocate space for
 *    yy and yd. Returns IDA_MEM_NULL if ida_mem is NULL,
 *    IDA_ILL_INPUT if the interpolation type previously specified
 *    is not IDA_HERMITE, or IDA_SUCCESS otherwise.
 * -----------------------------------------------------------------
 * IDAGetAdjDataPointPolynomial
 * -----------------------------------------------------------------
 *    Returns the vector stored for polynomial interpolation
 *    at the data point 'which'. The user must allocate space for
 *    y. Returns IDA_MEM_NULL if ida_mem is NULL, IDA_ILL_INPUT if
 *    the interpolation type previously specified is not
 *    IDA_POLYNOMIAL, or IDA_SUCCESS otherwise.
 * ----------------------------------------------------------------- */

SUNDIALS_EXPORT int IDAGetAdjDataPointHermite(void *ida_mem, int which,
                                              realtype *t, N_Vector yy,
                                              N_Vector yd);

SUNDIALS_EXPORT int IDAGetAdjDataPointPolynomial(void *ida_mem, int which,
                                                 realtype *t, int *order,
                                                 N_Vector y);

/* -----------------------------------------------------------------
 * IDAGetAdjCurrentCheckPoint
 *    Returns the address of the 'active' check point.
 * ----------------------------------------------------------------- */

SUNDIALS_EXPORT int IDAGetAdjCurrentCheckPoint(void *ida_mem, void **addr);


#ifdef __cplusplus
}
#endif

#endif
