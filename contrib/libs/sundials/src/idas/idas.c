/*
 * ----------------------------------------------------------------- 
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
 * This is the implementation file for the main IDAS solver.
 * It is independent of the linear solver in use.
 * -----------------------------------------------------------------
 *
 * EXPORTED FUNCTIONS
 * ------------------
 *   Creation, allocation and re-initialization functions
 *       IDACreate
 *       IDAInit
 *       IDAReInit
 *       IDAQuadInit
 *       IDAQuadReInit
 *       IDAQuadSStolerances
 *       IDAQuadSVtolerances
 *       IDASensInit
 *       IDASensReInit
 *       IDASensToggleOff
 *       IDASensSStolerances
 *       IDASensSVtolerances
 *       IDASensEEtolerances
 *       IDAQuadSensInit
 *       IDAQuadSensReInit
 *       IDARootInit
 *
 *   Main solver function
 *       IDASolve
 *
 *   Interpolated output and extraction functions
 *       IDAGetDky
 *       IDAGetQuad
 *       IDAGetQuadDky
 *       IDAGetSens
 *       IDAGetSens1
 *       IDAGetSensDky
 *       IDAGetSensDky1
 *
 *   Deallocation functions
 *       IDAFree
 *       IDAQuadFree
 *       IDASensFree
 *       IDAQuadSensFree
 *
 * PRIVATE FUNCTIONS 
 * -----------------
 *       IDACheckNvector
 *   Memory allocation/deallocation
 *       IDAAllocVectors
 *       IDAFreeVectors
 *       IDAQuadAllocVectors
 *       IDAQuadFreeVectors
 *       IDASensAllocVectors
 *       IDASensFreeVectors
 *       IDAQuadSensAllocVectors
 *       IDAQuadSensFreeVectors
 *   Initial setup
 *       IDAInitialSetup
 *       IDAEwtSet
 *       IDAEwtSetSS
 *       IDAEwtSetSV
 *       IDAQuadEwtSet
 *       IDAQuadEwtSetSS
 *       IDAQuadEwtSetSV
 *       IDASensEwtSet
 *       IDASensEwtSetEE
 *       IDASensEwtSetSS
 *       IDASensEwtSetSV
 *       IDAQuadSensEwtSet
 *       IDAQuadSensEwtSetEE
 *       IDAQuadSensEwtSetSS
 *       IDAQuadSensEwtSetSV
 *   Stopping tests
 *       IDAStopTest1
 *       IDAStopTest2
 *   Error handler
 *       IDAHandleFailure
 *   Main IDAStep function
 *       IDAStep
 *       IDASetCoeffs
 *   Nonlinear solver functions
 *       IDANls
 *       IDAPredict
 *       IDAQuadNls
 *       IDAQuadSensNls
 *       IDAQuadPredict
 *       IDAQuadSensPredict
 *       IDASensNls
 *       IDASensPredict
 *   Error test
 *       IDATestError
 *       IDAQuadTestError
 *       IDASensTestError
 *       IDAQuadSensTestError
 *       IDARestore
 *   Handler for convergence and/or error test failures
 *       IDAHandleNFlag
 *       IDAReset
 *   Function called after a successful step
 *       IDACompleteStep
 *   Get solution
 *       IDAGetSolution
 *   Norm functions
 *       IDAWrmsNorm
 *       IDASensWrmsNorm
 *       IDAQuadSensWrmsNorm
 *       IDAQuadWrmsNormUpdate
 *       IDASensWrmsNormUpdate
 *       IDAQuadSensWrmsNormUpdate
 *   Functions for rootfinding
 *       IDARcheck1
 *       IDARcheck2
 *       IDARcheck3
 *       IDARootfind
 *   IDA Error message handling functions 
 *       IDAProcessError
 *       IDAErrHandler
 *   Internal DQ approximations for sensitivity RHS
 *       IDASensResDQ
 *       IDASensRes1DQ
 *       IDAQuadSensResDQ
 *       IDAQuadSensRes1DQ
 * -----------------------------------------------------------------
 */

/* 
 * =================================================================
 * IMPORTED HEADER FILES
 * =================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

#include "idas_impl.h"
#include <sundials/sundials_math.h>
#include <sundials/sundials_types.h>
#include <sundials/sundials_nvector_senswrapper.h>
#include <sunnonlinsol/sunnonlinsol_newton.h>

/* 
 * =================================================================
 * IDAS PRIVATE CONSTANTS
 * =================================================================
 */

#define ZERO      RCONST(0.0)    /* real 0.0    */
#define HALF      RCONST(0.5)    /* real 0.5    */
#define QUARTER   RCONST(0.25)   /* real 0.25   */
#define TWOTHIRDS RCONST(0.667)  /* real 2/3    */
#define ONE       RCONST(1.0)    /* real 1.0    */
#define ONEPT5    RCONST(1.5)    /* real 1.5    */
#define TWO       RCONST(2.0)    /* real 2.0    */
#define FOUR      RCONST(4.0)    /* real 4.0    */
#define FIVE      RCONST(5.0)    /* real 5.0    */
#define TEN       RCONST(10.0)   /* real 10.0   */
#define TWELVE    RCONST(12.0)   /* real 12.0   */
#define TWENTY    RCONST(20.0)   /* real 20.0   */
#define HUNDRED   RCONST(100.0)  /* real 100.0  */
#define PT9       RCONST(0.9)    /* real 0.9    */
#define PT99      RCONST(0.99)   /* real 0.99   */
#define PT1       RCONST(0.1)    /* real 0.1    */
#define PT01      RCONST(0.01)   /* real 0.01   */
#define PT001     RCONST(0.001)  /* real 0.001  */
#define PT0001    RCONST(0.0001) /* real 0.0001 */

/* 
 * =================================================================
 * IDAS ROUTINE-SPECIFIC CONSTANTS
 * =================================================================
 */

/* 
 * Control constants for lower-level functions used by IDASolve 
 * ------------------------------------------------------------
 */

/* IDAStep control constants */

#define PREDICT_AGAIN 20

/* Return values for lower level routines used by IDASolve */

#define CONTINUE_STEPS   +99

/* IDACompleteStep constants */

#define UNSET    -1
#define LOWER     1 
#define RAISE     2 
#define MAINTAIN  3

/* IDATestError constants */

#define ERROR_TEST_FAIL +7

/*
 * Control constants for lower-level rootfinding functions
 * -------------------------------------------------------
 */

#define RTFOUND   1
#define CLOSERT   3

/*
 * Control constants for sensitivity DQ
 * ------------------------------------
 */

#define CENTERED1        +1
#define CENTERED2        +2
#define FORWARD1         +3
#define FORWARD2         +4

/*
 * Algorithmic constants
 * ---------------------
 */

#define MXNCF           10  /* max number of convergence failures allowed */
#define MXNEF           10  /* max number of error test failures allowed  */
#define MAXNH            5  /* max. number of h tries in IC calc. */
#define MAXNJ            4  /* max. number of J tries in IC calc. */
#define MAXNI           10  /* max. Newton iterations in IC calc. */
#define EPCON RCONST(0.33)  /* Newton convergence test constant */
#define MAXBACKS       100  /* max backtracks per Newton step in IDACalcIC */


/* IDANewtonIter constants */

#define MAXIT 4
#define XRATE RCONST(0.25) /* constant for updating Jacobian/preconditioner */

/* 
 * =================================================================
 * PRIVATE FUNCTION PROTOTYPES
 * =================================================================
 */

static booleantype IDACheckNvector(N_Vector tmpl);

/* Memory allocation/deallocation */

static booleantype IDAAllocVectors(IDAMem IDA_mem, N_Vector tmpl);
static void IDAFreeVectors(IDAMem IDA_mem);

static booleantype IDAQuadAllocVectors(IDAMem IDA_mem, N_Vector tmpl);
static void IDAQuadFreeVectors(IDAMem IDA_mem);

static booleantype IDASensAllocVectors(IDAMem IDA_mem, N_Vector tmpl);
static void IDASensFreeVectors(IDAMem IDA_mem);

static booleantype IDAQuadSensAllocVectors(IDAMem ida_mem, N_Vector tmpl);
static void IDAQuadSensFreeVectors(IDAMem ida_mem);

/* Initial setup */

int IDAInitialSetup(IDAMem IDA_mem);

static int IDAEwtSetSS(IDAMem IDA_mem, N_Vector ycur, N_Vector weight);
static int IDAEwtSetSV(IDAMem IDA_mem, N_Vector ycur, N_Vector weight);

static int IDAQuadEwtSet(IDAMem IDA_mem, N_Vector qcur, N_Vector weightQ);
static int IDAQuadEwtSetSS(IDAMem IDA_mem, N_Vector qcur, N_Vector weightQ);
static int IDAQuadEwtSetSV(IDAMem IDA_mem, N_Vector qcur, N_Vector weightQ);

/* Used in IC for sensitivities. */
int IDASensEwtSet(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS);
static int IDASensEwtSetEE(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS);
static int IDASensEwtSetSS(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS);
static int IDASensEwtSetSV(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS);

int IDAQuadSensEwtSet(IDAMem IDA_mem, N_Vector *yQScur, N_Vector *weightQS);
static int IDAQuadSensEwtSetEE(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS);
static int IDAQuadSensEwtSetSS(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS);
static int IDAQuadSensEwtSetSV(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS);

/* Main IDAStep function */

static int IDAStep(IDAMem IDA_mem);

/* Function called at beginning of step */

static void IDASetCoeffs(IDAMem IDA_mem, realtype *ck);

/* Nonlinear solver functions */

static void IDAPredict(IDAMem IDA_mem);
static void IDAQuadPredict(IDAMem IDA_mem);
static void IDASensPredict(IDAMem IDA_mem, N_Vector *yySens, N_Vector *ypSens);
static void IDAQuadSensPredict(IDAMem IDA_mem, N_Vector *yQS, N_Vector *ypQS);

static int IDANls(IDAMem IDA_mem);
static int IDASensNls(IDAMem IDA_mem);

static int IDAQuadNls(IDAMem IDA_mem);
static int IDAQuadSensNls(IDAMem IDA_mem);

/* Error tests */

static int IDATestError(IDAMem IDA_mem, realtype ck, 
                        realtype *err_k, realtype *err_km1, realtype *err_km2);
static int IDAQuadTestError(IDAMem IDA_mem, realtype ck, 
                            realtype *err_k, realtype *err_km1, realtype *err_km2);
static int IDASensTestError(IDAMem IDA_mem, realtype ck, 
                            realtype *err_k, realtype *err_km1, realtype *err_km2);
static int IDAQuadSensTestError(IDAMem IDA_mem, realtype ck, 
                                realtype *err_k, realtype *err_km1, realtype *err_km2);

/* Handling of convergence and/or error test failures */

static void IDARestore(IDAMem IDA_mem, realtype saved_t);
static int IDAHandleNFlag(IDAMem IDA_mem, int nflag, realtype err_k, realtype err_km1,
                          long int *ncfnPtr, int *ncfPtr, long int *netfPtr, int *nefPtr);
static void IDAReset(IDAMem IDA_mem);

/* Function called after a successful step */

static void IDACompleteStep(IDAMem IDA_mem, realtype err_k, realtype err_km1);

/* Function called to evaluate the solutions y(t) and y'(t) at t. Also used in IDAA */
int IDAGetSolution(void *ida_mem, realtype t, N_Vector yret, N_Vector ypret);

/* Stopping tests and failure handling */

static int IDAStopTest1(IDAMem IDA_mem, realtype tout,realtype *tret, 
                        N_Vector yret, N_Vector ypret, int itask);
static int IDAStopTest2(IDAMem IDA_mem, realtype tout, realtype *tret, 
                        N_Vector yret, N_Vector ypret, int itask);
static int IDAHandleFailure(IDAMem IDA_mem, int sflag);

/* Norm functions */

static realtype IDAQuadWrmsNormUpdate(IDAMem IDA_mem, realtype old_nrm,
                                      N_Vector xQ, N_Vector wQ);

static realtype IDAQuadSensWrmsNorm(IDAMem IDA_mem, N_Vector *xQS, N_Vector *wQS);
static realtype IDAQuadSensWrmsNormUpdate(IDAMem IDA_mem, realtype old_nrm, 
                                          N_Vector *xQS, N_Vector *wQS);

/* Functions for rootfinding */

static int IDARcheck1(IDAMem IDA_mem);
static int IDARcheck2(IDAMem IDA_mem);
static int IDARcheck3(IDAMem IDA_mem);
static int IDARootfind(IDAMem IDA_mem);

/* Sensitivity residual DQ function */

static int IDASensRes1DQ(int Ns, realtype t, 
                         N_Vector yy, N_Vector yp, N_Vector resval,
                         int iS,
                         N_Vector yyS, N_Vector ypS, N_Vector resvalS,
                         void *user_dataS,
                         N_Vector ytemp, N_Vector yptemp, N_Vector restemp);

static int IDAQuadSensRhsInternalDQ(int Ns, realtype t, 
                                    N_Vector yy,   N_Vector yp,
                                    N_Vector *yyS, N_Vector *ypS,
                                    N_Vector rrQ,  N_Vector *resvalQS,
                                    void *ida_mem,  
                                    N_Vector yytmp, N_Vector yptmp, N_Vector tmpQS);

static int IDAQuadSensRhs1InternalDQ(IDAMem IDA_mem, int is, realtype t, 
                                     N_Vector yy, N_Vector y, 
                                     N_Vector yyS, N_Vector ypS,
                                     N_Vector resvalQ, N_Vector resvalQS, 
                                     N_Vector yytmp, N_Vector yptmp, N_Vector tmpQS);
/* 
 * =================================================================
 * EXPORTED FUNCTIONS IMPLEMENTATION
 * =================================================================
 */

/* 
 * -----------------------------------------------------------------
 * Creation, allocation and re-initialization functions
 * -----------------------------------------------------------------
 */

/* 
 * IDACreate
 *
 * IDACreate creates an internal memory block for a problem to 
 * be solved by IDA.
 * If successful, IDACreate returns a pointer to the problem memory. 
 * This pointer should be passed to IDAInit.  
 * If an initialization error occurs, IDACreate prints an error 
 * message to standard err and returns NULL. 
 */

void *IDACreate(void)
{
  IDAMem IDA_mem;

  IDA_mem = NULL;
  IDA_mem = (IDAMem) malloc(sizeof(struct IDAMemRec));
  if (IDA_mem == NULL) {
    IDAProcessError(NULL, 0, "IDAS", "IDACreate", MSG_MEM_FAIL);
    return (NULL);
  }

  /* Zero out ida_mem */
  memset(IDA_mem, 0, sizeof(struct IDAMemRec));

  /* Set unit roundoff in IDA_mem */
  IDA_mem->ida_uround = UNIT_ROUNDOFF;

  /* Set default values for integrator optional inputs */
  IDA_mem->ida_res         = NULL;
  IDA_mem->ida_user_data   = NULL;
  IDA_mem->ida_itol        = IDA_NN;
  IDA_mem->ida_user_efun   = SUNFALSE;
  IDA_mem->ida_efun        = NULL;
  IDA_mem->ida_edata       = NULL;
  IDA_mem->ida_ehfun       = IDAErrHandler;
  IDA_mem->ida_eh_data     = IDA_mem;
  IDA_mem->ida_errfp       = stderr;
  IDA_mem->ida_maxord      = MAXORD_DEFAULT;
  IDA_mem->ida_mxstep      = MXSTEP_DEFAULT;
  IDA_mem->ida_hmax_inv    = HMAX_INV_DEFAULT;
  IDA_mem->ida_hin         = ZERO;
  IDA_mem->ida_epcon       = EPCON;
  IDA_mem->ida_maxnef      = MXNEF;
  IDA_mem->ida_maxncf      = MXNCF;
  IDA_mem->ida_maxcor      = MAXIT;
  IDA_mem->ida_suppressalg = SUNFALSE;
  IDA_mem->ida_id          = NULL;
  IDA_mem->ida_constraints = NULL;
  IDA_mem->ida_constraintsSet = SUNFALSE;
  IDA_mem->ida_tstopset    = SUNFALSE;

  /* set the saved value maxord_alloc */
  IDA_mem->ida_maxord_alloc = MAXORD_DEFAULT;

  /* Set default values for IC optional inputs */
  IDA_mem->ida_epiccon = PT01 * EPCON;
  IDA_mem->ida_maxnh   = MAXNH;
  IDA_mem->ida_maxnj   = MAXNJ;
  IDA_mem->ida_maxnit  = MAXNI;
  IDA_mem->ida_maxbacks  = MAXBACKS;
  IDA_mem->ida_lsoff   = SUNFALSE;
  IDA_mem->ida_steptol = SUNRpowerR(IDA_mem->ida_uround, TWOTHIRDS);

  /* Set default values for quad. optional inputs */
  IDA_mem->ida_quadr      = SUNFALSE;
  IDA_mem->ida_rhsQ       = NULL;
  IDA_mem->ida_errconQ    = SUNFALSE;
  IDA_mem->ida_itolQ      = IDA_NN;

  /* Set default values for sensi. optional inputs */
  IDA_mem->ida_sensi        = SUNFALSE;
  IDA_mem->ida_user_dataS   = (void *)IDA_mem;
  IDA_mem->ida_resS         = IDASensResDQ;
  IDA_mem->ida_resSDQ       = SUNTRUE;
  IDA_mem->ida_DQtype       = IDA_CENTERED;
  IDA_mem->ida_DQrhomax     = ZERO;
  IDA_mem->ida_p            = NULL;
  IDA_mem->ida_pbar         = NULL;
  IDA_mem->ida_plist        = NULL;
  IDA_mem->ida_errconS      = SUNFALSE;
  IDA_mem->ida_maxcorS      = MAXIT;
  IDA_mem->ida_itolS        = IDA_EE;
  IDA_mem->ida_ism          = -1;     /* initialize to invalid option */

  /* Defaults for sensi. quadr. optional inputs. */
  IDA_mem->ida_quadr_sensi  = SUNFALSE;
  IDA_mem->ida_user_dataQS  = (void *)IDA_mem;
  IDA_mem->ida_rhsQS        = IDAQuadSensRhsInternalDQ;
  IDA_mem->ida_rhsQSDQ      = SUNTRUE;
  IDA_mem->ida_errconQS     = SUNFALSE;
  IDA_mem->ida_itolQS       = IDA_EE;

  /* Set defaults for ASA. */
  IDA_mem->ida_adj     = SUNFALSE;
  IDA_mem->ida_adj_mem = NULL;

  /* Initialize lrw and liw */
  IDA_mem->ida_lrw = 25 + 5*MXORDP1;
  IDA_mem->ida_liw = 38;

  /* No mallocs have been done yet */

  IDA_mem->ida_VatolMallocDone       = SUNFALSE;
  IDA_mem->ida_constraintsMallocDone = SUNFALSE;
  IDA_mem->ida_idMallocDone          = SUNFALSE;
  IDA_mem->ida_MallocDone            = SUNFALSE;

  IDA_mem->ida_VatolQMallocDone      = SUNFALSE;
  IDA_mem->ida_quadMallocDone        = SUNFALSE;

  IDA_mem->ida_VatolSMallocDone      = SUNFALSE;
  IDA_mem->ida_SatolSMallocDone      = SUNFALSE;
  IDA_mem->ida_sensMallocDone        = SUNFALSE;

  IDA_mem->ida_VatolQSMallocDone      = SUNFALSE;
  IDA_mem->ida_SatolQSMallocDone      = SUNFALSE;
  IDA_mem->ida_quadSensMallocDone     = SUNFALSE;

  IDA_mem->ida_adjMallocDone          = SUNFALSE;

  /* Initialize nonlinear solver variables */
  IDA_mem->NLS    = NULL;
  IDA_mem->ownNLS = SUNFALSE;

  IDA_mem->NLSsim        = NULL;
  IDA_mem->ownNLSsim     = SUNFALSE;
  IDA_mem->ycor0Sim      = NULL;
  IDA_mem->ycorSim       = NULL;
  IDA_mem->ewtSim        = NULL;
  IDA_mem->simMallocDone = SUNFALSE;

  IDA_mem->NLSstg        = NULL;
  IDA_mem->ownNLSstg     = SUNFALSE;
  IDA_mem->ycor0Stg      = NULL;
  IDA_mem->ycorStg       = NULL;
  IDA_mem->ewtStg        = NULL;
  IDA_mem->stgMallocDone = SUNFALSE;

  /* Return pointer to IDA memory block */
  return((void *)IDA_mem);
}

/*-----------------------------------------------------------------*/

/*
 * IDAInit
 *
 * IDAInit allocates and initializes memory for a problem. All
 * problem specification inputs are checked for errors. If any
 * error occurs during initialization, it is reported to the 
 * error handler function.
 */

int IDAInit(void *ida_mem, IDAResFn res,
            realtype t0, N_Vector yy0, N_Vector yp0)
{
  int retval;
  IDAMem IDA_mem;
  booleantype nvectorOK, allocOK;
  sunindextype lrw1, liw1;
  SUNNonlinearSolver NLS;

  /* Check ida_mem */

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAInit", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;
  
  /* Check for legal input parameters */
  
  if (yy0 == NULL) { 
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInit", MSG_Y0_NULL);
    return(IDA_ILL_INPUT); 
  }
  
  if (yp0 == NULL) { 
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInit", MSG_YP0_NULL);
    return(IDA_ILL_INPUT); 
  }

  if (res == NULL) { 
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInit", MSG_RES_NULL);
    return(IDA_ILL_INPUT); 
  }

  /* Test if all required vector operations are implemented */

  nvectorOK = IDACheckNvector(yy0);
  if (!nvectorOK) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInit", MSG_BAD_NVECTOR);
    return(IDA_ILL_INPUT);
  }

  /* Set space requirements for one N_Vector */

  if (yy0->ops->nvspace != NULL) {
    N_VSpace(yy0, &lrw1, &liw1);
  } else {
    lrw1 = 0;
    liw1 = 0;
  }
  IDA_mem->ida_lrw1 = lrw1;
  IDA_mem->ida_liw1 = liw1;

  /* Allocate the vectors (using yy0 as a template) */

  allocOK = IDAAllocVectors(IDA_mem, yy0);
  if (!allocOK) {
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDAInit", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  /* Allocate temporary work arrays for fused vector ops */
  IDA_mem->ida_cvals = NULL;
  IDA_mem->ida_cvals = (realtype *) malloc(MXORDP1*sizeof(realtype));

  IDA_mem->ida_Xvecs = NULL;
  IDA_mem->ida_Xvecs = (N_Vector *) malloc(MXORDP1*sizeof(N_Vector));

  IDA_mem->ida_Zvecs = NULL;
  IDA_mem->ida_Zvecs = (N_Vector *) malloc(MXORDP1*sizeof(N_Vector));

  if ((IDA_mem->ida_cvals == NULL) ||
      (IDA_mem->ida_Xvecs == NULL) ||
      (IDA_mem->ida_Zvecs == NULL)) {
    IDAFreeVectors(IDA_mem);
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDAInit", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  /* create a Newton nonlinear solver object by default */
  NLS = SUNNonlinSol_Newton(yy0);

  /* check that nonlinear solver is non-NULL */
  if (NLS == NULL) {
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDAInit", MSG_MEM_FAIL);
    IDAFreeVectors(IDA_mem);
    return(IDA_MEM_FAIL);
  }

  /* attach the nonlinear solver to the IDA memory */
  retval = IDASetNonlinearSolver(IDA_mem, NLS);

  /* check that the nonlinear solver was successfully attached */
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, retval, "IDAS", "IDAInit",
                    "Setting the nonlinear solver failed");
    IDAFreeVectors(IDA_mem);
    SUNNonlinSolFree(NLS);
    return(IDA_MEM_FAIL);
  }

  /* set ownership flag */
  IDA_mem->ownNLS = SUNTRUE;

  /* All error checking is complete at this point */

  /* Copy the input parameters into IDA memory block */

  IDA_mem->ida_res = res;
  IDA_mem->ida_tn  = t0;

  /* Set the linear solver addresses to NULL */

  IDA_mem->ida_linit  = NULL;
  IDA_mem->ida_lsetup = NULL;
  IDA_mem->ida_lsolve = NULL;
  IDA_mem->ida_lperf  = NULL;
  IDA_mem->ida_lfree  = NULL;
  IDA_mem->ida_lmem   = NULL;

  /* Set forceSetup to SUNFALSE */

  IDA_mem->ida_forceSetup = SUNFALSE;

  /* Initialize the phi array */

  N_VScale(ONE, yy0, IDA_mem->ida_phi[0]);  
  N_VScale(ONE, yp0, IDA_mem->ida_phi[1]);  
 
  /* Initialize all the counters and other optional output values */

  IDA_mem->ida_nst     = 0;
  IDA_mem->ida_nre     = 0;
  IDA_mem->ida_ncfn    = 0;
  IDA_mem->ida_netf    = 0;
  IDA_mem->ida_nni     = 0;
  IDA_mem->ida_nsetups = 0;
  
  IDA_mem->ida_kused = 0;
  IDA_mem->ida_hused = ZERO;
  IDA_mem->ida_tolsf = ONE;

  IDA_mem->ida_nge = 0;

  IDA_mem->ida_irfnd = 0;

  /* Initialize counters specific to IC calculation. */
  IDA_mem->ida_nbacktr     = 0;  

  /* Initialize root-finding variables */

  IDA_mem->ida_glo     = NULL;
  IDA_mem->ida_ghi     = NULL;
  IDA_mem->ida_grout   = NULL;
  IDA_mem->ida_iroots  = NULL;
  IDA_mem->ida_rootdir = NULL;
  IDA_mem->ida_gfun    = NULL;
  IDA_mem->ida_nrtfn   = 0;
  IDA_mem->ida_gactive  = NULL;
  IDA_mem->ida_mxgnull  = 1;

  /* Initial setup not done yet */

  IDA_mem->ida_SetupDone = SUNFALSE;

  /* Problem memory has been successfully allocated */

  IDA_mem->ida_MallocDone = SUNTRUE;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * IDAReInit
 *
 * IDAReInit re-initializes IDA's memory for a problem, assuming
 * it has already beeen allocated in a prior IDAInit call.
 * All problem specification inputs are checked for errors.
 * The problem size Neq is assumed to be unchanged since the call
 * to IDAInit, and the maximum order maxord must not be larger.
 * If any error occurs during reinitialization, it is reported to
 * the error handler function.
 * The return value is IDA_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */

int IDAReInit(void *ida_mem,
              realtype t0, N_Vector yy0, N_Vector yp0)
{
  IDAMem IDA_mem;

  /* Check for legal input parameters */
  
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAReInit", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Check if problem was malloc'ed */
  
  if (IDA_mem->ida_MallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_MALLOC, "IDAS", "IDAReInit", MSG_NO_MALLOC);
    return(IDA_NO_MALLOC);
  }

  /* Check for legal input parameters */
  
  if (yy0 == NULL) { 
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAReInit", MSG_Y0_NULL);
    return(IDA_ILL_INPUT); 
  }
  
  if (yp0 == NULL) { 
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAReInit", MSG_YP0_NULL);
    return(IDA_ILL_INPUT); 
  }

  /* Copy the input parameters into IDA memory block */

  IDA_mem->ida_tn  = t0;

  /* Set forceSetup to SUNFALSE */

  IDA_mem->ida_forceSetup = SUNFALSE;

  /* Initialize the phi array */

  N_VScale(ONE, yy0, IDA_mem->ida_phi[0]);  
  N_VScale(ONE, yp0, IDA_mem->ida_phi[1]);  
 
  /* Initialize all the counters and other optional output values */
 
  IDA_mem->ida_nst     = 0;
  IDA_mem->ida_nre     = 0;
  IDA_mem->ida_ncfn    = 0;
  IDA_mem->ida_netf    = 0;
  IDA_mem->ida_nni     = 0;
  IDA_mem->ida_nsetups = 0;
  
  IDA_mem->ida_kused = 0;
  IDA_mem->ida_hused = ZERO;
  IDA_mem->ida_tolsf = ONE;

  IDA_mem->ida_nge = 0;

  IDA_mem->ida_irfnd = 0;

  /* Initial setup not done yet */

  IDA_mem->ida_SetupDone = SUNFALSE;
      
  /* Problem has been successfully re-initialized */

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * IDASStolerances
 * IDASVtolerances
 * IDAWFtolerances
 *
 * These functions specify the integration tolerances. One of them
 * MUST be called before the first call to IDA.
 *
 * IDASStolerances specifies scalar relative and absolute tolerances.
 * IDASVtolerances specifies scalar relative tolerance and a vector
 *   absolute tolerance (a potentially different absolute tolerance 
 *   for each vector component).
 * IDAWFtolerances specifies a user-provides function (of type IDAEwtFn)
 *   which will be called to set the error weight vector.
 */

int IDASStolerances(void *ida_mem, realtype reltol, realtype abstol)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASStolerances", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_MallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_MALLOC, "IDAS", "IDASStolerances", MSG_NO_MALLOC);
    return(IDA_NO_MALLOC);
  }

  /* Check inputs */
  if (reltol < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASStolerances", MSG_BAD_RTOL);
    return(IDA_ILL_INPUT);
  }

  if (abstol < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASStolerances", MSG_BAD_ATOL);
    return(IDA_ILL_INPUT);
  }

  /* Copy tolerances into memory */
  IDA_mem->ida_rtol = reltol;
  IDA_mem->ida_Satol = abstol;

  IDA_mem->ida_itol = IDA_SS;

  IDA_mem->ida_user_efun = SUNFALSE;
  IDA_mem->ida_efun = IDAEwtSet;
  IDA_mem->ida_edata = NULL; /* will be set to ida_mem in InitialSetup */ 

  return(IDA_SUCCESS);
}


int IDASVtolerances(void *ida_mem, realtype reltol, N_Vector abstol)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASVtolerances", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_MallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_MALLOC, "IDAS", "IDASVtolerances", MSG_NO_MALLOC);
    return(IDA_NO_MALLOC);
  }

  /* Check inputs */

  if (reltol < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASVtolerances", MSG_BAD_RTOL);
    return(IDA_ILL_INPUT);
  }

  if (N_VMin(abstol) < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASVtolerances", MSG_BAD_ATOL);
    return(IDA_ILL_INPUT);
  }

  /* Copy tolerances into memory */
  
  if ( !(IDA_mem->ida_VatolMallocDone) ) {
    IDA_mem->ida_Vatol = N_VClone(IDA_mem->ida_ewt);
    IDA_mem->ida_lrw += IDA_mem->ida_lrw1;
    IDA_mem->ida_liw += IDA_mem->ida_liw1;
    IDA_mem->ida_VatolMallocDone = SUNTRUE;
  }

  IDA_mem->ida_rtol = reltol;
  N_VScale(ONE, abstol, IDA_mem->ida_Vatol);

  IDA_mem->ida_itol = IDA_SV;

  IDA_mem->ida_user_efun = SUNFALSE;
  IDA_mem->ida_efun = IDAEwtSet;
  IDA_mem->ida_edata = NULL; /* will be set to ida_mem in InitialSetup */ 

  return(IDA_SUCCESS);
}


int IDAWFtolerances(void *ida_mem, IDAEwtFn efun)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAWFtolerances", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_MallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_MALLOC, "IDAS", "IDAWFtolerances", MSG_NO_MALLOC);
    return(IDA_NO_MALLOC);
  }

  IDA_mem->ida_itol = IDA_WF;

  IDA_mem->ida_user_efun = SUNTRUE;
  IDA_mem->ida_efun = efun;
  IDA_mem->ida_edata = NULL; /* will be set to user_data in InitialSetup */

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * IDAQuadMalloc
 *
 * IDAQuadMalloc allocates and initializes quadrature related 
 * memory for a problem. All problem specification inputs are 
 * checked for errors. If any error occurs during initialization, 
 * it is reported to the file whose file pointer is errfp. 
 * The return value is IDA_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */

int IDAQuadInit(void *ida_mem, IDAQuadRhsFn rhsQ, N_Vector yQ0)
{
  IDAMem IDA_mem;
  booleantype allocOK;
  sunindextype lrw1Q, liw1Q;
  int retval;

  /* Check ida_mem */
  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAQuadInit", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Set space requirements for one N_Vector */
  N_VSpace(yQ0, &lrw1Q, &liw1Q);
  IDA_mem->ida_lrw1Q = lrw1Q;
  IDA_mem->ida_liw1Q = liw1Q;

  /* Allocate the vectors (using yQ0 as a template) */
  allocOK = IDAQuadAllocVectors(IDA_mem, yQ0);
  if (!allocOK) {
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDAQuadInit", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  /* Initialize phiQ in the history array */
  N_VScale(ONE, yQ0, IDA_mem->ida_phiQ[0]);

  retval = N_VConstVectorArray(IDA_mem->ida_maxord, ZERO, IDA_mem->ida_phiQ+1);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  /* Copy the input parameters into IDAS state */
  IDA_mem->ida_rhsQ = rhsQ;

  /* Initialize counters */
  IDA_mem->ida_nrQe  = 0;
  IDA_mem->ida_netfQ = 0;

  /* Quadrature integration turned ON */
  IDA_mem->ida_quadr = SUNTRUE;
  IDA_mem->ida_quadMallocDone = SUNTRUE;

  /* Quadrature initialization was successfull */
  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * IDAQuadReInit
 *
 * IDAQuadReInit re-initializes IDAS's quadrature related memory 
 * for a problem, assuming it has already been allocated in prior 
 * calls to IDAInit and IDAQuadMalloc. 
 * All problem specification inputs are checked for errors.
 * If any error occurs during initialization, it is reported to the
 * file whose file pointer is errfp.
 * The return value is IDA_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */

int IDAQuadReInit(void *ida_mem, N_Vector yQ0)
{
  IDAMem IDA_mem;
  int retval;

  /* Check ida_mem */
  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAQuadReInit", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Ckeck if quadrature was initialized */
  if (IDA_mem->ida_quadMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUAD, "IDAS", "IDAQuadReInit", MSG_NO_QUAD);
    return(IDA_NO_QUAD);
  }

  /* Initialize phiQ in the history array */
  N_VScale(ONE, yQ0, IDA_mem->ida_phiQ[0]);

  retval = N_VConstVectorArray(IDA_mem->ida_maxord, ZERO, IDA_mem->ida_phiQ+1);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  /* Initialize counters */
  IDA_mem->ida_nrQe  = 0;
  IDA_mem->ida_netfQ = 0;

  /* Quadrature integration turned ON */
  IDA_mem->ida_quadr = SUNTRUE;

  /* Quadrature re-initialization was successfull */
  return(IDA_SUCCESS);
}


/*
 * IDAQuadSStolerances
 * IDAQuadSVtolerances
 * 
 *
 * These functions specify the integration tolerances for quadrature
 * variables. One of them MUST be called before the first call to
 * IDA IF error control on the quadrature variables is enabled
 * (see IDASetQuadErrCon).
 *
 * IDASStolerances specifies scalar relative and absolute tolerances.
 * IDASVtolerances specifies scalar relative tolerance and a vector
 *   absolute tolerance (a potentially different absolute tolerance 
 *   for each vector component). 
 */
int IDAQuadSStolerances(void *ida_mem, realtype reltolQ, realtype abstolQ)
{
  IDAMem IDA_mem;

  /*Check ida mem*/
  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAQuadSStolerances", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Ckeck if quadrature was initialized */
  if (IDA_mem->ida_quadMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUAD, "IDAS", "IDAQuadSStolerances", MSG_NO_QUAD);
    return(IDA_NO_QUAD);
  }
  
  /* Test user-supplied tolerances */
  if (reltolQ < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSStolerances", MSG_BAD_RTOLQ);
    return(IDA_ILL_INPUT);
  }

  if (abstolQ < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSStolerances", MSG_BAD_ATOLQ);
    return(IDA_ILL_INPUT);
  }

  /* Copy tolerances into memory */
  IDA_mem->ida_itolQ = IDA_SS;

  IDA_mem->ida_rtolQ  = reltolQ;
  IDA_mem->ida_SatolQ = abstolQ;


  return (IDA_SUCCESS);
}

int IDAQuadSVtolerances(void *ida_mem, realtype reltolQ, N_Vector abstolQ)
{
  IDAMem IDA_mem;

  /*Check ida mem*/
  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAQuadSVtolerances", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Ckeck if quadrature was initialized */
  if (IDA_mem->ida_quadMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUAD, "IDAS", "IDAQuadSVtolerances", MSG_NO_QUAD);
    return(IDA_NO_QUAD);
  }
  
  /* Test user-supplied tolerances */
  if (reltolQ < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSVtolerances", MSG_BAD_RTOLQ);
    return(IDA_ILL_INPUT);
  }
  
  if (abstolQ == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSVtolerances", MSG_NULL_ATOLQ);
    return(IDA_ILL_INPUT);
  }
  
  if (N_VMin(abstolQ)<ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSVtolerances", MSG_BAD_ATOLQ);
    return(IDA_ILL_INPUT);
  }

  /* Copy tolerances into memory */
  IDA_mem->ida_itolQ = IDA_SV;
  IDA_mem->ida_rtolQ = reltolQ;

  /* clone the absolute tolerances vector (if necessary) */
  if (SUNFALSE == IDA_mem->ida_VatolQMallocDone) {
    IDA_mem->ida_VatolQ = N_VClone(abstolQ);
    IDA_mem->ida_lrw += IDA_mem->ida_lrw1Q;
    IDA_mem->ida_liw += IDA_mem->ida_liw1Q;
    IDA_mem->ida_VatolQMallocDone = SUNTRUE;
  }

  N_VScale(ONE, abstolQ, IDA_mem->ida_VatolQ);

  return(IDA_SUCCESS);
}

/*
 * IDASenMalloc
 *
 * IDASensInit allocates and initializes sensitivity related 
 * memory for a problem. All problem specification inputs are 
 * checked for errors. If any error occurs during initialization, 
 * it is reported to the file whose file pointer is errfp. 
 * The return value is IDA_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */

int IDASensInit(void *ida_mem, int Ns, int ism, 
                IDASensResFn fS,
                N_Vector *yS0, N_Vector *ypS0)
  
{
  IDAMem IDA_mem;
  booleantype allocOK;
  int is, retval;
  SUNNonlinearSolver NLS;
  
  /* Check ida_mem */
  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASensInit", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Check if Ns is legal */
  if (Ns<=0) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASensInit", MSG_BAD_NS);
    return(IDA_ILL_INPUT);
  }
  IDA_mem->ida_Ns = Ns;

  /* Check if ism is legal */
  if ((ism!=IDA_SIMULTANEOUS) && (ism!=IDA_STAGGERED)) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASensInit", MSG_BAD_ISM);
    return(IDA_ILL_INPUT);
  }
  IDA_mem->ida_ism = ism;
   
  /* Check if yS0 and ypS0 are non-null */
  if (yS0 == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASensInit", MSG_NULL_YYS0);
    return(IDA_ILL_INPUT);
  }
  if (ypS0 == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASensInit", MSG_NULL_YPS0);
    return(IDA_ILL_INPUT);
  }

  /* Store sensitivity RHS-related data */

  if (fS != NULL) {
    IDA_mem->ida_resS    = fS;
    IDA_mem->ida_user_dataS  = IDA_mem->ida_user_data;
    IDA_mem->ida_resSDQ  = SUNFALSE;
  } else {
    IDA_mem->ida_resS       = IDASensResDQ;
    IDA_mem->ida_user_dataS = ida_mem;
    IDA_mem->ida_resSDQ     = SUNTRUE;
  }

  /* Allocate the vectors (using yS0[0] as a template) */

  allocOK = IDASensAllocVectors(IDA_mem, yS0[0]);
  if (!allocOK) {
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDASensInit", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  /* Allocate temporary work arrays for fused vector ops */
  if (Ns*MXORDP1 > MXORDP1) {
    free(IDA_mem->ida_cvals); IDA_mem->ida_cvals = NULL;
    free(IDA_mem->ida_Xvecs); IDA_mem->ida_Xvecs = NULL;
    free(IDA_mem->ida_Zvecs); IDA_mem->ida_Zvecs = NULL;

    IDA_mem->ida_cvals = (realtype *) malloc((Ns*MXORDP1)*sizeof(realtype));
    IDA_mem->ida_Xvecs = (N_Vector *) malloc((Ns*MXORDP1)*sizeof(N_Vector));
    IDA_mem->ida_Zvecs = (N_Vector *) malloc((Ns*MXORDP1)*sizeof(N_Vector));

    if ((IDA_mem->ida_cvals == NULL) ||
        (IDA_mem->ida_Xvecs == NULL) ||
        (IDA_mem->ida_Zvecs == NULL)) {
      IDASensFreeVectors(IDA_mem);
      IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDASensInit", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }
  }
  
  /*---------------------------------------------- 
    All error checking is complete at this point 
    -----------------------------------------------*/

  /* Initialize the phiS array */
  for (is=0; is<Ns; is++)
    IDA_mem->ida_cvals[is] = ONE;

  retval = N_VScaleVectorArray(Ns, IDA_mem->ida_cvals, yS0, IDA_mem->ida_phiS[0]);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  retval = N_VScaleVectorArray(Ns, IDA_mem->ida_cvals, ypS0, IDA_mem->ida_phiS[1]);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  /* Initialize all sensitivity related counters */
  IDA_mem->ida_nrSe     = 0;
  IDA_mem->ida_nreS     = 0;
  IDA_mem->ida_ncfnS    = 0;
  IDA_mem->ida_netfS    = 0;
  IDA_mem->ida_nniS     = 0;
  IDA_mem->ida_nsetupsS = 0;

  /* Set default values for plist and pbar */
  for (is=0; is<Ns; is++) {
    IDA_mem->ida_plist[is] = is;
    IDA_mem->ida_pbar[is] = ONE;
  }

  /* Sensitivities will be computed */
  IDA_mem->ida_sensi = SUNTRUE;
  IDA_mem->ida_sensMallocDone = SUNTRUE;

  /* create a Newton nonlinear solver object by default */
  if (ism == IDA_SIMULTANEOUS)
    NLS = SUNNonlinSol_NewtonSens(Ns+1, IDA_mem->ida_delta);
  else
    NLS = SUNNonlinSol_NewtonSens(Ns, IDA_mem->ida_delta);

  /* check that the nonlinear solver is non-NULL */
  if (NLS == NULL) {
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDASensInit", MSG_MEM_FAIL);
    IDASensFreeVectors(IDA_mem);
    return(IDA_MEM_FAIL);
  }

  /* attach the nonlinear solver to the IDA memory */
  if (ism == IDA_SIMULTANEOUS)
    retval = IDASetNonlinearSolverSensSim(IDA_mem, NLS);
  else
    retval = IDASetNonlinearSolverSensStg(IDA_mem, NLS);

  /* check that the nonlinear solver was successfully attached */
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, retval, "IDAS", "IDASensInit",
                    "Setting the nonlinear solver failed");
    IDASensFreeVectors(IDA_mem);
    SUNNonlinSolFree(NLS);
    return(IDA_MEM_FAIL);
  }

  /* set ownership flag */
  if (ism == IDA_SIMULTANEOUS)
    IDA_mem->ownNLSsim = SUNTRUE;
  else
    IDA_mem->ownNLSstg = SUNTRUE;

  /* Sensitivity initialization was successfull */
  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * IDASensReInit
 *
 * IDASensReInit re-initializes IDAS's sensitivity related memory
 * for a problem, assuming it has already been allocated in prior
 * calls to IDAInit and IDASensInit.
 * All problem specification inputs are checked for errors.
 * The number of sensitivities Ns is assumed to be unchanged since
 * the previous call to IDASensInit.
 * If any error occurs during initialization, it is reported to the
 * file whose file pointer is errfp.
 * The return value is IDA_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */

int IDASensReInit(void *ida_mem, int ism, N_Vector *yS0, N_Vector *ypS0)
{
  IDAMem IDA_mem;
  int is, retval;
  SUNNonlinearSolver NLS;

  /* Check ida_mem */
  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDASensReInit", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Was sensitivity initialized? */
  if (IDA_mem->ida_sensMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS",
                    "IDASensReInit", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  /* Check if ism is legal */
  if ((ism!=IDA_SIMULTANEOUS) && (ism!=IDA_STAGGERED)) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASensReInit", MSG_BAD_ISM);
    return(IDA_ILL_INPUT);
  }
  IDA_mem->ida_ism = ism;

  /* Check if yS0 and ypS0 are non-null */
  if (yS0 == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASensReInit", MSG_NULL_YYS0);
    return(IDA_ILL_INPUT);
  }
  if (ypS0 == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASensReInit", MSG_NULL_YPS0);
    return(IDA_ILL_INPUT);
  }

  /*-----------------------------------------------
    All error checking is complete at this point
    -----------------------------------------------*/

  /* Initialize the phiS array */
  for (is=0; is<IDA_mem->ida_Ns; is++)
    IDA_mem->ida_cvals[is] = ONE;

  retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                               yS0, IDA_mem->ida_phiS[0]);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                               ypS0, IDA_mem->ida_phiS[1]);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  /* Initialize all sensitivity related counters */
  IDA_mem->ida_nrSe     = 0;
  IDA_mem->ida_nreS     = 0;
  IDA_mem->ida_ncfnS    = 0;
  IDA_mem->ida_netfS    = 0;
  IDA_mem->ida_nniS     = 0;
  IDA_mem->ida_nsetupsS = 0;

  /* Set default values for plist and pbar */
  for (is=0; is<IDA_mem->ida_Ns; is++) {
    IDA_mem->ida_plist[is] = is;
    IDA_mem->ida_pbar[is] = ONE;
  }

  /* Sensitivities will be computed */
  IDA_mem->ida_sensi = SUNTRUE;

  /* Check if the NLS exists, create the default NLS if needed */
  if ((ism == IDA_SIMULTANEOUS && IDA_mem->NLSsim == NULL) ||
      (ism == IDA_STAGGERED && IDA_mem->NLSstg == NULL)) {

    /* create a Newton nonlinear solver object by default */
    if (ism == IDA_SIMULTANEOUS)
      NLS = SUNNonlinSol_NewtonSens(IDA_mem->ida_Ns+1, IDA_mem->ida_delta);
    else
      NLS = SUNNonlinSol_NewtonSens(IDA_mem->ida_Ns, IDA_mem->ida_delta);

    /* check that the nonlinear solver is non-NULL */
    if (NLS == NULL) {
      IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS",
                      "IDASensReInit", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    /* attach the nonlinear solver to the IDA memory */
    if (ism == IDA_SIMULTANEOUS)
      retval = IDASetNonlinearSolverSensSim(IDA_mem, NLS);
    else
      retval = IDASetNonlinearSolverSensStg(IDA_mem, NLS);

    /* check that the nonlinear solver was successfully attached */
    if (retval != IDA_SUCCESS) {
      IDAProcessError(IDA_mem, retval, "IDAS", "IDASensReInit",
                      "Setting the nonlinear solver failed");
      SUNNonlinSolFree(NLS);
      return(IDA_MEM_FAIL);
    }

    /* set ownership flag */
    if (ism == IDA_SIMULTANEOUS)
      IDA_mem->ownNLSsim = SUNTRUE;
    else
      IDA_mem->ownNLSstg = SUNTRUE;

    /* initialize the NLS object, this assumes that the linear solver has
       already been initialized in IDAInit */
    if (ism == IDA_SIMULTANEOUS)
      retval = idaNlsInitSensSim(IDA_mem);
    else
      retval = idaNlsInitSensStg(IDA_mem);

    if (retval != IDA_SUCCESS) {
      IDAProcessError(IDA_mem, IDA_NLS_INIT_FAIL, "IDAS",
                      "IDASensReInit", MSG_NLS_INIT_FAIL);
      return(IDA_NLS_INIT_FAIL);
    }
  }

  /* Sensitivity re-initialization was successfull */
  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * IDASensSStolerances
 * IDASensSVtolerances
 * IDASensEEtolerances
 *
 * These functions specify the integration tolerances for sensitivity
 * variables. One of them MUST be called before the first call to IDASolve.
 *
 * IDASensSStolerances specifies scalar relative and absolute tolerances.
 * IDASensSVtolerances specifies scalar relative tolerance and a vector
 *   absolute tolerance for each sensitivity vector (a potentially different
 *   absolute tolerance for each vector component).
 * IDASensEEtolerances specifies that tolerances for sensitivity variables
 *   should be estimated from those provided for the state variables.
 */


int IDASensSStolerances(void *ida_mem, realtype reltolS, realtype *abstolS)
{
  IDAMem IDA_mem;
  int is;

  /* Check ida_mem pointer */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASensSStolerances", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Was sensitivity initialized? */

  if (IDA_mem->ida_sensMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDASensSStolerances", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  } 

  /* Test user-supplied tolerances */
    
  if (reltolS < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASensSStolerances", MSG_BAD_RTOLS);
    return(IDA_ILL_INPUT);
  }

  if (abstolS == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASensSStolerances", MSG_NULL_ATOLS);
    return(IDA_ILL_INPUT);
  }

  for (is=0; is<IDA_mem->ida_Ns; is++)
    if (abstolS[is] < ZERO) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASensSStolerances", MSG_BAD_ATOLS);
      return(IDA_ILL_INPUT);
    }

  /* Copy tolerances into memory */

  IDA_mem->ida_itolS = IDA_SS;

  IDA_mem->ida_rtolS = reltolS;

  if ( !(IDA_mem->ida_SatolSMallocDone) ) {
    IDA_mem->ida_SatolS = NULL;
    IDA_mem->ida_SatolS = (realtype *)malloc(IDA_mem->ida_Ns*sizeof(realtype));
    IDA_mem->ida_lrw += IDA_mem->ida_Ns;
    IDA_mem->ida_SatolSMallocDone = SUNTRUE;
  }

  for (is=0; is<IDA_mem->ida_Ns; is++)
    IDA_mem->ida_SatolS[is] = abstolS[is];

  return(IDA_SUCCESS);
}


int IDASensSVtolerances(void *ida_mem,  realtype reltolS, N_Vector *abstolS)
{
  IDAMem IDA_mem;
  int is, retval;

  /* Check ida_mem pointer */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASensSVtolerances", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Was sensitivity initialized? */

  if (IDA_mem->ida_sensMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDASensSVtolerances", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  } 

  /* Test user-supplied tolerances */
    
  if (reltolS < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASensSVtolerances", MSG_BAD_RTOLS);
    return(IDA_ILL_INPUT);
  }

  if (abstolS == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASensSVtolerances", MSG_NULL_ATOLS);
    return(IDA_ILL_INPUT);
  }

  for (is=0; is<IDA_mem->ida_Ns; is++) {
    if (N_VMin(abstolS[is])<ZERO) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASensSStolerances", MSG_BAD_ATOLS);
      return(IDA_ILL_INPUT);      
    }
  }

  IDA_mem->ida_itolS = IDA_SV;
  IDA_mem->ida_rtolS = reltolS ;

  if ( SUNFALSE == IDA_mem->ida_VatolSMallocDone ) {
    IDA_mem->ida_VatolS = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_tempv1);
    IDA_mem->ida_lrw += IDA_mem->ida_Ns*IDA_mem->ida_lrw1;
    IDA_mem->ida_liw += IDA_mem->ida_Ns*IDA_mem->ida_liw1;
    IDA_mem->ida_VatolSMallocDone = SUNTRUE;
  }

  for (is=0; is<IDA_mem->ida_Ns; is++)
    IDA_mem->ida_cvals[is] = ONE;

  retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                               abstolS, IDA_mem->ida_VatolS);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
    
  return(IDA_SUCCESS);
}

int IDASensEEtolerances(void *ida_mem)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASensEEtolerances", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Was sensitivity initialized? */

  if (IDA_mem->ida_sensMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDASensEEtolerances", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  } 

  IDA_mem->ida_itolS = IDA_EE;

  return(IDA_SUCCESS);
}


int IDAQuadSensInit(void *ida_mem, IDAQuadSensRhsFn rhsQS, N_Vector *yQS0)
{
  IDAMem IDA_mem;
  booleantype allocOK;
  int is, retval;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAQuadSensInit", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Check if sensitivity analysis is active */
  if (!IDA_mem->ida_sensi) {
    IDAProcessError(NULL, IDA_NO_SENS, "IDAS", "IDAQuadSensInit", MSG_NO_SENSI);    
    return(IDA_NO_SENS);
  }

  /* Verifiy yQS0 parameter. */
  if (yQS0==NULL) {
    IDAProcessError(NULL, IDA_ILL_INPUT, "IDAS", "IDAQuadSensInit", MSG_NULL_YQS0);    
    return(IDA_ILL_INPUT);    
  }

  /* Allocate vector needed for quadratures' sensitivities. */
  allocOK = IDAQuadSensAllocVectors(IDA_mem, yQS0[0]);
  if (!allocOK) {    
    IDAProcessError(NULL, IDA_MEM_FAIL, "IDAS", "IDAQuadSensInit", MSG_MEM_FAIL);    
    return(IDA_MEM_FAIL);
  }

  /* Error checking complete. */
  if (rhsQS == NULL) {
    IDA_mem->ida_rhsQSDQ = SUNTRUE;
    IDA_mem->ida_rhsQS = IDAQuadSensRhsInternalDQ;

    IDA_mem->ida_user_dataQS = ida_mem;
  } else {
    IDA_mem->ida_rhsQSDQ = SUNFALSE;
    IDA_mem->ida_rhsQS = rhsQS;

    IDA_mem->ida_user_dataQS = IDA_mem->ida_user_data;
  }

  /* Initialize phiQS[0] in the history array */
  for (is=0; is<IDA_mem->ida_Ns; is++)
    IDA_mem->ida_cvals[is] = ONE;

  retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                               yQS0, IDA_mem->ida_phiQS[0]);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  /* Initialize all sensitivities related counters. */
  IDA_mem->ida_nrQSe  = 0;
  IDA_mem->ida_nrQeS  = 0;
  IDA_mem->ida_netfQS = 0;

  /* Everything allright, set the flags and return with success. */
  IDA_mem->ida_quadr_sensi = SUNTRUE;
  IDA_mem->ida_quadSensMallocDone = SUNTRUE;

  return(IDA_SUCCESS);
}

int IDAQuadSensReInit(void *ida_mem, N_Vector *yQS0)
{
  IDAMem IDA_mem;
  int is, retval;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAQuadSensReInit", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Check if sensitivity analysis is active */
  if (!IDA_mem->ida_sensi) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAQuadSensReInit", MSG_NO_SENSI);    
    return(IDA_NO_SENS);
  }
  
  /* Was sensitivity for quadrature already initialized? */
  if (!IDA_mem->ida_quadSensMallocDone) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAQuadSensReInit", MSG_NO_QUADSENSI);
    return(IDA_NO_QUADSENS);
  }

  /* Verifiy yQS0 parameter. */
  if (yQS0==NULL) {
    IDAProcessError(NULL, IDA_ILL_INPUT, "IDAS", "IDAQuadSensReInit", MSG_NULL_YQS0);    
    return(IDA_ILL_INPUT);    
  }
  
  /* Error checking complete at this point. */

  /* Initialize phiQS[0] in the history array */
  for (is=0; is<IDA_mem->ida_Ns; is++)
    IDA_mem->ida_cvals[is] = ONE;

  retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                               yQS0, IDA_mem->ida_phiQS[0]);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  /* Initialize all sensitivities related counters. */
  IDA_mem->ida_nrQSe  = 0;
  IDA_mem->ida_nrQeS  = 0;
  IDA_mem->ida_netfQS = 0;

  /* Everything allright, set the flags and return with success. */
  IDA_mem->ida_quadr_sensi = SUNTRUE;

  return(IDA_SUCCESS);
}

/*
 * IDAQuadSensSStolerances
 * IDAQuadSensSVtolerances
 * IDAQuadSensEEtolerances
 *
 * These functions specify the integration tolerances for quadrature
 * sensitivity variables. One of them MUST be called before the first
 * call to IDAS IF these variables are included in the error test.
 *
 * IDAQuadSensSStolerances specifies scalar relative and absolute tolerances.
 * IDAQuadSensSVtolerances specifies scalar relative tolerance and a vector
 *   absolute tolerance for each quadrature sensitivity vector (a potentially
 *   different absolute tolerance for each vector component).
 * IDAQuadSensEEtolerances specifies that tolerances for sensitivity variables
 *   should be estimated from those provided for the quadrature variables.
 *   In this case, tolerances for the quadrature variables must be
 *   specified through a call to one of IDAQuad**tolerances.
 */

int IDAQuadSensSStolerances(void *ida_mem, realtype reltolQS, realtype *abstolQS)
{
  IDAMem IDA_mem; 
  int is; 

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAQuadSensSStolerances", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Check if sensitivity analysis is active */
  if (!IDA_mem->ida_sensi) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAQuadSensSStolerances", MSG_NO_SENSI);    
    return(IDA_NO_SENS);
  }
  
  /* Was sensitivity for quadrature already initialized? */
  if (!IDA_mem->ida_quadSensMallocDone) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAQuadSensSStolerances", MSG_NO_QUADSENSI);
    return(IDA_NO_QUADSENS);
  }

  /* Test user-supplied tolerances */

  if (reltolQS < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSensSStolerances", MSG_BAD_RELTOLQS);
    return(IDA_ILL_INPUT);
  }

  if (abstolQS == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSensSStolerances", MSG_NULL_ABSTOLQS);
    return(IDA_ILL_INPUT);
  }

  for (is=0; is<IDA_mem->ida_Ns; is++)
    if (abstolQS[is] < ZERO) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSensSStolerances", MSG_BAD_ABSTOLQS);
      return(IDA_ILL_INPUT);
    }
  
  /* Save data. */
  IDA_mem->ida_itolQS = IDA_SS;
  IDA_mem->ida_rtolQS = reltolQS;

  if ( !(IDA_mem->ida_SatolQSMallocDone) ) {
    IDA_mem->ida_SatolQS = (realtype *)malloc(IDA_mem->ida_Ns*sizeof(realtype));
    IDA_mem->ida_lrw += IDA_mem->ida_Ns;
    IDA_mem->ida_SatolQSMallocDone = SUNTRUE;
  }

  for (is=0; is<IDA_mem->ida_Ns; is++)
    IDA_mem->ida_SatolQS[is] = abstolQS[is];

  return(IDA_SUCCESS);
}

int IDAQuadSensSVtolerances(void *ida_mem, realtype reltolQS, N_Vector *abstolQS)
{
  IDAMem IDA_mem; 
  int is, retval;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAQuadSensSVtolerances", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Check if sensitivity analysis is active */
  if (!IDA_mem->ida_sensi) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAQuadSensSVtolerances", MSG_NO_SENSI);    
    return(IDA_NO_SENS);
  }
  
  /* Was sensitivity for quadrature already initialized? */
  if (!IDA_mem->ida_quadSensMallocDone) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAQuadSensSVtolerances", MSG_NO_QUADSENSI);
    return(IDA_NO_QUADSENS);
  }

  /* Test user-supplied tolerances */

  if (reltolQS < ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSensSVtolerances", MSG_BAD_RELTOLQS);
    return(IDA_ILL_INPUT);
  }

  if (abstolQS == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSensSVtolerances", MSG_NULL_ABSTOLQS);
    return(IDA_ILL_INPUT);
  }

  for (is=0; is<IDA_mem->ida_Ns; is++)
    if (N_VMin(abstolQS[is]) < ZERO) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAQuadSensSVtolerances", MSG_BAD_ABSTOLQS);
      return(IDA_ILL_INPUT);
    }
  
  /* Save data. */
  IDA_mem->ida_itolQS = IDA_SV;
  IDA_mem->ida_rtolQS = reltolQS;

  if ( !(IDA_mem->ida_VatolQSMallocDone) ) {
    IDA_mem->ida_VatolQS = N_VCloneVectorArray(IDA_mem->ida_Ns, abstolQS[0]);
    IDA_mem->ida_lrw += IDA_mem->ida_Ns*IDA_mem->ida_lrw1Q;
    IDA_mem->ida_liw += IDA_mem->ida_Ns*IDA_mem->ida_liw1Q;
    IDA_mem->ida_VatolQSMallocDone = SUNTRUE;
  }
  
  for (is=0; is<IDA_mem->ida_Ns; is++)
    IDA_mem->ida_cvals[is] = ONE;

  retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                               abstolQS, IDA_mem->ida_VatolQS);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  return(IDA_SUCCESS);
}

int IDAQuadSensEEtolerances(void *ida_mem)
{
  IDAMem IDA_mem; 

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAQuadSensEEtolerances", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Check if sensitivity analysis is active */
  if (!IDA_mem->ida_sensi) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAQuadSensEEtolerances", MSG_NO_SENSI);    
    return(IDA_NO_SENS);
  }
  
  /* Was sensitivity for quadrature already initialized? */
  if (!IDA_mem->ida_quadSensMallocDone) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAQuadSensEEtolerances", MSG_NO_QUADSENSI);
    return(IDA_NO_QUADSENS);
  }

  IDA_mem->ida_itolQS = IDA_EE;

  return(IDA_SUCCESS);
}

/*
 * IDASensToggleOff
 *
 * IDASensToggleOff deactivates sensitivity calculations.
 * It does NOT deallocate sensitivity-related memory.
 */
int IDASensToggleOff(void *ida_mem)
{
  IDAMem IDA_mem;

  /* Check ida_mem */
  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDASensToggleOff", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Disable sensitivities */
  IDA_mem->ida_sensi = SUNFALSE;
  IDA_mem->ida_quadr_sensi = SUNFALSE;

  return(IDA_SUCCESS);
}

/*
 * IDARootInit
 *
 * IDARootInit initializes a rootfinding problem to be solved
 * during the integration of the DAE system.  It loads the root
 * function pointer and the number of root functions, and allocates
 * workspace memory.  The return value is IDA_SUCCESS = 0 if no
 * errors occurred, or a negative value otherwise.
 */

int IDARootInit(void *ida_mem, int nrtfn, IDARootFn g)
{
  IDAMem IDA_mem;
  int i, nrt;

  /* Check ida_mem pointer */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDARootInit", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  nrt = (nrtfn < 0) ? 0 : nrtfn;

  /* If rerunning IDARootInit() with a different number of root
     functions (changing number of gfun components), then free
     currently held memory resources */
  if ((nrt != IDA_mem->ida_nrtfn) && (IDA_mem->ida_nrtfn > 0)) {

    free(IDA_mem->ida_glo); IDA_mem->ida_glo = NULL;
    free(IDA_mem->ida_ghi); IDA_mem->ida_ghi = NULL;
    free(IDA_mem->ida_grout); IDA_mem->ida_grout = NULL;
    free(IDA_mem->ida_iroots); IDA_mem->ida_iroots = NULL;
    free(IDA_mem->ida_rootdir); IDA_mem->ida_rootdir = NULL;
    free(IDA_mem->ida_gactive); IDA_mem->ida_gactive = NULL;

    IDA_mem->ida_lrw -= 3 * (IDA_mem->ida_nrtfn);
    IDA_mem->ida_liw -= 3 * (IDA_mem->ida_nrtfn);

  }

  /* If IDARootInit() was called with nrtfn == 0, then set ida_nrtfn to
     zero and ida_gfun to NULL before returning */
  if (nrt == 0) {
    IDA_mem->ida_nrtfn = nrt;
    IDA_mem->ida_gfun = NULL;
    return(IDA_SUCCESS);
  }

  /* If rerunning IDARootInit() with the same number of root functions
     (not changing number of gfun components), then check if the root
     function argument has changed */
  /* If g != NULL then return as currently reserved memory resources
     will suffice */
  if (nrt == IDA_mem->ida_nrtfn) {
    if (g != IDA_mem->ida_gfun) {
      if (g == NULL) {
	free(IDA_mem->ida_glo); IDA_mem->ida_glo = NULL;
	free(IDA_mem->ida_ghi); IDA_mem->ida_ghi = NULL;
	free(IDA_mem->ida_grout); IDA_mem->ida_grout = NULL;
	free(IDA_mem->ida_iroots); IDA_mem->ida_iroots = NULL;
        free(IDA_mem->ida_rootdir); IDA_mem->ida_rootdir = NULL;
        free(IDA_mem->ida_gactive); IDA_mem->ida_gactive = NULL;

        IDA_mem->ida_lrw -= 3*nrt;
        IDA_mem->ida_liw -= 3*nrt;

        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDARootInit", MSG_ROOT_FUNC_NULL);
        return(IDA_ILL_INPUT);
      }
      else {
        IDA_mem->ida_gfun = g;
        return(IDA_SUCCESS);
      }
    }
    else return(IDA_SUCCESS);
  }

  /* Set variable values in IDA memory block */
  IDA_mem->ida_nrtfn = nrt;
  if (g == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDARootInit", MSG_ROOT_FUNC_NULL);
    return(IDA_ILL_INPUT);
  }
  else IDA_mem->ida_gfun = g;

  /* Allocate necessary memory and return */
  IDA_mem->ida_glo = NULL;
  IDA_mem->ida_glo = (realtype *) malloc(nrt*sizeof(realtype));
  if (IDA_mem->ida_glo == NULL) {
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDARootInit", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  IDA_mem->ida_ghi = NULL;
  IDA_mem->ida_ghi = (realtype *) malloc(nrt*sizeof(realtype));
  if (IDA_mem->ida_ghi == NULL) {
    free(IDA_mem->ida_glo); IDA_mem->ida_glo = NULL;
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDARootInit", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  IDA_mem->ida_grout = NULL;
  IDA_mem->ida_grout = (realtype *) malloc(nrt*sizeof(realtype));
  if (IDA_mem->ida_grout == NULL) {
    free(IDA_mem->ida_glo); IDA_mem->ida_glo = NULL;
    free(IDA_mem->ida_ghi); IDA_mem->ida_ghi = NULL;
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDARootInit", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  IDA_mem->ida_iroots = NULL;
  IDA_mem->ida_iroots = (int *) malloc(nrt*sizeof(int));
  if (IDA_mem->ida_iroots == NULL) {
    free(IDA_mem->ida_glo); IDA_mem->ida_glo = NULL;
    free(IDA_mem->ida_ghi); IDA_mem->ida_ghi = NULL;
    free(IDA_mem->ida_grout); IDA_mem->ida_grout = NULL;
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDARootInit", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  IDA_mem->ida_rootdir = NULL;
  IDA_mem->ida_rootdir = (int *) malloc(nrt*sizeof(int));
  if (IDA_mem->ida_rootdir == NULL) {
    free(IDA_mem->ida_glo); IDA_mem->ida_glo = NULL;
    free(IDA_mem->ida_ghi); IDA_mem->ida_ghi = NULL;
    free(IDA_mem->ida_grout); IDA_mem->ida_grout = NULL;
    free(IDA_mem->ida_iroots); IDA_mem->ida_iroots = NULL;
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS", "IDARootInit", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  IDA_mem->ida_gactive = NULL;
  IDA_mem->ida_gactive = (booleantype *) malloc(nrt*sizeof(booleantype));
  if (IDA_mem->ida_gactive == NULL) {
    free(IDA_mem->ida_glo); IDA_mem->ida_glo = NULL; 
    free(IDA_mem->ida_ghi); IDA_mem->ida_ghi = NULL;
    free(IDA_mem->ida_grout); IDA_mem->ida_grout = NULL;
    free(IDA_mem->ida_iroots); IDA_mem->ida_iroots = NULL;
    free(IDA_mem->ida_rootdir); IDA_mem->ida_rootdir = NULL;
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDA", "IDARootInit", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  /* Set default values for rootdir (both directions) */
  for(i=0; i<nrt; i++) IDA_mem->ida_rootdir[i] = 0;

  /* Set default values for gactive (all active) */
  for(i=0; i<nrt; i++) IDA_mem->ida_gactive[i] = SUNTRUE;

  IDA_mem->ida_lrw += 3*nrt;
  IDA_mem->ida_liw += 3*nrt;

  return(IDA_SUCCESS);
}



/* 
 * -----------------------------------------------------------------
 * Main solver function
 * -----------------------------------------------------------------
 */

/*
 * IDASolve
 *
 * This routine is the main driver of the IDA package. 
 *
 * It integrates over an independent variable interval defined by the user, 
 * by calling IDAStep to take internal independent variable steps.
 *
 * The first time that IDASolve is called for a successfully initialized
 * problem, it computes a tentative initial step size.
 *
 * IDASolve supports two modes, specified by itask:
 * In the IDA_NORMAL mode, the solver steps until it passes tout and then
 * interpolates to obtain y(tout) and yp(tout).
 * In the IDA_ONE_STEP mode, it takes one internal step and returns.
 *
 * IDASolve returns integer values corresponding to success and failure as below:
 *
 * successful returns: 
 *
 * IDA_SUCCESS        
 * IDA_TSTOP_RETURN   
 *
 * failed returns:
 *
 * IDA_ILL_INPUT
 * IDA_TOO_MUCH_WORK
 * IDA_MEM_NULL
 * IDA_TOO_MUCH_ACC
 * IDA_CONV_FAIL
 * IDA_LSETUP_FAIL
 * IDA_LSOLVE_FAIL    
 * IDA_CONSTR_FAIL
 * IDA_ERR_FAIL   
 * IDA_REP_RES_ERR
 * IDA_RES_FAIL
 */

int IDASolve(void *ida_mem, realtype tout, realtype *tret,
             N_Vector yret, N_Vector ypret, int itask)
{
  long int nstloc;
  int sflag, istate, ier, irfndp, is, ir;
  realtype tdist, troundoff, ypnorm, rh, nrm;
  IDAMem IDA_mem;
  booleantype inactive_roots;

  /* Check for legal inputs in all cases. */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASolve", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Check if problem was malloc'ed */
  if (IDA_mem->ida_MallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_MALLOC, "IDAS", "IDASolve", MSG_NO_MALLOC);
    return(IDA_NO_MALLOC);
  }

  /* Check for legal arguments */
  if (yret == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_YRET_NULL);
    return(IDA_ILL_INPUT);
  }
  IDA_mem->ida_yy = yret;  

  if (ypret == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_YPRET_NULL);
    return(IDA_ILL_INPUT);
  }
  IDA_mem->ida_yp = ypret;
  
  if (tret == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_TRET_NULL);
    return(IDA_ILL_INPUT);
  }

  if ((itask != IDA_NORMAL) && (itask != IDA_ONE_STEP)) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_BAD_ITASK);
    return(IDA_ILL_INPUT);
  }

  if (itask == IDA_NORMAL)  IDA_mem->ida_toutc = tout;
  IDA_mem->ida_taskc = itask;

  /* Sensitivity-specific tests (if using internal DQ functions) */
  if (IDA_mem->ida_sensi && IDA_mem->ida_resSDQ) {
    /* Make sure we have the right 'user data' */
    IDA_mem->ida_user_dataS = ida_mem;
    /* Test if we have the problem parameters */
    if(IDA_mem->ida_p == NULL) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_NULL_P);
      return(IDA_ILL_INPUT);
    }
  }

  if (IDA_mem->ida_quadr_sensi && IDA_mem->ida_rhsQSDQ) {
    IDA_mem->ida_user_dataQS = ida_mem;
    /* Test if we have the problem parameters */
    if(IDA_mem->ida_p == NULL) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_NULL_P);
      return(IDA_ILL_INPUT);
    }
  }

  if (IDA_mem->ida_nst == 0) {       /* This is the first call */

    /* Check inputs to IDA for correctness and consistency */
    if (IDA_mem->ida_SetupDone == SUNFALSE) {
      ier = IDAInitialSetup(IDA_mem);
      if (ier != IDA_SUCCESS) return(ier);
      IDA_mem->ida_SetupDone = SUNTRUE;
    }

    /* On first call, check for tout - tn too small, set initial hh,
       check for approach to tstop, and scale phi[1], phiQ[1], and phiS[1] by hh.
       Also check for zeros of root function g at and near t0.    */

    tdist = SUNRabs(tout - IDA_mem->ida_tn);
    if (tdist == ZERO) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_TOO_CLOSE);
      return(IDA_ILL_INPUT);
    }
    troundoff = TWO * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(tout));
    if (tdist < troundoff) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_TOO_CLOSE);
      return(IDA_ILL_INPUT);
    }

    IDA_mem->ida_hh = IDA_mem->ida_hin;
    if ( (IDA_mem->ida_hh != ZERO) && ((tout-IDA_mem->ida_tn)*IDA_mem->ida_hh < ZERO) ) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_BAD_HINIT);
      return(IDA_ILL_INPUT);
    }

    if (IDA_mem->ida_hh == ZERO) {
      IDA_mem->ida_hh = PT001*tdist;
      ypnorm = IDAWrmsNorm(IDA_mem, IDA_mem->ida_phi[1],
                           IDA_mem->ida_ewt, IDA_mem->ida_suppressalg);
      if (IDA_mem->ida_errconQ)
        ypnorm = IDAQuadWrmsNormUpdate(IDA_mem, ypnorm,
                                       IDA_mem->ida_phiQ[1], IDA_mem->ida_ewtQ);
      if (IDA_mem->ida_errconS)
        ypnorm = IDASensWrmsNormUpdate(IDA_mem, ypnorm, IDA_mem->ida_phiS[1],
                                       IDA_mem->ida_ewtS, IDA_mem->ida_suppressalg);
      if (IDA_mem->ida_errconQS)
        ypnorm = IDAQuadSensWrmsNormUpdate(IDA_mem, ypnorm, IDA_mem->ida_phiQS[1],
                                           IDA_mem->ida_ewtQS);

      if (ypnorm > HALF/IDA_mem->ida_hh) IDA_mem->ida_hh = HALF/ypnorm;
      if (tout < IDA_mem->ida_tn) IDA_mem->ida_hh = -IDA_mem->ida_hh;
    }

    rh = SUNRabs(IDA_mem->ida_hh) * IDA_mem->ida_hmax_inv;
    if (rh > ONE) IDA_mem->ida_hh /= rh;

    if (IDA_mem->ida_tstopset) {
      if ( (IDA_mem->ida_tstop - IDA_mem->ida_tn)*IDA_mem->ida_hh <= ZERO) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve",
                        MSG_BAD_TSTOP, IDA_mem->ida_tstop, IDA_mem->ida_tn);
        return(IDA_ILL_INPUT);
      }
      if ( (IDA_mem->ida_tn + IDA_mem->ida_hh - IDA_mem->ida_tstop)*IDA_mem->ida_hh > ZERO) 
        IDA_mem->ida_hh = (IDA_mem->ida_tstop - IDA_mem->ida_tn)*(ONE - FOUR * IDA_mem->ida_uround);
    }

    IDA_mem->ida_h0u = IDA_mem->ida_hh;
    IDA_mem->ida_kk = 0;
    IDA_mem->ida_kused = 0;  /* set in case of an error return before a step */

    /* Check for exact zeros of the root functions at or near t0. */
    if (IDA_mem->ida_nrtfn > 0) {
      ier = IDARcheck1(IDA_mem);
      if (ier == IDA_RTFUNC_FAIL) {
        IDAProcessError(IDA_mem, IDA_RTFUNC_FAIL, "IDAS", "IDARcheck1", MSG_RTFUNC_FAILED, IDA_mem->ida_tn);
        return(IDA_RTFUNC_FAIL);
      }
    }

    N_VScale(IDA_mem->ida_hh, IDA_mem->ida_phi[1], IDA_mem->ida_phi[1]);                /* set phi[1] = hh*y' */

    if (IDA_mem->ida_quadr)
      N_VScale(IDA_mem->ida_hh, IDA_mem->ida_phiQ[1], IDA_mem->ida_phiQ[1]);            /* set phiQ[1] = hh*yQ' */

    if (IDA_mem->ida_sensi || IDA_mem->ida_quadr_sensi)
      for (is=0; is<IDA_mem->ida_Ns; is++)
        IDA_mem->ida_cvals[is] = IDA_mem->ida_hh;

    if (IDA_mem->ida_sensi) {
      /* set phiS[1][i] = hh*yS_i' */
      ier = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                                IDA_mem->ida_phiS[1], IDA_mem->ida_phiS[1]);
      if (ier != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
    }

    if (IDA_mem->ida_quadr_sensi) {
      ier = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                                IDA_mem->ida_phiQS[1], IDA_mem->ida_phiQS[1]);
      if (ier != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
    }

    /* Set the convergence test constants epsNewt and toldel */
    IDA_mem->ida_epsNewt = IDA_mem->ida_epcon;
    IDA_mem->ida_toldel = PT0001 * IDA_mem->ida_epsNewt;

  } /* end of first-call block. */

  /* Call lperf function and set nstloc for later performance testing. */

  if (IDA_mem->ida_lperf != NULL)
    IDA_mem->ida_lperf(IDA_mem, 0);
  nstloc = 0;

  /* If not the first call, perform all stopping tests. */

  if (IDA_mem->ida_nst > 0) {

    /* First, check for a root in the last step taken, other than the
       last root found, if any.  If itask = IDA_ONE_STEP and y(tn) was not
       returned because of an intervening root, return y(tn) now.     */

    if (IDA_mem->ida_nrtfn > 0) {

      irfndp = IDA_mem->ida_irfnd;
      
      ier = IDARcheck2(IDA_mem);

      if (ier == CLOSERT) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDARcheck2", MSG_CLOSE_ROOTS, IDA_mem->ida_tlo);
        return(IDA_ILL_INPUT);
      } else if (ier == IDA_RTFUNC_FAIL) {
        IDAProcessError(IDA_mem, IDA_RTFUNC_FAIL, "IDAS", "IDARcheck2", MSG_RTFUNC_FAILED, IDA_mem->ida_tlo);
        return(IDA_RTFUNC_FAIL);
      } else if (ier == RTFOUND) {
        IDA_mem->ida_tretlast = *tret = IDA_mem->ida_tlo;
        return(IDA_ROOT_RETURN);
      }

      /* If tn is distinct from tretlast (within roundoff),
         check remaining interval for roots */
      troundoff = HUNDRED * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh));
      if ( SUNRabs(IDA_mem->ida_tn - IDA_mem->ida_tretlast) > troundoff ) {
        ier = IDARcheck3(IDA_mem);
        if (ier == IDA_SUCCESS) {     /* no root found */
          IDA_mem->ida_irfnd = 0;
          if ((irfndp == 1) && (itask == IDA_ONE_STEP)) {
            IDA_mem->ida_tretlast = *tret = IDA_mem->ida_tn;
            ier = IDAGetSolution(IDA_mem, IDA_mem->ida_tn, yret, ypret);
            return(IDA_SUCCESS);
          }
        } else if (ier == RTFOUND) {  /* a new root was found */
          IDA_mem->ida_irfnd = 1;
          IDA_mem->ida_tretlast = *tret = IDA_mem->ida_tlo;
          return(IDA_ROOT_RETURN);
        } else if (ier == IDA_RTFUNC_FAIL) {  /* g failed */
          IDAProcessError(IDA_mem, IDA_RTFUNC_FAIL, "IDAS", "IDARcheck3", MSG_RTFUNC_FAILED, IDA_mem->ida_tlo);
          return(IDA_RTFUNC_FAIL);
        }
      }

    } /* end of root stop check */


    /* Now test for all other stop conditions. */

    istate = IDAStopTest1(IDA_mem, tout, tret, yret, ypret, itask);
    if (istate != CONTINUE_STEPS) return(istate);
  }

  /* Looping point for internal steps. */

  for(;;) {
   
    /* Check for too many steps taken. */

    if ( (IDA_mem->ida_mxstep>0) && (nstloc >= IDA_mem->ida_mxstep) ) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_MAX_STEPS, IDA_mem->ida_tn);
      istate = IDA_TOO_MUCH_WORK;
      *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tn;
      break; /* Here yy=yret and yp=ypret already have the current solution. */
    }

    /* Call lperf to generate warnings of poor performance. */

    if (IDA_mem->ida_lperf != NULL)
      IDA_mem->ida_lperf(IDA_mem, 1);

    /* Reset and check ewt, ewtQ, ewtS and ewtQS (if not first call). */

    if (IDA_mem->ida_nst > 0) {

      ier = IDA_mem->ida_efun(IDA_mem->ida_phi[0],
                              IDA_mem->ida_ewt, IDA_mem->ida_edata);
      if (ier != 0) {
        if (IDA_mem->ida_itol == IDA_WF)
          IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_EWT_NOW_FAIL, IDA_mem->ida_tn);
        else
          IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_EWT_NOW_BAD, IDA_mem->ida_tn);
        istate = IDA_ILL_INPUT;
        ier = IDAGetSolution(IDA_mem, IDA_mem->ida_tn, yret, ypret);
        *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tn;
        break;
      }

      if (IDA_mem->ida_quadr && IDA_mem->ida_errconQ) {
        ier = IDAQuadEwtSet(IDA_mem, IDA_mem->ida_phiQ[0], IDA_mem->ida_ewtQ);
        if (ier != 0) {
          IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_EWTQ_NOW_BAD, IDA_mem->ida_tn);
          istate = IDA_ILL_INPUT;
          ier = IDAGetSolution(IDA_mem, IDA_mem->ida_tn, yret, ypret);
          *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tn;
          break;
        }
      }

      if (IDA_mem->ida_sensi) {
        ier = IDASensEwtSet(IDA_mem, IDA_mem->ida_phiS[0], IDA_mem->ida_ewtS);
        if (ier != 0) {
          IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_EWTS_NOW_BAD, IDA_mem->ida_tn);
          istate = IDA_ILL_INPUT;
          ier = IDAGetSolution(IDA_mem, IDA_mem->ida_tn, yret, ypret);
          *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tn;
          break;
        }
      }

      if (IDA_mem->ida_quadr_sensi && IDA_mem->ida_errconQS) {
        ier = IDAQuadSensEwtSet(IDA_mem, IDA_mem->ida_phiQS[0], IDA_mem->ida_ewtQS);
        if (ier != 0) {
          IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_EWTQS_NOW_BAD, IDA_mem->ida_tn);
          istate = IDA_ILL_INPUT;
          ier = IDAGetSolution(IDA_mem, IDA_mem->ida_tn, yret, ypret);
          IDA_mem->ida_tretlast = *tret = IDA_mem->ida_tn;
          break;
        }
      }

    }
    
    /* Check for too much accuracy requested. */
    
    nrm = IDAWrmsNorm(IDA_mem, IDA_mem->ida_phi[0],
                      IDA_mem->ida_ewt, IDA_mem->ida_suppressalg);
    if (IDA_mem->ida_errconQ)
      nrm = IDAQuadWrmsNormUpdate(IDA_mem, nrm, IDA_mem->ida_phiQ[0],
                                  IDA_mem->ida_ewtQ);
    if (IDA_mem->ida_errconS)
      nrm = IDASensWrmsNormUpdate(IDA_mem, nrm, IDA_mem->ida_phiS[0],
                                  IDA_mem->ida_ewtS, IDA_mem->ida_suppressalg);
    if (IDA_mem->ida_errconQS)
      nrm = IDAQuadSensWrmsNormUpdate(IDA_mem, nrm, IDA_mem->ida_phiQS[0],
                                      IDA_mem->ida_ewtQS);

    IDA_mem->ida_tolsf = IDA_mem->ida_uround * nrm;
    if (IDA_mem->ida_tolsf > ONE) {
      IDA_mem->ida_tolsf *= TEN;
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASolve", MSG_TOO_MUCH_ACC, IDA_mem->ida_tn);
      istate = IDA_TOO_MUCH_ACC;
      *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tn;
      if (IDA_mem->ida_nst > 0) ier = IDAGetSolution(IDA_mem, IDA_mem->ida_tn, yret, ypret);
      break;
    }

    /* Call IDAStep to take a step. */

    sflag = IDAStep(IDA_mem);

    /* Process all failed-step cases, and exit loop. */

    if (sflag != IDA_SUCCESS) {
      istate = IDAHandleFailure(IDA_mem, sflag);
      *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tn;
      ier = IDAGetSolution(IDA_mem, IDA_mem->ida_tn, yret, ypret);
      break;
    }
    
    nstloc++;

    /* If tstop is set and was reached, reset IDA_mem->ida_tn = tstop */
    if (IDA_mem->ida_tstopset) {
      troundoff = HUNDRED * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh));
      if (SUNRabs(IDA_mem->ida_tn - IDA_mem->ida_tstop) <= troundoff)
        IDA_mem->ida_tn = IDA_mem->ida_tstop;
    }

    /* After successful step, check for stop conditions; continue or break. */

    /* First check for root in the last step taken. */

    if (IDA_mem->ida_nrtfn > 0) {

      ier = IDARcheck3(IDA_mem);

      if (ier == RTFOUND) {  /* A new root was found */
        IDA_mem->ida_irfnd = 1;
        istate = IDA_ROOT_RETURN;
        IDA_mem->ida_tretlast = *tret = IDA_mem->ida_tlo;
        break;
      } else if (ier == IDA_RTFUNC_FAIL) { /* g failed */
        IDAProcessError(IDA_mem, IDA_RTFUNC_FAIL, "IDAS", "IDARcheck3", MSG_RTFUNC_FAILED, IDA_mem->ida_tlo);
        istate = IDA_RTFUNC_FAIL;
        break;
      }

      /* If we are at the end of the first step and we still have
       * some event functions that are inactive, issue a warning
       * as this may indicate a user error in the implementation
       * of the root function. */

      if (IDA_mem->ida_nst==1) {
        inactive_roots = SUNFALSE;
        for (ir=0; ir<IDA_mem->ida_nrtfn; ir++) { 
          if (!IDA_mem->ida_gactive[ir]) {
            inactive_roots = SUNTRUE;
            break;
          }
        }
        if ((IDA_mem->ida_mxgnull > 0) && inactive_roots) {
          IDAProcessError(IDA_mem, IDA_WARNING, "IDAS", "IDASolve", MSG_INACTIVE_ROOTS);
        }
      }

    }

    /* Now check all other stop conditions. */

    istate = IDAStopTest2(IDA_mem, tout, tret, yret, ypret, itask);
    if (istate != CONTINUE_STEPS) break;

  } /* End of step loop */

  return(istate);    
}

/* 
 * -----------------------------------------------------------------
 * Interpolated output and extraction functions
 * -----------------------------------------------------------------
 */



/* 
 * IDAGetDky
 *
 * This routine evaluates the k-th derivative of y(t) as the value of 
 * the k-th derivative of the interpolating polynomial at the independent 
 * variable t, and stores the results in the vector dky.  It uses the current
 * independent variable value, tn, and the method order last used, kused.
 * 
 * The return values are:
 *   IDA_SUCCESS  if t is legal, or
 *   IDA_BAD_T    if t is not within the interval of the last step taken.
 *   IDA_BAD_DKY  if the dky vector is NULL.
 *   IDA_BAD_K    if the requested k is not in the range 0,1,...,order used 
 *
 */

int IDAGetDky(void *ida_mem, realtype t, int k, N_Vector dky)
{
  IDAMem IDA_mem;
  realtype tfuzz, tp, delt, psij_1;
  int i, j, retval;
  realtype cjk  [MXORDP1];
  realtype cjk_1[MXORDP1];

  /* Check ida_mem */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetDky", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem; 

  if (dky == NULL) {
    IDAProcessError(IDA_mem, IDA_BAD_DKY, "IDAS", "IDAGetDky", MSG_NULL_DKY);
    return(IDA_BAD_DKY);
  }
  
  if ((k < 0) || (k > IDA_mem->ida_kused)) {
    IDAProcessError(IDA_mem, IDA_BAD_K, "IDAS", "IDAGetDky", MSG_BAD_K);
    return(IDA_BAD_K);
  }

  /* Check t for legality.  Here tn - hused is t_{n-1}. */

  tfuzz = HUNDRED * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh));
  if (IDA_mem->ida_hh < ZERO) tfuzz = - tfuzz;
  tp = IDA_mem->ida_tn - IDA_mem->ida_hused - tfuzz;
  if ((t - tp)*IDA_mem->ida_hh < ZERO) {
    IDAProcessError(IDA_mem, IDA_BAD_T, "IDAS", "IDAGetDky", MSG_BAD_T,
                    t, IDA_mem->ida_tn-IDA_mem->ida_hused, IDA_mem->ida_tn);
    return(IDA_BAD_T);
  }

  /* Initialize the c_j^(k) and c_k^(k-1) */
  for(i=0; i<MXORDP1; i++) {
    cjk  [i] = 0;
    cjk_1[i] = 0;
  }

  delt = t-IDA_mem->ida_tn;

  for(i=0; i<=k; i++) {

    /* The below reccurence is used to compute the k-th derivative of the solution:
       c_j^(k) = ( k * c_{j-1}^(k-1) + c_{j-1}^{k} (Delta+psi_{j-1}) ) / psi_j
       
       Translated in indexes notation:
       cjk[j] = ( k*cjk_1[j-1] + cjk[j-1]*(delt+psi[j-2]) ) / psi[j-1]

       For k=0, j=1: c_1 = c_0^(-1) + (delt+psi[-1]) / psi[0]

       In order to be able to deal with k=0 in the same way as for k>0, the
       following conventions were adopted:
         - c_0(t) = 1 , c_0^(-1)(t)=0 
         - psij_1 stands for psi[-1]=0 when j=1 
                         for psi[j-2]  when j>1
    */
    if(i==0) {

      cjk[i] = 1;
      psij_1 = 0;
    }else {
      /*                                                i       i-1          1
        c_i^(i) can be always updated since c_i^(i) = -----  --------  ... -----
                                                      psi_j  psi_{j-1}     psi_1
      */
      cjk[i] = cjk[i-1]*i / IDA_mem->ida_psi[i-1];
      psij_1 = IDA_mem->ida_psi[i-1];
    }

    /* update c_j^(i) */

    /*j does not need to go till kused */
    for(j=i+1; j<=IDA_mem->ida_kused-k+i; j++) {

      cjk[j] = ( i* cjk_1[j-1] + cjk[j-1] * (delt + psij_1) ) / IDA_mem->ida_psi[j-1];      
      psij_1 = IDA_mem->ida_psi[j-1];
    }

    /* save existing c_j^(i)'s */
    for(j=i+1; j<=IDA_mem->ida_kused-k+i; j++) cjk_1[j] = cjk[j];
  }

  /* Compute sum (c_j(t) * phi(t)) */

  retval = N_VLinearCombination(IDA_mem->ida_kused-k+1, cjk+k, IDA_mem->ida_phi+k, dky);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  return(IDA_SUCCESS);
}

/*
 * IDAGetQuad
 *
 * The following function can be called to obtain the quadrature 
 * variables after a successful integration step.                 
 *
 * This is just a wrapper that calls IDAGetQuadDky with k=0.
 */

int IDAGetQuad(void *ida_mem, realtype *ptret, N_Vector yQout)
{
  IDAMem IDA_mem;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuad", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem)ida_mem;

  *ptret = IDA_mem->ida_tretlast;

  return IDAGetQuadDky(ida_mem, IDA_mem->ida_tretlast, 0, yQout);
}

/*
 * IDAGetQuadDky
 *
 * Returns the quadrature variables (or their 
 * derivatives up to the current method order) at any time within
 * the last integration step (dense output).
 */
int IDAGetQuadDky(void *ida_mem, realtype t, int k, N_Vector dkyQ)
{
  IDAMem IDA_mem;
  realtype tfuzz, tp, delt, psij_1;
  int i, j, retval;
  realtype cjk  [MXORDP1];
  realtype cjk_1[MXORDP1];

  /* Check ida_mem */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadDky", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem; 

  /* Ckeck if quadrature was initialized */
  if (IDA_mem->ida_quadr != SUNTRUE) {
    IDAProcessError(IDA_mem, IDA_NO_QUAD, "IDAS", "IDAGetQuadDky", MSG_NO_QUAD);
    return(IDA_NO_QUAD);
  }

  if (dkyQ == NULL) {
    IDAProcessError(IDA_mem, IDA_BAD_DKY, "IDAS", "IDAGetQuadDky", MSG_NULL_DKY);
    return(IDA_BAD_DKY);
  }
  
  if ((k < 0) || (k > IDA_mem->ida_kk)) {
    IDAProcessError(IDA_mem, IDA_BAD_K, "IDAS", "IDAGetQuadDky", MSG_BAD_K);
    return(IDA_BAD_K);
  }

  /* Check t for legality.  Here tn - hused is t_{n-1}. */
 
  tfuzz = HUNDRED * IDA_mem->ida_uround * (IDA_mem->ida_tn + IDA_mem->ida_hh);
  tp = IDA_mem->ida_tn - IDA_mem->ida_hused - tfuzz;
  if ( (t - tp)*IDA_mem->ida_hh < ZERO) {
    IDAProcessError(IDA_mem, IDA_BAD_T, "IDAS", "IDAGetQuadDky", MSG_BAD_T,
                    t, IDA_mem->ida_tn-IDA_mem->ida_hused, IDA_mem->ida_tn);
    return(IDA_BAD_T);
  }

  /* Initialize the c_j^(k) and c_k^(k-1) */
  for(i=0; i<MXORDP1; i++) {
    cjk  [i] = 0;
    cjk_1[i] = 0;
  }
  delt = t-IDA_mem->ida_tn;

  for(i=0; i<=k; i++) {

    if(i==0) {
      cjk[i] = 1;
      psij_1 = 0;
    }else {
      cjk[i] = cjk[i-1]*i / IDA_mem->ida_psi[i-1];
      psij_1 = IDA_mem->ida_psi[i-1];
    }

    /* update c_j^(i) */
    for(j=i+1; j<=IDA_mem->ida_kused-k+i; j++) {

      cjk[j] = ( i* cjk_1[j-1] + cjk[j-1] * (delt + psij_1) ) / IDA_mem->ida_psi[j-1];
      psij_1 = IDA_mem->ida_psi[j-1];
    }

    /* save existing c_j^(i)'s */
    for(j=i+1; j<=IDA_mem->ida_kused-k+i; j++) cjk_1[j] = cjk[j];
  }

  /* Compute sum (c_j(t) * phi(t)) */

  retval = N_VLinearCombination(IDA_mem->ida_kused-k+1, cjk+k, IDA_mem->ida_phiQ+k, dkyQ);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  return(IDA_SUCCESS);
}


/* 
 * IDAGetSens
 *
 * This routine extracts sensitivity solution into yySout at the
 * time at which IDASolve returned the solution.
 * This is just a wrapper that calls IDAGetSensDky1 with k=0 and 
 * is=0, 1, ... ,NS-1.
 */

int IDAGetSens(void *ida_mem, realtype *ptret, N_Vector *yySout)
{
  IDAMem IDA_mem;
  int is, ierr=0;

  /* Check ida_mem */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSens", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /*Check the parameters */  
  if (yySout == NULL) {
    IDAProcessError(IDA_mem, IDA_BAD_DKY, "IDAS", "IDAGetSens", MSG_NULL_DKY);
    return(IDA_BAD_DKY);
  }

  /* are sensitivities enabled? */
  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetSens", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  *ptret = IDA_mem->ida_tretlast;
  
  for(is=0; is<IDA_mem->ida_Ns; is++)
    if( IDA_SUCCESS != (ierr = IDAGetSensDky1(ida_mem, *ptret, 0, is, yySout[is])) ) break;

  return(ierr);
}

/*
 * IDAGetSensDky
 *
 * Computes the k-th derivative of all sensitivities of the y function at 
 * time t. It repeatedly calls IDAGetSensDky1. The argument dkyS must be
 * a pointer to N_Vector and must be allocated by the user to hold at 
 * least Ns vectors.
 */
int IDAGetSensDky(void *ida_mem, realtype t, int k, N_Vector *dkySout)
{
  int is, ier=0;
  IDAMem IDA_mem;

  /* Check all inputs for legality */

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSensDky", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetSensDky", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  if (dkySout == NULL) {
    IDAProcessError(IDA_mem, IDA_BAD_DKY, "IDAS", "IDAGetSensDky", MSG_NULL_DKY);
    return(IDA_BAD_DKY);
  }
  
  if ((k < 0) || (k > IDA_mem->ida_kk)) {
    IDAProcessError(IDA_mem, IDA_BAD_K, "IDAS", "IDAGetSensDky", MSG_BAD_K);
    return(IDA_BAD_K);
  } 

  for (is=0; is<IDA_mem->ida_Ns; is++) {
    ier = IDAGetSensDky1(ida_mem, t, k, is, dkySout[is]);
    if (ier!=IDA_SUCCESS) break;
  }
  
  return(ier);
}


/*
 * IDAGetSens1
 *
 * This routine extracts the is-th sensitivity solution into ySout
 * at the time at which IDASolve returned the solution.
 * This is just a wrapper that calls IDASensDky1 with k=0.
 */

int IDAGetSens1(void *ida_mem, realtype *ptret, int is, N_Vector yySret)
{
  IDAMem IDA_mem;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSens1", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  *ptret = IDA_mem->ida_tretlast;

  return IDAGetSensDky1(ida_mem, *ptret, 0, is, yySret);
}

/*
 * IDAGetSensDky1
 *
 * IDASensDky1 computes the kth derivative of the yS[is] function
 * at time t, where tn-hu <= t <= tn, tn denotes the current         
 * internal time reached, and hu is the last internal step size   
 * successfully used by the solver. The user may request 
 * is=0, 1, ..., Ns-1 and k=0, 1, ..., kk, where kk is the current
 * order. The derivative vector is returned in dky. This vector 
 * must be allocated by the caller. It is only legal to call this         
 * function after a successful return from IDASolve with sensitivity 
 * computation enabled.
 */
int IDAGetSensDky1(void *ida_mem, realtype t, int k, int is, N_Vector dkyS)
{
  IDAMem IDA_mem;  
  realtype tfuzz, tp, delt, psij_1;
  int i, j, retval;
  realtype cjk  [MXORDP1];
  realtype cjk_1[MXORDP1];

  /* Check all inputs for legality */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSensDky1", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetSensDky1", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  if (dkyS == NULL) {
    IDAProcessError(IDA_mem, IDA_BAD_DKY, "IDAS", "IDAGetSensDky1", MSG_NULL_DKY);
    return(IDA_BAD_DKY);
  }

  /* Is the requested sensitivity index valid? */
  if(is<0 || is >= IDA_mem->ida_Ns) {
    IDAProcessError(IDA_mem, IDA_BAD_IS, "IDAS", "IDAGetSensDky1", MSG_BAD_IS);
  }
  
  /* Is the requested order valid? */
  if ((k < 0) || (k > IDA_mem->ida_kused)) {
    IDAProcessError(IDA_mem, IDA_BAD_K, "IDAS", "IDAGetSensDky1", MSG_BAD_K);
    return(IDA_BAD_K);
  } 

  /* Check t for legality.  Here tn - hused is t_{n-1}. */
 
  tfuzz = HUNDRED * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh));
  if (IDA_mem->ida_hh < ZERO) tfuzz = - tfuzz;
  tp = IDA_mem->ida_tn - IDA_mem->ida_hused - tfuzz;
  if ((t - tp)*IDA_mem->ida_hh < ZERO) {
    IDAProcessError(IDA_mem, IDA_BAD_T, "IDAS", "IDAGetSensDky1", MSG_BAD_T,
                    t, IDA_mem->ida_tn-IDA_mem->ida_hused, IDA_mem->ida_tn);
    return(IDA_BAD_T);
  }

  /* Initialize the c_j^(k) and c_k^(k-1) */
  for(i=0; i<MXORDP1; i++) {
    cjk  [i] = 0;
    cjk_1[i] = 0;
  }

  delt = t - IDA_mem->ida_tn;

  for(i=0; i<=k; i++) {
    
    if(i==0) {  
      cjk[i] = 1;
      psij_1 = 0;
    }else {     
      cjk[i] = cjk[i-1]*i / IDA_mem->ida_psi[i-1];
      psij_1 = IDA_mem->ida_psi[i-1];
    }

    /* Update cjk based on the reccurence */ 
    for(j=i+1; j<=IDA_mem->ida_kused-k+i; j++) {
      cjk[j] = ( i* cjk_1[j-1] + cjk[j-1] * (delt + psij_1) ) / IDA_mem->ida_psi[j-1];      
      psij_1 = IDA_mem->ida_psi[j-1];
    }

    /* Update cjk_1 for the next step */
    for(j=i+1; j<=IDA_mem->ida_kused-k+i; j++) cjk_1[j] = cjk[j];
  }  

  /* Compute sum (c_j(t) * phi(t)) */
  for(j=k; j<=IDA_mem->ida_kused; j++)
    IDA_mem->ida_Xvecs[j-k] = IDA_mem->ida_phiS[j][is];

  retval = N_VLinearCombination(IDA_mem->ida_kused-k+1, cjk+k,
                                IDA_mem->ida_Xvecs, dkyS);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  return(IDA_SUCCESS);
}

/* 
 * IDAGetQuadSens
 *
 * This routine extracts quadrature sensitivity solution into yyQSout at the
 * time at which IDASolve returned the solution.
 * This is just a wrapper that calls IDAGetQuadSensDky1 with k=0 and 
 * is=0, 1, ... ,NS-1.
 */

int IDAGetQuadSens(void *ida_mem, realtype *ptret, N_Vector *yyQSout)
{
  IDAMem IDA_mem;
  int is, ierr=0;

  /* Check ida_mem */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadSens", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /*Check the parameters */  
  if (yyQSout == NULL) {
    IDAProcessError(IDA_mem, IDA_BAD_DKY, "IDAS", "IDAGetQuadSens", MSG_NULL_DKY);
    return(IDA_BAD_DKY);
  }

  /* are sensitivities enabled? */
  if (IDA_mem->ida_quadr_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetQuadSens", MSG_NO_QUADSENSI);
    return(IDA_NO_SENS);
  }

  *ptret = IDA_mem->ida_tretlast;
  
  for(is=0; is<IDA_mem->ida_Ns; is++)
    if( IDA_SUCCESS != (ierr = IDAGetQuadSensDky1(ida_mem, *ptret, 0, is, yyQSout[is])) ) break;

  return(ierr);
}

/*
 * IDAGetQuadSensDky
 *
 * Computes the k-th derivative of all quadratures sensitivities of the y function at 
 * time t. It repeatedly calls IDAGetQuadSensDky. The argument dkyS must be 
 * a pointer to N_Vector and must be allocated by the user to hold at 
 * least Ns vectors.
 */
int IDAGetQuadSensDky(void *ida_mem, realtype t, int k, N_Vector *dkyQSout)
{
  int is, ier=0;
  IDAMem IDA_mem;

  /* Check all inputs for legality */

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadSensDky", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetQuadSensDky", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  if (IDA_mem->ida_quadr_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAGetQuadSensDky", MSG_NO_QUADSENSI);
    return(IDA_NO_QUADSENS);
  }

  if (dkyQSout == NULL) {
    IDAProcessError(IDA_mem, IDA_BAD_DKY, "IDAS", "IDAGetQuadSensDky", MSG_NULL_DKY);
    return(IDA_BAD_DKY);
  }
  
  if ((k < 0) || (k > IDA_mem->ida_kk)) {
    IDAProcessError(IDA_mem, IDA_BAD_K, "IDAS", "IDAGetQuadSensDky", MSG_BAD_K);
    return(IDA_BAD_K);
  } 

  for (is=0; is<IDA_mem->ida_Ns; is++) {
    ier = IDAGetQuadSensDky1(ida_mem, t, k, is, dkyQSout[is]);
    if (ier!=IDA_SUCCESS) break;
  }
  
  return(ier);
}


/*
 * IDAGetQuadSens1
 *
 * This routine extracts the is-th quadrature sensitivity solution into yQSout
 * at the time at which IDASolve returned the solution.
 * This is just a wrapper that calls IDASensDky1 with k=0.
 */

int IDAGetQuadSens1(void *ida_mem, realtype *ptret, int is, N_Vector yyQSret)
{
  IDAMem IDA_mem;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadSens1", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetQuadSens1", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  if (IDA_mem->ida_quadr_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAGetQuadSens1", MSG_NO_QUADSENSI);
    return(IDA_NO_QUADSENS);
  }

  if (yyQSret == NULL) {
    IDAProcessError(IDA_mem, IDA_BAD_DKY, "IDAS", "IDAGetQuadSens1", MSG_NULL_DKY);
    return(IDA_BAD_DKY);
  }

  *ptret = IDA_mem->ida_tretlast;

  return IDAGetQuadSensDky1(ida_mem, *ptret, 0, is, yyQSret);
}

/*
 * IDAGetQuadSensDky1
 *
 * IDAGetQuadSensDky1 computes the kth derivative of the yS[is] function
 * at time t, where tn-hu <= t <= tn, tn denotes the current         
 * internal time reached, and hu is the last internal step size   
 * successfully used by the solver. The user may request 
 * is=0, 1, ..., Ns-1 and k=0, 1, ..., kk, where kk is the current
 * order. The derivative vector is returned in dky. This vector 
 * must be allocated by the caller. It is only legal to call this         
 * function after a successful return from IDASolve with sensitivity 
 * computation enabled.
 */
int IDAGetQuadSensDky1(void *ida_mem, realtype t, int k, int is, N_Vector dkyQS)
{
  IDAMem IDA_mem;  
  realtype tfuzz, tp, delt, psij_1;
  int i, j, retval;
  realtype cjk  [MXORDP1];
  realtype cjk_1[MXORDP1];

  /* Check all inputs for legality */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadSensDky1", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetQuadSensDky1", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  if (IDA_mem->ida_quadr_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAGetQuadSensDky1", MSG_NO_QUADSENSI);
    return(IDA_NO_QUADSENS);
  }


  if (dkyQS == NULL) {
    IDAProcessError(IDA_mem, IDA_BAD_DKY, "IDAS", "IDAGetQuadSensDky1", MSG_NULL_DKY);
    return(IDA_BAD_DKY);
  }

  /* Is the requested sensitivity index valid*/
  if(is<0 || is >= IDA_mem->ida_Ns) {
    IDAProcessError(IDA_mem, IDA_BAD_IS, "IDAS", "IDAGetQuadSensDky1", MSG_BAD_IS);
  }
  
  /* Is the requested order valid? */
  if ((k < 0) || (k > IDA_mem->ida_kused)) {
    IDAProcessError(IDA_mem, IDA_BAD_K, "IDAS", "IDAGetQuadSensDky1", MSG_BAD_K);
    return(IDA_BAD_K);
  } 

  /* Check t for legality.  Here tn - hused is t_{n-1}. */
 
  tfuzz = HUNDRED * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh));
  if (IDA_mem->ida_hh < ZERO) tfuzz = - tfuzz;
  tp = IDA_mem->ida_tn - IDA_mem->ida_hused - tfuzz;
  if ((t - tp)*IDA_mem->ida_hh < ZERO) {
    IDAProcessError(IDA_mem, IDA_BAD_T, "IDAS", "IDAGetQuadSensDky1", MSG_BAD_T,
                    t, IDA_mem->ida_tn-IDA_mem->ida_hused, IDA_mem->ida_tn);
    return(IDA_BAD_T);
  }

  /* Initialize the c_j^(k) and c_k^(k-1) */
  for(i=0; i<MXORDP1; i++) {
    cjk  [i] = 0;
    cjk_1[i] = 0;
  }

  delt = t - IDA_mem->ida_tn;

  for(i=0; i<=k; i++) {
    
    if(i==0) {  
      cjk[i] = 1;
      psij_1 = 0;
    }else {     
      cjk[i] = cjk[i-1]*i / IDA_mem->ida_psi[i-1];
      psij_1 = IDA_mem->ida_psi[i-1];
    }

    /* Update cjk based on the reccurence */ 
    for(j=i+1; j<=IDA_mem->ida_kused-k+i; j++) {
      cjk[j] = ( i* cjk_1[j-1] + cjk[j-1] * (delt + psij_1) ) / IDA_mem->ida_psi[j-1];      
      psij_1 = IDA_mem->ida_psi[j-1];
    }

    /* Update cjk_1 for the next step */
    for(j=i+1; j<=IDA_mem->ida_kused-k+i; j++) cjk_1[j] = cjk[j];
  }  

  /* Compute sum (c_j(t) * phi(t)) */
  for(j=k; j<=IDA_mem->ida_kused; j++)
    IDA_mem->ida_Xvecs[j-k] = IDA_mem->ida_phiQS[j][is];

  retval = N_VLinearCombination(IDA_mem->ida_kused-k+1, cjk+k,
                                IDA_mem->ida_Xvecs, dkyQS);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  return(IDA_SUCCESS);
}

/* 
 * -----------------------------------------------------------------
 * Deallocation functions
 * -----------------------------------------------------------------
 */

/*
 * IDAFree
 *
 * This routine frees the problem memory allocated by IDAInit
 * Such memory includes all the vectors allocated by IDAAllocVectors,
 * and the memory lmem for the linear solver (deallocated by a call
 * to lfree).
 */

void IDAFree(void **ida_mem)
{
  IDAMem IDA_mem;

  if (*ida_mem == NULL) return;

  IDA_mem = (IDAMem) (*ida_mem);
  
  IDAFreeVectors(IDA_mem);

  IDAQuadFree(IDA_mem);

  IDASensFree(IDA_mem);

  IDAQuadSensFree(IDA_mem);

  IDAAdjFree(IDA_mem);

  if (IDA_mem->ida_lfree != NULL)
    IDA_mem->ida_lfree(IDA_mem);

  if (IDA_mem->ida_nrtfn > 0) {
    free(IDA_mem->ida_glo); IDA_mem->ida_glo = NULL; 
    free(IDA_mem->ida_ghi);  IDA_mem->ida_ghi = NULL;
    free(IDA_mem->ida_grout);  IDA_mem->ida_grout = NULL;
    free(IDA_mem->ida_iroots); IDA_mem->ida_iroots = NULL;
    free(IDA_mem->ida_rootdir); IDA_mem->ida_rootdir = NULL;
    free(IDA_mem->ida_gactive); IDA_mem->ida_gactive = NULL;
  }

  free(IDA_mem->ida_cvals); IDA_mem->ida_cvals = NULL;
  free(IDA_mem->ida_Xvecs); IDA_mem->ida_Xvecs = NULL;
  free(IDA_mem->ida_Zvecs); IDA_mem->ida_Zvecs = NULL;

  /* if IDA created the NLS object then free it */
  if (IDA_mem->ownNLS) {
    SUNNonlinSolFree(IDA_mem->NLS);
    IDA_mem->ownNLS = SUNFALSE;
    IDA_mem->NLS = NULL;
  }

  free(*ida_mem);
  *ida_mem = NULL;
}

/*
 * IDAQuadFree
 *
 * IDAQuadFree frees the problem memory in ida_mem allocated
 * for quadrature integration. Its only argument is the pointer
 * ida_mem returned by IDACreate. 
 */

void IDAQuadFree(void *ida_mem)
{
  IDAMem IDA_mem;
  
  if (ida_mem == NULL) return;
  IDA_mem = (IDAMem) ida_mem;

  if(IDA_mem->ida_quadMallocDone) {
    IDAQuadFreeVectors(IDA_mem);
    IDA_mem->ida_quadMallocDone = SUNFALSE;
    IDA_mem->ida_quadr = SUNFALSE;
  }
}

/*
 * IDASensFree
 *
 * IDASensFree frees the problem memory in ida_mem allocated
 * for sensitivity analysis. Its only argument is the pointer
 * ida_mem returned by IDACreate.
 */

void IDASensFree(void *ida_mem)
{
  IDAMem IDA_mem;

  /* return immediately if IDA memory is NULL */
  if (ida_mem == NULL) return;
  IDA_mem = (IDAMem) ida_mem;

  if(IDA_mem->ida_sensMallocDone) {
    IDASensFreeVectors(IDA_mem);
    IDA_mem->ida_sensMallocDone = SUNFALSE;
    IDA_mem->ida_sensi = SUNFALSE;
  }

  /* free any vector wrappers */
  if (IDA_mem->simMallocDone) {
    N_VDestroy(IDA_mem->ycor0Sim); IDA_mem->ycor0Sim = NULL;
    N_VDestroy(IDA_mem->ycorSim);  IDA_mem->ycorSim  = NULL;
    N_VDestroy(IDA_mem->ewtSim);   IDA_mem->ewtSim   = NULL;
    IDA_mem->simMallocDone = SUNFALSE;
  }
  if (IDA_mem->stgMallocDone) {
    N_VDestroy(IDA_mem->ycor0Stg); IDA_mem->ycor0Stg = NULL;
    N_VDestroy(IDA_mem->ycorStg);  IDA_mem->ycorStg  = NULL;
    N_VDestroy(IDA_mem->ewtStg);   IDA_mem->ewtStg   = NULL;
    IDA_mem->stgMallocDone = SUNFALSE;
  }

  /* if IDA created the NLS object then free it */
  if (IDA_mem->ownNLSsim) {
    SUNNonlinSolFree(IDA_mem->NLSsim);
    IDA_mem->ownNLSsim = SUNFALSE;
    IDA_mem->NLSsim = NULL;
  }
  if (IDA_mem->ownNLSstg) {
    SUNNonlinSolFree(IDA_mem->NLSstg);
    IDA_mem->ownNLSstg = SUNFALSE;
    IDA_mem->NLSstg = NULL;
  }
}

/*
 * IDAQuadSensFree
 *
 * IDAQuadSensFree frees the problem memory in ida_mem allocated
 * for quadrature sensitivity analysis. Its only argument is the 
 * pointer ida_mem returned by IDACreate. 
 */
void IDAQuadSensFree(void* ida_mem)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) return;
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_quadSensMallocDone) {
    IDAQuadSensFreeVectors(IDA_mem);
    IDA_mem->ida_quadSensMallocDone=SUNFALSE;
    IDA_mem->ida_quadr_sensi = SUNFALSE;
  }
}

/* 
 * =================================================================
 * PRIVATE FUNCTIONS
 * =================================================================
 */

/*
 * IDACheckNvector
 *
 * This routine checks if all required vector operations are present.
 * If any of them is missing it returns SUNFALSE.
 */

static booleantype IDACheckNvector(N_Vector tmpl)
{
  if ((tmpl->ops->nvclone        == NULL) ||
     (tmpl->ops->nvdestroy      == NULL) ||
     (tmpl->ops->nvlinearsum    == NULL) ||
     (tmpl->ops->nvconst        == NULL) ||
     (tmpl->ops->nvprod         == NULL) ||
     (tmpl->ops->nvscale        == NULL) ||
     (tmpl->ops->nvabs          == NULL) ||
     (tmpl->ops->nvinv          == NULL) ||
     (tmpl->ops->nvaddconst     == NULL) ||
     (tmpl->ops->nvwrmsnorm     == NULL) ||
     (tmpl->ops->nvmin          == NULL))
    return(SUNFALSE);
  else
    return(SUNTRUE);
}

/* 
 * -----------------------------------------------------------------
 * Memory allocation/deallocation
 * -----------------------------------------------------------------
 */

/*
 * IDAAllocVectors
 *
 * This routine allocates the IDA vectors ewt, tempv1, tempv2, and
 * phi[0], ..., phi[maxord].
 * If all memory allocations are successful, IDAAllocVectors returns 
 * SUNTRUE. Otherwise all allocated memory is freed and IDAAllocVectors 
 * returns SUNFALSE.
 * This routine also sets the optional outputs lrw and liw, which are
 * (respectively) the lengths of the real and integer work spaces
 * allocated here.
 */

static booleantype IDAAllocVectors(IDAMem IDA_mem, N_Vector tmpl)
{
  int i, j, maxcol;

  /* Allocate ewt, ee, delta, yypredict, yppredict, savres, tempv1, tempv2, tempv3 */
  
  IDA_mem->ida_ewt = N_VClone(tmpl);
  if (IDA_mem->ida_ewt == NULL) return(SUNFALSE);

  IDA_mem->ida_ee = N_VClone(tmpl);
  if (IDA_mem->ida_ee == NULL) {
    N_VDestroy(IDA_mem->ida_ewt);
    return(SUNFALSE);
  }

  IDA_mem->ida_delta = N_VClone(tmpl);
  if (IDA_mem->ida_delta == NULL) {
    N_VDestroy(IDA_mem->ida_ewt);
    N_VDestroy(IDA_mem->ida_ee);
    return(SUNFALSE);
  }

  IDA_mem->ida_yypredict = N_VClone(tmpl);
  if (IDA_mem->ida_yypredict == NULL) {
    N_VDestroy(IDA_mem->ida_ewt);
    N_VDestroy(IDA_mem->ida_ee);
    N_VDestroy(IDA_mem->ida_delta);
    return(SUNFALSE);
  }

  IDA_mem->ida_yppredict = N_VClone(tmpl);
  if (IDA_mem->ida_yppredict == NULL) {
    N_VDestroy(IDA_mem->ida_ewt);
    N_VDestroy(IDA_mem->ida_ee);
    N_VDestroy(IDA_mem->ida_delta);
    N_VDestroy(IDA_mem->ida_yypredict);
    return(SUNFALSE);
  }

  IDA_mem->ida_savres = N_VClone(tmpl);
  if (IDA_mem->ida_savres == NULL) {
    N_VDestroy(IDA_mem->ida_ewt);
    N_VDestroy(IDA_mem->ida_ee);
    N_VDestroy(IDA_mem->ida_delta);
    N_VDestroy(IDA_mem->ida_yypredict);
    N_VDestroy(IDA_mem->ida_yppredict);
    return(SUNFALSE);
  }

  IDA_mem->ida_tempv1 = N_VClone(tmpl);
  if (IDA_mem->ida_tempv1 == NULL) {
    N_VDestroy(IDA_mem->ida_ewt);
    N_VDestroy(IDA_mem->ida_ee);
    N_VDestroy(IDA_mem->ida_delta);
    N_VDestroy(IDA_mem->ida_yypredict);
    N_VDestroy(IDA_mem->ida_yppredict);
    N_VDestroy(IDA_mem->ida_savres);
    return(SUNFALSE);
  }

  IDA_mem->ida_tempv2 = N_VClone(tmpl);
  if (IDA_mem->ida_tempv2 == NULL) {
    N_VDestroy(IDA_mem->ida_ewt);
    N_VDestroy(IDA_mem->ida_ee);
    N_VDestroy(IDA_mem->ida_delta);
    N_VDestroy(IDA_mem->ida_yypredict);
    N_VDestroy(IDA_mem->ida_yppredict);
    N_VDestroy(IDA_mem->ida_savres);
    N_VDestroy(IDA_mem->ida_tempv1);
    return(SUNFALSE);
  }

  IDA_mem->ida_tempv3 = N_VClone(tmpl);
  if (IDA_mem->ida_tempv3 == NULL) {
    N_VDestroy(IDA_mem->ida_ewt);
    N_VDestroy(IDA_mem->ida_ee);
    N_VDestroy(IDA_mem->ida_delta);
    N_VDestroy(IDA_mem->ida_yypredict);
    N_VDestroy(IDA_mem->ida_yppredict);
    N_VDestroy(IDA_mem->ida_savres);
    N_VDestroy(IDA_mem->ida_tempv1);
    N_VDestroy(IDA_mem->ida_tempv2);
    return(SUNFALSE);
  }

  /* Allocate phi[0] ... phi[maxord].  Make sure phi[2] and phi[3] are
  allocated (for use as temporary vectors), regardless of maxord.       */

  maxcol = SUNMAX(IDA_mem->ida_maxord,3);
  for (j=0; j <= maxcol; j++) {
    IDA_mem->ida_phi[j] = N_VClone(tmpl);
    if (IDA_mem->ida_phi[j] == NULL) {
      N_VDestroy(IDA_mem->ida_ewt);
      N_VDestroy(IDA_mem->ida_ee);
      N_VDestroy(IDA_mem->ida_delta);
      N_VDestroy(IDA_mem->ida_yypredict);
      N_VDestroy(IDA_mem->ida_yppredict);
      N_VDestroy(IDA_mem->ida_savres);
      N_VDestroy(IDA_mem->ida_tempv1);
      N_VDestroy(IDA_mem->ida_tempv2);
      N_VDestroy(IDA_mem->ida_tempv3);
      for (i=0; i < j; i++)
        N_VDestroy(IDA_mem->ida_phi[i]);
      return(SUNFALSE);
    }
  }

  /* Update solver workspace lengths  */
  IDA_mem->ida_lrw += (maxcol + 10)*IDA_mem->ida_lrw1;
  IDA_mem->ida_liw += (maxcol + 10)*IDA_mem->ida_liw1;

  /* Store the value of maxord used here */
  IDA_mem->ida_maxord_alloc = IDA_mem->ida_maxord;

  return(SUNTRUE);
}

/*
 * IDAfreeVectors
 *
 * This routine frees the IDA vectors allocated for IDA.
 */

static void IDAFreeVectors(IDAMem IDA_mem)
{
  int j, maxcol;
  
  N_VDestroy(IDA_mem->ida_ewt);       IDA_mem->ida_ewt = NULL;
  N_VDestroy(IDA_mem->ida_ee);        IDA_mem->ida_ee = NULL;
  N_VDestroy(IDA_mem->ida_delta);     IDA_mem->ida_delta = NULL;
  N_VDestroy(IDA_mem->ida_yypredict); IDA_mem->ida_yypredict = NULL;
  N_VDestroy(IDA_mem->ida_yppredict); IDA_mem->ida_yppredict = NULL;
  N_VDestroy(IDA_mem->ida_savres);    IDA_mem->ida_savres = NULL;
  N_VDestroy(IDA_mem->ida_tempv1);    IDA_mem->ida_tempv1 = NULL;
  N_VDestroy(IDA_mem->ida_tempv2);    IDA_mem->ida_tempv2 = NULL;
  N_VDestroy(IDA_mem->ida_tempv3);    IDA_mem->ida_tempv3 = NULL;
  maxcol = SUNMAX(IDA_mem->ida_maxord_alloc,3);
  for(j=0; j <= maxcol; j++) {
    N_VDestroy(IDA_mem->ida_phi[j]);
    IDA_mem->ida_phi[j] = NULL;
  }

  IDA_mem->ida_lrw -= (maxcol + 10)*IDA_mem->ida_lrw1;
  IDA_mem->ida_liw -= (maxcol + 10)*IDA_mem->ida_liw1;

  if (IDA_mem->ida_VatolMallocDone) {
    N_VDestroy(IDA_mem->ida_Vatol); IDA_mem->ida_Vatol = NULL;
    IDA_mem->ida_lrw -= IDA_mem->ida_lrw1;
    IDA_mem->ida_liw -= IDA_mem->ida_liw1;
  }

  if (IDA_mem->ida_constraintsMallocDone) {
    N_VDestroy(IDA_mem->ida_constraints); IDA_mem->ida_constraints = NULL;
    IDA_mem->ida_lrw -= IDA_mem->ida_lrw1;
    IDA_mem->ida_liw -= IDA_mem->ida_liw1;
  }

  if (IDA_mem->ida_idMallocDone) {
    N_VDestroy(IDA_mem->ida_id); IDA_mem->ida_id = NULL;
    IDA_mem->ida_lrw -= IDA_mem->ida_lrw1;
    IDA_mem->ida_liw -= IDA_mem->ida_liw1;
  }

}

/*
 * IDAQuadAllocVectors
 *
 * NOTE: Space for ewtQ is allocated even when errconQ=SUNFALSE, 
 * although in this case, ewtQ is never used. The reason for this
 * decision is to allow the user to re-initialize the quadrature
 * computation with errconQ=SUNTRUE, after an initialization with
 * errconQ=SUNFALSE, without new memory allocation within 
 * IDAQuadReInit.
 */

static booleantype IDAQuadAllocVectors(IDAMem IDA_mem, N_Vector tmpl)
{
  int i, j;

  /* Allocate yyQ */
  IDA_mem->ida_yyQ = N_VClone(tmpl);
  if (IDA_mem->ida_yyQ == NULL) {
    return (SUNFALSE);
  }

  /* Allocate ypQ */
  IDA_mem->ida_ypQ = N_VClone(tmpl);
  if (IDA_mem->ida_ypQ == NULL) {
    N_VDestroy(IDA_mem->ida_yyQ);
    return (SUNFALSE);
  }

  /* Allocate ewtQ */
  IDA_mem->ida_ewtQ = N_VClone(tmpl);
  if (IDA_mem->ida_ewtQ == NULL) {
    N_VDestroy(IDA_mem->ida_yyQ);
    N_VDestroy(IDA_mem->ida_ypQ);
    return (SUNFALSE);
  }

  /* Allocate eeQ */
  IDA_mem->ida_eeQ = N_VClone(tmpl);
  if (IDA_mem->ida_eeQ == NULL) {
    N_VDestroy(IDA_mem->ida_yyQ);
    N_VDestroy(IDA_mem->ida_ypQ);
    N_VDestroy(IDA_mem->ida_ewtQ);
    return (SUNFALSE);
  }

  for (j=0; j <= IDA_mem->ida_maxord; j++) {
    IDA_mem->ida_phiQ[j] = N_VClone(tmpl);
    if (IDA_mem->ida_phiQ[j] == NULL) {
      N_VDestroy(IDA_mem->ida_yyQ);
      N_VDestroy(IDA_mem->ida_ypQ);
      N_VDestroy(IDA_mem->ida_ewtQ);
      N_VDestroy(IDA_mem->ida_eeQ);
      for (i=0; i < j; i++) N_VDestroy(IDA_mem->ida_phiQ[i]);
      return(SUNFALSE);
    }
  }

  IDA_mem->ida_lrw += (IDA_mem->ida_maxord+4)*IDA_mem->ida_lrw1Q;
  IDA_mem->ida_liw += (IDA_mem->ida_maxord+4)*IDA_mem->ida_liw1Q;

  return(SUNTRUE);
}



/*
 * IDAQuadFreeVectors
 *
 * This routine frees the IDAS vectors allocated in IDAQuadAllocVectors.
 */

static void IDAQuadFreeVectors(IDAMem IDA_mem)
{
  int j;

  N_VDestroy(IDA_mem->ida_yyQ);   IDA_mem->ida_yyQ = NULL;
  N_VDestroy(IDA_mem->ida_ypQ);   IDA_mem->ida_ypQ = NULL;
  N_VDestroy(IDA_mem->ida_ewtQ); IDA_mem->ida_ewtQ = NULL;
  N_VDestroy(IDA_mem->ida_eeQ);   IDA_mem->ida_eeQ = NULL;
  for(j=0; j <= IDA_mem->ida_maxord; j++) {
    N_VDestroy(IDA_mem->ida_phiQ[j]);
    IDA_mem->ida_phiQ[j] = NULL;
  }

  IDA_mem->ida_lrw -= (IDA_mem->ida_maxord+5)*IDA_mem->ida_lrw1Q;
  IDA_mem->ida_liw -= (IDA_mem->ida_maxord+5)*IDA_mem->ida_liw1Q;

  if (IDA_mem->ida_VatolQMallocDone) {
    N_VDestroy(IDA_mem->ida_VatolQ); IDA_mem->ida_VatolQ = NULL;
    IDA_mem->ida_lrw -= IDA_mem->ida_lrw1Q;
    IDA_mem->ida_liw -= IDA_mem->ida_liw1Q;
  }

  IDA_mem->ida_VatolQMallocDone = SUNFALSE;
}

/*
 * IDASensAllocVectors
 *
 * Allocates space for the N_Vectors, plist, and pbar required for FSA.
 */

static booleantype IDASensAllocVectors(IDAMem IDA_mem, N_Vector tmpl)
{
  int j, maxcol;
  
  IDA_mem->ida_tmpS1 = IDA_mem->ida_tempv1;
  IDA_mem->ida_tmpS2 = IDA_mem->ida_tempv2;

  /* Allocate space for workspace vectors */

  IDA_mem->ida_tmpS3 = N_VClone(tmpl);
  if (IDA_mem->ida_tmpS3==NULL) {
    return(SUNFALSE);
  }
  
  IDA_mem->ida_ewtS = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_ewtS==NULL) {
    N_VDestroy(IDA_mem->ida_tmpS3);
    return(SUNFALSE);
  }

  IDA_mem->ida_eeS = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_eeS==NULL) {
    N_VDestroy(IDA_mem->ida_tmpS3);
    N_VDestroyVectorArray(IDA_mem->ida_ewtS, IDA_mem->ida_Ns);
    return(SUNFALSE);
  }

  IDA_mem->ida_yyS = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_yyS==NULL) {
    N_VDestroyVectorArray(IDA_mem->ida_eeS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ewtS, IDA_mem->ida_Ns);
    N_VDestroy(IDA_mem->ida_tmpS3);
    return(SUNFALSE);
  }
  
  IDA_mem->ida_ypS = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_ypS==NULL) {
    N_VDestroyVectorArray(IDA_mem->ida_yyS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_eeS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ewtS, IDA_mem->ida_Ns);
    N_VDestroy(IDA_mem->ida_tmpS3);
    return(SUNFALSE);
  }

  IDA_mem->ida_yySpredict = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_yySpredict==NULL) {
    N_VDestroyVectorArray(IDA_mem->ida_ypS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_yyS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_eeS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ewtS, IDA_mem->ida_Ns);
    N_VDestroy(IDA_mem->ida_tmpS3);
    return(SUNFALSE);
  }

  IDA_mem->ida_ypSpredict = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_ypSpredict==NULL) {
    N_VDestroyVectorArray(IDA_mem->ida_yySpredict, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ypS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_yyS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_eeS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ewtS, IDA_mem->ida_Ns);
    N_VDestroy(IDA_mem->ida_tmpS3);
    return(SUNFALSE);
  }

  IDA_mem->ida_deltaS = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_deltaS==NULL) {
    N_VDestroyVectorArray(IDA_mem->ida_ypSpredict, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_yySpredict, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ypS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_yyS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_eeS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ewtS, IDA_mem->ida_Ns);
    N_VDestroy(IDA_mem->ida_tmpS3);
    return(SUNFALSE);
  }

  /* Update solver workspace lengths */
  IDA_mem->ida_lrw += (5*IDA_mem->ida_Ns+1)*IDA_mem->ida_lrw1;
  IDA_mem->ida_liw += (5*IDA_mem->ida_Ns+1)*IDA_mem->ida_liw1;

  /* Allocate space for phiS */
  /*  Make sure phiS[2], phiS[3] and phiS[4] are
      allocated (for use as temporary vectors), regardless of maxord.*/

  maxcol = SUNMAX(IDA_mem->ida_maxord,4);
  for (j=0; j <= maxcol; j++) {
    IDA_mem->ida_phiS[j] = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
    if (IDA_mem->ida_phiS[j] == NULL) {
      N_VDestroy(IDA_mem->ida_tmpS3);
      N_VDestroyVectorArray(IDA_mem->ida_ewtS, IDA_mem->ida_Ns);
      N_VDestroyVectorArray(IDA_mem->ida_eeS, IDA_mem->ida_Ns);
      N_VDestroyVectorArray(IDA_mem->ida_yyS, IDA_mem->ida_Ns);
      N_VDestroyVectorArray(IDA_mem->ida_ypS, IDA_mem->ida_Ns);
      N_VDestroyVectorArray(IDA_mem->ida_yySpredict, IDA_mem->ida_Ns);
      N_VDestroyVectorArray(IDA_mem->ida_ypSpredict, IDA_mem->ida_Ns);
      N_VDestroyVectorArray(IDA_mem->ida_deltaS, IDA_mem->ida_Ns);
      return(SUNFALSE);
    }
  }

  /* Update solver workspace lengths */
  IDA_mem->ida_lrw += maxcol*IDA_mem->ida_Ns*IDA_mem->ida_lrw1;
  IDA_mem->ida_liw += maxcol*IDA_mem->ida_Ns*IDA_mem->ida_liw1;

  /* Allocate space for pbar and plist */

  IDA_mem->ida_pbar = NULL;
  IDA_mem->ida_pbar = (realtype *)malloc(IDA_mem->ida_Ns*sizeof(realtype));
  if (IDA_mem->ida_pbar == NULL) {
    N_VDestroy(IDA_mem->ida_tmpS3);
    N_VDestroyVectorArray(IDA_mem->ida_ewtS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_eeS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_yyS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ypS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_yySpredict, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ypSpredict, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_deltaS, IDA_mem->ida_Ns);
    for (j=0; j<=maxcol; j++) N_VDestroyVectorArray(IDA_mem->ida_phiS[j], IDA_mem->ida_Ns);
    return(SUNFALSE);
  }

  IDA_mem->ida_plist = NULL;
  IDA_mem->ida_plist = (int *)malloc(IDA_mem->ida_Ns*sizeof(int));
  if (IDA_mem->ida_plist == NULL) {
    N_VDestroy(IDA_mem->ida_tmpS3);
    N_VDestroyVectorArray(IDA_mem->ida_ewtS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_eeS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_yyS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ypS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_yySpredict, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ypSpredict, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_deltaS, IDA_mem->ida_Ns);
    for (j=0; j<=maxcol; j++) N_VDestroyVectorArray(IDA_mem->ida_phiS[j], IDA_mem->ida_Ns);
    free(IDA_mem->ida_pbar); IDA_mem->ida_pbar = NULL;
    return(SUNFALSE);
  }

  /* Update solver workspace lengths */
  IDA_mem->ida_lrw += IDA_mem->ida_Ns;
  IDA_mem->ida_liw += IDA_mem->ida_Ns;

  return(SUNTRUE);
}

/*
 * IDASensFreeVectors
 *
 * Frees memory allocated by IDASensAllocVectors.
 */

static void IDASensFreeVectors(IDAMem IDA_mem)
{
  int j, maxcol;

  N_VDestroyVectorArray(IDA_mem->ida_deltaS, IDA_mem->ida_Ns);
  N_VDestroyVectorArray(IDA_mem->ida_ypSpredict, IDA_mem->ida_Ns);
  N_VDestroyVectorArray(IDA_mem->ida_yySpredict, IDA_mem->ida_Ns);
  N_VDestroyVectorArray(IDA_mem->ida_ypS, IDA_mem->ida_Ns);
  N_VDestroyVectorArray(IDA_mem->ida_yyS, IDA_mem->ida_Ns);
  N_VDestroyVectorArray(IDA_mem->ida_eeS, IDA_mem->ida_Ns);
  N_VDestroyVectorArray(IDA_mem->ida_ewtS, IDA_mem->ida_Ns);
  N_VDestroy(IDA_mem->ida_tmpS3);

  maxcol = SUNMAX(IDA_mem->ida_maxord_alloc, 4);
  for (j=0; j<=maxcol; j++) 
    N_VDestroyVectorArray(IDA_mem->ida_phiS[j], IDA_mem->ida_Ns);

  free(IDA_mem->ida_pbar); IDA_mem->ida_pbar = NULL;
  free(IDA_mem->ida_plist); IDA_mem->ida_plist = NULL;

  IDA_mem->ida_lrw -= ( (maxcol+3)*IDA_mem->ida_Ns + 1 ) * IDA_mem->ida_lrw1 + IDA_mem->ida_Ns;
  IDA_mem->ida_liw -= ( (maxcol+3)*IDA_mem->ida_Ns + 1 ) * IDA_mem->ida_liw1 + IDA_mem->ida_Ns;

  if (IDA_mem->ida_VatolSMallocDone) {
    N_VDestroyVectorArray(IDA_mem->ida_VatolS, IDA_mem->ida_Ns);
    IDA_mem->ida_lrw -= IDA_mem->ida_Ns*IDA_mem->ida_lrw1;
    IDA_mem->ida_liw -= IDA_mem->ida_Ns*IDA_mem->ida_liw1;
    IDA_mem->ida_VatolSMallocDone = SUNFALSE;
  } 
  if (IDA_mem->ida_SatolSMallocDone) {
    free(IDA_mem->ida_SatolS); IDA_mem->ida_SatolS = NULL;
    IDA_mem->ida_lrw -= IDA_mem->ida_Ns;
    IDA_mem->ida_SatolSMallocDone = SUNFALSE;
  }
}


/*
 * IDAQuadSensAllocVectors
 *
 * Create (through duplication) N_Vectors used for quadrature sensitivity analysis, 
 * using the N_Vector 'tmpl' as a template.
 */

static booleantype IDAQuadSensAllocVectors(IDAMem IDA_mem, N_Vector tmpl) 
{
  int i, j, maxcol;

  /* Allocate yQS */
  IDA_mem->ida_yyQS = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_yyQS == NULL) {
    return(SUNFALSE);
  }

  /* Allocate ewtQS */
  IDA_mem->ida_ewtQS = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_ewtQS == NULL) {
    N_VDestroyVectorArray(IDA_mem->ida_yyQS, IDA_mem->ida_Ns);
    return(SUNFALSE);
  }

  /* Allocate tempvQS */
  IDA_mem->ida_tempvQS = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_tempvQS == NULL) {
    N_VDestroyVectorArray(IDA_mem->ida_yyQS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ewtQS, IDA_mem->ida_Ns);
    return(SUNFALSE);
  }

  IDA_mem->ida_eeQS =  N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
  if (IDA_mem->ida_eeQS == NULL) {
    N_VDestroyVectorArray(IDA_mem->ida_yyQS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ewtQS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_tempvQS, IDA_mem->ida_Ns);
    return(SUNFALSE);
  }

  IDA_mem->ida_savrhsQ = N_VClone(tmpl);
  if (IDA_mem->ida_savrhsQ == NULL) {
    N_VDestroyVectorArray(IDA_mem->ida_yyQS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_ewtQS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_tempvQS, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDA_mem->ida_eeQS, IDA_mem->ida_Ns);
  }

  maxcol = SUNMAX(IDA_mem->ida_maxord,4);
  /* Allocate phiQS */
  for (j=0; j<=maxcol; j++) {
    IDA_mem->ida_phiQS[j] = N_VCloneVectorArray(IDA_mem->ida_Ns, tmpl);
    if (IDA_mem->ida_phiQS[j] == NULL) {
      N_VDestroyVectorArray(IDA_mem->ida_yyQS, IDA_mem->ida_Ns);
      N_VDestroyVectorArray(IDA_mem->ida_ewtQS, IDA_mem->ida_Ns);
      N_VDestroyVectorArray(IDA_mem->ida_tempvQS, IDA_mem->ida_Ns);
      N_VDestroyVectorArray(IDA_mem->ida_eeQS, IDA_mem->ida_Ns);
      N_VDestroy(IDA_mem->ida_savrhsQ);
      for (i=0; i<j; i++)
        N_VDestroyVectorArray(IDA_mem->ida_phiQS[i], IDA_mem->ida_Ns);
      return(SUNFALSE);
    }
  }

  /* Update solver workspace lengths */
  IDA_mem->ida_lrw += (maxcol + 5)*IDA_mem->ida_Ns*IDA_mem->ida_lrw1Q;
  IDA_mem->ida_liw += (maxcol + 5)*IDA_mem->ida_Ns*IDA_mem->ida_liw1Q;

  return(SUNTRUE);
}


/*
 * IDAQuadSensFreeVectors
 *
 * This routine frees the IDAS vectors allocated in IDAQuadSensAllocVectors.
 */

static void IDAQuadSensFreeVectors(IDAMem IDA_mem)
{
  int j, maxcol;

  maxcol = SUNMAX(IDA_mem->ida_maxord, 4);

  N_VDestroyVectorArray(IDA_mem->ida_yyQS, IDA_mem->ida_Ns);
  N_VDestroyVectorArray(IDA_mem->ida_ewtQS, IDA_mem->ida_Ns);
  N_VDestroyVectorArray(IDA_mem->ida_eeQS, IDA_mem->ida_Ns);
  N_VDestroyVectorArray(IDA_mem->ida_tempvQS, IDA_mem->ida_Ns);
  N_VDestroy(IDA_mem->ida_savrhsQ);

  for (j=0; j<=maxcol; j++) N_VDestroyVectorArray(IDA_mem->ida_phiQS[j], IDA_mem->ida_Ns);  

  IDA_mem->ida_lrw -= (maxcol + 5)*IDA_mem->ida_Ns*IDA_mem->ida_lrw1Q;
  IDA_mem->ida_liw -= (maxcol + 5)*IDA_mem->ida_Ns*IDA_mem->ida_liw1Q;

  if (IDA_mem->ida_VatolQSMallocDone) {
    N_VDestroyVectorArray(IDA_mem->ida_VatolQS, IDA_mem->ida_Ns);
    IDA_mem->ida_lrw -= IDA_mem->ida_Ns*IDA_mem->ida_lrw1Q;
    IDA_mem->ida_liw -= IDA_mem->ida_Ns*IDA_mem->ida_liw1Q;
  }
  if (IDA_mem->ida_SatolQSMallocDone) {
    free(IDA_mem->ida_SatolQS); IDA_mem->ida_SatolQS = NULL;
    IDA_mem->ida_lrw -= IDA_mem->ida_Ns;
  }
  IDA_mem->ida_VatolQSMallocDone = SUNFALSE;
  IDA_mem->ida_SatolQSMallocDone = SUNFALSE;
}


/* 
 * -----------------------------------------------------------------
 * Initial setup
 * -----------------------------------------------------------------
 */

/*
 * IDAInitialSetup
 *
 * This routine is called by IDASolve once at the first step. 
 * It performs all checks on optional inputs and inputs to 
 * IDAInit/IDAReInit that could not be done before.
 *
 * If no merror is encountered, IDAInitialSetup returns IDA_SUCCESS. 
 * Otherwise, it returns an error flag and reported to the error 
 * handler function.
 */

int IDAInitialSetup(IDAMem IDA_mem)
{
  booleantype conOK;
  int ier, retval;

  /* Test for more vector operations, depending on options */
  if (IDA_mem->ida_suppressalg)
    if (IDA_mem->ida_phi[0]->ops->nvwrmsnormmask == NULL) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_BAD_NVECTOR);
      return(IDA_ILL_INPUT);
  }

  /* Test id vector for legality */
  if (IDA_mem->ida_suppressalg && (IDA_mem->ida_id==NULL)){ 
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_MISSING_ID);
    return(IDA_ILL_INPUT); 
  }

  /* Did the user specify tolerances? */
  if (IDA_mem->ida_itol == IDA_NN) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_NO_TOLS);
    return(IDA_ILL_INPUT);
  }

  /* Set data for efun */
  if (IDA_mem->ida_user_efun) IDA_mem->ida_edata = IDA_mem->ida_user_data;
  else                        IDA_mem->ida_edata = IDA_mem;

  /* Initial error weight vectors */
  ier = IDA_mem->ida_efun(IDA_mem->ida_phi[0], IDA_mem->ida_ewt, IDA_mem->ida_edata);
  if (ier != 0) {
    if (IDA_mem->ida_itol == IDA_WF) 
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_FAIL_EWT);
    else
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_BAD_EWT);
    return(IDA_ILL_INPUT);
  }

  if (IDA_mem->ida_quadr) {

    /* Evaluate quadrature rhs and set phiQ[1] */
    retval = IDA_mem->ida_rhsQ(IDA_mem->ida_tn, IDA_mem->ida_phi[0],
                               IDA_mem->ida_phi[1], IDA_mem->ida_phiQ[1],
                               IDA_mem->ida_user_data);
    IDA_mem->ida_nrQe++;
    if (retval < 0) {
      IDAProcessError(IDA_mem, IDA_QRHS_FAIL, "IDAS", "IDAInitialSetup", MSG_QRHSFUNC_FAILED);
      return(IDA_QRHS_FAIL);
    } else if (retval > 0) {
      IDAProcessError(IDA_mem, IDA_FIRST_QRHS_ERR, "IDAS", "IDAInitialSetup", MSG_QRHSFUNC_FIRST);
      return(IDA_FIRST_QRHS_ERR);
    }

    if (IDA_mem->ida_errconQ) {

      /* Did the user specify tolerances? */
      if (IDA_mem->ida_itolQ == IDA_NN) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_NO_TOLQ);
        return(IDA_ILL_INPUT);
      }
      
      /* Load ewtQ */
      ier = IDAQuadEwtSet(IDA_mem, IDA_mem->ida_phiQ[0], IDA_mem->ida_ewtQ);
      if (ier != 0) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_BAD_EWTQ);
        return(IDA_ILL_INPUT);
      }
    }
  } else {
    IDA_mem->ida_errconQ = SUNFALSE;
  }

  if (IDA_mem->ida_sensi) {

    /* Did the user specify tolerances? */
    if (IDA_mem->ida_itolS == IDA_NN) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_NO_TOLS);
      return(IDA_ILL_INPUT);
    }
    
    /* Load ewtS */
    ier = IDASensEwtSet(IDA_mem, IDA_mem->ida_phiS[0], IDA_mem->ida_ewtS);
    if (ier != 0) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_BAD_EWTS);
      return(IDA_ILL_INPUT);
    }
  } else {
    IDA_mem->ida_errconS = SUNFALSE;
  }

  if (IDA_mem->ida_quadr_sensi) {

    /* store the quadrature sensitivity residual. */
    retval = IDA_mem->ida_rhsQS(IDA_mem->ida_Ns, IDA_mem->ida_tn,
                                IDA_mem->ida_phi[0], IDA_mem->ida_phi[1],
                                IDA_mem->ida_phiS[0], IDA_mem->ida_phiS[1],
                                IDA_mem->ida_phiQ[1], IDA_mem->ida_phiQS[1],
                                IDA_mem->ida_user_dataQS, IDA_mem->ida_tmpS1, IDA_mem->ida_tmpS2, IDA_mem->ida_tmpS3);
    IDA_mem->ida_nrQSe++;
    if (retval < 0) {
      IDAProcessError(IDA_mem, IDA_QSRHS_FAIL, "IDAS", "IDAInitialSetup", MSG_QSRHSFUNC_FAILED);
      return(IDA_QRHS_FAIL);
    } else if (retval > 0) {
      IDAProcessError(IDA_mem, IDA_FIRST_QSRHS_ERR, "IDAS", "IDAInitialSetup", MSG_QSRHSFUNC_FIRST);
      return(IDA_FIRST_QSRHS_ERR);
    }

    /* If using the internal DQ functions, we must have access to fQ
     * (i.e. quadrature integration must be enabled) and to the problem parameters */

    if (IDA_mem->ida_rhsQSDQ) {
      
      /* Test if quadratures are defined, so we can use fQ */
      if (!IDA_mem->ida_quadr) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_NULL_RHSQ);
        return(IDA_ILL_INPUT);
      }
      
      /* Test if we have the problem parameters */
      if (IDA_mem->ida_p == NULL) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_NULL_P);
        return(IDA_ILL_INPUT);
      }
    }

    if (IDA_mem->ida_errconQS) {
      /* Did the user specify tolerances? */
      if (IDA_mem->ida_itolQS == IDA_NN) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_NO_TOLQS);
        return(IDA_ILL_INPUT);
      }

      /* If needed, did the user provide quadrature tolerances? */
      if ( (IDA_mem->ida_itolQS == IDA_EE) && (IDA_mem->ida_itolQ == IDA_NN) ) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_NO_TOLQ);
        return(IDA_ILL_INPUT);
      }
    
      /* Load ewtS */
      ier = IDAQuadSensEwtSet(IDA_mem, IDA_mem->ida_phiQS[0], IDA_mem->ida_ewtQS);
      if (ier != 0) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_BAD_EWTQS);
        return(IDA_ILL_INPUT);
      }
    }
  } else {
    IDA_mem->ida_errconQS = SUNFALSE;
  }

  /* Check to see if y0 satisfies constraints. */
  if (IDA_mem->ida_constraintsSet) {

    if (IDA_mem->ida_sensi && (IDA_mem->ida_ism==IDA_SIMULTANEOUS)) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_BAD_ISM_CONSTR);
      return(IDA_ILL_INPUT);
    }

    conOK = N_VConstrMask(IDA_mem->ida_constraints, IDA_mem->ida_phi[0], IDA_mem->ida_tempv2);
    if (!conOK) { 
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAInitialSetup", MSG_Y0_FAIL_CONSTR);
      return(IDA_ILL_INPUT); 
    }
  }

  /* Call linit function if it exists. */
  if (IDA_mem->ida_linit != NULL) {
    retval = IDA_mem->ida_linit(IDA_mem);
    if (retval != 0) {
      IDAProcessError(IDA_mem, IDA_LINIT_FAIL, "IDAS", "IDAInitialSetup", MSG_LINIT_FAIL);
      return(IDA_LINIT_FAIL);
    }
  }

  /* Initialize the nonlinear solver (must occur after linear solver is initialize) so
   * that lsetup and lsolve pointers have been set */

  /* always initialize the DAE NLS in case the user disables sensitivities later */
  ier = idaNlsInit(IDA_mem);

  if (ier != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_NLS_INIT_FAIL, "IDAS",
                    "IDAInitialSetup", MSG_NLS_INIT_FAIL);
    return(IDA_NLS_INIT_FAIL);
  }

  if (IDA_mem->NLSsim != NULL) {
    ier = idaNlsInitSensSim(IDA_mem);

    if (ier != IDA_SUCCESS) {
      IDAProcessError(IDA_mem, IDA_NLS_INIT_FAIL, "IDAS",
                      "IDAInitialSetup", MSG_NLS_INIT_FAIL);
      return(IDA_NLS_INIT_FAIL);
    }
  }

  if (IDA_mem->NLSstg != NULL) {
    ier = idaNlsInitSensStg(IDA_mem);

    if (ier != IDA_SUCCESS) {
      IDAProcessError(IDA_mem, IDA_NLS_INIT_FAIL, "IDAS",
                      "IDAInitialSetup", MSG_NLS_INIT_FAIL);
      return(IDA_NLS_INIT_FAIL);
    }
  }

  return(IDA_SUCCESS);
}

/*
 * IDAEwtSet
 *
 * This routine is responsible for loading the error weight vector
 * ewt, according to itol, as follows:
 * (1) ewt[i] = 1 / (rtol * SUNRabs(ycur[i]) + atol), i=0,...,Neq-1
 *     if itol = IDA_SS
 * (2) ewt[i] = 1 / (rtol * SUNRabs(ycur[i]) + atol[i]), i=0,...,Neq-1
 *     if itol = IDA_SV
 *
 *  IDAEwtSet returns 0 if ewt is successfully set as above to a
 *  positive vector and -1 otherwise. In the latter case, ewt is
 *  considered undefined.
 *
 * All the real work is done in the routines IDAEwtSetSS, IDAEwtSetSV.
 */

int IDAEwtSet(N_Vector ycur, N_Vector weight, void *data)
{
  IDAMem IDA_mem;
  int flag = 0;

  /* data points to IDA_mem here */

  IDA_mem = (IDAMem) data;

  switch(IDA_mem->ida_itol) {
  case IDA_SS: 
    flag = IDAEwtSetSS(IDA_mem, ycur, weight); 
    break;
  case IDA_SV: 
    flag = IDAEwtSetSV(IDA_mem, ycur, weight); 
    break;
  }
  return(flag);
}

/*
 * IDAEwtSetSS
 *
 * This routine sets ewt as decribed above in the case itol=IDA_SS.
 * It tests for non-positive components before inverting. IDAEwtSetSS
 * returns 0 if ewt is successfully set to a positive vector
 * and -1 otherwise. In the latter case, ewt is considered
 * undefined.
 */

static int IDAEwtSetSS(IDAMem IDA_mem, N_Vector ycur, N_Vector weight)
{
  N_VAbs(ycur, IDA_mem->ida_tempv1);
  N_VScale(IDA_mem->ida_rtol, IDA_mem->ida_tempv1, IDA_mem->ida_tempv1);
  N_VAddConst(IDA_mem->ida_tempv1, IDA_mem->ida_Satol, IDA_mem->ida_tempv1);
  if (N_VMin(IDA_mem->ida_tempv1) <= ZERO) return(-1);
  N_VInv(IDA_mem->ida_tempv1, weight);
  return(0);
}

/*
 * IDAEwtSetSV
 *
 * This routine sets ewt as decribed above in the case itol=IDA_SV.
 * It tests for non-positive components before inverting. IDAEwtSetSV
 * returns 0 if ewt is successfully set to a positive vector
 * and -1 otherwise. In the latter case, ewt is considered
 * undefined.
 */

static int IDAEwtSetSV(IDAMem IDA_mem, N_Vector ycur, N_Vector weight)
{
  N_VAbs(ycur, IDA_mem->ida_tempv1);
  N_VLinearSum(IDA_mem->ida_rtol, IDA_mem->ida_tempv1, ONE, IDA_mem->ida_Vatol, IDA_mem->ida_tempv1);
  if (N_VMin(IDA_mem->ida_tempv1) <= ZERO) return(-1);
  N_VInv(IDA_mem->ida_tempv1, weight);
  return(0);
}

/*
 * IDAQuadEwtSet
 *
 */

static int IDAQuadEwtSet(IDAMem IDA_mem, N_Vector qcur, N_Vector weightQ)
{
  int flag=0;

  switch (IDA_mem->ida_itolQ) {
  case IDA_SS: 
    flag = IDAQuadEwtSetSS(IDA_mem, qcur, weightQ);
    break;
  case IDA_SV: 
    flag = IDAQuadEwtSetSV(IDA_mem, qcur, weightQ);
    break;
  }

  return(flag);

}

/*
 * IDAQuadEwtSetSS
 *
 */

static int IDAQuadEwtSetSS(IDAMem IDA_mem, N_Vector qcur, N_Vector weightQ)
{
  N_Vector tempvQ;

  /* Use ypQ as temporary storage */
  tempvQ = IDA_mem->ida_ypQ;

  N_VAbs(qcur, tempvQ);
  N_VScale(IDA_mem->ida_rtolQ, tempvQ, tempvQ);
  N_VAddConst(tempvQ, IDA_mem->ida_SatolQ, tempvQ);
  if (N_VMin(tempvQ) <= ZERO) return(-1);
  N_VInv(tempvQ, weightQ);

  return(0);
}

/*
 * IDAQuadEwtSetSV
 *
 */

static int IDAQuadEwtSetSV(IDAMem IDA_mem, N_Vector qcur, N_Vector weightQ)
{
  N_Vector tempvQ;

  /* Use ypQ as temporary storage */
  tempvQ = IDA_mem->ida_ypQ;

  N_VAbs(qcur, tempvQ);
  N_VLinearSum(IDA_mem->ida_rtolQ, tempvQ, ONE, IDA_mem->ida_VatolQ, tempvQ);
  if (N_VMin(tempvQ) <= ZERO) return(-1);
  N_VInv(tempvQ, weightQ);

  return(0);
}

/*
 * IDASensEwtSet
 *
 */

int IDASensEwtSet(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS)
{
  int flag=0;

  switch (IDA_mem->ida_itolS) {
  case IDA_EE:
    flag = IDASensEwtSetEE(IDA_mem, yScur, weightS);
    break;
  case IDA_SS: 
    flag = IDASensEwtSetSS(IDA_mem, yScur, weightS);
    break;
  case IDA_SV: 
    flag = IDASensEwtSetSV(IDA_mem, yScur, weightS);
    break;
  }

  return(flag);

}

/*
 * IDASensEwtSetEE
 *
 * In this case, the error weight vector for the i-th sensitivity is set to
 *
 * ewtS_i = pbar_i * efun(pbar_i*yS_i)
 *
 * In other words, the scaled sensitivity pbar_i * yS_i has the same error
 * weight vector calculation as the solution vector.
 *
 */

static int IDASensEwtSetEE(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS)
{
  int is;
  N_Vector pyS;
  int flag;

  /* Use tempv1 as temporary storage for the scaled sensitivity */
  pyS = IDA_mem->ida_tempv1;

  for (is=0; is<IDA_mem->ida_Ns; is++) {
    N_VScale(IDA_mem->ida_pbar[is], yScur[is], pyS);
    flag = IDA_mem->ida_efun(pyS, weightS[is], IDA_mem->ida_edata);
    if (flag != 0) return(-1);
    N_VScale(IDA_mem->ida_pbar[is], weightS[is], weightS[is]);
  }

  return(0);
}

/*
 * IDASensEwtSetSS
 *
 */

static int IDASensEwtSetSS(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS)
{
  int is;
  
  for (is=0; is<IDA_mem->ida_Ns; is++) {
    N_VAbs(yScur[is], IDA_mem->ida_tempv1);
    N_VScale(IDA_mem->ida_rtolS, IDA_mem->ida_tempv1, IDA_mem->ida_tempv1);
    N_VAddConst(IDA_mem->ida_tempv1, IDA_mem->ida_SatolS[is], IDA_mem->ida_tempv1);
    if (N_VMin(IDA_mem->ida_tempv1) <= ZERO) return(-1);
    N_VInv(IDA_mem->ida_tempv1, weightS[is]);
  }
  return(0);
}

/*
 * IDASensEwtSetSV
 *
 */

static int IDASensEwtSetSV(IDAMem IDA_mem, N_Vector *yScur, N_Vector *weightS)
{
  int is;
  
  for (is=0; is<IDA_mem->ida_Ns; is++) {
    N_VAbs(yScur[is], IDA_mem->ida_tempv1);
    N_VLinearSum(IDA_mem->ida_rtolS, IDA_mem->ida_tempv1, ONE, IDA_mem->ida_VatolS[is], IDA_mem->ida_tempv1);
    if (N_VMin(IDA_mem->ida_tempv1) <= ZERO) return(-1);
    N_VInv(IDA_mem->ida_tempv1, weightS[is]);
  }

  return(0);
}

/*
 * IDAQuadSensEwtSet
 *
 */

int IDAQuadSensEwtSet(IDAMem IDA_mem, N_Vector *yQScur, N_Vector *weightQS)
{
  int flag=0;

  switch (IDA_mem->ida_itolQS) {
  case IDA_EE:
    flag = IDAQuadSensEwtSetEE(IDA_mem, yQScur, weightQS);
    break;
  case IDA_SS: 
    flag = IDAQuadSensEwtSetSS(IDA_mem, yQScur, weightQS);
    break;
  case IDA_SV: 
    flag = IDAQuadSensEwtSetSV(IDA_mem, yQScur, weightQS);
    break;
  }

  return(flag);
}

/*
 * IDAQuadSensEwtSetEE
 *
 * In this case, the error weight vector for the i-th quadrature sensitivity
 * is set to
 *
 * ewtQS_i = pbar_i * IDAQuadEwtSet(pbar_i*yQS_i)
 *
 * In other words, the scaled sensitivity pbar_i * yQS_i has the same error
 * weight vector calculation as the quadrature vector.
 *
 */
static int IDAQuadSensEwtSetEE(IDAMem IDA_mem, N_Vector *yQScur, N_Vector *weightQS)
{
  int is;
  N_Vector pyS;
  int flag;

  /* Use tempvQS[0] as temporary storage for the scaled sensitivity */
  pyS = IDA_mem->ida_tempvQS[0];

  for (is=0; is<IDA_mem->ida_Ns; is++) {
    N_VScale(IDA_mem->ida_pbar[is], yQScur[is], pyS);
    flag = IDAQuadEwtSet(IDA_mem, pyS, weightQS[is]);
    if (flag != 0) return(-1);
    N_VScale(IDA_mem->ida_pbar[is], weightQS[is], weightQS[is]);
  }

  return(0);
}

static int IDAQuadSensEwtSetSS(IDAMem IDA_mem, N_Vector *yQScur, N_Vector *weightQS)
{
  int is;
  N_Vector tempvQ;

  /* Use ypQ as temporary storage */
  tempvQ = IDA_mem->ida_ypQ;

  for (is=0; is<IDA_mem->ida_Ns; is++) {
    N_VAbs(yQScur[is], tempvQ);
    N_VScale(IDA_mem->ida_rtolQS, tempvQ, tempvQ);
    N_VAddConst(tempvQ, IDA_mem->ida_SatolQS[is], tempvQ);
    if (N_VMin(tempvQ) <= ZERO) return(-1);
    N_VInv(tempvQ, weightQS[is]);
  }

  return(0);
}

static int IDAQuadSensEwtSetSV(IDAMem IDA_mem, N_Vector *yQScur, N_Vector *weightQS)
{
  int is;
  N_Vector tempvQ;

  /* Use ypQ as temporary storage */
  tempvQ = IDA_mem->ida_ypQ;
  
  for (is=0; is<IDA_mem->ida_Ns; is++) {
    N_VAbs(yQScur[is], tempvQ);
    N_VLinearSum(IDA_mem->ida_rtolQS, tempvQ, ONE, IDA_mem->ida_VatolQS[is], tempvQ);
    if (N_VMin(tempvQ) <= ZERO) return(-1);
    N_VInv(tempvQ, weightQS[is]);
  }

  return(0);
}


/* 
 * -----------------------------------------------------------------
 * Stopping tests
 * -----------------------------------------------------------------
 */

/*
 * IDAStopTest1
 *
 * This routine tests for stop conditions before taking a step.
 * The tests depend on the value of itask.
 * The variable tretlast is the previously returned value of tret.
 *
 * The return values are:
 * CONTINUE_STEPS       if no stop conditions were found
 * IDA_SUCCESS          for a normal return to the user
 * IDA_TSTOP_RETURN     for a tstop-reached return to the user
 * IDA_ILL_INPUT        for an illegal-input return to the user 
 *
 * In the tstop cases, this routine may adjust the stepsize hh to cause
 * the next step to reach tstop exactly.
 */

static int IDAStopTest1(IDAMem IDA_mem, realtype tout, realtype *tret, 
                        N_Vector yret, N_Vector ypret, int itask)
{
  int ier;
  realtype troundoff;

  switch (itask) {
    
  case IDA_NORMAL:

    if (IDA_mem->ida_tstopset) {
      /* Test for tn past tstop, tn = tretlast, tn past tout, tn near tstop. */
      if ( (IDA_mem->ida_tn - IDA_mem->ida_tstop)*IDA_mem->ida_hh > ZERO) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDA", "IDASolve",
                        MSG_BAD_TSTOP, IDA_mem->ida_tstop, IDA_mem->ida_tn);
        return(IDA_ILL_INPUT);
      }
    }

    /* Test for tout = tretlast, and for tn past tout. */
    if (tout == IDA_mem->ida_tretlast) {
      *tret = IDA_mem->ida_tretlast = tout;
      return(IDA_SUCCESS);
    }
    if ((IDA_mem->ida_tn - tout)*IDA_mem->ida_hh >= ZERO) {
      ier = IDAGetSolution(IDA_mem, tout, yret, ypret);
      if (ier != IDA_SUCCESS) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDA", "IDASolve", MSG_BAD_TOUT, tout);
        return(IDA_ILL_INPUT);
      }
      *tret = IDA_mem->ida_tretlast = tout;
      return(IDA_SUCCESS);
    }

    if (IDA_mem->ida_tstopset) {
      troundoff = HUNDRED * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh));
      if (SUNRabs(IDA_mem->ida_tn - IDA_mem->ida_tstop) <= troundoff) {
        ier = IDAGetSolution(IDA_mem, IDA_mem->ida_tstop, yret, ypret);
        if (ier != IDA_SUCCESS) {
          IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDA", "IDASolve",
                          MSG_BAD_TSTOP, IDA_mem->ida_tstop, IDA_mem->ida_tn);
          return(IDA_ILL_INPUT);
        }
        *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tstop;
        IDA_mem->ida_tstopset = SUNFALSE;
        return(IDA_TSTOP_RETURN);
      }
      if ((IDA_mem->ida_tn + IDA_mem->ida_hh - IDA_mem->ida_tstop)*IDA_mem->ida_hh > ZERO) 
        IDA_mem->ida_hh = (IDA_mem->ida_tstop - IDA_mem->ida_tn)*(ONE - FOUR * IDA_mem->ida_uround);
    }

    return(CONTINUE_STEPS);
    
  case IDA_ONE_STEP:

    if (IDA_mem->ida_tstopset) {
      /* Test for tn past tstop, tn past tretlast, and tn near tstop. */
      if ((IDA_mem->ida_tn - IDA_mem->ida_tstop)*IDA_mem->ida_hh > ZERO) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDA", "IDASolve",
                        MSG_BAD_TSTOP, IDA_mem->ida_tstop, IDA_mem->ida_tn);
        return(IDA_ILL_INPUT);
      }
    }

    /* Test for tn past tretlast. */
    if ((IDA_mem->ida_tn - IDA_mem->ida_tretlast)*IDA_mem->ida_hh > ZERO) {
      ier = IDAGetSolution(IDA_mem, IDA_mem->ida_tn, yret, ypret);
      *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tn;
      return(IDA_SUCCESS);
    }

    if (IDA_mem->ida_tstopset) {
      troundoff = HUNDRED * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh));
      if (SUNRabs(IDA_mem->ida_tn - IDA_mem->ida_tstop) <= troundoff) {
        ier = IDAGetSolution(IDA_mem, IDA_mem->ida_tstop, yret, ypret);
        if (ier != IDA_SUCCESS) {
          IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDA", "IDASolve",
                          MSG_BAD_TSTOP, IDA_mem->ida_tstop, IDA_mem->ida_tn);
          return(IDA_ILL_INPUT);
        }
        *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tstop;
        IDA_mem->ida_tstopset = SUNFALSE;
        return(IDA_TSTOP_RETURN);
      }
      if ((IDA_mem->ida_tn + IDA_mem->ida_hh - IDA_mem->ida_tstop)*IDA_mem->ida_hh > ZERO) 
        IDA_mem->ida_hh = (IDA_mem->ida_tstop - IDA_mem->ida_tn)*(ONE - FOUR * IDA_mem->ida_uround);
    }

    return(CONTINUE_STEPS);
        
  }
  return(IDA_ILL_INPUT);  /* This return should never happen. */
}

/*
 * IDAStopTest2
 *
 * This routine tests for stop conditions after taking a step.
 * The tests depend on the value of itask.
 *
 * The return values are:
 *  CONTINUE_STEPS     if no stop conditions were found
 *  IDA_SUCCESS        for a normal return to the user
 *  IDA_TSTOP_RETURN   for a tstop-reached return to the user
 *  IDA_ILL_INPUT      for an illegal-input return to the user 
 *
 * In the two cases with tstop, this routine may reset the stepsize hh
 * to cause the next step to reach tstop exactly.
 *
 * In the two cases with ONE_STEP mode, no interpolation to tn is needed
 * because yret and ypret already contain the current y and y' values.
 *
 * Note: No test is made for an error return from IDAGetSolution here,
 * because the same test was made prior to the step.
 */

static int IDAStopTest2(IDAMem IDA_mem, realtype tout, realtype *tret, 
                        N_Vector yret, N_Vector ypret, int itask)
{
  /* int ier; */
  realtype troundoff;

  switch (itask) {

    case IDA_NORMAL:  

      /* Test for tn past tout. */
      if ((IDA_mem->ida_tn - tout)*IDA_mem->ida_hh >= ZERO) {
        /* ier = */ IDAGetSolution(IDA_mem, tout, yret, ypret);
        *tret = IDA_mem->ida_tretlast = tout;
        return(IDA_SUCCESS);
      }

      if (IDA_mem->ida_tstopset) {
        /* Test for tn at tstop and for tn near tstop */
        troundoff = HUNDRED * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh));
        if (SUNRabs(IDA_mem->ida_tn - IDA_mem->ida_tstop) <= troundoff) {
          /* ier = */ IDAGetSolution(IDA_mem, IDA_mem->ida_tstop, yret, ypret);
          *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tstop;
          IDA_mem->ida_tstopset = SUNFALSE;
          return(IDA_TSTOP_RETURN);
        }
        if ((IDA_mem->ida_tn + IDA_mem->ida_hh - IDA_mem->ida_tstop)*IDA_mem->ida_hh > ZERO) 
          IDA_mem->ida_hh = (IDA_mem->ida_tstop - IDA_mem->ida_tn)*(ONE - FOUR * IDA_mem->ida_uround);
      }

      return(CONTINUE_STEPS);

    case IDA_ONE_STEP:

      if (IDA_mem->ida_tstopset) {
        /* Test for tn at tstop and for tn near tstop */
        troundoff = HUNDRED * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh));
        if (SUNRabs(IDA_mem->ida_tn - IDA_mem->ida_tstop) <= troundoff) {
          /* ier = */ IDAGetSolution(IDA_mem, IDA_mem->ida_tstop, yret, ypret);
          *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tstop;
          IDA_mem->ida_tstopset = SUNFALSE;
          return(IDA_TSTOP_RETURN);
        }
        if ((IDA_mem->ida_tn + IDA_mem->ida_hh - IDA_mem->ida_tstop)*IDA_mem->ida_hh > ZERO) 
          IDA_mem->ida_hh = (IDA_mem->ida_tstop - IDA_mem->ida_tn)*(ONE - FOUR * IDA_mem->ida_uround);
      }

      *tret = IDA_mem->ida_tretlast = IDA_mem->ida_tn;
      return(IDA_SUCCESS);

  }
  return IDA_ILL_INPUT;  /* This return should never happen. */
}

/* 
 * -----------------------------------------------------------------
 * Error handler
 * -----------------------------------------------------------------
 */

/*
 * IDAHandleFailure
 *
 * This routine prints error messages for all cases of failure by
 * IDAStep.  It returns to IDASolve the value that it is to return to
 * the user.
 */

static int IDAHandleFailure(IDAMem IDA_mem, int sflag)
{
  /* Depending on sflag, print error message and return error flag */
  switch (sflag) {

    case IDA_ERR_FAIL:
      IDAProcessError(IDA_mem, IDA_ERR_FAIL, "IDAS", "IDASolve", MSG_ERR_FAILS, IDA_mem->ida_tn, IDA_mem->ida_hh);
      return(IDA_ERR_FAIL);

    case IDA_CONV_FAIL:
      IDAProcessError(IDA_mem, IDA_CONV_FAIL, "IDAS", "IDASolve", MSG_CONV_FAILS, IDA_mem->ida_tn, IDA_mem->ida_hh);
      return(IDA_CONV_FAIL);

    case IDA_LSETUP_FAIL:  
      IDAProcessError(IDA_mem, IDA_LSETUP_FAIL, "IDAS", "IDASolve", MSG_SETUP_FAILED, IDA_mem->ida_tn);
      return(IDA_LSETUP_FAIL);

    case IDA_LSOLVE_FAIL: 
      IDAProcessError(IDA_mem, IDA_LSOLVE_FAIL, "IDAS", "IDASolve", MSG_SOLVE_FAILED, IDA_mem->ida_tn);
      return(IDA_LSOLVE_FAIL);

    case IDA_REP_RES_ERR:
      IDAProcessError(IDA_mem, IDA_REP_RES_ERR, "IDAS", "IDASolve", MSG_REP_RES_ERR, IDA_mem->ida_tn);
      return(IDA_REP_RES_ERR);

    case IDA_RES_FAIL: 
      IDAProcessError(IDA_mem, IDA_RES_FAIL, "IDAS", "IDASolve", MSG_RES_NONRECOV, IDA_mem->ida_tn);
      return(IDA_RES_FAIL);

    case IDA_CONSTR_FAIL: 
      IDAProcessError(IDA_mem, IDA_CONSTR_FAIL, "IDAS", "IDASolve", MSG_FAILED_CONSTR, IDA_mem->ida_tn);
      return(IDA_CONSTR_FAIL);

    case IDA_MEM_NULL:
      IDAProcessError(NULL, IDA_MEM_NULL, "IDA", "IDASolve", MSG_NO_MEM);
      return(IDA_MEM_NULL);

    case SUN_NLS_MEM_NULL:
      IDAProcessError(IDA_mem, IDA_MEM_NULL, "IDA", "IDASolve",
                      MSG_NLS_INPUT_NULL, IDA_mem->ida_tn);
      return(IDA_MEM_NULL);

    case IDA_NLS_SETUP_FAIL:
      IDAProcessError(IDA_mem, IDA_NLS_SETUP_FAIL, "IDA", "IDASolve",
                      MSG_NLS_SETUP_FAILED, IDA_mem->ida_tn);
      return(IDA_NLS_SETUP_FAIL);
  }

  /* This return should never happen */
  IDAProcessError(IDA_mem, IDA_UNRECOGNIZED_ERROR, "IDA", "IDASolve",
                  "IDA encountered an unrecognized error. Please report this to the Sundials developers at sundials-users@llnl.gov");
  return (IDA_UNRECOGNIZED_ERROR);
}

/* 
 * -----------------------------------------------------------------
 * Main IDAStep function
 * -----------------------------------------------------------------
 */

/*
 * IDAStep
 *
 * This routine performs one internal IDA step, from tn to tn + hh.
 * It calls other routines to do all the work.
 *
 * It solves a system of differential/algebraic equations of the form
 *       F(t,y,y') = 0, for one step. In IDA, tt is used for t,
 * yy is used for y, and yp is used for y'. The function F is supplied as 'res'
 * by the user.
 *
 * The methods used are modified divided difference, fixed leading 
 * coefficient forms of backward differentiation formulas.
 * The code adjusts the stepsize and order to control the local error per step.
 *
 * The main operations done here are as follows:
 *  * initialize various quantities;
 *  * setting of multistep method coefficients;
 *  * solution of the nonlinear system for yy at t = tn + hh;
 *  * deciding on order reduction and testing the local error;
 *  * attempting to recover from failure in nonlinear solver or error test;
 *  * resetting stepsize and order for the next step.
 *  * updating phi and other state data if successful;
 *
 * On a failure in the nonlinear system solution or error test, the
 * step may be reattempted, depending on the nature of the failure.
 *
 * Variables or arrays (all in the IDAMem structure) used in IDAStep are:
 *
 * tt -- Independent variable.
 * yy -- Solution vector at tt.
 * yp -- Derivative of solution vector after successful stelp.
 * res -- User-supplied function to evaluate the residual. See the 
 *        description given in file ida.h .
 * lsetup -- Routine to prepare for the linear solver call. It may either
 *        save or recalculate quantities used by lsolve. (Optional)
 * lsolve -- Routine to solve a linear system. A prior call to lsetup
 *        may be required. 
 * hh  -- Appropriate step size for next step.
 * ewt -- Vector of weights used in all convergence tests.
 * phi -- Array of divided differences used by IDAStep. This array is composed 
 *       of  (maxord+1) nvectors (each of size Neq). (maxord+1) is the maximum 
 *       order for the problem, maxord, plus 1.
 *
 *       Return values are:
 *       IDA_SUCCESS   IDA_RES_FAIL        LSETUP_ERROR_NONRECVR       
 *                     IDA_LSOLVE_FAIL   IDA_ERR_FAIL            
 *                     IDA_CONSTR_FAIL               IDA_CONV_FAIL          
 *                     IDA_REP_RES_ERR            
 */

static int IDAStep(IDAMem IDA_mem)
{
  realtype saved_t, ck;
  realtype err_k, err_km1, err_km2;
  int ncf, nef;
  int nflag, kflag;
  int retval;
  booleantype sensi_stg, sensi_sim;

  /* Are we computing sensitivities with the staggered or simultaneous approach? */
  sensi_stg = (IDA_mem->ida_sensi && (IDA_mem->ida_ism==IDA_STAGGERED));
  sensi_sim = (IDA_mem->ida_sensi && (IDA_mem->ida_ism==IDA_SIMULTANEOUS));

  saved_t = IDA_mem->ida_tn;
  ncf = nef = 0;

  if (IDA_mem->ida_nst == ZERO){
    IDA_mem->ida_kk = 1;
    IDA_mem->ida_kused = 0;
    IDA_mem->ida_hused = ZERO;
    IDA_mem->ida_psi[0] = IDA_mem->ida_hh;
    IDA_mem->ida_cj = ONE/IDA_mem->ida_hh;
    IDA_mem->ida_phase = 0;
    IDA_mem->ida_ns = 0;
  }

  /* To prevent 'unintialized variable' warnings */
  err_k = ZERO;
  err_km1 = ZERO;
  err_km2 = ZERO;

  /* Looping point for attempts to take a step */

  for(;;) {  

    /*-----------------------
      Set method coefficients
      -----------------------*/

    IDASetCoeffs(IDA_mem, &ck);

    kflag = IDA_SUCCESS;

    /*----------------------------------------------------
      If tn is past tstop (by roundoff), reset it to tstop.
      -----------------------------------------------------*/

    IDA_mem->ida_tn = IDA_mem->ida_tn + IDA_mem->ida_hh;
    if (IDA_mem->ida_tstopset) {
      if ((IDA_mem->ida_tn - IDA_mem->ida_tstop)*IDA_mem->ida_hh > ZERO)
        IDA_mem->ida_tn = IDA_mem->ida_tstop;
    }

    /*-----------------------
      Advance state variables
      -----------------------*/

    /* Compute predicted values for yy and yp */
    IDAPredict(IDA_mem);

    /* Compute predicted values for yyS and ypS (if simultaneous approach) */
    if (sensi_sim)
      IDASensPredict(IDA_mem, IDA_mem->ida_yySpredict, IDA_mem->ida_ypSpredict);

    /* Nonlinear system solution */
    nflag = IDANls(IDA_mem);

    /* If NLS was successful, perform error test */
    if (nflag == IDA_SUCCESS)
      nflag = IDATestError(IDA_mem, ck, &err_k, &err_km1, &err_km2);

    /* Test for convergence or error test failures */
    if (nflag != IDA_SUCCESS) {

      /* restore and decide what to do */
      IDARestore(IDA_mem, saved_t);
      kflag = IDAHandleNFlag(IDA_mem, nflag, err_k, err_km1, 
                             &(IDA_mem->ida_ncfn), &ncf,
                             &(IDA_mem->ida_netf), &nef);

      /* exit on nonrecoverable failure */ 
      if (kflag != PREDICT_AGAIN) return(kflag);

      /* recoverable error; predict again */
      if(IDA_mem->ida_nst==0) IDAReset(IDA_mem);
      continue;

    }

    /*----------------------------
      Advance quadrature variables 
      ----------------------------*/
    if (IDA_mem->ida_quadr) {

      nflag = IDAQuadNls(IDA_mem);

      /* If NLS was successful, perform error test */
      if (IDA_mem->ida_errconQ && (nflag == IDA_SUCCESS))
        nflag = IDAQuadTestError(IDA_mem, ck, &err_k, &err_km1, &err_km2);

      /* Test for convergence or error test failures */
      if (nflag != IDA_SUCCESS) {

        /* restore and decide what to do */
        IDARestore(IDA_mem, saved_t);
        kflag = IDAHandleNFlag(IDA_mem, nflag, err_k, err_km1, 
                               &(IDA_mem->ida_ncfnQ), &ncf,
                               &(IDA_mem->ida_netfQ), &nef);

        /* exit on nonrecoverable failure */ 
        if (kflag != PREDICT_AGAIN) return(kflag);
        
        /* recoverable error; predict again */
        if(IDA_mem->ida_nst==0) IDAReset(IDA_mem);
        continue;
      }
    }

    /*--------------------------------------------------
      Advance sensitivity variables (Staggered approach)
      --------------------------------------------------*/
    if (sensi_stg) {

      /* Evaluate res at converged y, needed for future evaluations of sens. RHS 
         If res() fails recoverably, treat it as a convergence failure and 
         attempt the step again */

      retval = IDA_mem->ida_res(IDA_mem->ida_tn,
                                IDA_mem->ida_yy, IDA_mem->ida_yp,
                                IDA_mem->ida_delta, IDA_mem->ida_user_data);

      if (retval < 0) return(IDA_RES_FAIL);
      if (retval > 0) continue;
        
      /* Compute predicted values for yyS and ypS */
      IDASensPredict(IDA_mem, IDA_mem->ida_yySpredict, IDA_mem->ida_ypSpredict);

      /* Nonlinear system solution */
      nflag = IDASensNls(IDA_mem);
      
      /* If NLS was successful, perform error test */
      if (IDA_mem->ida_errconS && (nflag == IDA_SUCCESS))
        nflag = IDASensTestError(IDA_mem, ck, &err_k, &err_km1, &err_km2);

      /* Test for convergence or error test failures */
      if (nflag != IDA_SUCCESS) {

        /* restore and decide what to do */
        IDARestore(IDA_mem, saved_t);
        kflag = IDAHandleNFlag(IDA_mem, nflag, err_k, err_km1, 
                               &(IDA_mem->ida_ncfnQ), &ncf,
                               &(IDA_mem->ida_netfQ), &nef);
        
        /* exit on nonrecoverable failure */ 
        if (kflag != PREDICT_AGAIN) return(kflag);
        
        /* recoverable error; predict again */
        if(IDA_mem->ida_nst==0) IDAReset(IDA_mem);
        continue;
      }
    }

    /*-------------------------------------------
      Advance quadrature sensitivity variables
      -------------------------------------------*/
    if (IDA_mem->ida_quadr_sensi) {

      nflag = IDAQuadSensNls(IDA_mem);

      /* If NLS was successful, perform error test */
      if (IDA_mem->ida_errconQS && (nflag == IDA_SUCCESS))
        nflag = IDAQuadSensTestError(IDA_mem, ck, &err_k, &err_km1, &err_km2);

      /* Test for convergence or error test failures */
      if (nflag != IDA_SUCCESS) {

        /* restore and decide what to do */
        IDARestore(IDA_mem, saved_t);
        kflag = IDAHandleNFlag(IDA_mem, nflag, err_k, err_km1, 
                               &(IDA_mem->ida_ncfnQ), &ncf,
                               &(IDA_mem->ida_netfQ), &nef);

        /* exit on nonrecoverable failure */ 
        if (kflag != PREDICT_AGAIN) return(kflag);
        
        /* recoverable error; predict again */
        if(IDA_mem->ida_nst==0) IDAReset(IDA_mem);
        continue;
      }
    }

    /* kflag == IDA_SUCCESS */
    break;

  } /* end loop */

  /* Nonlinear system solve and error test were both successful;
     update data, and consider change of step and/or order */

  IDACompleteStep(IDA_mem, err_k, err_km1);

  /* 
     Rescale ee vector to be the estimated local error
     Notes:
       (1) altering the value of ee is permissible since
           it will be overwritten by
           IDASolve()->IDAStep()->IDANls()
           before it is needed again
       (2) the value of ee is only valid if IDAHandleNFlag()
           returns either PREDICT_AGAIN or IDA_SUCCESS
  */

  N_VScale(ck, IDA_mem->ida_ee, IDA_mem->ida_ee);

  return(IDA_SUCCESS);
}

/*
 * IDAGetSolution
 *
 * This routine evaluates y(t) and y'(t) as the value and derivative of 
 * the interpolating polynomial at the independent variable t, and stores
 * the results in the vectors yret and ypret.  It uses the current
 * independent variable value, tn, and the method order last used, kused.
 * This function is called by IDASolve with t = tout, t = tn, or t = tstop.
 * 
 * If kused = 0 (no step has been taken), or if t = tn, then the order used
 * here is taken to be 1, giving yret = phi[0], ypret = phi[1]/psi[0].
 * 
 * The return values are:
 *   IDA_SUCCESS  if t is legal, or
 *   IDA_BAD_T    if t is not within the interval of the last step taken.
 */

int IDAGetSolution(void *ida_mem, realtype t, N_Vector yret, N_Vector ypret)
{
  IDAMem IDA_mem;
  realtype tfuzz, tp, delt, c, d, gam;
  int j, kord, retval;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSolution", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem; 

  /* Check t for legality.  Here tn - hused is t_{n-1}. */
 
  tfuzz = HUNDRED * IDA_mem->ida_uround * (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh));
  if (IDA_mem->ida_hh < ZERO) tfuzz = - tfuzz;
  tp = IDA_mem->ida_tn - IDA_mem->ida_hused - tfuzz;
  if ((t - tp)*IDA_mem->ida_hh < ZERO) {
    IDAProcessError(IDA_mem, IDA_BAD_T, "IDAS", "IDAGetSolution", MSG_BAD_T,
                    t, IDA_mem->ida_tn-IDA_mem->ida_hused, IDA_mem->ida_tn);
    return(IDA_BAD_T);
  }

  /* Initialize kord = (kused or 1). */

  kord = IDA_mem->ida_kused; 
  if (IDA_mem->ida_kused == 0) kord = 1;

  /* Accumulate multiples of columns phi[j] into yret and ypret. */

  delt = t - IDA_mem->ida_tn;
  c = ONE; d = ZERO;
  gam = delt / IDA_mem->ida_psi[0];

  IDA_mem->ida_cvals[0] = c;
  for (j=1; j <= kord; j++) {
    d = d*gam + c / IDA_mem->ida_psi[j-1];
    c = c*gam;
    gam = (delt + IDA_mem->ida_psi[j-1]) / IDA_mem->ida_psi[j];

    IDA_mem->ida_cvals[j]   = c;
    IDA_mem->ida_dvals[j-1] = d;
  }

  retval = N_VLinearCombination(kord+1, IDA_mem->ida_cvals,
                                IDA_mem->ida_phi,  yret);
  if (retval != IDA_SUCCESS) return(IDA_VECTOROP_ERR);

  retval = N_VLinearCombination(kord, IDA_mem->ida_dvals,
                                IDA_mem->ida_phi+1, ypret);
  if (retval != IDA_SUCCESS) return(IDA_VECTOROP_ERR);

  return(IDA_SUCCESS);
}


/*
 * IDASetCoeffs
 *
 *  This routine computes the coefficients relevant to the current step.
 *  The counter ns counts the number of consecutive steps taken at
 *  constant stepsize h and order k, up to a maximum of k + 2.
 *  Then the first ns components of beta will be one, and on a step  
 *  with ns = k + 2, the coefficients alpha, etc. need not be reset here.
 *  Also, IDACompleteStep prohibits an order increase until ns = k + 2.
 */

static void IDASetCoeffs(IDAMem IDA_mem, realtype *ck)
{
  int i, j, is;
  realtype temp1, temp2, alpha0, alphas;

  /* Set coefficients for the current stepsize h */

  if ( (IDA_mem->ida_hh != IDA_mem->ida_hused) ||
       (IDA_mem->ida_kk != IDA_mem->ida_kused) )
    IDA_mem->ida_ns = 0;
  IDA_mem->ida_ns = SUNMIN(IDA_mem->ida_ns+1, IDA_mem->ida_kused+2);
  if (IDA_mem->ida_kk+1 >= IDA_mem->ida_ns) {
    IDA_mem->ida_beta[0] = ONE;
    IDA_mem->ida_alpha[0] = ONE;
    temp1 = IDA_mem->ida_hh;
    IDA_mem->ida_gamma[0] = ZERO;
    IDA_mem->ida_sigma[0] = ONE;
    for(i=1;i<=IDA_mem->ida_kk;i++){
      temp2 = IDA_mem->ida_psi[i-1];
      IDA_mem->ida_psi[i-1] = temp1;
      IDA_mem->ida_beta[i] = IDA_mem->ida_beta[i-1] * IDA_mem->ida_psi[i-1] / temp2;
      temp1 = temp2 + IDA_mem->ida_hh;
      IDA_mem->ida_alpha[i] = IDA_mem->ida_hh / temp1;
      IDA_mem->ida_sigma[i] = i * IDA_mem->ida_sigma[i-1] * IDA_mem->ida_alpha[i]; 
      IDA_mem->ida_gamma[i] = IDA_mem->ida_gamma[i-1] + IDA_mem->ida_alpha[i-1] / IDA_mem->ida_hh;
   }
    IDA_mem->ida_psi[IDA_mem->ida_kk] = temp1;
  }
  /* compute alphas, alpha0 */
  alphas = ZERO;
  alpha0 = ZERO;
  for(i=0;i<IDA_mem->ida_kk;i++){
    alphas = alphas - ONE/(i+1);
    alpha0 = alpha0 - IDA_mem->ida_alpha[i];
  }

  /* compute leading coefficient cj  */
  IDA_mem->ida_cjlast = IDA_mem->ida_cj;
  IDA_mem->ida_cj = -alphas/IDA_mem->ida_hh;
  
  /* compute variable stepsize error coefficient ck */

  *ck = SUNRabs(IDA_mem->ida_alpha[IDA_mem->ida_kk] + alphas - alpha0);
  *ck = SUNMAX(*ck, IDA_mem->ida_alpha[IDA_mem->ida_kk]);

  /* change phi to phi-star  */
  if (IDA_mem->ida_ns <= IDA_mem->ida_kk) {

    for(i=IDA_mem->ida_ns; i<=IDA_mem->ida_kk; i++)
      IDA_mem->ida_cvals[i-IDA_mem->ida_ns] = IDA_mem->ida_beta[i];

    (void) N_VScaleVectorArray(IDA_mem->ida_kk - IDA_mem->ida_ns + 1,
                               IDA_mem->ida_cvals,
                               IDA_mem->ida_phi+IDA_mem->ida_ns,
                               IDA_mem->ida_phi+IDA_mem->ida_ns);

    if (IDA_mem->ida_quadr)
      (void) N_VScaleVectorArray(IDA_mem->ida_kk - IDA_mem->ida_ns + 1,
                                 IDA_mem->ida_cvals,
                                 IDA_mem->ida_phiQ+IDA_mem->ida_ns,
                                 IDA_mem->ida_phiQ+IDA_mem->ida_ns);

    if (IDA_mem->ida_sensi || IDA_mem->ida_quadr_sensi) {
      j = 0;
      for(i=IDA_mem->ida_ns; i<=IDA_mem->ida_kk; i++) {
        for(is=0; is<IDA_mem->ida_Ns; is++) {
          IDA_mem->ida_cvals[j] = IDA_mem->ida_beta[i];
          j++;
        }
      }
    }

    if (IDA_mem->ida_sensi) {
      j = 0;
      for(i=IDA_mem->ida_ns; i<=IDA_mem->ida_kk; i++) {
        for(is=0; is<IDA_mem->ida_Ns; is++) {
          IDA_mem->ida_Xvecs[j] = IDA_mem->ida_phiS[i][is];
          j++;
        }
      }

      (void) N_VScaleVectorArray(j, IDA_mem->ida_cvals, IDA_mem->ida_Xvecs,
                                 IDA_mem->ida_Xvecs);
    }

    if (IDA_mem->ida_quadr_sensi) {
      j = 0;
      for(i=IDA_mem->ida_ns; i<=IDA_mem->ida_kk; i++) {
        for(is=0; is<IDA_mem->ida_Ns; is++) {
          IDA_mem->ida_Xvecs[j] = IDA_mem->ida_phiQS[i][is];
          j++;
        }
      }

      (void) N_VScaleVectorArray(j, IDA_mem->ida_cvals, IDA_mem->ida_Xvecs,
                                 IDA_mem->ida_Xvecs);
    }
  }

}

/* 
 * -----------------------------------------------------------------
 * Nonlinear solver functions
 * -----------------------------------------------------------------
 */

/*
 * IDANls
 *
 * This routine attempts to solve the nonlinear system using the linear
 * solver specified. NOTE: this routine uses N_Vector ee as the scratch
 * vector tempv3 passed to lsetup.
 *
 *  Possible return values:
 *
 *  IDA_SUCCESS
 *
 *  IDA_RES_RECVR       IDA_RES_FAIL
 *  IDA_SRES_RECVR      IDA_SRES_FAIL
 *  IDA_LSETUP_RECVR    IDA_LSETUP_FAIL
 *  IDA_LSOLVE_RECVR    IDA_LSOLVE_FAIL
 *
 *  IDA_CONSTR_RECVR
 *  SUN_NLS_CONV_RECVR
 *  IDA_MEM_NULL
 */

static int IDANls(IDAMem IDA_mem)
{
  int retval;
  booleantype constraintsPassed, callLSetup, sensi_sim;
  realtype temp1, temp2, vnorm;

  /* Are we computing sensitivities with the IDA_SIMULTANEOUS approach? */
  sensi_sim = (IDA_mem->ida_sensi && (IDA_mem->ida_ism==IDA_SIMULTANEOUS));

  callLSetup = SUNFALSE;

  /* Initialize if the first time called */

  if (IDA_mem->ida_nst == 0){
    IDA_mem->ida_cjold = IDA_mem->ida_cj;
    IDA_mem->ida_ss = TWENTY;
    IDA_mem->ida_ssS = TWENTY;
    if (IDA_mem->ida_lsetup) callLSetup = SUNTRUE;
  }

  /* Decide if lsetup is to be called */

  if (IDA_mem->ida_lsetup) {
    IDA_mem->ida_cjratio = IDA_mem->ida_cj / IDA_mem->ida_cjold;
    temp1 = (ONE - XRATE) / (ONE + XRATE);
    temp2 = ONE/temp1;
    if (IDA_mem->ida_cjratio < temp1 || IDA_mem->ida_cjratio > temp2) callLSetup = SUNTRUE;
    if (IDA_mem->ida_forceSetup) callLSetup = SUNTRUE;
    if (IDA_mem->ida_cj != IDA_mem->ida_cjlast) {IDA_mem->ida_ss = HUNDRED; IDA_mem->ida_ssS = HUNDRED;}
  }

  /* initial guess for the correction to the predictor */
  if (sensi_sim)
    N_VConst(ZERO, IDA_mem->ycor0Sim);
  else
    N_VConst(ZERO, IDA_mem->ida_delta);

  /* call nonlinear solver setup if it exists */
  if ((IDA_mem->NLS)->ops->setup) {
    if (sensi_sim)
      retval = SUNNonlinSolSetup(IDA_mem->NLS, IDA_mem->ycor0Sim, IDA_mem);
    else
      retval = SUNNonlinSolSetup(IDA_mem->NLS, IDA_mem->ida_delta, IDA_mem);

    if (retval < 0) return(IDA_NLS_SETUP_FAIL);
    if (retval > 0) return(IDA_NLS_SETUP_RECVR);
  }

  /* solve the nonlinear system */
  if (sensi_sim)
    retval = SUNNonlinSolSolve(IDA_mem->NLSsim,
                               IDA_mem->ycor0Sim, IDA_mem->ycorSim,
                               IDA_mem->ewtSim, IDA_mem->ida_epsNewt,
                               callLSetup, IDA_mem);
  else
    retval = SUNNonlinSolSolve(IDA_mem->NLS,
                               IDA_mem->ida_delta, IDA_mem->ida_ee,
                               IDA_mem->ida_ewt, IDA_mem->ida_epsNewt,
                               callLSetup, IDA_mem);

  /* update the state using the final correction from the nonlinear solver */
  N_VLinearSum(ONE, IDA_mem->ida_yypredict, ONE, IDA_mem->ida_ee, IDA_mem->ida_yy);
  N_VLinearSum(ONE, IDA_mem->ida_yppredict, IDA_mem->ida_cj, IDA_mem->ida_ee, IDA_mem->ida_yp);

  /* update the sensitivities based on the final correction from the nonlinear solver */
  if (sensi_sim) {
    N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                            ONE, IDA_mem->ida_yySpredict,
                            ONE, IDA_mem->ida_eeS, IDA_mem->ida_yyS);
    N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                            ONE, IDA_mem->ida_ypSpredict,
                            IDA_mem->ida_cj, IDA_mem->ida_eeS, IDA_mem->ida_ypS);
  }

  /* return if nonlinear solver failed */
  if (retval != IDA_SUCCESS) return(retval);

  /* If otherwise successful, check and enforce inequality constraints. */

  if (IDA_mem->ida_constraintsSet){  /* Check constraints and get mask vector mm, 
                                        set where constraints failed */
    IDA_mem->ida_mm = IDA_mem->ida_tempv2;
    constraintsPassed = N_VConstrMask(IDA_mem->ida_constraints,IDA_mem->ida_yy,IDA_mem->ida_mm);
    if (constraintsPassed) return(IDA_SUCCESS);
    else {
      N_VCompare(ONEPT5, IDA_mem->ida_constraints, IDA_mem->ida_tempv1);  
      /* a , where a[i] =1. when |c[i]| = 2 ,  c the vector of constraints */
      N_VProd(IDA_mem->ida_tempv1, IDA_mem->ida_constraints, IDA_mem->ida_tempv1);       /* a * c */
      N_VDiv(IDA_mem->ida_tempv1, IDA_mem->ida_ewt, IDA_mem->ida_tempv1);                /* a * c * wt */
      N_VLinearSum(ONE, IDA_mem->ida_yy, -PT1, IDA_mem->ida_tempv1, IDA_mem->ida_tempv1);/* y - 0.1 * a * c * wt */
      N_VProd(IDA_mem->ida_tempv1, IDA_mem->ida_mm, IDA_mem->ida_tempv1);               /*  v = mm*(y-.1*a*c*wt) */
      vnorm = IDAWrmsNorm(IDA_mem, IDA_mem->ida_tempv1, IDA_mem->ida_ewt, SUNFALSE); /*  ||v|| */
      
      /* If vector v of constraint corrections is small
         in norm, correct and accept this step */      
      if (vnorm <= IDA_mem->ida_epsNewt){  
        N_VLinearSum(ONE, IDA_mem->ida_ee, -ONE, IDA_mem->ida_tempv1, IDA_mem->ida_ee);  /* ee <- ee - v */
        return(IDA_SUCCESS);
      }
      else {
        /* Constraints not met -- reduce h by computing rr = h'/h */
        N_VLinearSum(ONE, IDA_mem->ida_phi[0], -ONE, IDA_mem->ida_yy, IDA_mem->ida_tempv1);
        N_VProd(IDA_mem->ida_mm, IDA_mem->ida_tempv1, IDA_mem->ida_tempv1);
        IDA_mem->ida_rr = PT9*N_VMinQuotient(IDA_mem->ida_phi[0], IDA_mem->ida_tempv1);
        IDA_mem->ida_rr = SUNMAX(IDA_mem->ida_rr,PT1);
        return(IDA_CONSTR_RECVR);
      }
    }
  }

  return(IDA_SUCCESS);
}


/*
 * IDAPredict
 *
 * This routine predicts the new values for vectors yy and yp.
 */

static void IDAPredict(IDAMem IDA_mem)
{
  int j;

  for(j=0; j<=IDA_mem->ida_kk; j++)
    IDA_mem->ida_cvals[j] = ONE;

  (void) N_VLinearCombination(IDA_mem->ida_kk+1, IDA_mem->ida_cvals,
                              IDA_mem->ida_phi, IDA_mem->ida_yypredict);

  (void) N_VLinearCombination(IDA_mem->ida_kk, IDA_mem->ida_gamma+1,
                              IDA_mem->ida_phi+1, IDA_mem->ida_yppredict);
}

/*
 * IDAQuadNls
 * 
 * This routine solves for the quadrature variables at the new step.
 * It does not solve a nonlinear system, but rather updates the
 * quadrature variables. The name for this function is just for 
 * uniformity purposes.
 *
 */

static int IDAQuadNls(IDAMem IDA_mem)
{
  int retval;

  /* Predict: load yyQ and ypQ */
  IDAQuadPredict(IDA_mem);
  
  /* Compute correction eeQ */
  retval = IDA_mem->ida_rhsQ(IDA_mem->ida_tn, IDA_mem->ida_yy,
                             IDA_mem->ida_yp, IDA_mem->ida_eeQ,
                             IDA_mem->ida_user_data);
  IDA_mem->ida_nrQe++;
  if (retval < 0) return(IDA_QRHS_FAIL);
  else if (retval > 0) return(IDA_QRHS_RECVR);

  if (IDA_mem->ida_quadr_sensi)
    N_VScale(ONE, IDA_mem->ida_eeQ, IDA_mem->ida_savrhsQ);

  N_VLinearSum(ONE, IDA_mem->ida_eeQ, -ONE, IDA_mem->ida_ypQ, IDA_mem->ida_eeQ);
  N_VScale(ONE/IDA_mem->ida_cj, IDA_mem->ida_eeQ, IDA_mem->ida_eeQ);

  /* Apply correction: yyQ = yyQ + eeQ */
  N_VLinearSum(ONE, IDA_mem->ida_yyQ, ONE, IDA_mem->ida_eeQ, IDA_mem->ida_yyQ);

  return(IDA_SUCCESS);
}

/*
 * IDAQuadPredict
 *
 * This routine predicts the new value for vectors yyQ and ypQ
 */

static void IDAQuadPredict(IDAMem IDA_mem)
{
  int j;

  for(j=0; j<=IDA_mem->ida_kk; j++)
    IDA_mem->ida_cvals[j] = ONE;

  (void) N_VLinearCombination(IDA_mem->ida_kk+1, IDA_mem->ida_cvals,
                              IDA_mem->ida_phiQ, IDA_mem->ida_yyQ);

  (void) N_VLinearCombination(IDA_mem->ida_kk, IDA_mem->ida_gamma+1,
                              IDA_mem->ida_phiQ+1, IDA_mem->ida_ypQ);

}

/*
 * IDASensNls
 *
 * This routine attempts to solve, one by one, all the sensitivity 
 * linear systems using nonlinear iterations and the linear solver 
 * specified (Staggered approach).
 */

static int IDASensNls(IDAMem IDA_mem)
{
  booleantype callLSetup;
  int retval;

  callLSetup = SUNFALSE;

  /* initial guess for the correction to the predictor */
  N_VConst(ZERO, IDA_mem->ycor0Stg);

  /* solve the nonlinear system */
  retval = SUNNonlinSolSolve(IDA_mem->NLSstg,
                             IDA_mem->ycor0Stg, IDA_mem->ycorStg,
                             IDA_mem->ewtStg, IDA_mem->ida_epsNewt,
                             callLSetup, IDA_mem);

  /* update using the final correction from the nonlinear solver */
  N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                          ONE, IDA_mem->ida_yySpredict,
                          ONE, IDA_mem->ida_eeS, IDA_mem->ida_yyS);
  N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                          ONE, IDA_mem->ida_ypSpredict,
                          IDA_mem->ida_cj, IDA_mem->ida_eeS, IDA_mem->ida_ypS);

  if (retval != IDA_SUCCESS) 
    IDA_mem->ida_ncfnS++;

  return(retval);

}

/*
 * IDASensPredict
 *
 * This routine loads the predicted values for the is-th sensitivity 
 * in the vectors yySens and ypSens.
 *
 * When ism=IDA_STAGGERED,  yySens = yyS[is] and ypSens = ypS[is]
 */

static void IDASensPredict(IDAMem IDA_mem, N_Vector *yySens, N_Vector *ypSens)
{
  int j;

  for(j=0; j<=IDA_mem->ida_kk; j++)
    IDA_mem->ida_cvals[j] = ONE;

  (void) N_VLinearCombinationVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_kk+1,
                                         IDA_mem->ida_cvals,
                                         IDA_mem->ida_phiS, yySens);

  (void) N_VLinearCombinationVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_kk,
                                         IDA_mem->ida_gamma+1,
                                         IDA_mem->ida_phiS+1, ypSens);

}

/*
 * IDAQuadSensNls
 * 
 * This routine solves for the snesitivity quadrature variables at the 
 * new step. It does not solve a nonlinear system, but rather updates 
 * the sensitivity variables. The name for this function is just for 
 * uniformity purposes.
 *
 */

static int IDAQuadSensNls(IDAMem IDA_mem)
{
  int retval;
  N_Vector *ypQS;

  /* Predict: load yyQS and ypQS for each sensitivity. Store 
   1st order information in tempvQS. */
  
  ypQS = IDA_mem->ida_tempvQS;
  IDAQuadSensPredict(IDA_mem, IDA_mem->ida_yyQS, ypQS);

  /* Compute correction eeQS */
  retval = IDA_mem->ida_rhsQS(IDA_mem->ida_Ns, IDA_mem->ida_tn,
                              IDA_mem->ida_yy, IDA_mem->ida_yp,
                              IDA_mem->ida_yyS, IDA_mem->ida_ypS,
                              IDA_mem->ida_savrhsQ, IDA_mem->ida_eeQS,
                              IDA_mem->ida_user_dataQS, IDA_mem->ida_tmpS1,
                              IDA_mem->ida_tmpS2, IDA_mem->ida_tmpS3);
  IDA_mem->ida_nrQSe++;

  if (retval < 0) return(IDA_QSRHS_FAIL);
  else if (retval > 0) return(IDA_QSRHS_RECVR);

  retval = N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                                   ONE/IDA_mem->ida_cj, IDA_mem->ida_eeQS,
                                   -ONE/IDA_mem->ida_cj, ypQS,
                                   IDA_mem->ida_eeQS);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  /* Apply correction: yyQS[is] = yyQ[is] + eeQ[is] */
  retval = N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                                   ONE, IDA_mem->ida_yyQS,
                                   ONE, IDA_mem->ida_eeQS,
                                   IDA_mem->ida_yyQS);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  return(IDA_SUCCESS);
}

/*
 * IDAQuadSensPredict
 *
 * This routine predicts the new value for vectors yyQS and ypQS
 */

static void IDAQuadSensPredict(IDAMem IDA_mem, N_Vector *yQS, N_Vector *ypQS)
{
  int j;

  for(j=0; j<=IDA_mem->ida_kk; j++)
    IDA_mem->ida_cvals[j] = ONE;

  (void) N_VLinearCombinationVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_kk+1,
                                         IDA_mem->ida_cvals,
                                         IDA_mem->ida_phiQS, yQS);

  (void) N_VLinearCombinationVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_kk,
                                         IDA_mem->ida_gamma+1,
                                         IDA_mem->ida_phiQS+1, ypQS);

}


/* 
 * -----------------------------------------------------------------
 * Error test
 * -----------------------------------------------------------------
 */

/*
 * IDATestError
 *
 * This routine estimates errors at orders k, k-1, k-2, decides 
 * whether or not to suggest an order reduction, and performs 
 * the local error test. 
 *
 * IDATestError returns either IDA_SUCCESS or ERROR_TEST_FAIL.
 */

static int IDATestError(IDAMem IDA_mem, realtype ck, 
                        realtype *err_k, realtype *err_km1, realtype *err_km2)
{
  realtype enorm_k, enorm_km1, enorm_km2;   /* error norms */
  realtype terr_k, terr_km1, terr_km2;      /* local truncation error norms */

  /* Compute error for order k. */

  enorm_k = IDAWrmsNorm(IDA_mem, IDA_mem->ida_ee, IDA_mem->ida_ewt, IDA_mem->ida_suppressalg);
  *err_k = IDA_mem->ida_sigma[IDA_mem->ida_kk] * enorm_k;
  terr_k = (IDA_mem->ida_kk+1) * (*err_k);

  IDA_mem->ida_knew = IDA_mem->ida_kk;

  if ( IDA_mem->ida_kk > 1 ) {

    /* Compute error at order k-1 */

    N_VLinearSum(ONE, IDA_mem->ida_phi[IDA_mem->ida_kk], ONE, IDA_mem->ida_ee, IDA_mem->ida_delta);
    enorm_km1 = IDAWrmsNorm(IDA_mem, IDA_mem->ida_delta, IDA_mem->ida_ewt, IDA_mem->ida_suppressalg);
    *err_km1 = IDA_mem->ida_sigma[IDA_mem->ida_kk-1] * enorm_km1;
    terr_km1 = IDA_mem->ida_kk * (*err_km1);

    if ( IDA_mem->ida_kk > 2 ) {

      /* Compute error at order k-2 */

      N_VLinearSum(ONE, IDA_mem->ida_phi[IDA_mem->ida_kk-1], ONE, IDA_mem->ida_delta, IDA_mem->ida_delta);
      enorm_km2 = IDAWrmsNorm(IDA_mem, IDA_mem->ida_delta, IDA_mem->ida_ewt, IDA_mem->ida_suppressalg);
      *err_km2 = IDA_mem->ida_sigma[IDA_mem->ida_kk-2] * enorm_km2;
      terr_km2 = (IDA_mem->ida_kk-1) * (*err_km2);

      /* Reduce order if errors are reduced */

      if (SUNMAX(terr_km1, terr_km2) <= terr_k)
        IDA_mem->ida_knew = IDA_mem->ida_kk - 1;

    } else {

      /* Reduce order to 1 if errors are reduced by at least 1/2 */

      if (terr_km1 <= (HALF * terr_k) )
        IDA_mem->ida_knew = IDA_mem->ida_kk - 1; 

    }

  }

  /* Perform error test */
  
  if (ck * enorm_k > ONE) return(ERROR_TEST_FAIL);
  else                    return(IDA_SUCCESS);

}

/*
 * IDAQuadTestError
 *
 * This routine estimates quadrature errors and updates errors at 
 * orders k, k-1, k-2, decides whether or not to suggest an order reduction, 
 * and performs the local error test. 
 *
 * IDAQuadTestError returns the updated local error estimate at orders k, 
 * k-1, and k-2. These are norms of type SUNMAX(|err|,|errQ|).
 *
 * The return flag can be either IDA_SUCCESS or ERROR_TEST_FAIL.
 */

static int IDAQuadTestError(IDAMem IDA_mem, realtype ck, 
                            realtype *err_k, realtype *err_km1, realtype *err_km2)
{
  realtype enormQ;
  realtype errQ_k, errQ_km1, errQ_km2;
  realtype terr_k, terr_km1, terr_km2;
  N_Vector tempv;
  booleantype check_for_reduction = SUNFALSE;

  /* Rename ypQ */
  tempv = IDA_mem->ida_ypQ;

  /* Update error for order k. */
  enormQ = N_VWrmsNorm(IDA_mem->ida_eeQ, IDA_mem->ida_ewtQ);
  errQ_k = IDA_mem->ida_sigma[IDA_mem->ida_kk] * enormQ;
  if (errQ_k > *err_k) {
    *err_k = errQ_k;
    check_for_reduction = SUNTRUE;
  }
  terr_k = (IDA_mem->ida_kk+1) * (*err_k);
  
  if ( IDA_mem->ida_kk > 1 ) {
    
    /* Update error at order k-1 */
    N_VLinearSum(ONE, IDA_mem->ida_phiQ[IDA_mem->ida_kk], ONE, IDA_mem->ida_eeQ, tempv);
    errQ_km1 = IDA_mem->ida_sigma[IDA_mem->ida_kk-1] * N_VWrmsNorm(tempv, IDA_mem->ida_ewtQ);
    if (errQ_km1 > *err_km1) {
      *err_km1 = errQ_km1;
      check_for_reduction = SUNTRUE;
    }
    terr_km1 = IDA_mem->ida_kk * (*err_km1);

    /* Has an order decrease already been decided in IDATestError? */
    if (IDA_mem->ida_knew != IDA_mem->ida_kk)
      check_for_reduction = SUNFALSE;

    if (check_for_reduction) {

      if ( IDA_mem->ida_kk > 2 ) {

        /* Update error at order k-2 */
        N_VLinearSum(ONE, IDA_mem->ida_phiQ[IDA_mem->ida_kk-1], ONE, tempv, tempv);
        errQ_km2 = IDA_mem->ida_sigma[IDA_mem->ida_kk-2] * N_VWrmsNorm(tempv, IDA_mem->ida_ewtQ);
        if (errQ_km2 > *err_km2) {
          *err_km2 = errQ_km2;
        }
        terr_km2 = (IDA_mem->ida_kk-1) * (*err_km2);

        /* Decrease order if errors are reduced */
        if (SUNMAX(terr_km1, terr_km2) <= terr_k)
          IDA_mem->ida_knew = IDA_mem->ida_kk - 1;
      
      } else {
        
        /* Decrease order to 1 if errors are reduced by at least 1/2 */
        if (terr_km1 <= (HALF * terr_k) )
          IDA_mem->ida_knew = IDA_mem->ida_kk - 1; 
        
      }

    }

  }

  /* Perform error test */
  if (ck * enormQ > ONE) return(ERROR_TEST_FAIL);
  else                   return(IDA_SUCCESS);

}

/*
 * IDASensTestError
 *
 * This routine estimates sensitivity errors and updates errors at 
 * orders k, k-1, k-2, decides whether or not to suggest an order reduction, 
 * and performs the local error test. (Used only in staggered approach).
 *
 * IDASensTestError returns the updated local error estimate at orders k, 
 * k-1, and k-2. These are norms of type SUNMAX(|err|,|errQ|,|errS|).
 *
 * The return flag can be either IDA_SUCCESS or ERROR_TEST_FAIL.
 */

static int IDASensTestError(IDAMem IDA_mem, realtype ck, 
                            realtype *err_k, realtype *err_km1, realtype *err_km2)
{
  realtype enormS;
  realtype errS_k, errS_km1, errS_km2;
  realtype terr_k, terr_km1, terr_km2;
  N_Vector *tempv;
  booleantype check_for_reduction = SUNFALSE;
  int retval;

  /* Rename deltaS */
  tempv = IDA_mem->ida_deltaS;

  /* Update error for order k. */
  enormS = IDASensWrmsNorm(IDA_mem, IDA_mem->ida_eeS, IDA_mem->ida_ewtS, IDA_mem->ida_suppressalg);
  errS_k  = IDA_mem->ida_sigma[IDA_mem->ida_kk] * enormS;
  if (errS_k > *err_k) {
    *err_k = errS_k;
    check_for_reduction = SUNTRUE;
  }
  terr_k = (IDA_mem->ida_kk+1) * (*err_k);
  
  if ( IDA_mem->ida_kk > 1 ) {
    
    /* Update error at order k-1 */
    retval = N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                                     ONE, IDA_mem->ida_phiS[IDA_mem->ida_kk],
                                     ONE, IDA_mem->ida_eeS, tempv);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

    errS_km1 = IDA_mem->ida_sigma[IDA_mem->ida_kk-1] *
      IDASensWrmsNorm(IDA_mem, tempv, IDA_mem->ida_ewtS, IDA_mem->ida_suppressalg);

    if (errS_km1 > *err_km1) {
      *err_km1 = errS_km1;
      check_for_reduction = SUNTRUE;
    }
    terr_km1 = IDA_mem->ida_kk * (*err_km1);

    /* Has an order decrease already been decided in IDATestError? */
    if (IDA_mem->ida_knew != IDA_mem->ida_kk)
      check_for_reduction = SUNFALSE;

    if (check_for_reduction) {

      if ( IDA_mem->ida_kk > 2 ) {

        /* Update error at order k-2 */
        retval = N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                                         ONE, IDA_mem->ida_phiS[IDA_mem->ida_kk-1],
                                         ONE, tempv, tempv);
        if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

        errS_km2 = IDA_mem->ida_sigma[IDA_mem->ida_kk-2] *
          IDASensWrmsNorm(IDA_mem, tempv, IDA_mem->ida_ewtS, IDA_mem->ida_suppressalg);

        if (errS_km2 > *err_km2) {
          *err_km2 = errS_km2;
        }
        terr_km2 = (IDA_mem->ida_kk-1) * (*err_km2);

        /* Decrease order if errors are reduced */
        if (SUNMAX(terr_km1, terr_km2) <= terr_k)
          IDA_mem->ida_knew = IDA_mem->ida_kk - 1;
      
      } else {
        
        /* Decrease order to 1 if errors are reduced by at least 1/2 */
        if (terr_km1 <= (HALF * terr_k) )
          IDA_mem->ida_knew = IDA_mem->ida_kk - 1; 
        
      }

    }

  }

  /* Perform error test */
  if (ck * enormS > ONE) return(ERROR_TEST_FAIL);
  else                   return(IDA_SUCCESS);

}

/*
 * IDAQuadSensTestError
 *
 * This routine estimates quadrature sensitivity errors and updates 
 * errors at orders k, k-1, k-2, decides whether or not to suggest 
 * an order reduction and performs the local error test. (Used 
 * only in staggered approach).
 *
 * IDAQuadSensTestError returns the updated local error estimate at 
 * orders k, k-1, and k-2. These are norms of type 
 * SUNMAX(|err|,|errQ|,|errS|,|errQS|).
 *
 * The return flag can be either IDA_SUCCESS or ERROR_TEST_FAIL.
 */

static int IDAQuadSensTestError(IDAMem IDA_mem, realtype ck, 
                                realtype *err_k, realtype *err_km1, realtype *err_km2)
{
  realtype enormQS;
  realtype errQS_k, errQS_km1, errQS_km2;
  realtype terr_k, terr_km1, terr_km2;
  N_Vector *tempv;
  booleantype check_for_reduction = SUNFALSE;
  int retval;

  tempv = IDA_mem->ida_yyQS;

  enormQS = IDAQuadSensWrmsNorm(IDA_mem, IDA_mem->ida_eeQS, IDA_mem->ida_ewtQS);
  errQS_k = IDA_mem->ida_sigma[IDA_mem->ida_kk] * enormQS;

  if (errQS_k > *err_k) {
    *err_k = errQS_k;
    check_for_reduction = SUNTRUE;
  }
  terr_k = (IDA_mem->ida_kk+1) * (*err_k);
  
  if ( IDA_mem->ida_kk > 1 ) {
    
    /* Update error at order k-1 */
    retval = N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                                     ONE, IDA_mem->ida_phiQS[IDA_mem->ida_kk],
                                     ONE, IDA_mem->ida_eeQS, tempv);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

    errQS_km1 = IDA_mem->ida_sigma[IDA_mem->ida_kk-1] *
      IDAQuadSensWrmsNorm(IDA_mem, tempv, IDA_mem->ida_ewtQS);

    if (errQS_km1 > *err_km1) {
      *err_km1 = errQS_km1;
      check_for_reduction = SUNTRUE;
    }
    terr_km1 = IDA_mem->ida_kk * (*err_km1);

    /* Has an order decrease already been decided in IDATestError? */
    if (IDA_mem->ida_knew != IDA_mem->ida_kk)
      check_for_reduction = SUNFALSE;

    if (check_for_reduction) {
      if ( IDA_mem->ida_kk > 2 ) {

        /* Update error at order k-2 */
        retval = N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                                         ONE, IDA_mem->ida_phiQS[IDA_mem->ida_kk-1],
                                         ONE, tempv, tempv);
        if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

        errQS_km2 = IDA_mem->ida_sigma[IDA_mem->ida_kk-2] *
          IDAQuadSensWrmsNorm(IDA_mem, tempv, IDA_mem->ida_ewtQS);

        if (errQS_km2 > *err_km2) {
          *err_km2 = errQS_km2;
        }
        terr_km2 = (IDA_mem->ida_kk-1) * (*err_km2);

        /* Decrease order if errors are reduced */
        if (SUNMAX(terr_km1, terr_km2) <= terr_k)
          IDA_mem->ida_knew = IDA_mem->ida_kk - 1;

      } else {
        /* Decrease order to 1 if errors are reduced by at least 1/2 */
        if (terr_km1 <= (HALF * terr_k) )
          IDA_mem->ida_knew = IDA_mem->ida_kk - 1; 
      }
    }
  }

  /* Perform error test */
  if (ck * enormQS > ONE) return(ERROR_TEST_FAIL);
  else                    return(IDA_SUCCESS);
}
/*
 * IDARestore
 *
 * This routine restores IDA_mem->ida_tn, psi, and phi in the event of a failure.
 * It changes back phi-star to phi (changed in IDASetCoeffs)
 */

static void IDARestore(IDAMem IDA_mem, realtype saved_t)
{
  int i, j, is;

  IDA_mem->ida_tn = saved_t;
  
  for (i = 1; i <= IDA_mem->ida_kk; i++)
    IDA_mem->ida_psi[i-1] = IDA_mem->ida_psi[i] - IDA_mem->ida_hh;

  if (IDA_mem->ida_ns <= IDA_mem->ida_kk) {

    for(i=IDA_mem->ida_ns; i<=IDA_mem->ida_kk; i++)
      IDA_mem->ida_cvals[i-IDA_mem->ida_ns] = ONE/IDA_mem->ida_beta[i];

    (void) N_VScaleVectorArray(IDA_mem->ida_kk - IDA_mem->ida_ns + 1,
                               IDA_mem->ida_cvals,
                               IDA_mem->ida_phi+IDA_mem->ida_ns,
                               IDA_mem->ida_phi+IDA_mem->ida_ns);

    if (IDA_mem->ida_quadr)
      (void) N_VScaleVectorArray(IDA_mem->ida_kk - IDA_mem->ida_ns + 1,
                                 IDA_mem->ida_cvals,
                                 IDA_mem->ida_phiQ+IDA_mem->ida_ns,
                                 IDA_mem->ida_phiQ+IDA_mem->ida_ns);

    if (IDA_mem->ida_sensi || IDA_mem->ida_quadr_sensi) {
      j = 0;
      for(i=IDA_mem->ida_ns; i<=IDA_mem->ida_kk; i++) {
        for(is=0; is<IDA_mem->ida_Ns; is++) {
          IDA_mem->ida_cvals[j] = ONE/IDA_mem->ida_beta[i];
          j++;
        }
      }
    }

    if (IDA_mem->ida_sensi) {
      j = 0;
      for(i=IDA_mem->ida_ns; i<=IDA_mem->ida_kk; i++) {
        for(is=0; is<IDA_mem->ida_Ns; is++) {
          IDA_mem->ida_Xvecs[j] = IDA_mem->ida_phiS[i][is];
          j++;
        }
      }

      (void) N_VScaleVectorArray(j, IDA_mem->ida_cvals, IDA_mem->ida_Xvecs,
                                 IDA_mem->ida_Xvecs);
    }

    if (IDA_mem->ida_quadr_sensi) {
      j = 0;
      for(i=IDA_mem->ida_ns; i<=IDA_mem->ida_kk; i++) {
        for(is=0; is<IDA_mem->ida_Ns; is++) {
          IDA_mem->ida_Xvecs[j] = IDA_mem->ida_phiQS[i][is];
          j++;
        }
      }

      (void) N_VScaleVectorArray(j, IDA_mem->ida_cvals, IDA_mem->ida_Xvecs,
                                 IDA_mem->ida_Xvecs);
    }
  }

}

/* 
 * -----------------------------------------------------------------
 * Handler for convergence and/or error test failures
 * -----------------------------------------------------------------
 */

/*
 * IDAHandleNFlag
 *
 * This routine handles failures indicated by the input variable nflag. 
 * Positive values indicate various recoverable failures while negative
 * values indicate nonrecoverable failures. This routine adjusts the
 * step size for recoverable failures. 
 *
 *  Possible nflag values (input):
 *
 *   --convergence failures--
 *   IDA_RES_RECVR              > 0
 *   IDA_LSOLVE_RECVR           > 0
 *   IDA_CONSTR_RECVR           > 0
 *   SUN_NLS_CONV_RECVR         > 0
 *   IDA_QRHS_RECVR             > 0
 *   IDA_QSRHS_RECVR            > 0
 *   IDA_RES_FAIL               < 0
 *   IDA_LSOLVE_FAIL            < 0
 *   IDA_LSETUP_FAIL            < 0
 *   IDA_QRHS_FAIL              < 0
 *
 *   --error test failure--
 *   ERROR_TEST_FAIL            > 0
 *
 *  Possible kflag values (output):
 *
 *   --recoverable--
 *   PREDICT_AGAIN
 *
 *   --nonrecoverable--
 *   IDA_CONSTR_FAIL   
 *   IDA_REP_RES_ERR    
 *   IDA_ERR_FAIL  
 *   IDA_CONV_FAIL 
 *   IDA_RES_FAIL
 *   IDA_LSETUP_FAIL
 *   IDA_LSOLVE_FAIL
 *   IDA_QRHS_FAIL
 *   IDA_REP_QRHS_ERR
 */

static int IDAHandleNFlag(IDAMem IDA_mem, int nflag, realtype err_k, realtype err_km1,
                          long int *ncfnPtr, int *ncfPtr, long int *netfPtr, int *nefPtr)
{
  realtype err_knew;

  IDA_mem->ida_phase = 1;
    
  if (nflag != ERROR_TEST_FAIL) {

    /*-----------------------
      Nonlinear solver failed 
      -----------------------*/

    (*ncfPtr)++;      /* local counter for convergence failures */
    (*ncfnPtr)++;     /* global counter for convergence failures */
    
    if (nflag < 0) {  /* nonrecoverable failure */

      return(nflag);

    } else {          /* recoverable failure    */
      
      /* Reduce step size for a new prediction
         Note that if nflag=IDA_CONSTR_RECVR then rr was already set in IDANls */
      if (nflag != IDA_CONSTR_RECVR) IDA_mem->ida_rr = QUARTER;
      IDA_mem->ida_hh *= IDA_mem->ida_rr;

      /* Test if there were too many convergence failures */
      if (*ncfPtr < IDA_mem->ida_maxncf)  return(PREDICT_AGAIN);
      else if (nflag == IDA_RES_RECVR)    return(IDA_REP_RES_ERR);
      else if (nflag == IDA_SRES_RECVR)   return(IDA_REP_SRES_ERR);
      else if (nflag == IDA_QRHS_RECVR)   return(IDA_REP_QRHS_ERR);
      else if (nflag == IDA_QSRHS_RECVR)  return(IDA_REP_QSRHS_ERR);
      else if (nflag == IDA_CONSTR_RECVR) return(IDA_CONSTR_FAIL);
      else                                return(IDA_CONV_FAIL);
    }
    
  } else { 

    /*-----------------
      Error Test failed 
      -----------------*/

    (*nefPtr)++;      /* local counter for error test failures */
    (*netfPtr)++;     /* global counter for error test failures */
    
    if (*nefPtr == 1) {
      
      /* On first error test failure, keep current order or lower order by one. 
         Compute new stepsize based on differences of the solution. */
      
      err_knew = (IDA_mem->ida_kk==IDA_mem->ida_knew)? err_k : err_km1;

      IDA_mem->ida_kk = IDA_mem->ida_knew;      
      IDA_mem->ida_rr = PT9 * SUNRpowerR( TWO * err_knew + PT0001,(-ONE/(IDA_mem->ida_kk+1)) );
      IDA_mem->ida_rr = SUNMAX(QUARTER, SUNMIN(PT9,IDA_mem->ida_rr));
      IDA_mem->ida_hh *= IDA_mem->ida_rr;
      return(PREDICT_AGAIN);
      
    } else if (*nefPtr == 2) {
      
      /* On second error test failure, use current order or decrease order by one. 
         Reduce stepsize by factor of 1/4. */

      IDA_mem->ida_kk = IDA_mem->ida_knew;
      IDA_mem->ida_rr = QUARTER;
      IDA_mem->ida_hh *= IDA_mem->ida_rr;
      return(PREDICT_AGAIN);
      
    } else if (*nefPtr < IDA_mem->ida_maxnef) {
      
      /* On third and subsequent error test failures, set order to 1.
         Reduce stepsize by factor of 1/4. */
      IDA_mem->ida_kk = 1;
      IDA_mem->ida_rr = QUARTER;
      IDA_mem->ida_hh *= IDA_mem->ida_rr;
      return(PREDICT_AGAIN);

    } else {

      /* Too many error test failures */
      return(IDA_ERR_FAIL);
      
    }
    
  }

}

/*
 * IDAReset
 *
 * This routine is called only if we need to predict again at the 
 * very first step. In such a case, reset phi[1] and psi[0].
 */

static void IDAReset(IDAMem IDA_mem)
{
  int is;

  IDA_mem->ida_psi[0] = IDA_mem->ida_hh;

  N_VScale(IDA_mem->ida_rr, IDA_mem->ida_phi[1], IDA_mem->ida_phi[1]);

  if (IDA_mem->ida_quadr)
    N_VScale(IDA_mem->ida_rr, IDA_mem->ida_phiQ[1], IDA_mem->ida_phiQ[1]);

  if (IDA_mem->ida_sensi || IDA_mem->ida_quadr_sensi)
    for(is=0; is<IDA_mem->ida_Ns; is++)
      IDA_mem->ida_cvals[is] = IDA_mem->ida_rr;

  if (IDA_mem->ida_sensi)
    (void) N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                               IDA_mem->ida_phiS[1], IDA_mem->ida_phiS[1]);

  if (IDA_mem->ida_quadr_sensi)
    (void) N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                               IDA_mem->ida_phiQS[1], IDA_mem->ida_phiQS[1]);
}

/* 
 * -----------------------------------------------------------------
 * Function called after a successful step
 * -----------------------------------------------------------------
 */

/*
 * IDACompleteStep
 *
 * This routine completes a successful step.  It increments nst,
 * saves the stepsize and order used, makes the final selection of
 * stepsize and order for the next step, and updates the phi array.
 * Its return value is IDA_SUCCESS = 0.
 */

static void IDACompleteStep(IDAMem IDA_mem, realtype err_k, realtype err_km1)
{
  int i, j, is, kdiff, action;
  realtype terr_k, terr_km1, terr_kp1;
  realtype err_knew, err_kp1;
  realtype enorm, tmp, hnew;
  N_Vector tempvQ, *tempvS;

  IDA_mem->ida_nst++;
  kdiff = IDA_mem->ida_kk - IDA_mem->ida_kused;
  IDA_mem->ida_kused = IDA_mem->ida_kk;
  IDA_mem->ida_hused = IDA_mem->ida_hh;

  if ( (IDA_mem->ida_knew == IDA_mem->ida_kk-1) ||
       (IDA_mem->ida_kk == IDA_mem->ida_maxord) )
    IDA_mem->ida_phase = 1;

  /* For the first few steps, until either a step fails, or the order is 
     reduced, or the order reaches its maximum, we raise the order and double 
     the stepsize. During these steps, phase = 0. Thereafter, phase = 1, and
     stepsize and order are set by the usual local error algorithm.         
     
     Note that, after the first step, the order is not increased, as not all 
     of the neccessary information is available yet. */
  
  if (IDA_mem->ida_phase == 0) {

    if(IDA_mem->ida_nst > 1) {
      IDA_mem->ida_kk++;
      hnew = TWO * IDA_mem->ida_hh;
      if( (tmp = SUNRabs(hnew) * IDA_mem->ida_hmax_inv) > ONE )
        hnew /= tmp;
      IDA_mem->ida_hh = hnew;
    }

  } else {

    action = UNSET;
    
    /* Set action = LOWER/MAINTAIN/RAISE to specify order decision */
    
    if (IDA_mem->ida_knew == IDA_mem->ida_kk-1)      {action = LOWER;    goto takeaction;}
    if (IDA_mem->ida_kk == IDA_mem->ida_maxord)      {action = MAINTAIN; goto takeaction;}
    if ( (IDA_mem->ida_kk+1 >= IDA_mem->ida_ns ) || (kdiff == 1)) {action = MAINTAIN; goto takeaction;}
    
    /* Estimate the error at order k+1, unless already decided to
       reduce order, or already using maximum order, or stepsize has not
       been constant, or order was just raised. */
    
    N_VLinearSum(ONE, IDA_mem->ida_ee, -ONE,
                 IDA_mem->ida_phi[IDA_mem->ida_kk+1],
                 IDA_mem->ida_tempv1);
    enorm = IDAWrmsNorm(IDA_mem, IDA_mem->ida_tempv1,
                        IDA_mem->ida_ewt, IDA_mem->ida_suppressalg);
    
    if (IDA_mem->ida_errconQ) {
      tempvQ = IDA_mem->ida_ypQ;
      N_VLinearSum (ONE, IDA_mem->ida_eeQ, -ONE,
                    IDA_mem->ida_phiQ[IDA_mem->ida_kk+1], tempvQ);
      enorm = IDAQuadWrmsNormUpdate(IDA_mem, enorm, tempvQ, IDA_mem->ida_ewtQ);
    }

    if (IDA_mem->ida_errconS) {
      tempvS = IDA_mem->ida_ypS;

      (void) N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                                     ONE,  IDA_mem->ida_eeS,
                                     -ONE, IDA_mem->ida_phiS[IDA_mem->ida_kk+1],
                                     tempvS);

      enorm = IDASensWrmsNormUpdate(IDA_mem, enorm, tempvS,
                                    IDA_mem->ida_ewtS, IDA_mem->ida_suppressalg);
    }

    if (IDA_mem->ida_errconQS) {
      (void) N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                                     ONE,  IDA_mem->ida_eeQS,
                                     -ONE, IDA_mem->ida_phiQS[IDA_mem->ida_kk+1],
                                     IDA_mem->ida_tempvQS);

      enorm = IDAQuadSensWrmsNormUpdate(IDA_mem, enorm,
                                        IDA_mem->ida_tempvQS, IDA_mem->ida_ewtQS);
    }
    err_kp1= enorm/(IDA_mem->ida_kk+2);

    /* Choose among orders k-1, k, k+1 using local truncation error norms. */

    terr_k   = (IDA_mem->ida_kk+1) * err_k;
    terr_kp1 = (IDA_mem->ida_kk+2) * err_kp1;

    if (IDA_mem->ida_kk == 1) {
      if (terr_kp1 >= HALF * terr_k)         {action = MAINTAIN; goto takeaction;}
      else                                   {action = RAISE;    goto takeaction;}
    } else {
      terr_km1 = IDA_mem->ida_kk * err_km1;
      if (terr_km1 <= SUNMIN(terr_k, terr_kp1)) {action = LOWER;    goto takeaction;}
      else if (terr_kp1  >= terr_k)          {action = MAINTAIN; goto takeaction;}
      else                                   {action = RAISE;    goto takeaction;}
    }
    
  takeaction:
    
    /* Set the estimated error norm and, on change of order, reset kk. */
    if      (action == RAISE) { IDA_mem->ida_kk++; err_knew = err_kp1; }
    else if (action == LOWER) { IDA_mem->ida_kk--; err_knew = err_km1; }
    else                      {       err_knew = err_k;   }  

    /* Compute rr = tentative ratio hnew/hh from error norm.
       Reduce hh if rr <= 1, double hh if rr >= 2, else leave hh as is.
       If hh is reduced, hnew/hh is restricted to be between .5 and .9. */
    
    hnew = IDA_mem->ida_hh;
    IDA_mem->ida_rr = SUNRpowerR( (TWO * err_knew + PT0001) , (-ONE/(IDA_mem->ida_kk+1) ) );
    
    if (IDA_mem->ida_rr >= TWO) {
      hnew = TWO * IDA_mem->ida_hh;
      if( (tmp = SUNRabs(hnew) * IDA_mem->ida_hmax_inv) > ONE )
        hnew /= tmp;
    } else if (IDA_mem->ida_rr <= ONE ) { 
      IDA_mem->ida_rr = SUNMAX(HALF, SUNMIN(PT9,IDA_mem->ida_rr));
      hnew = IDA_mem->ida_hh * IDA_mem->ida_rr;
    }
    
    IDA_mem->ida_hh = hnew;
    
  } /* end of phase if block */
  
  /* Save ee etc. for possible order increase on next step */
  
  if (IDA_mem->ida_kused < IDA_mem->ida_maxord) {

    N_VScale(ONE, IDA_mem->ida_ee, IDA_mem->ida_phi[IDA_mem->ida_kused+1]);

    if (IDA_mem->ida_quadr)
      N_VScale(ONE, IDA_mem->ida_eeQ, IDA_mem->ida_phiQ[IDA_mem->ida_kused+1]);

    if (IDA_mem->ida_sensi || IDA_mem->ida_quadr_sensi)
      for (is=0; is<IDA_mem->ida_Ns; is++)
        IDA_mem->ida_cvals[is] = ONE;

    if (IDA_mem->ida_sensi)
      (void) N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                                 IDA_mem->ida_eeS,
                                 IDA_mem->ida_phiS[IDA_mem->ida_kused+1]);

    if (IDA_mem->ida_quadr_sensi)
      (void) N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                                 IDA_mem->ida_eeQS,
                                 IDA_mem->ida_phiQS[IDA_mem->ida_kused+1]);
  }

  /* Update phi arrays */

  /* To update phi arrays compute X += Z where                  */
  /* X = [ phi[kused], phi[kused-1], phi[kused-2], ... phi[1] ] */
  /* Z = [ ee,         phi[kused],   phi[kused-1], ... phi[0] ] */

  IDA_mem->ida_Zvecs[0] = IDA_mem->ida_ee;
  IDA_mem->ida_Xvecs[0] = IDA_mem->ida_phi[IDA_mem->ida_kused];
  for (j=1; j<=IDA_mem->ida_kused; j++) {
    IDA_mem->ida_Zvecs[j] = IDA_mem->ida_phi[IDA_mem->ida_kused-j+1];
    IDA_mem->ida_Xvecs[j] = IDA_mem->ida_phi[IDA_mem->ida_kused-j];
  }

  (void) N_VLinearSumVectorArray(IDA_mem->ida_kused+1,
                                 ONE, IDA_mem->ida_Xvecs,
                                 ONE, IDA_mem->ida_Zvecs,
                                 IDA_mem->ida_Xvecs);

  if (IDA_mem->ida_quadr) {

    IDA_mem->ida_Zvecs[0] = IDA_mem->ida_eeQ;
    IDA_mem->ida_Xvecs[0] = IDA_mem->ida_phiQ[IDA_mem->ida_kused];
    for (j=1; j<=IDA_mem->ida_kused; j++) {
      IDA_mem->ida_Zvecs[j] = IDA_mem->ida_phiQ[IDA_mem->ida_kused-j+1];
      IDA_mem->ida_Xvecs[j] = IDA_mem->ida_phiQ[IDA_mem->ida_kused-j];
    }

    (void) N_VLinearSumVectorArray(IDA_mem->ida_kused+1,
                                   ONE, IDA_mem->ida_Xvecs,
                                   ONE, IDA_mem->ida_Zvecs,
                                   IDA_mem->ida_Xvecs);
  }

  if (IDA_mem->ida_sensi) {

    i=0;
    for (is=0; is<IDA_mem->ida_Ns; is++) {
      IDA_mem->ida_Zvecs[i] = IDA_mem->ida_eeS[is];
      IDA_mem->ida_Xvecs[i] = IDA_mem->ida_phiS[IDA_mem->ida_kused][is];
      i++;
      for (j=1; j<=IDA_mem->ida_kused; j++) {
        IDA_mem->ida_Zvecs[i] = IDA_mem->ida_phiS[IDA_mem->ida_kused-j+1][is];
        IDA_mem->ida_Xvecs[i] = IDA_mem->ida_phiS[IDA_mem->ida_kused-j][is];
        i++;
      }
    }

    (void) N_VLinearSumVectorArray(IDA_mem->ida_Ns*(IDA_mem->ida_kused+1),
                                   ONE, IDA_mem->ida_Xvecs,
                                   ONE, IDA_mem->ida_Zvecs,
                                   IDA_mem->ida_Xvecs);
  }

  if (IDA_mem->ida_quadr_sensi) {

    i=0;
    for (is=0; is<IDA_mem->ida_Ns; is++) {
      IDA_mem->ida_Zvecs[i] = IDA_mem->ida_eeQS[is];
      IDA_mem->ida_Xvecs[i] = IDA_mem->ida_phiQS[IDA_mem->ida_kused][is];
      i++;
      for (j=1; j<=IDA_mem->ida_kused; j++) {
        IDA_mem->ida_Zvecs[i] = IDA_mem->ida_phiQS[IDA_mem->ida_kused-j+1][is];
        IDA_mem->ida_Xvecs[i] = IDA_mem->ida_phiQS[IDA_mem->ida_kused-j][is];
        i++;
      }
    }

    (void) N_VLinearSumVectorArray(IDA_mem->ida_Ns*(IDA_mem->ida_kused+1),
                                   ONE, IDA_mem->ida_Xvecs,
                                   ONE, IDA_mem->ida_Zvecs,
                                   IDA_mem->ida_Xvecs);
  }

}

/* 
 * -----------------------------------------------------------------
 * Norm functions
 * -----------------------------------------------------------------
 */

/*
 * IDAWrmsNorm
 *
 *  Returns the WRMS norm of vector x with weights w.
 *  If mask = SUNTRUE, the weight vector w is masked by id, i.e.,
 *      nrm = N_VWrmsNormMask(x,w,id);
 *  Otherwise,
 *      nrm = N_VWrmsNorm(x,w);
 * 
 * mask = SUNFALSE       when the call is made from the nonlinear solver.
 * mask = suppressalg otherwise.
 */

realtype IDAWrmsNorm(IDAMem IDA_mem, N_Vector x, N_Vector w, 
                     booleantype mask)
{
  realtype nrm;

  if (mask) nrm = N_VWrmsNormMask(x, w, IDA_mem->ida_id);
  else      nrm = N_VWrmsNorm(x, w);

  return(nrm);
}

/*
 * IDASensWrmsNorm
 *
 * This routine returns the maximum over the weighted root mean 
 * square norm of xS with weight vectors wS:
 *
 *   max { wrms(xS[0],wS[0]) ... wrms(xS[Ns-1],wS[Ns-1]) }    
 *
 * Called by IDASensUpdateNorm or directly in the IDA_STAGGERED approach 
 * during the NLS solution and before the error test.
 *
 * Declared global for use in the computation of IC for sensitivities.
 */

realtype IDASensWrmsNorm(IDAMem IDA_mem, N_Vector *xS, N_Vector *wS,
                                booleantype mask)
{
  int is;
  realtype nrm;

  if (mask)
    (void) N_VWrmsNormMaskVectorArray(IDA_mem->ida_Ns, xS, wS,
                                      IDA_mem->ida_id, IDA_mem->ida_cvals);
  else
    (void) N_VWrmsNormVectorArray(IDA_mem->ida_Ns, xS, wS,
                                  IDA_mem->ida_cvals);

  nrm = IDA_mem->ida_cvals[0];
  for (is=1; is<IDA_mem->ida_Ns; is++)
    if ( IDA_mem->ida_cvals[is] > nrm ) nrm = IDA_mem->ida_cvals[is];

  return (nrm);
}

/*
 * IDAQuadSensWrmsNorm
 *
 * This routine returns the maximum over the weighted root mean 
 * square norm of xQS with weight vectors wQS:
 *
 *   max { wrms(xQS[0],wQS[0]) ... wrms(xQS[Ns-1],wQS[Ns-1]) }    
 */

static realtype IDAQuadSensWrmsNorm(IDAMem IDA_mem, N_Vector *xQS, N_Vector *wQS)
{
  int is;
  realtype nrm;

  (void) N_VWrmsNormVectorArray(IDA_mem->ida_Ns, xQS, wQS,
                                IDA_mem->ida_cvals);

  nrm = IDA_mem->ida_cvals[0];
  for (is=1; is<IDA_mem->ida_Ns; is++)
    if ( IDA_mem->ida_cvals[is] > nrm ) nrm = IDA_mem->ida_cvals[is];

  return (nrm);
}

/*
 * IDAQuadWrmsNormUpdate
 *
 * Updates the norm old_nrm to account for all quadratures.
 */

static realtype IDAQuadWrmsNormUpdate(IDAMem IDA_mem, realtype old_nrm,
                                      N_Vector xQ, N_Vector wQ)
{
  realtype qnrm;

  qnrm = N_VWrmsNorm(xQ, wQ);
  if (old_nrm > qnrm) return(old_nrm);
  else                return(qnrm);
}

/*
 * IDASensWrmsNormUpdate
 *
 * Updates the norm old_nrm to account for all sensitivities.
 *
 * This function is declared global since it is used for finding 
 * IC for sensitivities,
 */

realtype IDASensWrmsNormUpdate(IDAMem IDA_mem, realtype old_nrm,
                                      N_Vector *xS, N_Vector *wS,
                                      booleantype mask)
{
  realtype snrm;
  
  snrm = IDASensWrmsNorm(IDA_mem, xS, wS, mask);
  if (old_nrm > snrm) return(old_nrm);
  else                return(snrm);
}

static realtype IDAQuadSensWrmsNormUpdate(IDAMem IDA_mem, realtype old_nrm, 
                                          N_Vector *xQS, N_Vector *wQS)
{
  realtype qsnrm;

  qsnrm = IDAQuadSensWrmsNorm(IDA_mem, xQS, wQS);
  if (old_nrm > qsnrm) return(old_nrm);
  else                 return(qsnrm);
}

/* 
 * -----------------------------------------------------------------
 * Functions for rootfinding
 * -----------------------------------------------------------------
 */

/*
 * IDARcheck1
 *
 * This routine completes the initialization of rootfinding memory
 * information, and checks whether g has a zero both at and very near
 * the initial point of the IVP.
 *
 * This routine returns an int equal to:
 *  IDA_RTFUNC_FAIL < 0 if the g function failed, or
 *  IDA_SUCCESS     = 0 otherwise.
 */

static int IDARcheck1(IDAMem IDA_mem)
{
  int i, retval;
  realtype smallh, hratio, tplus;
  booleantype zroot;

  for (i = 0; i < IDA_mem->ida_nrtfn; i++)
    IDA_mem->ida_iroots[i] = 0;
  IDA_mem->ida_tlo = IDA_mem->ida_tn;
  IDA_mem->ida_ttol = (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh)) *
    IDA_mem->ida_uround * HUNDRED;

  /* Evaluate g at initial t and check for zero values. */
  retval = IDA_mem->ida_gfun(IDA_mem->ida_tlo, IDA_mem->ida_phi[0], IDA_mem->ida_phi[1],
                             IDA_mem->ida_glo, IDA_mem->ida_user_data);
  IDA_mem->ida_nge = 1;
  if (retval != 0) return(IDA_RTFUNC_FAIL);

  zroot = SUNFALSE;
  for (i = 0; i < IDA_mem->ida_nrtfn; i++) {
    if (SUNRabs(IDA_mem->ida_glo[i]) == ZERO) {
      zroot = SUNTRUE;
      IDA_mem->ida_gactive[i] = SUNFALSE;
    }
  }
  if (!zroot) return(IDA_SUCCESS);

  /* Some g_i is zero at t0; look at g at t0+(small increment). */
  hratio = SUNMAX(IDA_mem->ida_ttol / SUNRabs(IDA_mem->ida_hh), PT1);
  smallh = hratio*IDA_mem->ida_hh;
  tplus = IDA_mem->ida_tlo + smallh;
  N_VLinearSum(ONE, IDA_mem->ida_phi[0], smallh, IDA_mem->ida_phi[1], IDA_mem->ida_yy);
  retval = IDA_mem->ida_gfun(tplus, IDA_mem->ida_yy, IDA_mem->ida_phi[1],
                             IDA_mem->ida_ghi, IDA_mem->ida_user_data);  
  IDA_mem->ida_nge++;
  if (retval != 0) return(IDA_RTFUNC_FAIL);

  /* We check now only the components of g which were exactly 0.0 at t0
   * to see if we can 'activate' them. */
  for (i = 0; i < IDA_mem->ida_nrtfn; i++) {
    if (!IDA_mem->ida_gactive[i] &&
        SUNRabs(IDA_mem->ida_ghi[i]) != ZERO) {
      IDA_mem->ida_gactive[i] = SUNTRUE;
      IDA_mem->ida_glo[i] = IDA_mem->ida_ghi[i];
    }
  }
  return(IDA_SUCCESS);
}

/*
 * IDARcheck2
 *
 * This routine checks for exact zeros of g at the last root found,
 * if the last return was a root.  It then checks for a close pair of
 * zeros (an error condition), and for a new root at a nearby point.
 * The array glo = g(tlo) at the left endpoint of the search interval
 * is adjusted if necessary to assure that all g_i are nonzero
 * there, before returning to do a root search in the interval.
 *
 * On entry, tlo = tretlast is the last value of tret returned by
 * IDASolve.  This may be the previous tn, the previous tout value,
 * or the last root location.
 *
 * This routine returns an int equal to:
 *     IDA_RTFUNC_FAIL < 0 if the g function failed, or
 *     CLOSERT         = 3 if a close pair of zeros was found, or
 *     RTFOUND         = 1 if a new zero of g was found near tlo, or
 *     IDA_SUCCESS     = 0 otherwise.
 */

static int IDARcheck2(IDAMem IDA_mem)
{
  int i, retval;
  realtype smallh, hratio, tplus;
  booleantype zroot;

  if (IDA_mem->ida_irfnd == 0) return(IDA_SUCCESS);

  (void) IDAGetSolution(IDA_mem, IDA_mem->ida_tlo, IDA_mem->ida_yy, IDA_mem->ida_yp);
  retval = IDA_mem->ida_gfun(IDA_mem->ida_tlo, IDA_mem->ida_yy,
                             IDA_mem->ida_yp, IDA_mem->ida_glo,
                             IDA_mem->ida_user_data);  
  IDA_mem->ida_nge++;
  if (retval != 0) return(IDA_RTFUNC_FAIL);

  zroot = SUNFALSE;
  for (i = 0; i < IDA_mem->ida_nrtfn; i++)
    IDA_mem->ida_iroots[i] = 0;
  for (i = 0; i < IDA_mem->ida_nrtfn; i++) {
    if (!IDA_mem->ida_gactive[i]) continue;
    if (SUNRabs(IDA_mem->ida_glo[i]) == ZERO) {
      zroot = SUNTRUE;
      IDA_mem->ida_iroots[i] = 1;
    }
  }
  if (!zroot) return(IDA_SUCCESS);

  /* One or more g_i has a zero at tlo.  Check g at tlo+smallh. */
  IDA_mem->ida_ttol = (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh)) *
    IDA_mem->ida_uround * HUNDRED;
  smallh = (IDA_mem->ida_hh > ZERO) ? IDA_mem->ida_ttol : -IDA_mem->ida_ttol;
  tplus = IDA_mem->ida_tlo + smallh;
  if ( (tplus - IDA_mem->ida_tn)*IDA_mem->ida_hh >= ZERO) {
    hratio = smallh/IDA_mem->ida_hh;
    N_VLinearSum(ONE, IDA_mem->ida_yy, hratio, IDA_mem->ida_phi[1], IDA_mem->ida_yy);
  } else {
    (void) IDAGetSolution(IDA_mem, tplus, IDA_mem->ida_yy, IDA_mem->ida_yp);
  }
  retval = IDA_mem->ida_gfun(tplus, IDA_mem->ida_yy, IDA_mem->ida_yp,
                             IDA_mem->ida_ghi, IDA_mem->ida_user_data);  
  IDA_mem->ida_nge++;
  if (retval != 0) return(IDA_RTFUNC_FAIL);

  /* Check for close roots (error return), for a new zero at tlo+smallh,
  and for a g_i that changed from zero to nonzero. */
  zroot = SUNFALSE;
  for (i = 0; i < IDA_mem->ida_nrtfn; i++) {
    if (!IDA_mem->ida_gactive[i]) continue;
    if (SUNRabs(IDA_mem->ida_ghi[i]) == ZERO) {
      if (IDA_mem->ida_iroots[i] == 1) return(CLOSERT);
      zroot = SUNTRUE;
      IDA_mem->ida_iroots[i] = 1;
    } else {
      if (IDA_mem->ida_iroots[i] == 1)
        IDA_mem->ida_glo[i] = IDA_mem->ida_ghi[i];
    }
  }
  if (zroot) return(RTFOUND);
  return(IDA_SUCCESS);
}

/*
 * IDARcheck3
 *
 * This routine interfaces to IDARootfind to look for a root of g
 * between tlo and either tn or tout, whichever comes first.
 * Only roots beyond tlo in the direction of integration are sought.
 *
 * This routine returns an int equal to:
 *     IDA_RTFUNC_FAIL < 0 if the g function failed, or
 *     RTFOUND         = 1 if a root of g was found, or
 *     IDA_SUCCESS     = 0 otherwise.
 */

static int IDARcheck3(IDAMem IDA_mem)
{
  int i, ier, retval;

  /* Set thi = tn or tout, whichever comes first. */
  if (IDA_mem->ida_taskc == IDA_ONE_STEP)
    IDA_mem->ida_thi = IDA_mem->ida_tn;
  if (IDA_mem->ida_taskc == IDA_NORMAL) {
    IDA_mem->ida_thi = ((IDA_mem->ida_toutc - IDA_mem->ida_tn)*IDA_mem->ida_hh >= ZERO) ?
      IDA_mem->ida_tn : IDA_mem->ida_toutc;
  }

  /* Get y and y' at thi. */
  (void) IDAGetSolution(IDA_mem, IDA_mem->ida_thi, IDA_mem->ida_yy, IDA_mem->ida_yp);


  /* Set ghi = g(thi) and call IDARootfind to search (tlo,thi) for roots. */
  retval = IDA_mem->ida_gfun(IDA_mem->ida_thi, IDA_mem->ida_yy,
                             IDA_mem->ida_yp, IDA_mem->ida_ghi,
                             IDA_mem->ida_user_data);  
  IDA_mem->ida_nge++;
  if (retval != 0) return(IDA_RTFUNC_FAIL);

  IDA_mem->ida_ttol = (SUNRabs(IDA_mem->ida_tn) + SUNRabs(IDA_mem->ida_hh)) *
    IDA_mem->ida_uround * HUNDRED;
  ier = IDARootfind(IDA_mem);
  if (ier == IDA_RTFUNC_FAIL) return(IDA_RTFUNC_FAIL);
  for(i=0; i<IDA_mem->ida_nrtfn; i++) {
    if(!IDA_mem->ida_gactive[i] &&
       IDA_mem->ida_grout[i] != ZERO)
      IDA_mem->ida_gactive[i] = SUNTRUE;
  }
  IDA_mem->ida_tlo = IDA_mem->ida_trout;
  for (i = 0; i < IDA_mem->ida_nrtfn; i++)
    IDA_mem->ida_glo[i] = IDA_mem->ida_grout[i];

  /* If no root found, return IDA_SUCCESS. */  
  if (ier == IDA_SUCCESS) return(IDA_SUCCESS);

  /* If a root was found, interpolate to get y(trout) and return.  */
  (void) IDAGetSolution(IDA_mem, IDA_mem->ida_trout, IDA_mem->ida_yy, IDA_mem->ida_yp);
  return(RTFOUND);
}

/*
 * IDARootfind
 *
 * This routine solves for a root of g(t) between tlo and thi, if
 * one exists.  Only roots of odd multiplicity (i.e. with a change
 * of sign in one of the g_i), or exact zeros, are found.
 * Here the sign of tlo - thi is arbitrary, but if multiple roots
 * are found, the one closest to tlo is returned.
 *
 * The method used is the Illinois algorithm, a modified secant method.
 * Reference: Kathie L. Hiebert and Lawrence F. Shampine, Implicitly
 * Defined Output Points for Solutions of ODEs, Sandia National
 * Laboratory Report SAND80-0180, February 1980.
 *
 * This routine uses the following parameters for communication:
 *
 * nrtfn    = number of functions g_i, or number of components of
 *            the vector-valued function g(t).  Input only.
 *
 * gfun     = user-defined function for g(t).  Its form is
 *            (void) gfun(t, y, yp, gt, user_data)
 *
 * rootdir  = in array specifying the direction of zero-crossings.
 *            If rootdir[i] > 0, search for roots of g_i only if
 *            g_i is increasing; if rootdir[i] < 0, search for
 *            roots of g_i only if g_i is decreasing; otherwise
 *            always search for roots of g_i.
 *
 * gactive  = array specifying whether a component of g should
 *            or should not be monitored. gactive[i] is initially
 *            set to SUNTRUE for all i=0,...,nrtfn-1, but it may be
 *            reset to SUNFALSE if at the first step g[i] is 0.0
 *            both at the I.C. and at a small perturbation of them.
 *            gactive[i] is then set back on SUNTRUE only after the 
 *            corresponding g function moves away from 0.0.
 *
 * nge      = cumulative counter for gfun calls.
 *
 * ttol     = a convergence tolerance for trout.  Input only.
 *            When a root at trout is found, it is located only to
 *            within a tolerance of ttol.  Typically, ttol should
 *            be set to a value on the order of
 *               100 * UROUND * max (SUNRabs(tlo), SUNRabs(thi))
 *            where UROUND is the unit roundoff of the machine.
 *
 * tlo, thi = endpoints of the interval in which roots are sought.
 *            On input, these must be distinct, but tlo - thi may
 *            be of either sign.  The direction of integration is
 *            assumed to be from tlo to thi.  On return, tlo and thi
 *            are the endpoints of the final relevant interval.
 *
 * glo, ghi = arrays of length nrtfn containing the vectors g(tlo)
 *            and g(thi) respectively.  Input and output.  On input,
 *            none of the glo[i] should be zero.
 *
 * trout    = root location, if a root was found, or thi if not.
 *            Output only.  If a root was found other than an exact
 *            zero of g, trout is the endpoint thi of the final
 *            interval bracketing the root, with size at most ttol.
 *
 * grout    = array of length nrtfn containing g(trout) on return.
 *
 * iroots   = int array of length nrtfn with root information.
 *            Output only.  If a root was found, iroots indicates
 *            which components g_i have a root at trout.  For
 *            i = 0, ..., nrtfn-1, iroots[i] = 1 if g_i has a root
 *            and g_i is increasing, iroots[i] = -1 if g_i has a
 *            root and g_i is decreasing, and iroots[i] = 0 if g_i
 *            has no roots or g_i varies in the direction opposite
 *            to that indicated by rootdir[i].
 *
 * This routine returns an int equal to:
 *      IDA_RTFUNC_FAIL < 0 if the g function failed, or
 *      RTFOUND         = 1 if a root of g was found, or
 *      IDA_SUCCESS     = 0 otherwise.
 *
 */

static int IDARootfind(IDAMem IDA_mem)
{
  realtype alph, tmid, gfrac, maxfrac, fracint, fracsub;
  int i, retval, imax, side, sideprev;
  booleantype zroot, sgnchg;

  imax = 0;

  /* First check for change in sign in ghi or for a zero in ghi. */
  maxfrac = ZERO;
  zroot = SUNFALSE;
  sgnchg = SUNFALSE;
  for (i = 0;  i < IDA_mem->ida_nrtfn; i++) {
    if(!IDA_mem->ida_gactive[i]) continue;
    if (SUNRabs(IDA_mem->ida_ghi[i]) == ZERO) {
      if(IDA_mem->ida_rootdir[i]*IDA_mem->ida_glo[i] <= ZERO) {
        zroot = SUNTRUE;
      }
    } else {
      if ( (IDA_mem->ida_glo[i]*IDA_mem->ida_ghi[i] < ZERO) &&
           (IDA_mem->ida_rootdir[i]*IDA_mem->ida_glo[i] <= ZERO) ) {
        gfrac = SUNRabs(IDA_mem->ida_ghi[i]/(IDA_mem->ida_ghi[i] - IDA_mem->ida_glo[i]));
        if (gfrac > maxfrac) {
          sgnchg = SUNTRUE;
          maxfrac = gfrac;
          imax = i;
        }
      }
    }
  }

  /* If no sign change was found, reset trout and grout.  Then return
     IDA_SUCCESS if no zero was found, or set iroots and return RTFOUND.  */ 
  if (!sgnchg) {
    IDA_mem->ida_trout = IDA_mem->ida_thi;
    for (i = 0; i < IDA_mem->ida_nrtfn; i++)
      IDA_mem->ida_grout[i] = IDA_mem->ida_ghi[i];
    if (!zroot) return(IDA_SUCCESS);
    for (i = 0; i < IDA_mem->ida_nrtfn; i++) {
      IDA_mem->ida_iroots[i] = 0;
      if(!IDA_mem->ida_gactive[i]) continue;
      if ( (SUNRabs(IDA_mem->ida_ghi[i]) == ZERO) &&
           (IDA_mem->ida_rootdir[i]*IDA_mem->ida_glo[i] <= ZERO) )
        IDA_mem->ida_iroots[i] = IDA_mem->ida_glo[i] > 0 ? -1:1;
    }
    return(RTFOUND);
  }

  /* Initialize alph to avoid compiler warning */
  alph = ONE;

  /* A sign change was found.  Loop to locate nearest root. */

  side = 0;  sideprev = -1;
  for(;;) {                                    /* Looping point */

    /* If interval size is already less than tolerance ttol, break. */
      if (SUNRabs(IDA_mem->ida_thi - IDA_mem->ida_tlo) <= IDA_mem->ida_ttol)
        break;

    /* Set weight alph.
       On the first two passes, set alph = 1.  Thereafter, reset alph
       according to the side (low vs high) of the subinterval in which
       the sign change was found in the previous two passes.
       If the sides were opposite, set alph = 1.
       If the sides were the same, then double alph (if high side),
       or halve alph (if low side).
       The next guess tmid is the secant method value if alph = 1, but
       is closer to tlo if alph < 1, and closer to thi if alph > 1.    */

    if (sideprev == side) {
      alph = (side == 2) ? alph*TWO : alph*HALF;
    } else {
      alph = ONE;
    }

    /* Set next root approximation tmid and get g(tmid).
       If tmid is too close to tlo or thi, adjust it inward,
       by a fractional distance that is between 0.1 and 0.5.  */
    tmid = IDA_mem->ida_thi - (IDA_mem->ida_thi - IDA_mem->ida_tlo) *
      IDA_mem->ida_ghi[imax]/(IDA_mem->ida_ghi[imax] - alph*IDA_mem->ida_glo[imax]);
    if (SUNRabs(tmid - IDA_mem->ida_tlo) < HALF * IDA_mem->ida_ttol) {
      fracint = SUNRabs(IDA_mem->ida_thi - IDA_mem->ida_tlo) / IDA_mem->ida_ttol;
      fracsub = (fracint > FIVE) ? PT1 : HALF/fracint;
      tmid = IDA_mem->ida_tlo + fracsub*(IDA_mem->ida_thi - IDA_mem->ida_tlo);
    }
    if (SUNRabs(IDA_mem->ida_thi - tmid) < HALF * IDA_mem->ida_ttol) {
      fracint = SUNRabs(IDA_mem->ida_thi - IDA_mem->ida_tlo) / IDA_mem->ida_ttol;
      fracsub = (fracint > FIVE) ? PT1 : HALF/fracint;
      tmid = IDA_mem->ida_thi - fracsub*(IDA_mem->ida_thi - IDA_mem->ida_tlo);
    }

    (void) IDAGetSolution(IDA_mem, tmid, IDA_mem->ida_yy, IDA_mem->ida_yp);
    retval = IDA_mem->ida_gfun(tmid, IDA_mem->ida_yy, IDA_mem->ida_yp,
                               IDA_mem->ida_grout, IDA_mem->ida_user_data);  
    IDA_mem->ida_nge++;
    if (retval != 0) return(IDA_RTFUNC_FAIL);

    /* Check to see in which subinterval g changes sign, and reset imax.
       Set side = 1 if sign change is on low side, or 2 if on high side.  */  
    maxfrac = ZERO;
    zroot = SUNFALSE;
    sgnchg = SUNFALSE;
    sideprev = side;
    for (i = 0;  i < IDA_mem->ida_nrtfn; i++) {
      if(!IDA_mem->ida_gactive[i]) continue;
      if (SUNRabs(IDA_mem->ida_grout[i]) == ZERO) {
        if(IDA_mem->ida_rootdir[i]*IDA_mem->ida_glo[i] <= ZERO)
          zroot = SUNTRUE;
      } else {
        if ( (IDA_mem->ida_glo[i]*IDA_mem->ida_grout[i] < ZERO) &&
             (IDA_mem->ida_rootdir[i]*IDA_mem->ida_glo[i] <= ZERO) ) {
          gfrac = SUNRabs(IDA_mem->ida_grout[i] /
                          (IDA_mem->ida_grout[i] - IDA_mem->ida_glo[i]));
          if (gfrac > maxfrac) {
            sgnchg = SUNTRUE;
            maxfrac = gfrac;
            imax = i;
          }
        }
      }
    }
    if (sgnchg) {
      /* Sign change found in (tlo,tmid); replace thi with tmid. */
      IDA_mem->ida_thi = tmid;
      for (i = 0; i < IDA_mem->ida_nrtfn; i++)
        IDA_mem->ida_ghi[i] = IDA_mem->ida_grout[i];
      side = 1;
      /* Stop at root thi if converged; otherwise loop. */
      if (SUNRabs(IDA_mem->ida_thi - IDA_mem->ida_tlo) <= IDA_mem->ida_ttol)
        break;
      continue;  /* Return to looping point. */
    }

    if (zroot) {
      /* No sign change in (tlo,tmid), but g = 0 at tmid; return root tmid. */
      IDA_mem->ida_thi = tmid;
      for (i = 0; i < IDA_mem->ida_nrtfn; i++)
        IDA_mem->ida_ghi[i] = IDA_mem->ida_grout[i];
      break;
    }

    /* No sign change in (tlo,tmid), and no zero at tmid.
       Sign change must be in (tmid,thi).  Replace tlo with tmid. */
    IDA_mem->ida_tlo = tmid;
    for (i = 0; i < IDA_mem->ida_nrtfn; i++)
      IDA_mem->ida_glo[i] = IDA_mem->ida_grout[i];
    side = 2;
    /* Stop at root thi if converged; otherwise loop back. */
    if (SUNRabs(IDA_mem->ida_thi - IDA_mem->ida_tlo) <= IDA_mem->ida_ttol)
      break;

  } /* End of root-search loop */

  /* Reset trout and grout, set iroots, and return RTFOUND. */
  IDA_mem->ida_trout = IDA_mem->ida_thi;
  for (i = 0; i < IDA_mem->ida_nrtfn; i++) {
    IDA_mem->ida_grout[i] = IDA_mem->ida_ghi[i];
    IDA_mem->ida_iroots[i] = 0;
    if(!IDA_mem->ida_gactive[i]) continue;
    if ( (SUNRabs(IDA_mem->ida_ghi[i]) == ZERO) &&
         (IDA_mem->ida_rootdir[i]*IDA_mem->ida_glo[i] <= ZERO) )
      IDA_mem->ida_iroots[i] = IDA_mem->ida_glo[i] > 0 ? -1:1;
    if ( (IDA_mem->ida_glo[i]*IDA_mem->ida_ghi[i] < ZERO) &&
         (IDA_mem->ida_rootdir[i]*IDA_mem->ida_glo[i] <= ZERO) ) 
      IDA_mem->ida_iroots[i] = IDA_mem->ida_glo[i] > 0 ? -1:1;
  }
  return(RTFOUND);
}

/* 
 * =================================================================
 * Internal DQ approximations for sensitivity RHS
 * =================================================================
 */

#undef user_dataS

/*
 * IDASensResDQ
 *
 * IDASensRhsDQ computes the residuals of the sensitivity equations
 * by finite differences. It is of type IDASensResFn.
 * Returns 0 if successful, <0 if an unrecoverable failure occurred,
 * >0 for a recoverable error.
 */

int IDASensResDQ(int Ns, realtype t, 
                 N_Vector yy, N_Vector yp, N_Vector resval,
                 N_Vector *yyS, N_Vector *ypS, N_Vector *resvalS,
                 void *user_dataS,
                 N_Vector ytemp, N_Vector yptemp, N_Vector restemp)
{
  int retval, is;

  for (is=0; is<Ns; is++) {
    retval = IDASensRes1DQ(Ns, t, 
                           yy, yp, resval, 
                           is, yyS[is], ypS[is], resvalS[is], 
                           user_dataS,
                           ytemp, yptemp, restemp);
    if (retval != 0) return(retval);
  }
  return(0);
}

/*
 * IDASensRes1DQ
 *
 * IDASensRes1DQ computes the residual of the is-th sensitivity 
 * equation by finite differences.
 *
 * Returns 0 if successful or the return value of res if res fails
 * (<0 if res fails unrecoverably, >0 if res has a recoverable error).
 */

static int IDASensRes1DQ(int Ns, realtype t, 
                         N_Vector yy, N_Vector yp, N_Vector resval,
                         int is,
                         N_Vector yyS, N_Vector ypS, N_Vector resvalS,
                         void *user_dataS,
                         N_Vector ytemp, N_Vector yptemp, N_Vector restemp)
{
  IDAMem IDA_mem;
  int method;
  int which;
  int retval;
  realtype psave, pbari;
  realtype del , rdel;
  realtype Delp, rDelp, r2Delp;
  realtype Dely, rDely, r2Dely;
  realtype Del , rDel , r2Del ;
  realtype norms, ratio;

  /* user_dataS points to IDA_mem */
  IDA_mem = (IDAMem) user_dataS;

  /* Set base perturbation del */
  del  = SUNRsqrt(SUNMAX(IDA_mem->ida_rtol, IDA_mem->ida_uround));
  rdel = ONE/del;

  pbari = IDA_mem->ida_pbar[is];

  which = IDA_mem->ida_plist[is];

  psave = IDA_mem->ida_p[which];

  Delp  = pbari * del;
  rDelp = ONE/Delp;
  norms = N_VWrmsNorm(yyS, IDA_mem->ida_ewt) * pbari;
  rDely = SUNMAX(norms, rdel) / pbari;
  Dely  = ONE/rDely;

  if (IDA_mem->ida_DQrhomax == ZERO) {
    /* No switching */
    method = (IDA_mem->ida_DQtype==IDA_CENTERED) ? CENTERED1 : FORWARD1;
  } else {
    /* switch between simultaneous/separate DQ */
    ratio = Dely * rDelp;
    if ( SUNMAX(ONE/ratio, ratio) <= IDA_mem->ida_DQrhomax )
      method = (IDA_mem->ida_DQtype==IDA_CENTERED) ? CENTERED1 : FORWARD1;
    else
      method = (IDA_mem->ida_DQtype==IDA_CENTERED) ? CENTERED2 : FORWARD2;
  }
  
  switch (method) {

  case CENTERED1:

    Del = SUNMIN(Dely, Delp);
    r2Del = HALF/Del;

    /* Forward perturb y, y' and parameter */
    N_VLinearSum(Del, yyS, ONE, yy, ytemp);
    N_VLinearSum(Del, ypS, ONE, yp, yptemp);
    IDA_mem->ida_p[which] = psave + Del;

    /* Save residual in resvalS */
    retval = IDA_mem->ida_res(t, ytemp, yptemp, resvalS, IDA_mem->ida_user_data);
    IDA_mem->ida_nreS++;
    if (retval != 0) return(retval);
    
    /* Backward perturb y, y' and parameter */
    N_VLinearSum(-Del, yyS, ONE, yy, ytemp);
    N_VLinearSum(-Del, ypS, ONE, yp, yptemp);
    IDA_mem->ida_p[which] = psave - Del;

    /* Save residual in restemp */
    retval = IDA_mem->ida_res(t, ytemp, yptemp, restemp, IDA_mem->ida_user_data);
    IDA_mem->ida_nreS++;
    if (retval != 0) return(retval);

    /* Estimate the residual for the i-th sensitivity equation */
    N_VLinearSum(r2Del, resvalS, -r2Del, restemp, resvalS);
    
    break;

  case CENTERED2:

    r2Delp = HALF/Delp;
    r2Dely = HALF/Dely;

    /* Forward perturb y and y' */
    N_VLinearSum(Dely, yyS, ONE, yy, ytemp);
    N_VLinearSum(Dely, ypS, ONE, yp, yptemp);
    
    /* Save residual in resvalS */
    retval = IDA_mem->ida_res(t, ytemp, yptemp, resvalS, IDA_mem->ida_user_data);
    IDA_mem->ida_nreS++;
    if (retval != 0) return(retval);
    
    /* Backward perturb y and y' */
    N_VLinearSum(-Dely, yyS, ONE, yy, ytemp);
    N_VLinearSum(-Dely, ypS, ONE, yp, yptemp);

    /* Save residual in restemp */
    retval = IDA_mem->ida_res(t, ytemp, yptemp, restemp, IDA_mem->ida_user_data);
    IDA_mem->ida_nreS++;
    if (retval != 0) return(retval);

    /* Save the first difference quotient in resvalS */
    N_VLinearSum(r2Dely, resvalS, -r2Dely, restemp, resvalS);

    /* Forward perturb parameter */
    IDA_mem->ida_p[which] = psave + Delp;

    /* Save residual in ytemp */
    retval = IDA_mem->ida_res(t, yy, yp, ytemp, IDA_mem->ida_user_data);
    IDA_mem->ida_nreS++;
    if (retval != 0) return(retval);

    /* Backward perturb parameter */
    IDA_mem->ida_p[which] = psave - Delp;

    /* Save residual in yptemp */
    retval = IDA_mem->ida_res(t, yy, yp, yptemp, IDA_mem->ida_user_data);
    IDA_mem->ida_nreS++;
    if (retval != 0) return(retval);
    
    /* Save the second difference quotient in restemp */
    N_VLinearSum(r2Delp, ytemp, -r2Delp, yptemp, restemp);
    
    /* Add the difference quotients for the sensitivity residual */
    N_VLinearSum(ONE, resvalS, ONE, restemp, resvalS);
    
    break;

  case FORWARD1:

    Del = SUNMIN(Dely, Delp);
    rDel = ONE/Del;

    /* Forward perturb y, y' and parameter */
    N_VLinearSum(Del, yyS, ONE, yy, ytemp);
    N_VLinearSum(Del, ypS, ONE, yp, yptemp);
    IDA_mem->ida_p[which] = psave + Del;

    /* Save residual in resvalS */
    retval = IDA_mem->ida_res(t, ytemp, yptemp, resvalS, IDA_mem->ida_user_data);
    IDA_mem->ida_nreS++;
    if (retval != 0) return(retval);

    /* Estimate the residual for the i-th sensitivity equation */
    N_VLinearSum(rDel, resvalS, -rDel, resval, resvalS);

    break;

  case FORWARD2:

    /* Forward perturb y and y' */
    N_VLinearSum(Dely, yyS, ONE, yy, ytemp);
    N_VLinearSum(Dely, ypS, ONE, yp, yptemp);

    /* Save residual in resvalS */
    retval = IDA_mem->ida_res(t, ytemp, yptemp, resvalS, IDA_mem->ida_user_data);
    IDA_mem->ida_nreS++;
    if (retval != 0) return(retval);

    /* Save the first difference quotient in resvalS */
    N_VLinearSum(rDely, resvalS, -rDely, resval, resvalS);

    /* Forward perturb parameter */
    IDA_mem->ida_p[which] = psave + Delp;

    /* Save residual in restemp */
    retval = IDA_mem->ida_res(t, yy, yp, restemp, IDA_mem->ida_user_data);
    IDA_mem->ida_nreS++;
    if (retval != 0) return(retval);

    /* Save the second difference quotient in restemp */
    N_VLinearSum(rDelp, restemp, -rDelp, resval, restemp);

    /* Add the difference quotients for the sensitivity residual */
    N_VLinearSum(ONE, resvalS, ONE, restemp, resvalS);

    break;

  }

  /* Restore original value of parameter */
  IDA_mem->ida_p[which] = psave;
  
  return(0);

}


/* IDAQuadSensRhsInternalDQ   - internal IDAQuadSensRhsFn
 *
 * IDAQuadSensRhsInternalDQ computes right hand side of all quadrature
 * sensitivity equations by finite differences. All work is actually
 * done in IDAQuadSensRhs1InternalDQ.
 */

static int IDAQuadSensRhsInternalDQ(int Ns, realtype t, 
                                    N_Vector yy,   N_Vector yp,
                                    N_Vector *yyS, N_Vector *ypS,
                                    N_Vector rrQ,  N_Vector *resvalQS,
                                    void *ida_mem,  
                                    N_Vector yytmp, N_Vector yptmp, N_Vector tmpQS)
{
  IDAMem IDA_mem;
  int is, retval;
  
  /* cvode_mem is passed here as user data */
  IDA_mem = (IDAMem) ida_mem;

  for (is=0; is<Ns; is++) {
    retval = IDAQuadSensRhs1InternalDQ(IDA_mem, is, t,
                                      yy, yp, yyS[is], ypS[is], 
                                      rrQ, resvalQS[is],
                                      yytmp, yptmp, tmpQS);
    if (retval!=0) return(retval);
  }

  return(0);
}

static int IDAQuadSensRhs1InternalDQ(IDAMem IDA_mem, int is, realtype t, 
                                    N_Vector yy, N_Vector yp, 
                                    N_Vector yyS, N_Vector ypS,
                                    N_Vector resvalQ, N_Vector resvalQS, 
                                    N_Vector yytmp, N_Vector yptmp, N_Vector tmpQS)
{
  int retval, method;
  int nfel = 0, which;
  realtype psave, pbari;
  realtype del , rdel;
  realtype Delp;
  realtype Dely, rDely;
  realtype Del , r2Del ;
  realtype norms;

  del = SUNRsqrt(SUNMAX(IDA_mem->ida_rtol, IDA_mem->ida_uround));
  rdel = ONE/del;
  
  pbari = IDA_mem->ida_pbar[is];

  which = IDA_mem->ida_plist[is];

  psave = IDA_mem->ida_p[which];
  
  Delp  = pbari * del;
  norms   = N_VWrmsNorm(yyS, IDA_mem->ida_ewt) * pbari;
  rDely = SUNMAX(norms, rdel) / pbari;
  Dely  = ONE/rDely;
  
  method = (IDA_mem->ida_DQtype==IDA_CENTERED) ? CENTERED1 : FORWARD1;

  switch(method) {

  case CENTERED1:
    
    Del = SUNMIN(Dely, Delp);
    r2Del = HALF/Del;
    
    N_VLinearSum(ONE, yy, Del, yyS, yytmp);
    N_VLinearSum(ONE, yp, Del, ypS, yptmp);
    IDA_mem->ida_p[which] = psave + Del;

    retval = IDA_mem->ida_rhsQ(t, yytmp, yptmp, resvalQS, IDA_mem->ida_user_data);
    nfel++;
    if (retval != 0) return(retval);
    
    N_VLinearSum(-Del, yyS, ONE, yy, yytmp);
    N_VLinearSum(-Del, ypS, ONE, yp, yptmp);

    IDA_mem->ida_p[which] = psave - Del;

    retval = IDA_mem->ida_rhsQ(t, yytmp, yptmp, tmpQS, IDA_mem->ida_user_data);
    nfel++;
    if (retval != 0) return(retval);

    N_VLinearSum(r2Del, resvalQS, -r2Del, tmpQS, resvalQS);
    
    break;

  case FORWARD1:
    
    Del = SUNMIN(Dely, Delp);
    rdel = ONE/Del;
    
    N_VLinearSum(ONE, yy, Del, yyS, yytmp);
    N_VLinearSum(ONE, yp, Del, ypS, yptmp);
    IDA_mem->ida_p[which] = psave + Del;

    retval = IDA_mem->ida_rhsQ(t, yytmp, yptmp, resvalQS, IDA_mem->ida_user_data);
    nfel++;
    if (retval != 0) return(retval);
    
    N_VLinearSum(rdel, resvalQS, -rdel, resvalQ, resvalQS);
    
    break;
  }

  IDA_mem->ida_p[which] = psave;
  /* Increment counter nrQeS */
  IDA_mem->ida_nrQeS += nfel;
  
  return(0);
}


/* 
 * =================================================================
 * IDA Error message handling functions 
 * =================================================================
 */

/*
 * IDAProcessError is a high level error handling function.
 * - If ida_mem==NULL it prints the error message to stderr.
 * - Otherwise, it sets up and calls the error handling function 
 *   pointed to by ida_ehfun.
 */

void IDAProcessError(IDAMem IDA_mem, 
                    int error_code, const char *module, const char *fname, 
                    const char *msgfmt, ...)
{
  va_list ap;
  char msg[256];

  /* Initialize the argument pointer variable 
     (msgfmt is the last required argument to IDAProcessError) */

  va_start(ap, msgfmt);

  /* Compose the message */

  vsprintf(msg, msgfmt, ap);

  if (IDA_mem == NULL) {    /* We write to stderr */
#ifndef NO_FPRINTF_OUTPUT
    fprintf(stderr, "\n[%s ERROR]  %s\n  ", module, fname);
    fprintf(stderr, "%s\n\n", msg);
#endif

  } else {                 /* We can call ehfun */
    IDA_mem->ida_ehfun(error_code, module, fname, msg, IDA_mem->ida_eh_data);
  }

  /* Finalize argument processing */
  va_end(ap);

  return;
}

/* IDAErrHandler is the default error handling function.
   It sends the error message to the stream pointed to by ida_errfp */

void IDAErrHandler(int error_code, const char *module,
                   const char *function, char *msg, void *data)
{
  IDAMem IDA_mem;
  char err_type[10];

  /* data points to IDA_mem here */

  IDA_mem = (IDAMem) data;

  if (error_code == IDA_WARNING)
    sprintf(err_type,"WARNING");
  else
    sprintf(err_type,"ERROR");

#ifndef NO_FPRINTF_OUTPUT
  if (IDA_mem->ida_errfp != NULL) {
    fprintf(IDA_mem->ida_errfp,"\n[%s %s]  %s\n",module,err_type,function);
    fprintf(IDA_mem->ida_errfp,"  %s\n\n",msg);
  }
#endif

  return;
}
