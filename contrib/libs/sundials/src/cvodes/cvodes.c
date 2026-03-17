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
 * This is the implementation file for the main CVODES integrator
 * with sensitivity analysis capabilities.
 * -----------------------------------------------------------------
 * 
 * EXPORTED FUNCTIONS
 * ------------------
 *
 *   Creation, allocation and re-initialization functions
 *
 *      CVodeCreate
 *
 *      CVodeInit 
 *      CVodeReInit
 *      CVodeSStolerances
 *      CVodeSVtolerances
 *      CVodeWFtolerances
 *
 *      CVodeQuadInit
 *      CVodeQuadReInit   
 *      CVodeQuadSStolerances
 *      CVodeQuadSVtolerances
 *
 *      CVodeSensInit
 *      CVodeSensInit1
 *      CVodeSensReInit             
 *      CVodeSensSStolerances
 *      CVodeSensSVtolerances
 *      CVodeSensEEtolerances
 *
 *      CVodeQuadSensInit
 *      CVodeQuadSensReInit
 *
 *      CVodeSensToggleOff
 *
 *      CVodeRootInit      
 *
 *   Main solver function
 *      CVode
 *
 *   Interpolated output and extraction functions
 *      CVodeGetDky
 *      CVodeGetQuad             
 *      CVodeGetQuadDky
 *      CVodeGetSens             
 *      CVodeGetSens1
 *      CVodeGetSensDky         
 *      CVodeGetSensDky1
 *      CVodeGetQuadSens             
 *      CVodeGetQuadSens1
 *      CVodeGetQuadSensDky         
 *      CVodeGetQuadSensDky1
 *
 *   Deallocation functions
 *      CVodeFree              
 *      CVodeQuadFree
 *      CVodeSensFree  
 *      CVodeQuadSensFree  
 *
 * PRIVATE FUNCTIONS
 * -----------------
 *
 *      cvCheckNvector
 *
 *   Memory allocation/deallocation
 *      cvAllocVectors         
 *      cvFreeVectors
 *      cvQuadAllocVectors      
 *      cvQuadFreeVectors
 *      cvSensAllocVectors     
 *      cvSensFreeVectors
 *      cvQuadSensAllocVectors     
 *      cvQuadSensFreeVectors
 *
 *   Initial stepsize calculation
 *      cvHin                 
 *      cvUpperBoundH0
 *      cvYddNorm     
 *
 *   Initial setup
 *      cvInitialSetup
 *      cvEwtSet           
 *      cvEwtSetSS
 *      cvEwtSetSV         
 *      cvQuadEwtSet
 *      cvQuadEwtSetSS     
 *      cvQuadEwtSetSV
 *      cvSensEwtSet      
 *      cvSensEwtSetEE
 *      cvSensEwtSetSS    
 *      cvSensEwtSetSV
 *      cvQuadSensEwtSet      
 *      cvQuadSensEwtSetEE
 *      cvQuadSensEwtSetSS    
 *      cvQuadSensEwtSetSV
 *
 *   Main cvStep function
 *      cvStep
 *
 *   Functions called at beginning of step
 *      cvAdjustParams
 *      cvAdjustOrder    
 *      cvAdjustAdams
 *      cvAdjustBDF       
 *      cvIncreaseBDF
 *      cvDecreaseBDF     
 *      cvRescale
 *      cvPredict         
 *      cvSet
 *      cvSetAdams        
 *      cvAdamsStart
 *      cvAdamsFinish      
 *      cvAltSum
 *      cvSetBDF           
 *      cvSetTqBDF
 *
 *   Nonlinear solver functions
 *      cvNls              
 *      cvQuadNls           
 *      cvStgrNls
 *      cvStgr1Nls
 *      cvQuadSensNls 
 *      cvHandleNFlag
 *      cvRestore             
 *
 *   Error Test
 *      cvDoErrorTest
 *
 *   Functions called after a successful step
 *      cvCompleteStep      
 *      cvPrepareNextStep
 *      cvSetEta          
 *      cvComputeEtaqm1
 *      cvComputeEtaqp1   
 *      cvChooseEta
 *
 *   Function to handle failures
 *      cvHandleFailure   
 *
 *   Functions for BDF Stability Limit Detection  
 *      cvBDFStab
 *      cvSLdet   
 *
 *   Functions for rootfinding
 *      cvRcheck1
 *      cvRcheck2         
 *      cvRcheck3
 *      cvRootfind  
 *
 *   Functions for combined norms
 *      cvQuadUpdateNorm
 *      cvSensNorm
 *      cvSensUpdateNorm    
 *      cvQuadSensNorm
 *      cvQuadSensUpdateNorm    
 *
 *   Wrappers for sensitivity RHS
 *      cvSensRhsWrapper           
 *      cvSensRhs1Wrapper
 *
 *   Internal DQ approximations for sensitivity RHS
 *      cvSensRhsInternalDQ         
 *      cvSensRhs1InternalDQ
 *      cvQuadSensRhsDQ         
 *
 *   Error message handling functions
 *      cvProcessError      
 *      cvErrHandler
 *
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

#include "cvodes_impl.h"
#include <sundials/sundials_math.h>
#include <sundials/sundials_types.h>
#include "sunnonlinsol/sunnonlinsol_newton.h"

/* 
 * =================================================================
 * CVODES PRIVATE CONSTANTS
 * =================================================================
 */

#define ZERO    RCONST(0.0)
#define TINY    RCONST(1.0e-10)
#define PT1     RCONST(0.1)
#define POINT2  RCONST(0.2)
#define FOURTH  RCONST(0.25)
#define HALF    RCONST(0.5)
#define PT9     RCONST(0.9)
#define ONE     RCONST(1.0)
#define ONEPT5  RCONST(1.5)
#define TWO     RCONST(2.0)
#define THREE   RCONST(3.0)
#define FOUR    RCONST(4.0)
#define FIVE    RCONST(5.0)
#define TWELVE  RCONST(12.0)
#define HUNDRED RCONST(100.0)

/* 
 * =================================================================
 * CVODES ROUTINE-SPECIFIC CONSTANTS
 * =================================================================
 */

/* 
 * Control constants for lower-level functions used by cvStep 
 * ----------------------------------------------------------
 *
 * cvHin return values:
 *    CV_SUCCESS,
 *    CV_RHSFUNC_FAIL,  CV_RPTD_RHSFUNC_ERR,
 *    CV_QRHSFUNC_FAIL, CV_RPTD_QRHSFUNC_ERR,
 *    CV_SRHSFUNC_FAIL, CV_RPTD_SRHSFUNC_ERR,
 *    CV_TOO_CLOSE
 *
 * cvStep control constants:
 *    DO_ERROR_TEST
 *    PREDICT_AGAIN
 *
 * cvStep return values: 
 *    CV_SUCCESS,
 *    CV_CONV_FAILURE,      CV_ERR_FAILURE,
 *    CV_LSETUP_FAIL,       CV_LSOLVE_FAIL, 
 *    CV_RTFUNC_FAIL,
 *    CV_RHSFUNC_FAIL,      CV_QRHSFUNC_FAIL,      CV_SRHSFUNC_FAIL,      CV_QSRHSFUNC_FAIL, 
 *    CV_FIRST_RHSFUNC_ERR, CV_FIRST_QRHSFUNC_ERR, CV_FIRST_SRHSFUNC_ERR, CV_FIRST_QSRHSFUNC_ERR,
 *    CV_UNREC_RHSFUNC_ERR, CV_UNREC_QRHSFUNC_ERR, CV_UNREC_SRHSFUNC_ERR, CV_UNREC_QSRHSFUNC_ERR,
 *    CV_REPTD_RHSFUNC_ERR, CV_REPTD_QRHSFUNC_ERR, CV_REPTD_SRHSFUNC_ERR, CV_REPTD_QSRHSFUNC_ERR,
 *
 * cvNls input nflag values:
 *    FIRST_CALL
 *    PREV_CONV_FAIL
 *    PREV_ERR_FAIL
 *
 * cvNls return values:
 *    CV_SUCCESS,
 *    CV_LSETUP_FAIL,     CV_LSOLVE_FAIL,
 *    CV_RHSFUNC_FAIL,    CV_SRHSFUNC_FAIL,
 *    SUN_NLS_CONV_RECVR,
 *    RHSFUNC_RECVR,      SRHSFUNC_RECVR
 *
 */

#define DO_ERROR_TEST    +2
#define PREDICT_AGAIN    +3

#define CONV_FAIL        +4 
#define TRY_AGAIN        +5
#define FIRST_CALL       +6
#define PREV_CONV_FAIL   +7
#define PREV_ERR_FAIL    +8

#define CONSTR_RECVR     +10

#define QRHSFUNC_RECVR   +11
#define QSRHSFUNC_RECVR  +13

/*
 * Control constants for lower-level rootfinding functions
 * -------------------------------------------------------
 *
 * cvRcheck1 return values:
 *    CV_SUCCESS,
 *    CV_RTFUNC_FAIL,
 * cvRcheck2 return values:
 *    CV_SUCCESS,
 *    CV_RTFUNC_FAIL,
 *    CLOSERT,
 *    RTFOUND
 * cvRcheck3 return values:
 *    CV_SUCCESS,
 *    CV_RTFUNC_FAIL,
 *    RTFOUND
 * cvRootfind return values:
 *    CV_SUCCESS,
 *    CV_RTFUNC_FAIL,
 *    RTFOUND
 */

#define RTFOUND          +1
#define CLOSERT          +3

/*
 * Control constants for sensitivity DQ
 * ------------------------------------
 */

#define CENTERED1        +1
#define CENTERED2        +2
#define FORWARD1         +3
#define FORWARD2         +4

/*
 * Control constants for type of sensitivity RHS
 * ---------------------------------------------
 */

#define CV_ONESENS  1
#define CV_ALLSENS  2

/*
 * Control constants for tolerances
 * --------------------------------
 */

#define CV_NN  0
#define CV_SS  1
#define CV_SV  2
#define CV_WF  3
#define CV_EE  4

/*
 * Algorithmic constants
 * ---------------------
 *
 * CVodeGetDky and cvStep
 *
 *    FUZZ_FACTOR fuzz factor used to estimate infinitesimal time intervals
 *
 * cvHin
 *
 *    HLB_FACTOR  factor for upper bound on initial step size
 *    HUB_FACTOR  factor for lower bound on initial step size
 *    H_BIAS      bias factor in selection of initial step size
 *    MAX_ITERS   maximum attempts to compute the initial step size
 *
 * CVodeCreate 
 *
 *   CORTES       constant in nonlinear iteration convergence test
 *
 * cvStep
 *
 *    THRESH      if eta < THRESH reject a change in step size or order
 *    ETAMX1      -+
 *    ETAMX2       |
 *    ETAMX3       |-> bounds for eta (step size change)
 *    ETAMXF       |
 *    ETAMIN       |
 *    ETACF       -+
 *    ADDON       safety factor in computing eta
 *    BIAS1       -+
 *    BIAS2        |-> bias factors in eta selection
 *    BIAS3       -+
 *    ONEPSM      (1+epsilon) used in testing if the step size is below its bound
 *
 *    SMALL_NST   nst > SMALL_NST => use ETAMX3 
 *    MXNCF       max no. of convergence failures during one step try
 *    MXNEF       max no. of error test failures during one step try
 *    MXNEF1      max no. of error test failures before forcing a reduction of order
 *    SMALL_NEF   if an error failure occurs and SMALL_NEF <= nef <= MXNEF1, then
 *                reset eta =  SUNMIN(eta, ETAMXF)
 *    LONG_WAIT   number of steps to wait before considering an order change when
 *                q==1 and MXNEF1 error test failures have occurred
 *
 * cvNls
 *
 *    DGMAX       |gamma/gammap-1| > DGMAX => call lsetup
 *    MSBP        max no. of steps between lsetup calls
 *
 */


#define FUZZ_FACTOR RCONST(100.0)

#define HLB_FACTOR RCONST(100.0)
#define HUB_FACTOR RCONST(0.1)
#define H_BIAS     HALF
#define MAX_ITERS  4

#define CORTES RCONST(0.1)

#define THRESH RCONST(1.5)
#define ETAMX1 RCONST(10000.0) 
#define ETAMX2 RCONST(10.0)
#define ETAMX3 RCONST(10.0)
#define ETAMXF RCONST(0.2)
#define ETAMIN RCONST(0.1)
#define ETACF  RCONST(0.25)
#define ADDON  RCONST(0.000001)
#define BIAS1  RCONST(6.0)
#define BIAS2  RCONST(6.0)
#define BIAS3  RCONST(10.0)
#define ONEPSM RCONST(1.000001)

#define SMALL_NST    10
#define MXNCF        10
#define MXNEF         7
#define MXNEF1        3
#define SMALL_NEF     2
#define LONG_WAIT    10

#define DGMAX  RCONST(0.3)
#define MSBP   20

/* 
 * =================================================================
 * PRIVATE FUNCTION PROTOTYPES
 * =================================================================
 */

static booleantype cvCheckNvector(N_Vector tmpl);

/* Memory allocation/deallocation */

static booleantype cvAllocVectors(CVodeMem cv_mem, N_Vector tmpl);
static void cvFreeVectors(CVodeMem cv_mem);

static booleantype cvQuadAllocVectors(CVodeMem cv_mem, N_Vector tmpl);
static void cvQuadFreeVectors(CVodeMem cv_mem);

static booleantype cvSensAllocVectors(CVodeMem cv_mem, N_Vector tmpl);
static void cvSensFreeVectors(CVodeMem cv_mem);

static booleantype cvQuadSensAllocVectors(CVodeMem cv_mem, N_Vector tmpl);
static void cvQuadSensFreeVectors(CVodeMem cv_mem);

/* Initial stepsize calculation */

static int cvHin(CVodeMem cv_mem, realtype tout);
static realtype cvUpperBoundH0(CVodeMem cv_mem, realtype tdist);
static int cvYddNorm(CVodeMem cv_mem, realtype hg, realtype *yddnrm);

/* Initial setup */

static int cvInitialSetup(CVodeMem cv_mem);

static int cvEwtSetSS(CVodeMem cv_mem, N_Vector ycur, N_Vector weight);
static int cvEwtSetSV(CVodeMem cv_mem, N_Vector ycur, N_Vector weight);

static int cvQuadEwtSet(CVodeMem cv_mem, N_Vector qcur, N_Vector weightQ);
static int cvQuadEwtSetSS(CVodeMem cv_mem, N_Vector qcur, N_Vector weightQ);
static int cvQuadEwtSetSV(CVodeMem cv_mem, N_Vector qcur, N_Vector weightQ);

static int cvSensEwtSet(CVodeMem cv_mem, N_Vector *yScur, N_Vector *weightS);
static int cvSensEwtSetEE(CVodeMem cv_mem, N_Vector *yScur, N_Vector *weightS);
static int cvSensEwtSetSS(CVodeMem cv_mem, N_Vector *yScur, N_Vector *weightS);
static int cvSensEwtSetSV(CVodeMem cv_mem, N_Vector *yScur, N_Vector *weightS);

static int cvQuadSensEwtSet(CVodeMem cv_mem, N_Vector *yQScur, N_Vector *weightQS);
static int cvQuadSensEwtSetEE(CVodeMem cv_mem, N_Vector *yQScur, N_Vector *weightQS);
static int cvQuadSensEwtSetSS(CVodeMem cv_mem, N_Vector *yQScur, N_Vector *weightQS);
static int cvQuadSensEwtSetSV(CVodeMem cv_mem, N_Vector *yQScur, N_Vector *weightQS);

/* Main cvStep function */

static int cvStep(CVodeMem cv_mem);

/* Function called at beginning of step */

static void cvAdjustParams(CVodeMem cv_mem);
static void cvAdjustOrder(CVodeMem cv_mem, int deltaq);
static void cvAdjustAdams(CVodeMem cv_mem, int deltaq);
static void cvAdjustBDF(CVodeMem cv_mem, int deltaq);
static void cvIncreaseBDF(CVodeMem cv_mem);
static void cvDecreaseBDF(CVodeMem cv_mem);
static void cvRescale(CVodeMem cv_mem);
static void cvPredict(CVodeMem cv_mem);
static void cvSet(CVodeMem cv_mem);
static void cvSetAdams(CVodeMem cv_mem);
static realtype cvAdamsStart(CVodeMem cv_mem, realtype m[]);
static void cvAdamsFinish(CVodeMem cv_mem, realtype m[], realtype M[], realtype hsum);
static realtype cvAltSum(int iend, realtype a[], int k);
static void cvSetBDF(CVodeMem cv_mem);
static void cvSetTqBDF(CVodeMem cv_mem, realtype hsum, realtype alpha0,
                       realtype alpha0_hat, realtype xi_inv, realtype xistar_inv);

/* Nonlinear solver functions */

static int cvNls(CVodeMem cv_mem, int nflag);
static int cvQuadNls(CVodeMem cv_mem);
static int cvStgrNls(CVodeMem cv_mem);
static int cvStgr1Nls(CVodeMem cv_mem, int is);
static int cvQuadSensNls(CVodeMem cv_mem);

static int cvCheckConstraints(CVodeMem cv_mem);

static int cvHandleNFlag(CVodeMem cv_mem, int *nflagPtr, realtype saved_t,
                         int *ncfPtr, long int *ncfnPtr);

static void cvRestore(CVodeMem cv_mem, realtype saved_t);

/* Error Test */

static int cvDoErrorTest(CVodeMem cv_mem, int *nflagPtr, realtype saved_t, 
                         realtype acor_nrm,
                         int *nefPtr, long int *netfPtr, realtype *dsmPtr);

/* Function called after a successful step */

static void cvCompleteStep(CVodeMem cv_mem);
static void cvPrepareNextStep(CVodeMem cv_mem, realtype dsm);
static void cvSetEta(CVodeMem cv_mem);
static realtype cvComputeEtaqm1(CVodeMem cv_mem);
static realtype cvComputeEtaqp1(CVodeMem cv_mem);
static void cvChooseEta(CVodeMem cv_mem);

/* Function to handle failures */

static int cvHandleFailure(CVodeMem cv_mem,int flag);

/* Functions for BDF Stability Limit Detection */

static void cvBDFStab(CVodeMem cv_mem);
static int cvSLdet(CVodeMem cv_mem);

/* Functions for rootfinding */

static int cvRcheck1(CVodeMem cv_mem);
static int cvRcheck2(CVodeMem cv_mem);
static int cvRcheck3(CVodeMem cv_mem);
static int cvRootfind(CVodeMem cv_mem);

/* Function for combined norms */

static realtype cvQuadUpdateNorm(CVodeMem cv_mem, realtype old_nrm,
                                 N_Vector xQ, N_Vector wQ);

static realtype cvQuadSensNorm(CVodeMem cv_mem, N_Vector *xQS, N_Vector *wQS);
static realtype cvQuadSensUpdateNorm(CVodeMem cv_mem, realtype old_nrm,
                                     N_Vector *xQS, N_Vector *wQS);

/* Internal sensitivity RHS DQ functions */

static int cvQuadSensRhsInternalDQ(int Ns, realtype t, 
                                   N_Vector y, N_Vector *yS,
                                   N_Vector yQdot, N_Vector *yQSdot,
                                   void *cvode_mem,  
                                   N_Vector tmp, N_Vector tmpQ);

static int cvQuadSensRhs1InternalDQ(CVodeMem cv_mem, int is, realtype t, 
                                    N_Vector y, N_Vector yS,
                                    N_Vector yQdot, N_Vector yQSdot, 
                                    N_Vector tmp, N_Vector tmpQ);

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
 * CVodeCreate
 *
 * CVodeCreate creates an internal memory block for a problem to 
 * be solved by CVODES.
 * If successful, CVodeCreate returns a pointer to the problem memory. 
 * This pointer should be passed to CVodeInit.  
 * If an initialization error occurs, CVodeCreate prints an error 
 * message to standard err and returns NULL. 
 */

void *CVodeCreate(int lmm)
{
  int maxord;
  CVodeMem cv_mem;

  /* Test inputs */

  if ((lmm != CV_ADAMS) && (lmm != CV_BDF)) {
    cvProcessError(NULL, 0, "CVODES", "CVodeCreate", MSGCV_BAD_LMM);
    return(NULL);
  }

  cv_mem = NULL;
  cv_mem = (CVodeMem) malloc(sizeof(struct CVodeMemRec));
  if (cv_mem == NULL) {
    cvProcessError(NULL, 0, "CVODES", "CVodeCreate", MSGCV_CVMEM_FAIL);
    return(NULL);
  }

  /* Zero out cv_mem */
  memset(cv_mem, 0, sizeof(struct CVodeMemRec));

  maxord = (lmm == CV_ADAMS) ? ADAMS_Q_MAX : BDF_Q_MAX;

  /* copy input parameter into cv_mem */

  cv_mem->cv_lmm  = lmm;

  /* Set uround */

  cv_mem->cv_uround = UNIT_ROUNDOFF;

  /* Set default values for integrator optional inputs */

  cv_mem->cv_f          = NULL;
  cv_mem->cv_user_data  = NULL;
  cv_mem->cv_itol       = CV_NN;
  cv_mem->cv_user_efun  = SUNFALSE;
  cv_mem->cv_efun       = NULL;
  cv_mem->cv_e_data     = NULL;
  cv_mem->cv_ehfun      = cvErrHandler;
  cv_mem->cv_eh_data    = cv_mem;
  cv_mem->cv_errfp      = stderr;
  cv_mem->cv_qmax       = maxord;
  cv_mem->cv_mxstep     = MXSTEP_DEFAULT;
  cv_mem->cv_mxhnil     = MXHNIL_DEFAULT;
  cv_mem->cv_sldeton    = SUNFALSE;
  cv_mem->cv_hin        = ZERO;
  cv_mem->cv_hmin       = HMIN_DEFAULT;
  cv_mem->cv_hmax_inv   = HMAX_INV_DEFAULT;
  cv_mem->cv_tstopset   = SUNFALSE;
  cv_mem->cv_maxnef     = MXNEF;
  cv_mem->cv_maxncf     = MXNCF;
  cv_mem->cv_nlscoef    = CORTES;
  cv_mem->convfail      = CV_NO_FAILURES;
  cv_mem->cv_constraints = NULL;
  cv_mem->cv_constraintsSet = SUNFALSE;

  /* Initialize root finding variables */

  cv_mem->cv_glo        = NULL;
  cv_mem->cv_ghi        = NULL;
  cv_mem->cv_grout      = NULL;
  cv_mem->cv_iroots     = NULL;
  cv_mem->cv_rootdir    = NULL;
  cv_mem->cv_gfun       = NULL;
  cv_mem->cv_nrtfn      = 0;  
  cv_mem->cv_gactive    = NULL;
  cv_mem->cv_mxgnull    = 1;

  /* Set default values for quad. optional inputs */

  cv_mem->cv_quadr      = SUNFALSE;
  cv_mem->cv_fQ         = NULL;
  cv_mem->cv_errconQ    = SUNFALSE;
  cv_mem->cv_itolQ      = CV_NN;

  /* Set default values for sensi. optional inputs */

  cv_mem->cv_sensi      = SUNFALSE;
  cv_mem->cv_fS_data    = NULL;
  cv_mem->cv_fS         = cvSensRhsInternalDQ;
  cv_mem->cv_fS1        = cvSensRhs1InternalDQ;
  cv_mem->cv_fSDQ       = SUNTRUE;
  cv_mem->cv_ifS        = CV_ONESENS;
  cv_mem->cv_DQtype     = CV_CENTERED;
  cv_mem->cv_DQrhomax   = ZERO;
  cv_mem->cv_p          = NULL;
  cv_mem->cv_pbar       = NULL;
  cv_mem->cv_plist      = NULL;
  cv_mem->cv_errconS    = SUNFALSE;
  cv_mem->cv_ncfS1      = NULL;
  cv_mem->cv_ncfnS1     = NULL;
  cv_mem->cv_nniS1      = NULL;
  cv_mem->cv_itolS      = CV_NN;

  /* Set default values for quad. sensi. optional inputs */

  cv_mem->cv_quadr_sensi = SUNFALSE;
  cv_mem->cv_fQS         = NULL;
  cv_mem->cv_fQS_data    = NULL;
  cv_mem->cv_fQSDQ       = SUNTRUE;
  cv_mem->cv_errconQS    = SUNFALSE;
  cv_mem->cv_itolQS      = CV_NN;

  /* Set default for ASA */

  cv_mem->cv_adj         = SUNFALSE;
  cv_mem->cv_adj_mem     = NULL;

  /* Set the saved values for qmax_alloc */

  cv_mem->cv_qmax_alloc  = maxord;
  cv_mem->cv_qmax_allocQ = maxord;
  cv_mem->cv_qmax_allocS = maxord;

  /* Initialize lrw and liw */

  cv_mem->cv_lrw = 65 + 2*L_MAX + NUM_TESTS;
  cv_mem->cv_liw = 52;

  /* No mallocs have been done yet */

  cv_mem->cv_VabstolMallocDone     = SUNFALSE;
  cv_mem->cv_MallocDone            = SUNFALSE;
  cv_mem->cv_constraintsMallocDone = SUNFALSE;

  cv_mem->cv_VabstolQMallocDone  = SUNFALSE;
  cv_mem->cv_QuadMallocDone      = SUNFALSE;

  cv_mem->cv_VabstolSMallocDone  = SUNFALSE;
  cv_mem->cv_SabstolSMallocDone  = SUNFALSE;
  cv_mem->cv_SensMallocDone      = SUNFALSE;

  cv_mem->cv_VabstolQSMallocDone = SUNFALSE;
  cv_mem->cv_SabstolQSMallocDone = SUNFALSE;
  cv_mem->cv_QuadSensMallocDone  = SUNFALSE;

  cv_mem->cv_adjMallocDone       = SUNFALSE;

  /* Initialize nonlinear solver variables */
  cv_mem->NLS    = NULL;
  cv_mem->ownNLS = SUNFALSE;

  cv_mem->NLSsim        = NULL;
  cv_mem->ownNLSsim     = SUNFALSE;
  cv_mem->ycor0Sim      = NULL;
  cv_mem->ycorSim       = NULL;
  cv_mem->ewtSim        = NULL;
  cv_mem->simMallocDone = SUNFALSE;

  cv_mem->NLSstg        = NULL;
  cv_mem->ownNLSstg     = SUNFALSE;
  cv_mem->ycor0Stg      = NULL;
  cv_mem->ycorStg       = NULL;
  cv_mem->ewtStg        = NULL;
  cv_mem->stgMallocDone = SUNFALSE;
  
  cv_mem->NLSstg1       = NULL;
  cv_mem->ownNLSstg1    = SUNFALSE;

  cv_mem->sens_solve     = SUNFALSE;
  cv_mem->sens_solve_idx = -1;

  /* Return pointer to CVODES memory block */

  return((void *)cv_mem);
}

/*-----------------------------------------------------------------*/

/*
 * CVodeInit
 * 
 * CVodeInit allocates and initializes memory for a problem. All 
 * problem inputs are checked for errors. If any error occurs during 
 * initialization, it is reported to the file whose file pointer is 
 * errfp and an error flag is returned. Otherwise, it returns CV_SUCCESS
 */

int CVodeInit(void *cvode_mem, CVRhsFn f, realtype t0, N_Vector y0)
{
  CVodeMem cv_mem;
  booleantype nvectorOK, allocOK;
  sunindextype lrw1, liw1;
  int i,k, retval;
  SUNNonlinearSolver NLS;

  /* Check cvode_mem */

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeInit",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Check for legal input parameters */

  if (y0==NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeInit",
                   MSGCV_NULL_Y0);
    return(CV_ILL_INPUT);
  }

  if (f == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeInit",
                   MSGCV_NULL_F);
    return(CV_ILL_INPUT);
  }

  /* Test if all required vector operations are implemented */

  nvectorOK = cvCheckNvector(y0);
  if(!nvectorOK) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeInit",
                   MSGCV_BAD_NVECTOR);
    return(CV_ILL_INPUT);
  }

  /* Set space requirements for one N_Vector */

  if (y0->ops->nvspace != NULL) {
    N_VSpace(y0, &lrw1, &liw1);
  } else {
    lrw1 = 0;
    liw1 = 0;
  }
  cv_mem->cv_lrw1 = lrw1;
  cv_mem->cv_liw1 = liw1;

  /* Allocate the vectors (using y0 as a template) */

  allocOK = cvAllocVectors(cv_mem, y0);
  if (!allocOK) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeInit",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  /* Allocate temporary work arrays for fused vector ops */
  cv_mem->cv_cvals = NULL;
  cv_mem->cv_cvals = (realtype *) malloc(L_MAX*sizeof(realtype));

  cv_mem->cv_Xvecs = NULL;
  cv_mem->cv_Xvecs = (N_Vector *) malloc(L_MAX*sizeof(N_Vector));

  cv_mem->cv_Zvecs = NULL;
  cv_mem->cv_Zvecs = (N_Vector *) malloc(L_MAX*sizeof(N_Vector));

  if ((cv_mem->cv_cvals == NULL) ||
      (cv_mem->cv_Xvecs == NULL) ||
      (cv_mem->cv_Zvecs == NULL)) {
    cvFreeVectors(cv_mem);
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeInit",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  /* create a Newton nonlinear solver object by default */
  NLS = SUNNonlinSol_Newton(y0);

  /* check that nonlinear solver is non-NULL */
  if (NLS == NULL) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeInit", MSGCV_MEM_FAIL);
    cvFreeVectors(cv_mem);
    return(CV_MEM_FAIL);
  }

  /* attach the nonlinear solver to the CVODE memory */
  retval = CVodeSetNonlinearSolver(cv_mem, NLS);

  /* check that the nonlinear solver was successfully attached */
  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, retval, "CVODES", "CVodeInit",
                   "Setting the nonlinear solver failed");
    cvFreeVectors(cv_mem);
    SUNNonlinSolFree(NLS);
    return(CV_MEM_FAIL);
  }

  /* set ownership flag */
  cv_mem->ownNLS = SUNTRUE;

  /* All error checking is complete at this point */

  /* Copy the input parameters into CVODES state */

  cv_mem->cv_f  = f;
  cv_mem->cv_tn = t0;

  /* Set step parameters */

  cv_mem->cv_q      = 1;
  cv_mem->cv_L      = 2;
  cv_mem->cv_qwait  = cv_mem->cv_L;
  cv_mem->cv_etamax = ETAMX1;

  cv_mem->cv_qu     = 0;
  cv_mem->cv_hu     = ZERO;
  cv_mem->cv_tolsf  = ONE;

  /* Set the linear solver addresses to NULL.
     (We check != NULL later, in CVode) */

  cv_mem->cv_linit  = NULL;
  cv_mem->cv_lsetup = NULL;
  cv_mem->cv_lsolve = NULL;
  cv_mem->cv_lfree  = NULL;
  cv_mem->cv_lmem   = NULL;

  /* Set forceSetup to SUNFALSE */

  cv_mem->cv_forceSetup = SUNFALSE;

  /* Initialize zn[0] in the history array */

  N_VScale(ONE, y0, cv_mem->cv_zn[0]);

  /* Initialize all the counters */

  cv_mem->cv_nst     = 0;
  cv_mem->cv_nfe     = 0;
  cv_mem->cv_ncfn    = 0;
  cv_mem->cv_netf    = 0;
  cv_mem->cv_nni     = 0;
  cv_mem->cv_nsetups = 0;
  cv_mem->cv_nhnil   = 0;
  cv_mem->cv_nstlp   = 0;
  cv_mem->cv_nscon   = 0;
  cv_mem->cv_nge     = 0;

  cv_mem->cv_irfnd   = 0;

  /* Initialize other integrator optional outputs */

  cv_mem->cv_h0u      = ZERO;
  cv_mem->cv_next_h   = ZERO;
  cv_mem->cv_next_q   = 0;

  /* Initialize Stablilty Limit Detection data */
  /* NOTE: We do this even if stab lim det was not
     turned on yet. This way, the user can turn it
     on at any time */

  cv_mem->cv_nor = 0;
  for (i = 1; i <= 5; i++)
    for (k = 1; k <= 3; k++) 
      cv_mem->cv_ssdat[i-1][k-1] = ZERO;

  /* Problem has been successfully initialized */

  cv_mem->cv_MallocDone = SUNTRUE;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * CVodeReInit
 *
 * CVodeReInit re-initializes CVODES's memory for a problem, assuming
 * it has already been allocated in a prior CVodeInit call.
 * All problem specification inputs are checked for errors.
 * If any error occurs during initialization, it is reported to the
 * file whose file pointer is errfp.
 * The return value is CV_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */

int CVodeReInit(void *cvode_mem, realtype t0, N_Vector y0)
{
  CVodeMem cv_mem;
  int i,k;
 
  /* Check cvode_mem */

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeReInit",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Check if cvode_mem was allocated */

  if (cv_mem->cv_MallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_MALLOC, "CVODES", "CVodeReInit",
                   MSGCV_NO_MALLOC);
    return(CV_NO_MALLOC);
  }

  /* Check for legal input parameters */

  if (y0 == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeReInit",
                   MSGCV_NULL_Y0);
    return(CV_ILL_INPUT);
  }
  
  /* Copy the input parameters into CVODES state */

  cv_mem->cv_tn = t0;

  /* Set step parameters */

  cv_mem->cv_q      = 1;
  cv_mem->cv_L      = 2;
  cv_mem->cv_qwait  = cv_mem->cv_L;
  cv_mem->cv_etamax = ETAMX1;

  cv_mem->cv_qu     = 0;
  cv_mem->cv_hu     = ZERO;
  cv_mem->cv_tolsf  = ONE;

  /* Set forceSetup to SUNFALSE */

  cv_mem->cv_forceSetup = SUNFALSE;

  /* Initialize zn[0] in the history array */

  N_VScale(ONE, y0, cv_mem->cv_zn[0]);
 
  /* Initialize all the counters */

  cv_mem->cv_nst     = 0;
  cv_mem->cv_nfe     = 0;
  cv_mem->cv_ncfn    = 0;
  cv_mem->cv_netf    = 0;
  cv_mem->cv_nni     = 0;
  cv_mem->cv_nsetups = 0;
  cv_mem->cv_nhnil   = 0;
  cv_mem->cv_nstlp   = 0;
  cv_mem->cv_nscon   = 0;
  cv_mem->cv_nge     = 0;

  cv_mem->cv_irfnd   = 0;

  /* Initialize other integrator optional outputs */

  cv_mem->cv_h0u      = ZERO;
  cv_mem->cv_next_h   = ZERO;
  cv_mem->cv_next_q   = 0;

  /* Initialize Stablilty Limit Detection data */

  cv_mem->cv_nor = 0;
  for (i = 1; i <= 5; i++)
    for (k = 1; k <= 3; k++) 
      cv_mem->cv_ssdat[i-1][k-1] = ZERO;
  
  /* Problem has been successfully re-initialized */

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * CVodeSStolerances
 * CVodeSVtolerances
 * CVodeWFtolerances
 *
 * These functions specify the integration tolerances. One of them
 * MUST be called before the first call to CVode.
 *
 * CVodeSStolerances specifies scalar relative and absolute tolerances.
 * CVodeSVtolerances specifies scalar relative tolerance and a vector
 *   absolute tolerance (a potentially different absolute tolerance 
 *   for each vector component).
 * CVodeWFtolerances specifies a user-provides function (of type CVEwtFn)
 *   which will be called to set the error weight vector.
 */

int CVodeSStolerances(void *cvode_mem, realtype reltol, realtype abstol)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", 
                   "CVodeSStolerances", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_MallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_MALLOC, "CVODES",
                   "CVodeSStolerances", MSGCV_NO_MALLOC);
    return(CV_NO_MALLOC);
  }

  /* Check inputs */

  if (reltol < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSStolerances", MSGCV_BAD_RELTOL);
    return(CV_ILL_INPUT);
  }

  if (abstol < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSStolerances", MSGCV_BAD_ABSTOL);
    return(CV_ILL_INPUT);
  }

  /* Copy tolerances into memory */
  
  cv_mem->cv_reltol = reltol;
  cv_mem->cv_Sabstol = abstol;

  cv_mem->cv_itol = CV_SS;

  cv_mem->cv_user_efun = SUNFALSE;
  cv_mem->cv_efun = cvEwtSet;
  cv_mem->cv_e_data = NULL; /* will be set to cvode_mem in InitialSetup */

  return(CV_SUCCESS);
}


int CVodeSVtolerances(void *cvode_mem, realtype reltol, N_Vector abstol)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeSVtolerances", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_MallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_MALLOC, "CVODES",
                   "CVodeSVtolerances", MSGCV_NO_MALLOC);
    return(CV_NO_MALLOC);
  }

  /* Check inputs */

  if (reltol < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSVtolerances", MSGCV_BAD_RELTOL);
    return(CV_ILL_INPUT);
  }

  if (N_VMin(abstol) < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSVtolerances", MSGCV_BAD_ABSTOL);
    return(CV_ILL_INPUT);
  }

  /* Copy tolerances into memory */
  
  if ( !(cv_mem->cv_VabstolMallocDone) ) {
    cv_mem->cv_Vabstol = N_VClone(cv_mem->cv_ewt);
    cv_mem->cv_lrw += cv_mem->cv_lrw1;
    cv_mem->cv_liw += cv_mem->cv_liw1;
    cv_mem->cv_VabstolMallocDone = SUNTRUE;
  }

  cv_mem->cv_reltol = reltol;
  N_VScale(ONE, abstol, cv_mem->cv_Vabstol);

  cv_mem->cv_itol = CV_SV;

  cv_mem->cv_user_efun = SUNFALSE;
  cv_mem->cv_efun = cvEwtSet;
  cv_mem->cv_e_data = NULL; /* will be set to cvode_mem in InitialSetup */

  return(CV_SUCCESS);
}


int CVodeWFtolerances(void *cvode_mem, CVEwtFn efun)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeWFtolerances", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_MallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_MALLOC, "CVODES",
                   "CVodeWFtolerances", MSGCV_NO_MALLOC);
    return(CV_NO_MALLOC);
  }

  cv_mem->cv_itol = CV_WF;

  cv_mem->cv_user_efun = SUNTRUE;
  cv_mem->cv_efun = efun;
  cv_mem->cv_e_data = NULL; /* will be set to user_data in InitialSetup */

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * CVodeQuadInit
 *
 * CVodeQuadInit allocates and initializes quadrature related 
 * memory for a problem. All problem specification inputs are 
 * checked for errors. If any error occurs during initialization, 
 * it is reported to the file whose file pointer is errfp. 
 * The return value is CV_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */

int CVodeQuadInit(void *cvode_mem, CVQuadRhsFn fQ, N_Vector yQ0)
{
  CVodeMem cv_mem;
  booleantype allocOK;
  sunindextype lrw1Q, liw1Q;

  /* Check cvode_mem */
  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeQuadInit",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Set space requirements for one N_Vector */
  N_VSpace(yQ0, &lrw1Q, &liw1Q);
  cv_mem->cv_lrw1Q = lrw1Q;
  cv_mem->cv_liw1Q = liw1Q;

  /* Allocate the vectors (using yQ0 as a template) */
  allocOK = cvQuadAllocVectors(cv_mem, yQ0);
  if (!allocOK) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                   "CVodeQuadInit", MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  /* Initialize znQ[0] in the history array */
  N_VScale(ONE, yQ0, cv_mem->cv_znQ[0]);

  /* Copy the input parameters into CVODES state */
  cv_mem->cv_fQ = fQ;

  /* Initialize counters */
  cv_mem->cv_nfQe  = 0;
  cv_mem->cv_netfQ = 0;

  /* Quadrature integration turned ON */
  cv_mem->cv_quadr = SUNTRUE;
  cv_mem->cv_QuadMallocDone = SUNTRUE;

  /* Quadrature initialization was successfull */
  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * CVodeQuadReInit
 *
 * CVodeQuadReInit re-initializes CVODES's quadrature related memory 
 * for a problem, assuming it has already been allocated in prior 
 * calls to CVodeInit and CVodeQuadInit. 
 * All problem specification inputs are checked for errors.
 * If any error occurs during initialization, it is reported to the
 * file whose file pointer is errfp.
 * The return value is CV_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */

int CVodeQuadReInit(void *cvode_mem, N_Vector yQ0)
{
  CVodeMem cv_mem;

  /* Check cvode_mem */
  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeQuadReInit", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Ckeck if quadrature was initialized? */
  if (cv_mem->cv_QuadMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUAD, "CVODES",
                   "CVodeQuadReInit", MSGCV_NO_QUAD);
    return(CV_NO_QUAD);
  }

  /* Initialize znQ[0] in the history array */
  N_VScale(ONE, yQ0, cv_mem->cv_znQ[0]);

  /* Initialize counters */
  cv_mem->cv_nfQe  = 0;
  cv_mem->cv_netfQ = 0;

  /* Quadrature integration turned ON */
  cv_mem->cv_quadr = SUNTRUE;

  /* Quadrature re-initialization was successfull */
  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * CVodeQuadSStolerances
 * CVodeQuadSVtolerances
 *
 * These functions specify the integration tolerances for sensitivity
 * variables. One of them MUST be called before the first call to 
 * CVode IF error control on the quadrature variables is enabled
 * (see CVodeSetQuadErrCon).
 *
 * CVodeQuadSStolerances specifies scalar relative and absolute tolerances.
 * CVodeQuadSVtolerances specifies scalar relative tolerance and a vector
 *   absolute toleranc (a potentially different absolute tolerance for each
 *   vector component).
 */

int CVodeQuadSStolerances(void *cvode_mem, realtype reltolQ, realtype abstolQ)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeQuadSStolerances", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Ckeck if quadrature was initialized? */

  if (cv_mem->cv_QuadMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUAD, "CVODES",
                   "CVodeQuadSStolerances", MSGCV_NO_QUAD); 
    return(CV_NO_QUAD);
  }

  /* Test user-supplied tolerances */

  if (reltolQ < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeQuadSStolerances", MSGCV_BAD_RELTOLQ);
    return(CV_ILL_INPUT);
  }

  if (abstolQ < 0) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeQuadSStolerances", MSGCV_BAD_ABSTOLQ);
    return(CV_ILL_INPUT);
  }

  /* Copy tolerances into memory */

  cv_mem->cv_itolQ = CV_SS;

  cv_mem->cv_reltolQ  = reltolQ;
  cv_mem->cv_SabstolQ = abstolQ;

  return(CV_SUCCESS);
}

int CVodeQuadSVtolerances(void *cvode_mem, realtype reltolQ, N_Vector abstolQ)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeQuadSVtolerances", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Ckeck if quadrature was initialized? */

  if (cv_mem->cv_QuadMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUAD, "CVODES",
                   "CVodeQuadSVtolerances", MSGCV_NO_QUAD); 
    return(CV_NO_QUAD);
  }

  /* Test user-supplied tolerances */

  if (reltolQ < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeQuadSVtolerances", MSGCV_BAD_RELTOLQ);
    return(CV_ILL_INPUT);
  }

  if (abstolQ == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeQuadSVtolerances", MSGCV_NULL_ABSTOLQ);
    return(CV_ILL_INPUT);
  }

  if (N_VMin(abstolQ) < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeQuadSVtolerances", MSGCV_BAD_ABSTOLQ);
    return(CV_ILL_INPUT);
  }

  /* Copy tolerances into memory */

  cv_mem->cv_itolQ = CV_SV;

  cv_mem->cv_reltolQ  = reltolQ;

  if ( !(cv_mem->cv_VabstolQMallocDone) ) {
    cv_mem->cv_VabstolQ = N_VClone(cv_mem->cv_tempvQ);
    cv_mem->cv_lrw += cv_mem->cv_lrw1Q;
    cv_mem->cv_liw += cv_mem->cv_liw1Q;
    cv_mem->cv_VabstolQMallocDone = SUNTRUE;
  }
  
  N_VScale(ONE, abstolQ, cv_mem->cv_VabstolQ);

  return(CV_SUCCESS);
}


/*-----------------------------------------------------------------*/

/*
 * CVodeSensInit
 *
 * CVodeSensInit allocates and initializes sensitivity related 
 * memory for a problem (using a sensitivity RHS function of type
 * CVSensRhsFn). All problem specification inputs are checked for 
 * errors.
 * The return value is CV_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */

int CVodeSensInit(void *cvode_mem, int Ns, int ism, CVSensRhsFn fS, N_Vector *yS0)
{
  CVodeMem cv_mem;
  booleantype allocOK;
  int is, retval;
  SUNNonlinearSolver NLS;

  /* Check cvode_mem */

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSensInit",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Check if CVodeSensInit or CVodeSensInit1 was already called */

  if (cv_mem->cv_SensMallocDone) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensInit",
                   MSGCV_SENSINIT_2);
    return(CV_ILL_INPUT);
  }

  /* Check if Ns is legal */

  if (Ns<=0) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensInit",
                   MSGCV_BAD_NS);
    return(CV_ILL_INPUT);
  }
  cv_mem->cv_Ns = Ns;

  /* Check if ism is compatible */

  if (ism==CV_STAGGERED1) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensInit",
                   MSGCV_BAD_ISM_IFS);
    return(CV_ILL_INPUT);
  }

  /* Check if ism is legal */

  if ((ism!=CV_SIMULTANEOUS) && (ism!=CV_STAGGERED)) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensInit",
                   MSGCV_BAD_ISM);
    return(CV_ILL_INPUT);
  }
  cv_mem->cv_ism = ism;

  /* Check if yS0 is non-null */

  if (yS0 == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensInit",
                   MSGCV_NULL_YS0);
    return(CV_ILL_INPUT);
  }

  /* Store sensitivity RHS-related data */

  cv_mem->cv_ifS = CV_ALLSENS;
  cv_mem->cv_fS1 = NULL;

  if (fS == NULL) {

    cv_mem->cv_fSDQ = SUNTRUE;
    cv_mem->cv_fS   = cvSensRhsInternalDQ;
    cv_mem->cv_fS_data = cvode_mem;

  } else {

    cv_mem->cv_fSDQ = SUNFALSE;
    cv_mem->cv_fS   = fS;
    cv_mem->cv_fS_data = cv_mem->cv_user_data;

  }

  /* No memory allocation for STAGGERED1 */

  cv_mem->cv_stgr1alloc = SUNFALSE;

  /* Allocate the vectors (using yS0[0] as a template) */

  allocOK = cvSensAllocVectors(cv_mem, yS0[0]);
  if (!allocOK) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeSensInit",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }
  
  /* Check if larger temporary work arrays are needed for fused vector ops */
  if (Ns*L_MAX > L_MAX) {
    free(cv_mem->cv_cvals); cv_mem->cv_cvals = NULL;
    free(cv_mem->cv_Xvecs); cv_mem->cv_Xvecs = NULL;
    free(cv_mem->cv_Zvecs); cv_mem->cv_Zvecs = NULL;

    cv_mem->cv_cvals = (realtype *) malloc((Ns*L_MAX)*sizeof(realtype));
    cv_mem->cv_Xvecs = (N_Vector *) malloc((Ns*L_MAX)*sizeof(N_Vector));
    cv_mem->cv_Zvecs = (N_Vector *) malloc((Ns*L_MAX)*sizeof(N_Vector));

    if ((cv_mem->cv_cvals == NULL) ||
        (cv_mem->cv_Xvecs == NULL) ||
        (cv_mem->cv_Zvecs == NULL)) {
      cvSensFreeVectors(cv_mem);
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeSensInit",
                     MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }
  }

  /*---------------------------------------------- 
    All error checking is complete at this point 
    -----------------------------------------------*/

  /* Initialize znS[0] in the history array */

  for (is=0; is<Ns; is++)
    cv_mem->cv_cvals[is] = ONE;
    
  retval = N_VScaleVectorArray(Ns, cv_mem->cv_cvals, yS0, cv_mem->cv_znS[0]);
  if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

  /* Initialize all sensitivity related counters */

  cv_mem->cv_nfSe     = 0;
  cv_mem->cv_nfeS     = 0;
  cv_mem->cv_ncfnS    = 0;
  cv_mem->cv_netfS    = 0;
  cv_mem->cv_nniS     = 0;
  cv_mem->cv_nsetupsS = 0;

  /* Set default values for plist and pbar */

  for (is=0; is<Ns; is++) {
    cv_mem->cv_plist[is] = is;
    cv_mem->cv_pbar[is] = ONE;
  }

  /* Sensitivities will be computed */

  cv_mem->cv_sensi = SUNTRUE;
  cv_mem->cv_SensMallocDone = SUNTRUE;

  /* create a Newton nonlinear solver object by default */
  if (ism == CV_SIMULTANEOUS)
    NLS = SUNNonlinSol_NewtonSens(Ns+1, cv_mem->cv_acor);
  else
    NLS = SUNNonlinSol_NewtonSens(Ns, cv_mem->cv_acor);

  /* check that the nonlinear solver is non-NULL */
  if (NLS == NULL) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                   "CVodeSensInit", MSGCV_MEM_FAIL);
    cvSensFreeVectors(cv_mem);
    return(CV_MEM_FAIL);
  }

  /* attach the nonlinear solver to the CVODE memory */
  if (ism == CV_SIMULTANEOUS)
    retval = CVodeSetNonlinearSolverSensSim(cv_mem, NLS);
  else
    retval = CVodeSetNonlinearSolverSensStg(cv_mem, NLS);

  /* check that the nonlinear solver was successfully attached */
  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, retval, "CVODES", "CVodeSensInit",
                   "Setting the nonlinear solver failed");
    cvSensFreeVectors(cv_mem);
    SUNNonlinSolFree(NLS);
    return(CV_MEM_FAIL);
  }

  /* set ownership flag */
  if (ism == CV_SIMULTANEOUS)
    cv_mem->ownNLSsim = SUNTRUE;
  else
    cv_mem->ownNLSstg = SUNTRUE;

  /* Sensitivity initialization was successfull */
  return(CV_SUCCESS);
}

/*
 * CVodeSensInit1
 *
 * CVodeSensInit1 allocates and initializes sensitivity related 
 * memory for a problem (using a sensitivity RHS function of type
 * CVSensRhs1Fn). All problem specification inputs are checked for 
 * errors.
 * The return value is CV_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */

int CVodeSensInit1(void *cvode_mem, int Ns, int ism, CVSensRhs1Fn fS1, N_Vector *yS0)
{
  CVodeMem cv_mem;
  booleantype allocOK;
  int is, retval;
  SUNNonlinearSolver NLS;
  
  /* Check cvode_mem */

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSensInit1",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Check if CVodeSensInit or CVodeSensInit1 was already called */

  if (cv_mem->cv_SensMallocDone) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensInit1",
                   MSGCV_SENSINIT_2);
    return(CV_ILL_INPUT);
  }

  /* Check if Ns is legal */

  if (Ns<=0) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensInit1",
                   MSGCV_BAD_NS);
    return(CV_ILL_INPUT);
  }
  cv_mem->cv_Ns = Ns;

  /* Check if ism is legal */

  if ((ism!=CV_SIMULTANEOUS) && (ism!=CV_STAGGERED) && (ism!=CV_STAGGERED1)) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensInit1",
                   MSGCV_BAD_ISM);
    return(CV_ILL_INPUT);
  }
  cv_mem->cv_ism = ism;

  /* Check if yS0 is non-null */

  if (yS0 == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensInit1",
                   MSGCV_NULL_YS0);
    return(CV_ILL_INPUT);
  }

  /* Store sensitivity RHS-related data */

  cv_mem->cv_ifS = CV_ONESENS;
  cv_mem->cv_fS  = NULL;

  if (fS1 == NULL) {

    cv_mem->cv_fSDQ = SUNTRUE;
    cv_mem->cv_fS1  = cvSensRhs1InternalDQ;
    cv_mem->cv_fS_data = cvode_mem;

  } else {

    cv_mem->cv_fSDQ = SUNFALSE;
    cv_mem->cv_fS1  = fS1;
    cv_mem->cv_fS_data = cv_mem->cv_user_data;

  }

  /* Allocate ncfS1, ncfnS1, and nniS1 if needed */

  if (ism == CV_STAGGERED1) {
    cv_mem->cv_stgr1alloc = SUNTRUE;
    cv_mem->cv_ncfS1 = NULL;
    cv_mem->cv_ncfS1 = (int*)malloc(Ns*sizeof(int));
    cv_mem->cv_ncfnS1 = NULL;
    cv_mem->cv_ncfnS1 = (long int*)malloc(Ns*sizeof(long int));
    cv_mem->cv_nniS1 = NULL;
    cv_mem->cv_nniS1 = (long int*)malloc(Ns*sizeof(long int));
    if ( (cv_mem->cv_ncfS1 == NULL) ||
         (cv_mem->cv_ncfnS1 == NULL) ||
         (cv_mem->cv_nniS1 == NULL) ) {
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeSensInit1",
                     MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }
  } else {
    cv_mem->cv_stgr1alloc = SUNFALSE;
  }

  /* Allocate the vectors (using yS0[0] as a template) */

  allocOK = cvSensAllocVectors(cv_mem, yS0[0]);
  if (!allocOK) {
    if (cv_mem->cv_stgr1alloc) {
      free(cv_mem->cv_ncfS1);  cv_mem->cv_ncfS1 = NULL;
      free(cv_mem->cv_ncfnS1); cv_mem->cv_ncfnS1 = NULL;
      free(cv_mem->cv_nniS1);  cv_mem->cv_nniS1 = NULL;
    }
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeSensInit1",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  /* Check if larger temporary work arrays are needed for fused vector ops */
  if (Ns*L_MAX > L_MAX) {
    free(cv_mem->cv_cvals); cv_mem->cv_cvals = NULL;
    free(cv_mem->cv_Xvecs); cv_mem->cv_Xvecs = NULL;
    free(cv_mem->cv_Zvecs); cv_mem->cv_Zvecs = NULL;

    cv_mem->cv_cvals = (realtype *) malloc((Ns*L_MAX)*sizeof(realtype));
    cv_mem->cv_Xvecs = (N_Vector *) malloc((Ns*L_MAX)*sizeof(N_Vector));
    cv_mem->cv_Zvecs = (N_Vector *) malloc((Ns*L_MAX)*sizeof(N_Vector));

    if ((cv_mem->cv_cvals == NULL) ||
        (cv_mem->cv_Xvecs == NULL) ||
        (cv_mem->cv_Zvecs == NULL)) {
      if (cv_mem->cv_stgr1alloc) {
        free(cv_mem->cv_ncfS1);  cv_mem->cv_ncfS1 = NULL;
        free(cv_mem->cv_ncfnS1); cv_mem->cv_ncfnS1 = NULL;
        free(cv_mem->cv_nniS1);  cv_mem->cv_nniS1 = NULL;
      }
      cvSensFreeVectors(cv_mem);
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeSensInit1",
                     MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }
  }
  
  /*---------------------------------------------- 
    All error checking is complete at this point 
    -----------------------------------------------*/

  /* Initialize znS[0] in the history array */

  for (is=0; is<Ns; is++)
    cv_mem->cv_cvals[is] = ONE;
    
  retval = N_VScaleVectorArray(Ns, cv_mem->cv_cvals, yS0, cv_mem->cv_znS[0]);
  if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

  /* Initialize all sensitivity related counters */

  cv_mem->cv_nfSe     = 0;
  cv_mem->cv_nfeS     = 0;
  cv_mem->cv_ncfnS    = 0;
  cv_mem->cv_netfS    = 0;
  cv_mem->cv_nniS     = 0;
  cv_mem->cv_nsetupsS = 0;
  if (ism==CV_STAGGERED1)
    for (is=0; is<Ns; is++) {
      cv_mem->cv_ncfnS1[is] = 0;
      cv_mem->cv_nniS1[is] = 0;
    }

  /* Set default values for plist and pbar */

  for (is=0; is<Ns; is++) {
    cv_mem->cv_plist[is] = is;
    cv_mem->cv_pbar[is] = ONE;
  }

  /* Sensitivities will be computed */

  cv_mem->cv_sensi = SUNTRUE;
  cv_mem->cv_SensMallocDone = SUNTRUE;

  /* create a Newton nonlinear solver object by default */
  if (ism == CV_SIMULTANEOUS)
    NLS = SUNNonlinSol_NewtonSens(Ns+1, cv_mem->cv_acor);
  else if (ism == CV_STAGGERED)
    NLS = SUNNonlinSol_NewtonSens(Ns, cv_mem->cv_acor);
  else
    NLS = SUNNonlinSol_Newton(cv_mem->cv_acor);

  /* check that the nonlinear solver is non-NULL */
  if (NLS == NULL) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                   "CVodeSensInit1", MSGCV_MEM_FAIL);
    cvSensFreeVectors(cv_mem);
    return(CV_MEM_FAIL);
  }

  /* attach the nonlinear solver to the CVODE memory */
  if (ism == CV_SIMULTANEOUS)
    retval = CVodeSetNonlinearSolverSensSim(cv_mem, NLS);
  else if (ism == CV_STAGGERED)
    retval = CVodeSetNonlinearSolverSensStg(cv_mem, NLS);
  else
    retval = CVodeSetNonlinearSolverSensStg1(cv_mem, NLS);

  /* check that the nonlinear solver was successfully attached */
  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, retval, "CVODES", "CVodeSensInit1",
                   "Setting the nonlinear solver failed");
    cvSensFreeVectors(cv_mem);
    SUNNonlinSolFree(NLS);
    return(CV_MEM_FAIL);
  }

  /* set ownership flag */
  if (ism == CV_SIMULTANEOUS)
    cv_mem->ownNLSsim = SUNTRUE;
  else if (ism == CV_STAGGERED)
    cv_mem->ownNLSstg = SUNTRUE;
  else
    cv_mem->ownNLSstg1 = SUNTRUE;

  /* Sensitivity initialization was successfull */
  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * CVodeSensReInit
 *
 * CVodeSensReInit re-initializes CVODES's sensitivity related memory 
 * for a problem, assuming it has already been allocated in prior 
 * calls to CVodeInit and CVodeSensInit/CVodeSensInit1. 
 * All problem specification inputs are checked for errors.
 * The number of sensitivities Ns is assumed to be unchanged since
 * the previous call to CVodeSensInit.
 * If any error occurs during initialization, it is reported to the
 * file whose file pointer is errfp.
 * The return value is CV_SUCCESS = 0 if no errors occurred, or
 * a negative value otherwise.
 */ 

int CVodeSensReInit(void *cvode_mem, int ism, N_Vector *yS0)
{
  CVodeMem cv_mem;
  int is, retval;
  SUNNonlinearSolver NLS;

  /* Check cvode_mem */

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSensReInit",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was sensitivity initialized? */

  if (cv_mem->cv_SensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeSensReInit",
                   MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  } 

  /* Check if ism is compatible */

  if ((cv_mem->cv_ifS==CV_ALLSENS) && (ism==CV_STAGGERED1)) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSensReInit", MSGCV_BAD_ISM_IFS);
    return(CV_ILL_INPUT);
  }
  
  /* Check if ism is legal */

  if ((ism!=CV_SIMULTANEOUS) && (ism!=CV_STAGGERED) && (ism!=CV_STAGGERED1)) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSensReInit", MSGCV_BAD_ISM);
    return(CV_ILL_INPUT);
  }
  cv_mem->cv_ism = ism;

  /* Check if yS0 is non-null */

  if (yS0 == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSensReInit", MSGCV_NULL_YS0);
    return(CV_ILL_INPUT);
  }  

  /* Allocate ncfS1, ncfnS1, and nniS1 if needed */

  if ( (ism==CV_STAGGERED1) && (cv_mem->cv_stgr1alloc==SUNFALSE) ) {
    cv_mem->cv_stgr1alloc = SUNTRUE;
    cv_mem->cv_ncfS1 = NULL;
    cv_mem->cv_ncfS1 = (int*)malloc(cv_mem->cv_Ns*sizeof(int));
    cv_mem->cv_ncfnS1 = NULL;
    cv_mem->cv_ncfnS1 = (long int*)malloc(cv_mem->cv_Ns*sizeof(long int));
    cv_mem->cv_nniS1 = NULL;
    cv_mem->cv_nniS1 = (long int*)malloc(cv_mem->cv_Ns*sizeof(long int));
    if ( (cv_mem->cv_ncfS1==NULL) ||
         (cv_mem->cv_ncfnS1==NULL) ||
         (cv_mem->cv_nniS1==NULL) ) {
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                     "CVodeSensReInit", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }
  }

  /*---------------------------------------------- 
    All error checking is complete at this point 
    -----------------------------------------------*/

  /* Initialize znS[0] in the history array */

  for (is=0; is<cv_mem->cv_Ns; is++)
    cv_mem->cv_cvals[is] = ONE;
    
  retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                               yS0, cv_mem->cv_znS[0]);
  if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

  /* Initialize all sensitivity related counters */

  cv_mem->cv_nfSe     = 0;
  cv_mem->cv_nfeS     = 0;
  cv_mem->cv_ncfnS    = 0;
  cv_mem->cv_netfS    = 0;
  cv_mem->cv_nniS     = 0;
  cv_mem->cv_nsetupsS = 0;
  if (ism==CV_STAGGERED1)
    for (is=0; is<cv_mem->cv_Ns; is++) {
      cv_mem->cv_ncfnS1[is] = 0;
      cv_mem->cv_nniS1[is] = 0;
    }

  /* Problem has been successfully re-initialized */

  cv_mem->cv_sensi = SUNTRUE;

  /* Check if the NLS exists, create the default NLS if needed */
  if ( (ism == CV_SIMULTANEOUS && cv_mem->NLSsim  == NULL) ||
       (ism == CV_STAGGERED    && cv_mem->NLSstg  == NULL) ||
       (ism == CV_STAGGERED1   && cv_mem->NLSstg1 == NULL) ) {

    /* create a Newton nonlinear solver object by default */
    if (ism == CV_SIMULTANEOUS)
      NLS = SUNNonlinSol_NewtonSens(cv_mem->cv_Ns+1, cv_mem->cv_acor);
    else if (ism == CV_STAGGERED)
      NLS = SUNNonlinSol_NewtonSens(cv_mem->cv_Ns, cv_mem->cv_acor);
    else
      NLS = SUNNonlinSol_Newton(cv_mem->cv_acor);

    /* check that the nonlinear solver is non-NULL */
    if (NLS == NULL) {
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                     "CVodeSensReInit", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    /* attach the nonlinear solver to the CVODES memory */
    if (ism == CV_SIMULTANEOUS)
      retval = CVodeSetNonlinearSolverSensSim(cv_mem, NLS);
    else if (ism == CV_STAGGERED)
      retval = CVodeSetNonlinearSolverSensStg(cv_mem, NLS);
    else
      retval = CVodeSetNonlinearSolverSensStg1(cv_mem, NLS);

    /* check that the nonlinear solver was successfully attached */
    if (retval != CV_SUCCESS) {
      cvProcessError(cv_mem, retval, "CVODES", "CVodeSensReInit",
                     "Setting the nonlinear solver failed");
      SUNNonlinSolFree(NLS);
      return(CV_MEM_FAIL);
    }

    /* set ownership flag */
    if (ism == CV_SIMULTANEOUS)
      cv_mem->ownNLSsim = SUNTRUE;
    else if (ism == CV_STAGGERED)
      cv_mem->ownNLSstg = SUNTRUE;
    else
      cv_mem->ownNLSstg1 = SUNTRUE;

    /* initialize the NLS object, this assumes that the linear solver has
       already been initialized in CVodeInit */
    if (ism == CV_SIMULTANEOUS)
      retval = cvNlsInitSensSim(cv_mem);
    else if (ism == CV_STAGGERED)
      retval = cvNlsInitSensStg(cv_mem);
    else
      retval = cvNlsInitSensStg1(cv_mem);

    if (retval != CV_SUCCESS) {
      cvProcessError(cv_mem, CV_NLS_INIT_FAIL, "CVODES",
                     "CVodeSensReInit", MSGCV_NLS_INIT_FAIL);
      return(CV_NLS_INIT_FAIL);
    }
  }

  /* Sensitivity re-initialization was successfull */
  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * CVodeSensSStolerances
 * CVodeSensSVtolerances
 * CVodeSensEEtolerances
 *
 * These functions specify the integration tolerances for sensitivity
 * variables. One of them MUST be called before the first call to CVode.
 *
 * CVodeSensSStolerances specifies scalar relative and absolute tolerances.
 * CVodeSensSVtolerances specifies scalar relative tolerance and a vector
 *   absolute tolerance for each sensitivity vector (a potentially different
 *   absolute tolerance for each vector component).
 * CVodeEEtolerances specifies that tolerances for sensitivity variables
 *   should be estimated from those provided for the state variables.
 */

int CVodeSensSStolerances(void *cvode_mem, realtype reltolS, realtype *abstolS)
{
  CVodeMem cv_mem;
  int is;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSensSStolerances",
                   MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was sensitivity initialized? */

  if (cv_mem->cv_SensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeSensSStolerances",
                   MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  } 

  /* Test user-supplied tolerances */
    
  if (reltolS < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensSStolerances",
                   MSGCV_BAD_RELTOLS);
    return(CV_ILL_INPUT);
  }

  if (abstolS == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensSStolerances",
                   MSGCV_NULL_ABSTOLS);
    return(CV_ILL_INPUT);
  }

  for (is=0; is<cv_mem->cv_Ns; is++)
    if (abstolS[is] < ZERO) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSensSStolerances",
                     MSGCV_BAD_ABSTOLS);
      return(CV_ILL_INPUT);
    }

  /* Copy tolerances into memory */

  cv_mem->cv_itolS = CV_SS;

  cv_mem->cv_reltolS = reltolS;

  if ( !(cv_mem->cv_SabstolSMallocDone) ) {
    cv_mem->cv_SabstolS = NULL;
    cv_mem->cv_SabstolS = (realtype *)malloc(cv_mem->cv_Ns*sizeof(realtype));
    cv_mem->cv_lrw += cv_mem->cv_Ns;
    cv_mem->cv_SabstolSMallocDone = SUNTRUE;
  }

  for (is=0; is<cv_mem->cv_Ns; is++)
    cv_mem->cv_SabstolS[is] = abstolS[is];

  return(CV_SUCCESS);
}

int CVodeSensSVtolerances(void *cvode_mem,  realtype reltolS, N_Vector *abstolS)
{
  CVodeMem cv_mem;
  int is, retval;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSensSVtolerances",
                   MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was sensitivity initialized? */

  if (cv_mem->cv_SensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeSensSVtolerances",
                   MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  } 

  /* Test user-supplied tolerances */
    
  if (reltolS < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSensSVtolerances", MSGCV_BAD_RELTOLS);
    return(CV_ILL_INPUT);
  }

  if (abstolS == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSensSVtolerances", MSGCV_NULL_ABSTOLS);
    return(CV_ILL_INPUT);
  }

  for (is=0; is<cv_mem->cv_Ns; is++) 
    if (N_VMin(abstolS[is]) < ZERO) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                     "CVodeSensSVtolerances", MSGCV_BAD_ABSTOLS);
      return(CV_ILL_INPUT);
    }

  /* Copy tolerances into memory */

  cv_mem->cv_itolS = CV_SV;

  cv_mem->cv_reltolS = reltolS;

  if ( !(cv_mem->cv_VabstolSMallocDone) ) {
    cv_mem->cv_VabstolS = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempv);
    cv_mem->cv_lrw += cv_mem->cv_Ns*cv_mem->cv_lrw1;
    cv_mem->cv_liw += cv_mem->cv_Ns*cv_mem->cv_liw1;
    cv_mem->cv_VabstolSMallocDone = SUNTRUE;
  }
  
  for (is=0; is<cv_mem->cv_Ns; is++)
    cv_mem->cv_cvals[is] = ONE;

  retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                               abstolS, cv_mem->cv_VabstolS);
  if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

  return(CV_SUCCESS);
}


int CVodeSensEEtolerances(void *cvode_mem)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSensEEtolerances",
                   MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was sensitivity initialized? */

  if (cv_mem->cv_SensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeSensEEtolerances",
                   MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  } 

  cv_mem->cv_itolS = CV_EE;

  return(CV_SUCCESS);
}


/*-----------------------------------------------------------------*/

/*
 * CVodeQuadSensInit
 *
 */

int CVodeQuadSensInit(void *cvode_mem, CVQuadSensRhsFn fQS, N_Vector *yQS0)
{
  CVodeMem    cv_mem;
  booleantype allocOK;
  int is, retval;

  /* Check cvode_mem */
  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeQuadSensInit",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Check if sensitivity analysis is active */
  if (!cv_mem->cv_sensi) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeQuadSensInit",
                   MSGCV_NO_SENSI);
    return(CV_ILL_INPUT);
  }

  /* Check if yQS0 is non-null */
  if (yQS0 == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeQuadSensInit",
                   MSGCV_NULL_YQS0);
    return(CV_ILL_INPUT);
  }

  /* Allocate the vectors (using yQS0[0] as a template) */
  allocOK = cvQuadSensAllocVectors(cv_mem, yQS0[0]);
  if (!allocOK) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeQuadSensInit",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  /*---------------------------------------------- 
    All error checking is complete at this point 
    -----------------------------------------------*/

  /* Set fQS */
  if (fQS == NULL) {

    cv_mem->cv_fQSDQ = SUNTRUE;
    cv_mem->cv_fQS = cvQuadSensRhsInternalDQ;

    cv_mem->cv_fQS_data = cvode_mem;

  } else {

    cv_mem->cv_fQSDQ = SUNFALSE;
    cv_mem->cv_fQS = fQS;

    cv_mem->cv_fQS_data = cv_mem->cv_user_data;

  }

  /* Initialize znQS[0] in the history array */
  for (is=0; is<cv_mem->cv_Ns; is++)
    cv_mem->cv_cvals[is] = ONE;
    
  retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                               yQS0, cv_mem->cv_znQS[0]);
  if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

  /* Initialize all sensitivity related counters */
  cv_mem->cv_nfQSe  = 0;
  cv_mem->cv_nfQeS  = 0;
  cv_mem->cv_netfQS = 0;
  
  /* Quadrature sensitivities will be computed */
  cv_mem->cv_quadr_sensi = SUNTRUE;
  cv_mem->cv_QuadSensMallocDone = SUNTRUE;

  /* Sensitivity initialization was successfull */
  return(CV_SUCCESS);
}

/*
 * CVodeQuadSensReInit
 *
 */

int CVodeQuadSensReInit(void *cvode_mem, N_Vector *yQS0)
{
  CVodeMem cv_mem;
  int is, retval;

  /* Check cvode_mem */
  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeQuadSensReInit",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Check if sensitivity analysis is active */
  if (!cv_mem->cv_sensi) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeQuadSensReInit", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  /* Was quadrature sensitivity initialized? */
  if (cv_mem->cv_QuadSensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUADSENS, "CVODES",
                   "CVodeQuadSensReInit", MSGCV_NO_QUADSENSI);
    return(CV_NO_QUADSENS);
  } 

  /* Check if yQS0 is non-null */
  if (yQS0 == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeQuadSensReInit", MSGCV_NULL_YQS0);
    return(CV_ILL_INPUT);
  }

  /*---------------------------------------------- 
    All error checking is complete at this point 
    -----------------------------------------------*/

  /* Initialize znQS[0] in the history array */
  for (is=0; is<cv_mem->cv_Ns; is++) 
    cv_mem->cv_cvals[is] = ONE;
  
  retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                               yQS0, cv_mem->cv_znQS[0]);
  if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

  /* Initialize all sensitivity related counters */
  cv_mem->cv_nfQSe  = 0;
  cv_mem->cv_nfQeS  = 0;
  cv_mem->cv_netfQS = 0;

  /* Quadrature sensitivities will be computed */
  cv_mem->cv_quadr_sensi = SUNTRUE;

  /* Problem has been successfully re-initialized */
  return(CV_SUCCESS);
}


/*
 * CVodeQuadSensSStolerances
 * CVodeQuadSensSVtolerances
 * CVodeQuadSensEEtolerances
 *
 * These functions specify the integration tolerances for quadrature
 * sensitivity variables. One of them MUST be called before the first
 * call to CVode IF these variables are included in the error test.
 *
 * CVodeQuadSensSStolerances specifies scalar relative and absolute tolerances.
 * CVodeQuadSensSVtolerances specifies scalar relative tolerance and a vector
 *   absolute tolerance for each quadrature sensitivity vector (a potentially
 *   different absolute tolerance for each vector component).
 * CVodeQuadSensEEtolerances specifies that tolerances for sensitivity variables
 *   should be estimated from those provided for the quadrature variables.
 *   In this case, tolerances for the quadrature variables must be
 *   specified through a call to one of CVodeQuad**tolerances.
 */

int CVodeQuadSensSStolerances(void *cvode_mem, realtype reltolQS, realtype *abstolQS)
{
  CVodeMem cv_mem;
  int is;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeQuadSensSStolerances", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Check if sensitivity was initialized */

  if (cv_mem->cv_SensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES",
                   "CVodeQuadSensSStolerances", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  } 

  /* Ckeck if quadrature sensitivity was initialized? */

  if (cv_mem->cv_QuadSensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUADSENS, "CVODES",
                   "CVodeQuadSSensSStolerances", MSGCV_NO_QUADSENSI); 
    return(CV_NO_QUAD);
  }

  /* Test user-supplied tolerances */
    
  if (reltolQS < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeQuadSensSStolerances", MSGCV_BAD_RELTOLQS);
    return(CV_ILL_INPUT);
  }

  if (abstolQS == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeQuadSensSStolerances", MSGCV_NULL_ABSTOLQS);
    return(CV_ILL_INPUT);
  }

  for (is=0; is<cv_mem->cv_Ns; is++)
    if (abstolQS[is] < ZERO) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                     "CVodeQuadSensSStolerances", MSGCV_BAD_ABSTOLQS);
      return(CV_ILL_INPUT);
    }

  /* Copy tolerances into memory */

  cv_mem->cv_itolQS = CV_SS;

  cv_mem->cv_reltolQS = reltolQS;

  if ( !(cv_mem->cv_SabstolQSMallocDone) ) {
    cv_mem->cv_SabstolQS = NULL;
    cv_mem->cv_SabstolQS = (realtype *)malloc(cv_mem->cv_Ns*sizeof(realtype));
    cv_mem->cv_lrw += cv_mem->cv_Ns;
    cv_mem->cv_SabstolQSMallocDone = SUNTRUE;
  }
  
  for (is=0; is<cv_mem->cv_Ns; is++)
    cv_mem->cv_SabstolQS[is] = abstolQS[is];

  return(CV_SUCCESS);
}

int CVodeQuadSensSVtolerances(void *cvode_mem,  realtype reltolQS, N_Vector *abstolQS)
{
  CVodeMem cv_mem;
  int is, retval;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeQuadSensSVtolerances", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* check if sensitivity was initialized */

  if (cv_mem->cv_SensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES",
                   "CVodeQuadSensSVtolerances", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  } 

  /* Ckeck if quadrature sensitivity was initialized? */

  if (cv_mem->cv_QuadSensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUADSENS, "CVODES",
                   "CVodeQuadSensSVtolerances", MSGCV_NO_QUADSENSI); 
    return(CV_NO_QUAD);
  }

  /* Test user-supplied tolerances */
    
  if (reltolQS < ZERO) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeQuadSensSVtolerances", MSGCV_BAD_RELTOLQS);
    return(CV_ILL_INPUT);
  }

  if (abstolQS == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSensSVtolerances", MSGCV_NULL_ABSTOLQS);
    return(CV_ILL_INPUT);
  }

  for (is=0; is<cv_mem->cv_Ns; is++) 
    if (N_VMin(abstolQS[is]) < ZERO) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                     "CVodeQuadSensSVtolerances", MSGCV_BAD_ABSTOLQS);
      return(CV_ILL_INPUT);
    }

  /* Copy tolerances into memory */

  cv_mem->cv_itolQS = CV_SV;

  cv_mem->cv_reltolQS = reltolQS;

  if ( !(cv_mem->cv_VabstolQSMallocDone) ) {
    cv_mem->cv_VabstolQS = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempvQ);
    cv_mem->cv_lrw += cv_mem->cv_Ns*cv_mem->cv_lrw1Q;
    cv_mem->cv_liw += cv_mem->cv_Ns*cv_mem->cv_liw1Q;
    cv_mem->cv_VabstolQSMallocDone = SUNTRUE;
  }
  
  for (is=0; is<cv_mem->cv_Ns; is++)
    cv_mem->cv_cvals[is] = ONE;

  retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                               abstolQS, cv_mem->cv_VabstolQS);
  if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

  return(CV_SUCCESS);
}


int CVodeQuadSensEEtolerances(void *cvode_mem)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeQuadSensEEtolerances", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* check if sensitivity was initialized */

  if (cv_mem->cv_SensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES",
                   "CVodeQuadSensEEtolerances", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  } 

  /* Ckeck if quadrature sensitivity was initialized? */

  if (cv_mem->cv_QuadSensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUADSENS, "CVODES",
                   "CVodeQuadSensEEtolerances", MSGCV_NO_QUADSENSI); 
    return(CV_NO_QUAD);
  }

  cv_mem->cv_itolQS = CV_EE;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * CVodeSensToggleOff
 *
 * CVodeSensToggleOff deactivates sensitivity calculations.
 * It does NOT deallocate sensitivity-related memory.
 */

int CVodeSensToggleOff(void *cvode_mem)
{
  CVodeMem cv_mem;

  /* Check cvode_mem */
  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSensToggleOff",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Disable sensitivities */
  cv_mem->cv_sensi = SUNFALSE;
  cv_mem->cv_quadr_sensi = SUNFALSE;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

/*
 * CVodeRootInit
 *
 * CVodeRootInit initializes a rootfinding problem to be solved
 * during the integration of the ODE system.  It loads the root
 * function pointer and the number of root functions, and allocates
 * workspace memory.  The return value is CV_SUCCESS = 0 if no errors
 * occurred, or a negative value otherwise.
 */

int CVodeRootInit(void *cvode_mem, int nrtfn, CVRootFn g)
{
  CVodeMem cv_mem;
  int i, nrt;

  /* Check cvode_mem */
  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeRootInit",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  nrt = (nrtfn < 0) ? 0 : nrtfn;

  /* If rerunning CVodeRootInit() with a different number of root
     functions (changing number of gfun components), then free
     currently held memory resources */
  if ((nrt != cv_mem->cv_nrtfn) && (cv_mem->cv_nrtfn > 0)) {
    free(cv_mem->cv_glo); cv_mem->cv_glo = NULL;
    free(cv_mem->cv_ghi); cv_mem->cv_ghi = NULL;
    free(cv_mem->cv_grout); cv_mem->cv_grout = NULL;
    free(cv_mem->cv_iroots); cv_mem->cv_iroots = NULL;
    free(cv_mem->cv_rootdir); cv_mem->cv_rootdir = NULL;
    free(cv_mem->cv_gactive); cv_mem->cv_gactive = NULL;

    cv_mem->cv_lrw -= 3 * (cv_mem->cv_nrtfn);
    cv_mem->cv_liw -= 3 * (cv_mem->cv_nrtfn);

  }

  /* If CVodeRootInit() was called with nrtfn == 0, then set cv_nrtfn to
     zero and cv_gfun to NULL before returning */
  if (nrt == 0) {
    cv_mem->cv_nrtfn = nrt;
    cv_mem->cv_gfun = NULL;
    return(CV_SUCCESS);
  }

  /* If rerunning CVodeRootInit() with the same number of root functions
     (not changing number of gfun components), then check if the root
     function argument has changed */
  /* If g != NULL then return as currently reserved memory resources
     will suffice */
  if (nrt == cv_mem->cv_nrtfn) {
    if (g != cv_mem->cv_gfun) {
      if (g == NULL) {
	free(cv_mem->cv_glo); cv_mem->cv_glo = NULL;
	free(cv_mem->cv_ghi); cv_mem->cv_ghi = NULL;
	free(cv_mem->cv_grout); cv_mem->cv_grout = NULL;
	free(cv_mem->cv_iroots); cv_mem->cv_iroots = NULL;
        free(cv_mem->cv_rootdir); cv_mem->cv_rootdir = NULL;
        free(cv_mem->cv_gactive); cv_mem->cv_gactive = NULL;

        cv_mem->cv_lrw -= 3*nrt;
        cv_mem->cv_liw -= 3*nrt;

        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeRootInit",
                       MSGCV_NULL_G);
        return(CV_ILL_INPUT);
      }
      else {
	cv_mem->cv_gfun = g;
        return(CV_SUCCESS);
      }
    }
    else return(CV_SUCCESS);
  }

  /* Set variable values in CVode memory block */
  cv_mem->cv_nrtfn = nrt;
  if (g == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeRootInit",
                   MSGCV_NULL_G);
    return(CV_ILL_INPUT);
  }
  else cv_mem->cv_gfun = g;

  /* Allocate necessary memory and return */
  cv_mem->cv_glo = NULL;
  cv_mem->cv_glo = (realtype *) malloc(nrt*sizeof(realtype));
  if (cv_mem->cv_glo == NULL) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeRootInit",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }
    
  cv_mem->cv_ghi = NULL;
  cv_mem->cv_ghi = (realtype *) malloc(nrt*sizeof(realtype));
  if (cv_mem->cv_ghi == NULL) {
    free(cv_mem->cv_glo); cv_mem->cv_glo = NULL;
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeRootInit",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }
    
  cv_mem->cv_grout = NULL;
  cv_mem->cv_grout = (realtype *) malloc(nrt*sizeof(realtype));
  if (cv_mem->cv_grout == NULL) {
    free(cv_mem->cv_glo); cv_mem->cv_glo = NULL;
    free(cv_mem->cv_ghi); cv_mem->cv_ghi = NULL;
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeRootInit",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  cv_mem->cv_iroots = NULL;
  cv_mem->cv_iroots = (int *) malloc(nrt*sizeof(int));
  if (cv_mem->cv_iroots == NULL) {
    free(cv_mem->cv_glo); cv_mem->cv_glo = NULL;
    free(cv_mem->cv_ghi); cv_mem->cv_ghi = NULL;
    free(cv_mem->cv_grout); cv_mem->cv_grout = NULL;
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeRootInit",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  cv_mem->cv_rootdir = NULL;
  cv_mem->cv_rootdir = (int *) malloc(nrt*sizeof(int));
  if (cv_mem->cv_rootdir == NULL) {
    free(cv_mem->cv_glo); cv_mem->cv_glo = NULL; 
    free(cv_mem->cv_ghi); cv_mem->cv_ghi = NULL;
    free(cv_mem->cv_grout); cv_mem->cv_grout = NULL;
    free(cv_mem->cv_iroots); cv_mem->cv_iroots = NULL;
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeRootInit",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }


  cv_mem->cv_gactive = NULL;
  cv_mem->cv_gactive = (booleantype *) malloc(nrt*sizeof(booleantype));
  if (cv_mem->cv_gactive == NULL) {
    free(cv_mem->cv_glo); cv_mem->cv_glo = NULL; 
    free(cv_mem->cv_ghi); cv_mem->cv_ghi = NULL;
    free(cv_mem->cv_grout); cv_mem->cv_grout = NULL;
    free(cv_mem->cv_iroots); cv_mem->cv_iroots = NULL;
    free(cv_mem->cv_rootdir); cv_mem->cv_rootdir = NULL;
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES", "CVodeRootInit",
                   MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }


  /* Set default values for rootdir (both directions) */
  for(i=0; i<nrt; i++) cv_mem->cv_rootdir[i] = 0;

  /* Set default values for gactive (all active) */
  for(i=0; i<nrt; i++) cv_mem->cv_gactive[i] = SUNTRUE;

  cv_mem->cv_lrw += 3*nrt;
  cv_mem->cv_liw += 3*nrt;

  return(CV_SUCCESS);
}

/* 
 * -----------------------------------------------------------------
 * Main solver function
 * -----------------------------------------------------------------
 */

/*
 * CVode
 *
 * This routine is the main driver of the CVODES package. 
 *
 * It integrates over a time interval defined by the user, by calling
 * cvStep to do internal time steps.
 *
 * The first time that CVode is called for a successfully initialized
 * problem, it computes a tentative initial step size h.
 *
 * CVode supports two modes, specified by itask: CV_NORMAL, CV_ONE_STEP.
 * In the CV_NORMAL mode, the solver steps until it reaches or passes tout
 * and then interpolates to obtain y(tout).
 * In the CV_ONE_STEP mode, it takes one internal step and returns.
 */

int CVode(void *cvode_mem, realtype tout, N_Vector yout, 
          realtype *tret, int itask)
{
  CVodeMem cv_mem;
  long int nstloc; 
  int retval, hflag, kflag, istate, is, ir, ier, irfndp;
  realtype troundoff, tout_hin, rh, nrm;
  booleantype inactive_roots;

  /*
   * -------------------------------------
   * 1. Check and process inputs
   * -------------------------------------
   */

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVode",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Check if cvode_mem was allocated */
  if (cv_mem->cv_MallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_MALLOC, "CVODES", "CVode",
                   MSGCV_NO_MALLOC);
    return(CV_NO_MALLOC);
  }
  
  /* Check for yout != NULL */
  if ((cv_mem->cv_y = yout) == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                   MSGCV_YOUT_NULL);
    return(CV_ILL_INPUT);
  }
  
  /* Check for tret != NULL */
  if (tret == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                   MSGCV_TRET_NULL);
    return(CV_ILL_INPUT);
  }

  /* Check for valid itask */
  if ( (itask != CV_NORMAL) && (itask != CV_ONE_STEP) ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                   MSGCV_BAD_ITASK);
    return(CV_ILL_INPUT);
  }

  if (itask == CV_NORMAL) cv_mem->cv_toutc = tout;
  cv_mem->cv_taskc = itask;

  /*
   * ----------------------------------------
   * 2. Initializations performed only at
   *    the first step (nst=0):
   *    - initial setup
   *    - initialize Nordsieck history array
   *    - compute initial step size
   *    - check for approach to tstop
   *    - check for approach to a root
   * ----------------------------------------
   */

  if (cv_mem->cv_nst == 0) {

    cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;

    /* Check inputs for corectness */

    ier = cvInitialSetup(cv_mem);
    if (ier!= CV_SUCCESS) return(ier);

    /* 
     * Call f at (t0,y0), set zn[1] = y'(t0). 
     * If computing any quadratures, call fQ at (t0,y0), set znQ[1] = yQ'(t0)
     * If computing sensitivities, call fS at (t0,y0,yS0), set znS[1][is] = yS'(t0), is=1,...,Ns.
     * If computing quadr. sensi., call fQS at (t0,y0,yS0), set znQS[1][is] = yQS'(t0), is=1,...,Ns.
     */

    retval = cv_mem->cv_f(cv_mem->cv_tn, cv_mem->cv_zn[0],
                          cv_mem->cv_zn[1], cv_mem->cv_user_data); 
    cv_mem->cv_nfe++;
    if (retval < 0) {
      cvProcessError(cv_mem, CV_RHSFUNC_FAIL, "CVODES", "CVode",
                     MSGCV_RHSFUNC_FAILED, cv_mem->cv_tn);
      return(CV_RHSFUNC_FAIL);
    }
    if (retval > 0) {
      cvProcessError(cv_mem, CV_FIRST_RHSFUNC_ERR, "CVODES", "CVode",
                     MSGCV_RHSFUNC_FIRST);
      return(CV_FIRST_RHSFUNC_ERR);
    }

    if (cv_mem->cv_quadr) {
      retval = cv_mem->cv_fQ(cv_mem->cv_tn, cv_mem->cv_zn[0],
                             cv_mem->cv_znQ[1], cv_mem->cv_user_data);
      cv_mem->cv_nfQe++;
      if (retval < 0) {
        cvProcessError(cv_mem, CV_QRHSFUNC_FAIL, "CVODES", "CVode",
                       MSGCV_QRHSFUNC_FAILED, cv_mem->cv_tn);
        return(CV_QRHSFUNC_FAIL);
      }
      if (retval > 0) {
        cvProcessError(cv_mem, CV_FIRST_QRHSFUNC_ERR, "CVODES",
                       "CVode", MSGCV_QRHSFUNC_FIRST);
        return(CV_FIRST_QRHSFUNC_ERR);
      }
    }

    if (cv_mem->cv_sensi) {
      retval = cvSensRhsWrapper(cv_mem, cv_mem->cv_tn, cv_mem->cv_zn[0],
                                cv_mem->cv_zn[1], cv_mem->cv_znS[0],
                                cv_mem->cv_znS[1], cv_mem->cv_tempv,
                                cv_mem->cv_ftemp);
      if (retval < 0) {
        cvProcessError(cv_mem, CV_SRHSFUNC_FAIL, "CVODES", "CVode",
                       MSGCV_SRHSFUNC_FAILED, cv_mem->cv_tn);
        return(CV_SRHSFUNC_FAIL);
      } 
      if (retval > 0) {
        cvProcessError(cv_mem, CV_FIRST_SRHSFUNC_ERR, "CVODES",
                       "CVode", MSGCV_SRHSFUNC_FIRST);
        return(CV_FIRST_SRHSFUNC_ERR);
      }
    }

    if (cv_mem->cv_quadr_sensi) {
      retval = cv_mem->cv_fQS(cv_mem->cv_Ns, cv_mem->cv_tn, cv_mem->cv_zn[0],
                              cv_mem->cv_znS[0], cv_mem->cv_znQ[1],
                              cv_mem->cv_znQS[1], cv_mem->cv_fQS_data,
                              cv_mem->cv_tempv, cv_mem->cv_tempvQ); 
      cv_mem->cv_nfQSe++;
      if (retval < 0) {
        cvProcessError(cv_mem, CV_QSRHSFUNC_FAIL, "CVODES", "CVode",
                       MSGCV_QSRHSFUNC_FAILED, cv_mem->cv_tn);
        return(CV_QSRHSFUNC_FAIL);
      } 
      if (retval > 0) {
        cvProcessError(cv_mem, CV_FIRST_QSRHSFUNC_ERR, "CVODES",
                       "CVode", MSGCV_QSRHSFUNC_FIRST);
        return(CV_FIRST_QSRHSFUNC_ERR);
      }
    }

    /* Test input tstop for legality. */

    if (cv_mem->cv_tstopset) {
      if ( (cv_mem->cv_tstop - cv_mem->cv_tn)*(tout - cv_mem->cv_tn) <= ZERO ) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                       MSGCV_BAD_TSTOP, cv_mem->cv_tstop, cv_mem->cv_tn);
        return(CV_ILL_INPUT);
      }
    }

    /* Set initial h (from H0 or cvHin). */
    
    cv_mem->cv_h = cv_mem->cv_hin;
    if ( (cv_mem->cv_h != ZERO) && ((tout-cv_mem->cv_tn)*cv_mem->cv_h < ZERO) ) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode", MSGCV_BAD_H0);
      return(CV_ILL_INPUT);
    }
    if (cv_mem->cv_h == ZERO) {
      tout_hin = tout;
      if ( cv_mem->cv_tstopset &&
           (tout-cv_mem->cv_tn)*(tout-cv_mem->cv_tstop) > ZERO )
        tout_hin = cv_mem->cv_tstop; 
      hflag = cvHin(cv_mem, tout_hin);
      if (hflag != CV_SUCCESS) {
        istate = cvHandleFailure(cv_mem, hflag);
        return(istate);
      }
    }
    rh = SUNRabs(cv_mem->cv_h)*cv_mem->cv_hmax_inv;
    if (rh > ONE) cv_mem->cv_h /= rh;
    if (SUNRabs(cv_mem->cv_h) < cv_mem->cv_hmin)
      cv_mem->cv_h *= cv_mem->cv_hmin/SUNRabs(cv_mem->cv_h);

    /* Check for approach to tstop */

    if (cv_mem->cv_tstopset) {
      if ( (cv_mem->cv_tn + cv_mem->cv_h - cv_mem->cv_tstop)*cv_mem->cv_h > ZERO ) 
        cv_mem->cv_h = (cv_mem->cv_tstop - cv_mem->cv_tn)*(ONE-FOUR*cv_mem->cv_uround);
    }

    /* 
     * Scale zn[1] by h.
     * If computing any quadratures, scale znQ[1] by h.
     * If computing sensitivities,  scale znS[1][is] by h. 
     * If computing quadrature sensitivities,  scale znQS[1][is] by h. 
     */

    cv_mem->cv_hscale = cv_mem->cv_h;
    cv_mem->cv_h0u    = cv_mem->cv_h;
    cv_mem->cv_hprime = cv_mem->cv_h;

    N_VScale(cv_mem->cv_h, cv_mem->cv_zn[1], cv_mem->cv_zn[1]);
    
    if (cv_mem->cv_quadr)
      N_VScale(cv_mem->cv_h, cv_mem->cv_znQ[1], cv_mem->cv_znQ[1]);

    if (cv_mem->cv_sensi) {
      for (is=0; is<cv_mem->cv_Ns; is++)
        cv_mem->cv_cvals[is] = cv_mem->cv_h;

      retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                   cv_mem->cv_znS[1], cv_mem->cv_znS[1]);
      if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
    }

    if (cv_mem->cv_quadr_sensi) {
      for (is=0; is<cv_mem->cv_Ns; is++)
        cv_mem->cv_cvals[is] = cv_mem->cv_h;

      retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                   cv_mem->cv_znQS[1], cv_mem->cv_znQS[1]);
      if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
    }
    
    /* Check for zeros of root function g at and near t0. */

    if (cv_mem->cv_nrtfn > 0) {

      retval = cvRcheck1(cv_mem);

      if (retval == CV_RTFUNC_FAIL) {
        cvProcessError(cv_mem, CV_RTFUNC_FAIL, "CVODES", "cvRcheck1",
                       MSGCV_RTFUNC_FAILED, cv_mem->cv_tn);
        return(CV_RTFUNC_FAIL);
      }

    }

  } /* end first call block */

  /*
   * ------------------------------------------------------
   * 3. At following steps, perform stop tests:
   *    - check for root in last step
   *    - check if we passed tstop
   *    - check if we passed tout (NORMAL mode)
   *    - check if current tn was returned (ONE_STEP mode)
   *    - check if we are close to tstop
   *      (adjust step size if needed)
   * -------------------------------------------------------
   */

  if (cv_mem->cv_nst > 0) {

    /* Estimate an infinitesimal time interval to be used as
       a roundoff for time quantities (based on current time 
       and step size) */
    troundoff = FUZZ_FACTOR * cv_mem->cv_uround *
      (SUNRabs(cv_mem->cv_tn) + SUNRabs(cv_mem->cv_h));

    /* First check for a root in the last step taken, other than the
       last root found, if any.  If itask = CV_ONE_STEP and y(tn) was not
       returned because of an intervening root, return y(tn) now.     */
    if (cv_mem->cv_nrtfn > 0) {
      
      irfndp = cv_mem->cv_irfnd;
      
      retval = cvRcheck2(cv_mem);

      if (retval == CLOSERT) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvRcheck2",
                       MSGCV_CLOSE_ROOTS, cv_mem->cv_tlo);
        return(CV_ILL_INPUT);
      } else if (retval == CV_RTFUNC_FAIL) {
        cvProcessError(cv_mem, CV_RTFUNC_FAIL, "CVODES", "cvRcheck2",
                       MSGCV_RTFUNC_FAILED, cv_mem->cv_tlo);
        return(CV_RTFUNC_FAIL);
      } else if (retval == RTFOUND) {
        cv_mem->cv_tretlast = *tret = cv_mem->cv_tlo;
        return(CV_ROOT_RETURN);
      }
      
      /* If tn is distinct from tretlast (within roundoff),
         check remaining interval for roots */
      if ( SUNRabs(cv_mem->cv_tn - cv_mem->cv_tretlast) > troundoff ) {

        retval = cvRcheck3(cv_mem);

        if (retval == CV_SUCCESS) {     /* no root found */
          cv_mem->cv_irfnd = 0;
          if ((irfndp == 1) && (itask == CV_ONE_STEP)) {
            cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;
            N_VScale(ONE, cv_mem->cv_zn[0], yout);
            return(CV_SUCCESS);
          }
        } else if (retval == RTFOUND) {  /* a new root was found */
          cv_mem->cv_irfnd = 1;
          cv_mem->cv_tretlast = *tret = cv_mem->cv_tlo;
          return(CV_ROOT_RETURN);
        } else if (retval == CV_RTFUNC_FAIL) {  /* g failed */
          cvProcessError(cv_mem, CV_RTFUNC_FAIL, "CVODES", "cvRcheck3", 
                         MSGCV_RTFUNC_FAILED, cv_mem->cv_tlo);
          return(CV_RTFUNC_FAIL);
        }

      }
      
    } /* end of root stop check */
    
    /* In CV_NORMAL mode, test if tout was reached */
    if ( (itask == CV_NORMAL) && ((cv_mem->cv_tn-tout)*cv_mem->cv_h >= ZERO) ) {
      cv_mem->cv_tretlast = *tret = tout;
      ier =  CVodeGetDky(cv_mem, tout, 0, yout);
      if (ier != CV_SUCCESS) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                       MSGCV_BAD_TOUT, tout);
        return(CV_ILL_INPUT);
      }
      return(CV_SUCCESS);
    }
    
    /* In CV_ONE_STEP mode, test if tn was returned */
    if ( itask == CV_ONE_STEP &&
         SUNRabs(cv_mem->cv_tn - cv_mem->cv_tretlast) > troundoff ) {
      cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;
      N_VScale(ONE, cv_mem->cv_zn[0], yout);
      return(CV_SUCCESS);
    }
    
    /* Test for tn at tstop or near tstop */
    if ( cv_mem->cv_tstopset ) {
      
      if ( SUNRabs(cv_mem->cv_tn - cv_mem->cv_tstop) <= troundoff ) {
        ier =  CVodeGetDky(cv_mem, cv_mem->cv_tstop, 0, yout);
        if (ier != CV_SUCCESS) {
          cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                         MSGCV_BAD_TSTOP, cv_mem->cv_tstop, cv_mem->cv_tn);
          return(CV_ILL_INPUT);
        }
        cv_mem->cv_tretlast = *tret = cv_mem->cv_tstop;
        cv_mem->cv_tstopset = SUNFALSE;
        return(CV_TSTOP_RETURN);
      }
      
      /* If next step would overtake tstop, adjust stepsize */
      if ( (cv_mem->cv_tn + cv_mem->cv_hprime - cv_mem->cv_tstop)*cv_mem->cv_h > ZERO ) {
        cv_mem->cv_hprime = (cv_mem->cv_tstop - cv_mem->cv_tn)*(ONE-FOUR*cv_mem->cv_uround);
        cv_mem->cv_eta = cv_mem->cv_hprime / cv_mem->cv_h;
      }
      
    }
    
  } /* end stopping tests block at nst>0 */  
  
  /*
   * --------------------------------------------------
   * 4. Looping point for internal steps
   *
   *    4.1. check for errors (too many steps, too much
   *         accuracy requested, step size too small)
   *    4.2. take a new step (call cvStep)
   *    4.3. stop on error 
   *    4.4. perform stop tests:
   *         - check for root in last step
   *         - check if tout was passed
   *         - check if close to tstop
   *         - check if in ONE_STEP mode (must return)
   * --------------------------------------------------
   */  
  
  nstloc = 0;
  for(;;) {
   
    cv_mem->cv_next_h = cv_mem->cv_h;
    cv_mem->cv_next_q = cv_mem->cv_q;
    
    /* Reset and check ewt, ewtQ, ewtS */   
    if (cv_mem->cv_nst > 0) {

      ier = cv_mem->cv_efun(cv_mem->cv_zn[0], cv_mem->cv_ewt, cv_mem->cv_e_data);
      if(ier != 0) {
        if (cv_mem->cv_itol == CV_WF)
          cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                         MSGCV_EWT_NOW_FAIL, cv_mem->cv_tn);
        else
          cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                         MSGCV_EWT_NOW_BAD, cv_mem->cv_tn);
        istate = CV_ILL_INPUT;
        cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;
        N_VScale(ONE, cv_mem->cv_zn[0], yout);
        break;
      }

      if (cv_mem->cv_quadr && cv_mem->cv_errconQ) {
        ier = cvQuadEwtSet(cv_mem, cv_mem->cv_znQ[0], cv_mem->cv_ewtQ);
        if(ier != 0) {
          cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                         MSGCV_EWTQ_NOW_BAD, cv_mem->cv_tn);
          istate = CV_ILL_INPUT;
          cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;
          N_VScale(ONE, cv_mem->cv_zn[0], yout);
          break;
        }
      }

      if (cv_mem->cv_sensi) {
        ier = cvSensEwtSet(cv_mem, cv_mem->cv_znS[0], cv_mem->cv_ewtS);
        if (ier != 0) {
          cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                         MSGCV_EWTS_NOW_BAD, cv_mem->cv_tn);
          istate = CV_ILL_INPUT;
          cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;
          N_VScale(ONE, cv_mem->cv_zn[0], yout);
          break;
        }
      }

      if (cv_mem->cv_quadr_sensi && cv_mem->cv_errconQS) {
        ier = cvQuadSensEwtSet(cv_mem, cv_mem->cv_znQS[0], cv_mem->cv_ewtQS);
        if (ier != 0) {
          cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVode",
                         MSGCV_EWTQS_NOW_BAD, cv_mem->cv_tn);
          istate = CV_ILL_INPUT;
          cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;
          N_VScale(ONE, cv_mem->cv_zn[0], yout);
          break;
        }
      }

    }

    /* Check for too many steps */
    if ( (cv_mem->cv_mxstep>0) && (nstloc >= cv_mem->cv_mxstep) ) {
      cvProcessError(cv_mem, CV_TOO_MUCH_WORK, "CVODES", "CVode",
                     MSGCV_MAX_STEPS, cv_mem->cv_tn);
      istate = CV_TOO_MUCH_WORK;
      cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;
      N_VScale(ONE, cv_mem->cv_zn[0], yout);
      break;
    }

    /* Check for too much accuracy requested */
    nrm = N_VWrmsNorm(cv_mem->cv_zn[0], cv_mem->cv_ewt);
    if (cv_mem->cv_quadr && cv_mem->cv_errconQ) {
      nrm = cvQuadUpdateNorm(cv_mem, nrm, cv_mem->cv_znQ[0], cv_mem->cv_ewtQ); 
    }
    if (cv_mem->cv_sensi && cv_mem->cv_errconS) {
      nrm = cvSensUpdateNorm(cv_mem, nrm, cv_mem->cv_znS[0], cv_mem->cv_ewtS);
    }
    if (cv_mem->cv_quadr_sensi && cv_mem->cv_errconQS) {
      nrm = cvQuadSensUpdateNorm(cv_mem, nrm, cv_mem->cv_znQS[0], cv_mem->cv_ewtQS);
    }
    cv_mem->cv_tolsf = cv_mem->cv_uround * nrm;
    if (cv_mem->cv_tolsf > ONE) {
      cvProcessError(cv_mem, CV_TOO_MUCH_ACC, "CVODES", "CVode",
                     MSGCV_TOO_MUCH_ACC, cv_mem->cv_tn);
      istate = CV_TOO_MUCH_ACC;
      cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;
      N_VScale(ONE, cv_mem->cv_zn[0], yout);
      cv_mem->cv_tolsf *= TWO;
      break;
    } else {
      cv_mem->cv_tolsf = ONE;
    }
    
    /* Check for h below roundoff level in tn */
    if (cv_mem->cv_tn + cv_mem->cv_h == cv_mem->cv_tn) {
      cv_mem->cv_nhnil++;
      if (cv_mem->cv_nhnil <= cv_mem->cv_mxhnil) 
        cvProcessError(cv_mem, CV_WARNING, "CVODES", "CVode", MSGCV_HNIL,
                       cv_mem->cv_tn, cv_mem->cv_h);
      if (cv_mem->cv_nhnil == cv_mem->cv_mxhnil) 
        cvProcessError(cv_mem, CV_WARNING, "CVODES", "CVode", MSGCV_HNIL_DONE);
    }

    /* Call cvStep to take a step */
    kflag = cvStep(cv_mem);

    /* Process failed step cases, and exit loop */
    if (kflag != CV_SUCCESS) {
      istate = cvHandleFailure(cv_mem, kflag);
      cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;
      N_VScale(ONE, cv_mem->cv_zn[0], yout);
      break;
    }
    
    nstloc++;

    /* If tstop is set and was reached, reset tn = tstop */
    if ( cv_mem->cv_tstopset ) {
      troundoff = FUZZ_FACTOR * cv_mem->cv_uround *
        (SUNRabs(cv_mem->cv_tn) + SUNRabs(cv_mem->cv_h));
      if ( SUNRabs(cv_mem->cv_tn - cv_mem->cv_tstop) <= troundoff)
        cv_mem->cv_tn = cv_mem->cv_tstop;
    }

    /* Check for root in last step taken. */    
    if (cv_mem->cv_nrtfn > 0) {
      
      retval = cvRcheck3(cv_mem);
      
      if (retval == RTFOUND) {  /* A new root was found */
        cv_mem->cv_irfnd = 1;
        istate = CV_ROOT_RETURN;
        cv_mem->cv_tretlast = *tret = cv_mem->cv_tlo;
        break;
      } else if (retval == CV_RTFUNC_FAIL) { /* g failed */
        cvProcessError(cv_mem, CV_RTFUNC_FAIL, "CVODES", "cvRcheck3",
                       MSGCV_RTFUNC_FAILED, cv_mem->cv_tlo);
        istate = CV_RTFUNC_FAIL;
        break;
      }

      /* If we are at the end of the first step and we still have
       * some event functions that are inactive, issue a warning
       * as this may indicate a user error in the implementation
       * of the root function. */

      if (cv_mem->cv_nst==1) {
        inactive_roots = SUNFALSE;
        for (ir=0; ir<cv_mem->cv_nrtfn; ir++) { 
          if (!cv_mem->cv_gactive[ir]) {
            inactive_roots = SUNTRUE;
            break;
          }
        }
        if ((cv_mem->cv_mxgnull > 0) && inactive_roots) {
          cvProcessError(cv_mem, CV_WARNING, "CVODES", "CVode",
                         MSGCV_INACTIVE_ROOTS);
        }
      }

    }

    /* In NORMAL mode, check if tout reached */
    if ( (itask == CV_NORMAL) &&  (cv_mem->cv_tn-tout)*cv_mem->cv_h >= ZERO ) {
      istate = CV_SUCCESS;
      cv_mem->cv_tretlast = *tret = tout;
      (void) CVodeGetDky(cv_mem, tout, 0, yout);
      cv_mem->cv_next_q = cv_mem->cv_qprime;
      cv_mem->cv_next_h = cv_mem->cv_hprime;
      break;
    }

    /* Check if tn is at tstop, or about to pass tstop */
    if ( cv_mem->cv_tstopset ) {

      troundoff = FUZZ_FACTOR * cv_mem->cv_uround *
        (SUNRabs(cv_mem->cv_tn) + SUNRabs(cv_mem->cv_h));
      if ( SUNRabs(cv_mem->cv_tn - cv_mem->cv_tstop) <= troundoff) {
        (void) CVodeGetDky(cv_mem, cv_mem->cv_tstop, 0, yout);
        cv_mem->cv_tretlast = *tret = cv_mem->cv_tstop;
        cv_mem->cv_tstopset = SUNFALSE;
        istate = CV_TSTOP_RETURN;
        break;
      }

      if ( (cv_mem->cv_tn + cv_mem->cv_hprime - cv_mem->cv_tstop)*cv_mem->cv_h > ZERO ) {
        cv_mem->cv_hprime = (cv_mem->cv_tstop - cv_mem->cv_tn)*(ONE-FOUR*cv_mem->cv_uround);
        cv_mem->cv_eta = cv_mem->cv_hprime / cv_mem->cv_h;
      }

    }

    /* In ONE_STEP mode, copy y and exit loop */
    if (itask == CV_ONE_STEP) {
      istate = CV_SUCCESS;
      cv_mem->cv_tretlast = *tret = cv_mem->cv_tn;
      N_VScale(ONE, cv_mem->cv_zn[0], yout);
      cv_mem->cv_next_q = cv_mem->cv_qprime;
      cv_mem->cv_next_h = cv_mem->cv_hprime;
      break;
    }

  } /* end looping for internal steps */
  
  /* Load optional output */
  if (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_STAGGERED1)) { 
    cv_mem->cv_nniS  = 0;
    cv_mem->cv_ncfnS = 0;
    for (is=0; is<cv_mem->cv_Ns; is++) {
      cv_mem->cv_nniS  += cv_mem->cv_nniS1[is];
      cv_mem->cv_ncfnS += cv_mem->cv_ncfnS1[is];
    }
  }
  
  return(istate);

}

/* 
 * -----------------------------------------------------------------
 * Interpolated output and extraction functions
 * -----------------------------------------------------------------
 */

/*
 * CVodeGetDky
 *
 * This routine computes the k-th derivative of the interpolating
 * polynomial at the time t and stores the result in the vector dky.
 * The formula is:
 *         q 
 *  dky = SUM c(j,k) * (t - tn)^(j-k) * h^(-j) * zn[j] , 
 *        j=k 
 * where c(j,k) = j*(j-1)*...*(j-k+1), q is the current order, and
 * zn[j] is the j-th column of the Nordsieck history array.
 *
 * This function is called by CVode with k = 0 and t = tout, but
 * may also be called directly by the user.
 */

int CVodeGetDky(void *cvode_mem, realtype t, int k, N_Vector dky)
{
  realtype s, r;
  realtype tfuzz, tp, tn1;
  int i, j, nvec, ier;
  CVodeMem cv_mem;
  
  /* Check all inputs for legality */
 
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetDky", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  if (dky == NULL) {
    cvProcessError(cv_mem, CV_BAD_DKY, "CVODES", "CVodeGetDky", MSGCV_NULL_DKY);
    return(CV_BAD_DKY);
  }

  if ((k < 0) || (k > cv_mem->cv_q)) {
    cvProcessError(cv_mem, CV_BAD_K, "CVODES", "CVodeGetDky", MSGCV_BAD_K);
    return(CV_BAD_K);
  }
  
  /* Allow for some slack */
  tfuzz = FUZZ_FACTOR * cv_mem->cv_uround *
    (SUNRabs(cv_mem->cv_tn) + SUNRabs(cv_mem->cv_hu));
  if (cv_mem->cv_hu < ZERO) tfuzz = -tfuzz;
  tp = cv_mem->cv_tn - cv_mem->cv_hu - tfuzz;
  tn1 = cv_mem->cv_tn + tfuzz;
  if ((t-tp)*(t-tn1) > ZERO) {
    cvProcessError(cv_mem, CV_BAD_T, "CVODES", "CVodeGetDky", MSGCV_BAD_T,
                   t, cv_mem->cv_tn-cv_mem->cv_hu, cv_mem->cv_tn);
    return(CV_BAD_T);
  }

  /* Sum the differentiated interpolating polynomial */
  nvec = 0;

  s = (t - cv_mem->cv_tn) / cv_mem->cv_h;
  for (j=cv_mem->cv_q; j >= k; j--) {
    cv_mem->cv_cvals[nvec] = ONE;
    for (i=j; i >= j-k+1; i--)
      cv_mem->cv_cvals[nvec] *= i;
    for (i=0; i < j-k; i++)
      cv_mem->cv_cvals[nvec] *= s;
    cv_mem->cv_Xvecs[nvec] = cv_mem->cv_zn[j];
    nvec += 1;
  }
  ier = N_VLinearCombination(nvec, cv_mem->cv_cvals, cv_mem->cv_Xvecs, dky);
  if (ier != CV_SUCCESS) return (CV_VECTOROP_ERR);

  if (k == 0) return(CV_SUCCESS);
  r = SUNRpowerI(cv_mem->cv_h, -k);
  N_VScale(r, dky, dky);
  return(CV_SUCCESS);

}

/* 
 * CVodeGetQuad
 *
 * This routine extracts quadrature solution into yQout at the
 * time which CVode returned the solution.
 * This is just a wrapper that calls CVodeGetQuadDky with k=0.
 */
 
int CVodeGetQuad(void *cvode_mem, realtype *tret, N_Vector yQout)
{
  CVodeMem cv_mem;
  int flag;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuad", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;  

  *tret = cv_mem->cv_tretlast;
  
  flag = CVodeGetQuadDky(cvode_mem,cv_mem->cv_tretlast,0,yQout);

  return(flag);
}

/*
 * CVodeGetQuadDky
 *
 * CVodeQuadDky computes the kth derivative of the yQ function at
 * time t, where tn-hu <= t <= tn, tn denotes the current         
 * internal time reached, and hu is the last internal step size   
 * successfully used by the solver. The user may request 
 * k=0, 1, ..., qu, where qu is the current order. 
 * The derivative vector is returned in dky. This vector 
 * must be allocated by the caller. It is only legal to call this         
 * function after a successful return from CVode with quadrature
 * computation enabled.
 */

int CVodeGetQuadDky(void *cvode_mem, realtype t, int k, N_Vector dkyQ)
{ 
  realtype s, r;
  realtype tfuzz, tp, tn1;
  int i, j, nvec, ier;
  CVodeMem cv_mem;
  
  /* Check all inputs for legality */
  
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadDky", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;  

  if(cv_mem->cv_quadr != SUNTRUE) {
    cvProcessError(cv_mem, CV_NO_QUAD, "CVODES", "CVodeGetQuadDky", MSGCV_NO_QUAD);
    return(CV_NO_QUAD);
  }

  if (dkyQ == NULL) {
    cvProcessError(cv_mem, CV_BAD_DKY, "CVODES", "CVodeGetQuadDky", MSGCV_NULL_DKY);
    return(CV_BAD_DKY);
  }
  
  if ((k < 0) || (k > cv_mem->cv_q)) {
    cvProcessError(cv_mem, CV_BAD_K, "CVODES", "CVodeGetQuadDky", MSGCV_BAD_K);
    return(CV_BAD_K);
  }
  
  /* Allow for some slack */
  tfuzz = FUZZ_FACTOR * cv_mem->cv_uround *
    (SUNRabs(cv_mem->cv_tn) + SUNRabs(cv_mem->cv_hu));
  if (cv_mem->cv_hu < ZERO) tfuzz = -tfuzz;
  tp = cv_mem->cv_tn - cv_mem->cv_hu - tfuzz;
  tn1 = cv_mem->cv_tn + tfuzz;
  if ((t-tp)*(t-tn1) > ZERO) {
    cvProcessError(cv_mem, CV_BAD_T, "CVODES", "CVodeGetQuadDky", MSGCV_BAD_T);
    return(CV_BAD_T);
  }
  
  /* Sum the differentiated interpolating polynomial */
  nvec = 0;

  s = (t - cv_mem->cv_tn) / cv_mem->cv_h;
  for (j=cv_mem->cv_q; j >= k; j--) {
    cv_mem->cv_cvals[nvec] = ONE;
    for (i=j; i >= j-k+1; i--)
      cv_mem->cv_cvals[nvec] *= i;
    for (i=0; i < j-k; i++)
      cv_mem->cv_cvals[nvec] *= s;
    cv_mem->cv_Xvecs[nvec] = cv_mem->cv_znQ[j];
    nvec += 1;
  }
  ier = N_VLinearCombination(nvec, cv_mem->cv_cvals, cv_mem->cv_Xvecs, dkyQ);
  if (ier != CV_SUCCESS) return (CV_VECTOROP_ERR);

  if (k == 0) return(CV_SUCCESS);
  r = SUNRpowerI(cv_mem->cv_h, -k);
  N_VScale(r, dkyQ, dkyQ);
  return(CV_SUCCESS);
  
}

/* 
 * CVodeGetSens
 *
 * This routine extracts sensitivity solution into ySout at the
 * time at which CVode returned the solution.
 * This is just a wrapper that calls CVodeSensDky with k=0.
 */
 
int CVodeGetSens(void *cvode_mem, realtype *tret, N_Vector *ySout)
{
  CVodeMem cv_mem;
  int flag;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetSens", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;  

  *tret = cv_mem->cv_tretlast;

  flag = CVodeGetSensDky(cvode_mem,cv_mem->cv_tretlast,0,ySout);

  return(flag);
}
    
/* 
 * CVodeGetSens1
 *
 * This routine extracts the is-th sensitivity solution into ySout
 * at the time at which CVode returned the solution.
 * This is just a wrapper that calls CVodeSensDky1 with k=0.
 */
 
int CVodeGetSens1(void *cvode_mem, realtype *tret, int is, N_Vector ySout)
{
  CVodeMem cv_mem;
  int flag;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetSens1", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;  

  *tret = cv_mem->cv_tretlast;

  flag = CVodeGetSensDky1(cvode_mem,cv_mem->cv_tretlast,0,is,ySout);

  return(flag);
}
    
/*
 * CVodeGetSensDky
 *
 * If the user calls directly CVodeSensDky then s must be allocated
 * prior to this call. When CVodeSensDky is called by 
 * CVodeGetSens, only ier=CV_SUCCESS, ier=CV_NO_SENS, or 
 * ier=CV_BAD_T are possible.
 */

int CVodeGetSensDky(void *cvode_mem, realtype t, int k, N_Vector *dkyS)
{
  int ier=CV_SUCCESS;
  int is;
  CVodeMem cv_mem;
  
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetSensDky", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;  
  
  if (dkyS == NULL) {
    cvProcessError(cv_mem, CV_BAD_DKY, "CVODES",
                   "CVodeGetSensDky", MSGCV_NULL_DKYA);
    return(CV_BAD_DKY);
  }
  
  for (is=0; is<cv_mem->cv_Ns; is++) {
    ier = CVodeGetSensDky1(cvode_mem,t,k,is,dkyS[is]);
    if (ier!=CV_SUCCESS) break;
  }
  
  return(ier);
}
    
/*
 * CVodeGetSensDky1
 *
 * CVodeSensDky1 computes the kth derivative of the yS[is] function at
 * time t, where tn-hu <= t <= tn, tn denotes the current         
 * internal time reached, and hu is the last internal step size   
 * successfully used by the solver. The user may request 
 * is=0, 1, ..., Ns-1 and k=0, 1, ..., qu, where qu is the current
 * order. The derivative vector is returned in dky. This vector 
 * must be allocated by the caller. It is only legal to call this         
 * function after a successful return from CVode with sensitivity 
 * computation enabled.
 */

int CVodeGetSensDky1(void *cvode_mem, realtype t, int k, int is, N_Vector dkyS)
{ 
  realtype s, r;
  realtype tfuzz, tp, tn1;
  int i, j, nvec, ier;
  CVodeMem cv_mem;
  
  /* Check all inputs for legality */
  
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetSensDky1",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;  
  
  if(cv_mem->cv_sensi != SUNTRUE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeGetSensDky1",
                   MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  if (dkyS == NULL) {
    cvProcessError(cv_mem, CV_BAD_DKY, "CVODES", "CVodeGetSensDky1",
                   MSGCV_NULL_DKY);
    return(CV_BAD_DKY);
  }
  
  if ((k < 0) || (k > cv_mem->cv_q)) {
    cvProcessError(cv_mem, CV_BAD_K, "CVODES", "CVodeGetSensDky1",
                   MSGCV_BAD_K);
    return(CV_BAD_K);
  }
  
  if ((is < 0) || (is > cv_mem->cv_Ns-1)) {
    cvProcessError(cv_mem, CV_BAD_IS, "CVODES", "CVodeGetSensDky1",
                   MSGCV_BAD_IS);
    return(CV_BAD_IS);
  }
  
  /* Allow for some slack */
  tfuzz = FUZZ_FACTOR * cv_mem->cv_uround *
    (SUNRabs(cv_mem->cv_tn) + SUNRabs(cv_mem->cv_hu));
  if (cv_mem->cv_hu < ZERO) tfuzz = -tfuzz;
  tp = cv_mem->cv_tn - cv_mem->cv_hu - tfuzz;
  tn1 = cv_mem->cv_tn + tfuzz;
  if ((t-tp)*(t-tn1) > ZERO) {
    cvProcessError(cv_mem, CV_BAD_T, "CVODES", "CVodeGetSensDky1",
                   MSGCV_BAD_T);
    return(CV_BAD_T);
  }
  
  /* Sum the differentiated interpolating polynomial */
  nvec = 0;

  s = (t - cv_mem->cv_tn) / cv_mem->cv_h;
  for (j=cv_mem->cv_q; j >= k; j--) {
    cv_mem->cv_cvals[nvec] = ONE;
    for (i=j; i >= j-k+1; i--)
      cv_mem->cv_cvals[nvec] *= i;
    for (i=0; i < j-k; i++)
      cv_mem->cv_cvals[nvec] *= s;
    cv_mem->cv_Xvecs[nvec] = cv_mem->cv_znS[j][is];
    nvec += 1;
  }
  ier = N_VLinearCombination(nvec, cv_mem->cv_cvals, cv_mem->cv_Xvecs, dkyS);
  if (ier != CV_SUCCESS) return (CV_VECTOROP_ERR);

  if (k == 0) return(CV_SUCCESS);
  r = SUNRpowerI(cv_mem->cv_h, -k);
  N_VScale(r, dkyS, dkyS);
  return(CV_SUCCESS);
  
}

/* 
 * CVodeGetQuadSens and CVodeGetQuadSens1
 *
 * Extraction functions for all or only one of the quadrature sensitivity
 * vectors at the time at which CVode returned the ODE solution.
 */

int CVodeGetQuadSens(void *cvode_mem, realtype *tret, N_Vector *yQSout)
{
  CVodeMem cv_mem;
  int flag;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadSens",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;  

  *tret = cv_mem->cv_tretlast;

  flag = CVodeGetQuadSensDky(cvode_mem,cv_mem->cv_tretlast,0,yQSout);

  return(flag);
}

int CVodeGetQuadSens1(void *cvode_mem, realtype *tret, int is, N_Vector yQSout)
{
  CVodeMem cv_mem;
  int flag;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadSens1",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;  

  *tret = cv_mem->cv_tretlast;

  flag = CVodeGetQuadSensDky1(cvode_mem,cv_mem->cv_tretlast,0,is,yQSout);

  return(flag);
}

/* 
 * CVodeGetQuadSensDky and CVodeGetQuadSensDky1
 *
 * Dense output functions for all or only one of the quadrature sensitivity
 * vectors (or derivative thereof).
 */

int CVodeGetQuadSensDky(void *cvode_mem, realtype t, int k, N_Vector *dkyQS_all)
{
  int ier=CV_SUCCESS;
  int is;
  CVodeMem cv_mem;
  
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadSensDky",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;  
  
  if (dkyQS_all == NULL) {
    cvProcessError(cv_mem, CV_BAD_DKY, "CVODES", "CVodeGetSensDky",
                   MSGCV_NULL_DKYA);
    return(CV_BAD_DKY);
  }
  
  for (is=0; is<cv_mem->cv_Ns; is++) {
    ier = CVodeGetQuadSensDky1(cvode_mem,t,k,is,dkyQS_all[is]);
    if (ier!=CV_SUCCESS) break;
  }
  
  return(ier);
}

int CVodeGetQuadSensDky1(void *cvode_mem, realtype t, int k, int is, N_Vector dkyQS)
{
  realtype s, r;
  realtype tfuzz, tp, tn1;
  int i, j, nvec, ier;
  CVodeMem cv_mem;
  
  /* Check all inputs for legality */
  
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadSensDky1",
                   MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;  
  
  if(cv_mem->cv_quadr_sensi != SUNTRUE) {
    cvProcessError(cv_mem, CV_NO_QUADSENS, "CVODES", "CVodeGetQuadSensDky1",
                   MSGCV_NO_QUADSENSI);
    return(CV_NO_QUADSENS);
  }

  if (dkyQS == NULL) {
    cvProcessError(cv_mem, CV_BAD_DKY, "CVODES", "CVodeGetQuadSensDky1",
                   MSGCV_NULL_DKY);
    return(CV_BAD_DKY);
  }
  
  if ((k < 0) || (k > cv_mem->cv_q)) {
    cvProcessError(cv_mem, CV_BAD_K, "CVODES", "CVodeGetQuadSensDky1",
                   MSGCV_BAD_K);
    return(CV_BAD_K);
  }
  
  if ((is < 0) || (is > cv_mem->cv_Ns-1)) {
    cvProcessError(cv_mem, CV_BAD_IS, "CVODES", "CVodeGetQuadSensDky1",
                   MSGCV_BAD_IS);
    return(CV_BAD_IS);
  }
  
  /* Allow for some slack */
  tfuzz = FUZZ_FACTOR * cv_mem->cv_uround *
    (SUNRabs(cv_mem->cv_tn) + SUNRabs(cv_mem->cv_hu));
  if (cv_mem->cv_hu < ZERO) tfuzz = -tfuzz;
  tp = cv_mem->cv_tn - cv_mem->cv_hu - tfuzz;
  tn1 = cv_mem->cv_tn + tfuzz;
  if ((t-tp)*(t-tn1) > ZERO) {
    cvProcessError(cv_mem, CV_BAD_T, "CVODES", "CVodeGetQuadSensDky1",
                   MSGCV_BAD_T);
    return(CV_BAD_T);
  }
  
  /* Sum the differentiated interpolating polynomial */
  nvec = 0;
  
  s = (t - cv_mem->cv_tn) / cv_mem->cv_h;
  for (j=cv_mem->cv_q; j >= k; j--) {
    cv_mem->cv_cvals[nvec] = ONE;
    for (i=j; i >= j-k+1; i--)
      cv_mem->cv_cvals[nvec] *= i;
    for (i=0; i < j-k; i++)
      cv_mem->cv_cvals[nvec] *= s;
    cv_mem->cv_Xvecs[nvec] = cv_mem->cv_znQS[j][is];
    nvec += 1;
  }
  ier = N_VLinearCombination(nvec, cv_mem->cv_cvals, cv_mem->cv_Xvecs, dkyQS);
  if (ier != CV_SUCCESS) return (CV_VECTOROP_ERR);

  if (k == 0) return(CV_SUCCESS);
  r = SUNRpowerI(cv_mem->cv_h, -k);
  N_VScale(r, dkyQS, dkyQS);
  return(CV_SUCCESS);

}

/* 
 * -----------------------------------------------------------------
 * Deallocation functions
 * -----------------------------------------------------------------
 */

/*
 * CVodeFree
 *
 * This routine frees the problem memory allocated by CVodeInit.
 * Such memory includes all the vectors allocated by cvAllocVectors,
 * and the memory lmem for the linear solver (deallocated by a call
 * to lfree), as well as (if Ns!=0) all memory allocated for 
 * sensitivity computations by CVodeSensInit.
 */

void CVodeFree(void **cvode_mem)
{
  CVodeMem cv_mem;

  if (*cvode_mem == NULL) return;

  cv_mem = (CVodeMem) (*cvode_mem);
  
  cvFreeVectors(cv_mem);

  if (cv_mem->ownNLS) {
    SUNNonlinSolFree(cv_mem->NLS);
    cv_mem->ownNLS = SUNFALSE;
    cv_mem->NLS = NULL;
  }

  CVodeQuadFree(cv_mem);

  CVodeSensFree(cv_mem);

  CVodeQuadSensFree(cv_mem);

  CVodeAdjFree(cv_mem);

  if (cv_mem->cv_lfree != NULL) cv_mem->cv_lfree(cv_mem);

  if (cv_mem->cv_nrtfn > 0) {
    free(cv_mem->cv_glo); cv_mem->cv_glo = NULL; 
    free(cv_mem->cv_ghi);  cv_mem->cv_ghi = NULL;
    free(cv_mem->cv_grout);  cv_mem->cv_grout = NULL;
    free(cv_mem->cv_iroots); cv_mem->cv_iroots = NULL;
    free(cv_mem->cv_rootdir); cv_mem->cv_rootdir = NULL;
    free(cv_mem->cv_gactive); cv_mem->cv_gactive = NULL;
  }

  free(cv_mem->cv_cvals); cv_mem->cv_cvals = NULL;
  free(cv_mem->cv_Xvecs); cv_mem->cv_Xvecs = NULL;
  free(cv_mem->cv_Zvecs); cv_mem->cv_Zvecs = NULL;

  free(*cvode_mem);
  *cvode_mem = NULL;
}

/*
 * CVodeQuadFree
 *
 * CVodeQuadFree frees the problem memory in cvode_mem allocated
 * for quadrature integration. Its only argument is the pointer
 * cvode_mem returned by CVodeCreate. 
 */

void CVodeQuadFree(void *cvode_mem)
{
  CVodeMem cv_mem;
  
  if (cvode_mem == NULL) return;
  cv_mem = (CVodeMem) cvode_mem;

  if(cv_mem->cv_QuadMallocDone) {
    cvQuadFreeVectors(cv_mem);
    cv_mem->cv_QuadMallocDone = SUNFALSE;
    cv_mem->cv_quadr = SUNFALSE;
  }
}

/*
 * CVodeSensFree
 *
 * CVodeSensFree frees the problem memory in cvode_mem allocated
 * for sensitivity analysis. Its only argument is the pointer
 * cvode_mem returned by CVodeCreate. 
 */

void CVodeSensFree(void *cvode_mem)
{
  CVodeMem cv_mem;
  
  if (cvode_mem == NULL) return;
  cv_mem = (CVodeMem) cvode_mem;

  if(cv_mem->cv_SensMallocDone) {
    if (cv_mem->cv_stgr1alloc) {
      free(cv_mem->cv_ncfS1);  cv_mem->cv_ncfS1 = NULL;
      free(cv_mem->cv_ncfnS1); cv_mem->cv_ncfnS1 = NULL;
      free(cv_mem->cv_nniS1);  cv_mem->cv_nniS1 = NULL;
      cv_mem->cv_stgr1alloc = SUNFALSE;
    }
    cvSensFreeVectors(cv_mem);
    cv_mem->cv_SensMallocDone = SUNFALSE;
    cv_mem->cv_sensi = SUNFALSE;
  }

  /* free any vector wrappers */
  if (cv_mem->simMallocDone) {
    N_VDestroy(cv_mem->ycor0Sim); cv_mem->ycor0Sim = NULL;
    N_VDestroy(cv_mem->ycorSim);  cv_mem->ycorSim  = NULL;
    N_VDestroy(cv_mem->ewtSim);   cv_mem->ewtSim   = NULL;
    cv_mem->simMallocDone = SUNFALSE;
  }
  if (cv_mem->stgMallocDone) {
    N_VDestroy(cv_mem->ycor0Stg); cv_mem->ycor0Stg = NULL;
    N_VDestroy(cv_mem->ycorStg);  cv_mem->ycorStg  = NULL;
    N_VDestroy(cv_mem->ewtStg);   cv_mem->ewtStg   = NULL;
    cv_mem->stgMallocDone = SUNFALSE;
  }

  /* if CVODES created a NLS object then free it */
  if (cv_mem->ownNLSsim) {
    SUNNonlinSolFree(cv_mem->NLSsim);
    cv_mem->ownNLSsim = SUNFALSE;
    cv_mem->NLSsim = NULL;
  }
  if (cv_mem->ownNLSstg) {
    SUNNonlinSolFree(cv_mem->NLSstg);
    cv_mem->ownNLSstg = SUNFALSE;
    cv_mem->NLSstg = NULL;
  }
  if (cv_mem->ownNLSstg1) {
    SUNNonlinSolFree(cv_mem->NLSstg1);
    cv_mem->ownNLSstg1 = SUNFALSE;
    cv_mem->NLSstg1 = NULL;
  }

}

/*
 * CVodeQuadSensFree
 *
 * CVodeQuadSensFree frees the problem memory in cvode_mem allocated
 * for quadrature sensitivity analysis. Its only argument is the pointer
 * cvode_mem returned by CVodeCreate. 
 */

void CVodeQuadSensFree(void *cvode_mem)
{
  CVodeMem cv_mem;
  
  if (cvode_mem == NULL) return;
  cv_mem = (CVodeMem) cvode_mem;

  if(cv_mem->cv_QuadSensMallocDone) {
    cvQuadSensFreeVectors(cv_mem);
    cv_mem->cv_QuadSensMallocDone = SUNFALSE;
    cv_mem->cv_quadr_sensi = SUNFALSE;
  }
}


/* 
 * =================================================================
 * PRIVATE FUNCTIONS
 * =================================================================
 */

/*
 * cvCheckNvector
 * This routine checks if all required vector operations are present.
 * If any of them is missing it returns SUNFALSE.
 */

static booleantype cvCheckNvector(N_Vector tmpl)
{
  if((tmpl->ops->nvclone     == NULL) ||
     (tmpl->ops->nvdestroy   == NULL) ||
     (tmpl->ops->nvlinearsum == NULL) ||
     (tmpl->ops->nvconst     == NULL) ||
     (tmpl->ops->nvprod      == NULL) ||
     (tmpl->ops->nvdiv       == NULL) ||
     (tmpl->ops->nvscale     == NULL) ||
     (tmpl->ops->nvabs       == NULL) ||
     (tmpl->ops->nvinv       == NULL) ||
     (tmpl->ops->nvaddconst  == NULL) ||
     (tmpl->ops->nvmaxnorm   == NULL) ||
     (tmpl->ops->nvwrmsnorm  == NULL) ||
     (tmpl->ops->nvmin       == NULL))
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
 * cvAllocVectors
 *
 * This routine allocates the CVODES vectors ewt, acor, tempv, ftemp, and
 * zn[0], ..., zn[maxord].
 * If all memory allocations are successful, cvAllocVectors returns SUNTRUE. 
 * Otherwise all allocated memory is freed and cvAllocVectors returns SUNFALSE.
 * This routine also sets the optional outputs lrw and liw, which are
 * (respectively) the lengths of the real and integer work spaces
 * allocated here.
 */

static booleantype cvAllocVectors(CVodeMem cv_mem, N_Vector tmpl)
{
  int i, j;

  /* Allocate ewt, acor, tempv, ftemp */
  
  cv_mem->cv_ewt = N_VClone(tmpl);
  if (cv_mem->cv_ewt == NULL) return(SUNFALSE);

  cv_mem->cv_acor = N_VClone(tmpl);
  if (cv_mem->cv_acor == NULL) {
    N_VDestroy(cv_mem->cv_ewt);
    return(SUNFALSE);
  }

  cv_mem->cv_tempv = N_VClone(tmpl);
  if (cv_mem->cv_tempv == NULL) {
    N_VDestroy(cv_mem->cv_ewt);
    N_VDestroy(cv_mem->cv_acor);
    return(SUNFALSE);
  }

  cv_mem->cv_ftemp = N_VClone(tmpl);
  if (cv_mem->cv_ftemp == NULL) {
    N_VDestroy(cv_mem->cv_tempv);
    N_VDestroy(cv_mem->cv_ewt);
    N_VDestroy(cv_mem->cv_acor);
    return(SUNFALSE);
  }

  cv_mem->cv_vtemp1 = N_VClone(tmpl);
  if (cv_mem->cv_vtemp1 == NULL) {
    N_VDestroy(cv_mem->cv_ftemp);
    N_VDestroy(cv_mem->cv_tempv);
    N_VDestroy(cv_mem->cv_ewt);
    N_VDestroy(cv_mem->cv_acor);
    return(SUNFALSE);
  }

  cv_mem->cv_vtemp2 = N_VClone(tmpl);
  if (cv_mem->cv_vtemp2 == NULL) {
    N_VDestroy(cv_mem->cv_vtemp1);
    N_VDestroy(cv_mem->cv_ftemp);
    N_VDestroy(cv_mem->cv_tempv);
    N_VDestroy(cv_mem->cv_ewt);
    N_VDestroy(cv_mem->cv_acor);
    return(SUNFALSE);
  }

  cv_mem->cv_vtemp3 = N_VClone(tmpl);
  if (cv_mem->cv_vtemp3 == NULL) {
    N_VDestroy(cv_mem->cv_vtemp2);
    N_VDestroy(cv_mem->cv_vtemp1);
    N_VDestroy(cv_mem->cv_ftemp);
    N_VDestroy(cv_mem->cv_tempv);
    N_VDestroy(cv_mem->cv_ewt);
    N_VDestroy(cv_mem->cv_acor);
    return(SUNFALSE);
  }

  /* Allocate zn[0] ... zn[qmax] */

  for (j=0; j <= cv_mem->cv_qmax; j++) {
    cv_mem->cv_zn[j] = N_VClone(tmpl);
    if (cv_mem->cv_zn[j] == NULL) {
      N_VDestroy(cv_mem->cv_ewt);
      N_VDestroy(cv_mem->cv_acor);
      N_VDestroy(cv_mem->cv_tempv);
      N_VDestroy(cv_mem->cv_ftemp);
      N_VDestroy(cv_mem->cv_vtemp1);
      N_VDestroy(cv_mem->cv_vtemp2);
      N_VDestroy(cv_mem->cv_vtemp3);
      for (i=0; i < j; i++) N_VDestroy(cv_mem->cv_zn[i]);
      return(SUNFALSE);
    }
  }

  /* Update solver workspace lengths  */
  cv_mem->cv_lrw += (cv_mem->cv_qmax + 8)*cv_mem->cv_lrw1;
  cv_mem->cv_liw += (cv_mem->cv_qmax + 8)*cv_mem->cv_liw1;

  /* Store the value of qmax used here */
  cv_mem->cv_qmax_alloc = cv_mem->cv_qmax;

  return(SUNTRUE);
}

/*  
 * cvFreeVectors
 *
 * This routine frees the CVODES vectors allocated in cvAllocVectors.
 */

static void cvFreeVectors(CVodeMem cv_mem)
{
  int j, maxord;
  
  maxord = cv_mem->cv_qmax_alloc;

  N_VDestroy(cv_mem->cv_ewt);
  N_VDestroy(cv_mem->cv_acor);
  N_VDestroy(cv_mem->cv_tempv);
  N_VDestroy(cv_mem->cv_ftemp);
  N_VDestroy(cv_mem->cv_vtemp1);
  N_VDestroy(cv_mem->cv_vtemp2);
  N_VDestroy(cv_mem->cv_vtemp3);
  for (j=0; j <= maxord; j++) N_VDestroy(cv_mem->cv_zn[j]);

  cv_mem->cv_lrw -= (maxord + 8)*cv_mem->cv_lrw1;
  cv_mem->cv_liw -= (maxord + 8)*cv_mem->cv_liw1;

  if (cv_mem->cv_VabstolMallocDone) {
    N_VDestroy(cv_mem->cv_Vabstol);
    cv_mem->cv_lrw -= cv_mem->cv_lrw1;
    cv_mem->cv_liw -= cv_mem->cv_liw1;
  }

  if (cv_mem->cv_constraintsMallocDone) {
    N_VDestroy(cv_mem->cv_constraints);
    cv_mem->cv_lrw -= cv_mem->cv_lrw1;
    cv_mem->cv_liw -= cv_mem->cv_liw1;
  }
}

/*
 * CVodeQuadAllocVectors
 *
 * NOTE: Space for ewtQ is allocated even when errconQ=SUNFALSE, 
 * although in this case, ewtQ is never used. The reason for this
 * decision is to allow the user to re-initialize the quadrature
 * computation with errconQ=SUNTRUE, after an initialization with
 * errconQ=SUNFALSE, without new memory allocation within 
 * CVodeQuadReInit.
 */

static booleantype cvQuadAllocVectors(CVodeMem cv_mem, N_Vector tmpl) 
{
  int i, j;

  /* Allocate ewtQ */
  cv_mem->cv_ewtQ = N_VClone(tmpl);
  if (cv_mem->cv_ewtQ == NULL) {
    return(SUNFALSE);
  }
  
  /* Allocate acorQ */
  cv_mem->cv_acorQ = N_VClone(tmpl);
  if (cv_mem->cv_acorQ == NULL) {
    N_VDestroy(cv_mem->cv_ewtQ);
    return(SUNFALSE);
  }

  /* Allocate yQ */
  cv_mem->cv_yQ = N_VClone(tmpl);
  if (cv_mem->cv_yQ == NULL) {
    N_VDestroy(cv_mem->cv_ewtQ);
    N_VDestroy(cv_mem->cv_acorQ);
    return(SUNFALSE);
  }

  /* Allocate tempvQ */
  cv_mem->cv_tempvQ = N_VClone(tmpl);
  if (cv_mem->cv_tempvQ == NULL) {
    N_VDestroy(cv_mem->cv_ewtQ);
    N_VDestroy(cv_mem->cv_acorQ);
    N_VDestroy(cv_mem->cv_yQ);
    return(SUNFALSE);
  }

  /* Allocate zQn[0] ... zQn[maxord] */

  for (j=0; j <= cv_mem->cv_qmax; j++) {
    cv_mem->cv_znQ[j] = N_VClone(tmpl);
    if (cv_mem->cv_znQ[j] == NULL) {
      N_VDestroy(cv_mem->cv_ewtQ);
      N_VDestroy(cv_mem->cv_acorQ);
      N_VDestroy(cv_mem->cv_yQ);
      N_VDestroy(cv_mem->cv_tempvQ);
      for (i=0; i < j; i++) N_VDestroy(cv_mem->cv_znQ[i]);
      return(SUNFALSE);
    }
  }

  /* Store the value of qmax used here */
  cv_mem->cv_qmax_allocQ = cv_mem->cv_qmax;

  /* Update solver workspace lengths */
  cv_mem->cv_lrw += (cv_mem->cv_qmax + 5)*cv_mem->cv_lrw1Q;
  cv_mem->cv_liw += (cv_mem->cv_qmax + 5)*cv_mem->cv_liw1Q;

  return(SUNTRUE);
}

/*
 * cvQuadFreeVectors
 *
 * This routine frees the CVODES vectors allocated in cvQuadAllocVectors.
 */

static void cvQuadFreeVectors(CVodeMem cv_mem)
{
  int j, maxord;
  
  maxord = cv_mem->cv_qmax_allocQ;

  N_VDestroy(cv_mem->cv_ewtQ);
  N_VDestroy(cv_mem->cv_acorQ);
  N_VDestroy(cv_mem->cv_yQ);
  N_VDestroy(cv_mem->cv_tempvQ);
  
  for (j=0; j<=maxord; j++) N_VDestroy(cv_mem->cv_znQ[j]);

  cv_mem->cv_lrw -= (maxord + 5)*cv_mem->cv_lrw1Q;
  cv_mem->cv_liw -= (maxord + 5)*cv_mem->cv_liw1Q;

  if (cv_mem->cv_VabstolQMallocDone) {
    N_VDestroy(cv_mem->cv_VabstolQ);
    cv_mem->cv_lrw -= cv_mem->cv_lrw1Q;
    cv_mem->cv_liw -= cv_mem->cv_liw1Q;
  }

  cv_mem->cv_VabstolQMallocDone = SUNFALSE;
}

/*
 * cvSensAllocVectors
 *
 * Create (through duplication) N_Vectors used for sensitivity analysis, 
 * using the N_Vector 'tmpl' as a template.
 */

static booleantype cvSensAllocVectors(CVodeMem cv_mem, N_Vector tmpl) 
{
  int i, j;
  
  /* Allocate yS */
  cv_mem->cv_yS = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
  if (cv_mem->cv_yS == NULL) {
    return(SUNFALSE);
  }

  /* Allocate ewtS */
  cv_mem->cv_ewtS = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
  if (cv_mem->cv_ewtS == NULL) {
    N_VDestroyVectorArray(cv_mem->cv_yS, cv_mem->cv_Ns);
    return(SUNFALSE);
  }
  
  /* Allocate acorS */
  cv_mem->cv_acorS = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
  if (cv_mem->cv_acorS == NULL) {
    N_VDestroyVectorArray(cv_mem->cv_yS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_ewtS, cv_mem->cv_Ns);
    return(SUNFALSE);
  }
  
  /* Allocate tempvS */
  cv_mem->cv_tempvS = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
  if (cv_mem->cv_tempvS == NULL) {
    N_VDestroyVectorArray(cv_mem->cv_yS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_ewtS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_acorS, cv_mem->cv_Ns);
    return(SUNFALSE);
  }
    
  /* Allocate ftempS */
  cv_mem->cv_ftempS = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
  if (cv_mem->cv_ftempS == NULL) {
    N_VDestroyVectorArray(cv_mem->cv_yS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_ewtS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_acorS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_tempvS, cv_mem->cv_Ns);
    return(SUNFALSE);
  }
  
  /* Allocate znS */
  for (j=0; j<=cv_mem->cv_qmax; j++) {
    cv_mem->cv_znS[j] = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
    if (cv_mem->cv_znS[j] == NULL) {
      N_VDestroyVectorArray(cv_mem->cv_yS, cv_mem->cv_Ns);
      N_VDestroyVectorArray(cv_mem->cv_ewtS, cv_mem->cv_Ns);
      N_VDestroyVectorArray(cv_mem->cv_acorS, cv_mem->cv_Ns);
      N_VDestroyVectorArray(cv_mem->cv_tempvS, cv_mem->cv_Ns);
      N_VDestroyVectorArray(cv_mem->cv_ftempS, cv_mem->cv_Ns);
      for (i=0; i<j; i++)
        N_VDestroyVectorArray(cv_mem->cv_znS[i], cv_mem->cv_Ns);
      return(SUNFALSE);
    }
  }
  
  /* Allocate space for pbar and plist */
  cv_mem->cv_pbar = NULL;
  cv_mem->cv_pbar = (realtype *)malloc(cv_mem->cv_Ns*sizeof(realtype));
  if (cv_mem->cv_pbar == NULL) {
    N_VDestroyVectorArray(cv_mem->cv_yS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_ewtS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_acorS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_tempvS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_ftempS, cv_mem->cv_Ns);
    for (i=0; i<=cv_mem->cv_qmax; i++)
      N_VDestroyVectorArray(cv_mem->cv_znS[i], cv_mem->cv_Ns);
    return(SUNFALSE);
  }

  cv_mem->cv_plist = NULL;
  cv_mem->cv_plist = (int *)malloc(cv_mem->cv_Ns*sizeof(int));
  if (cv_mem->cv_plist == NULL) {
    N_VDestroyVectorArray(cv_mem->cv_yS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_ewtS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_acorS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_tempvS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_ftempS, cv_mem->cv_Ns);
    for (i=0; i<=cv_mem->cv_qmax; i++)
      N_VDestroyVectorArray(cv_mem->cv_znS[i], cv_mem->cv_Ns);
    free(cv_mem->cv_pbar); cv_mem->cv_pbar = NULL;
    return(SUNFALSE);
  }

  /* Update solver workspace lengths */
  cv_mem->cv_lrw += (cv_mem->cv_qmax + 6)*cv_mem->cv_Ns*cv_mem->cv_lrw1 + cv_mem->cv_Ns;
  cv_mem->cv_liw += (cv_mem->cv_qmax + 6)*cv_mem->cv_Ns*cv_mem->cv_liw1 + cv_mem->cv_Ns;
  
  /* Store the value of qmax used here */
  cv_mem->cv_qmax_allocS = cv_mem->cv_qmax;

  return(SUNTRUE);
}

/*
 * cvSensFreeVectors
 *
 * This routine frees the CVODES vectors allocated in cvSensAllocVectors.
 */

static void cvSensFreeVectors(CVodeMem cv_mem) 
{
  int j, maxord;
  
  maxord = cv_mem->cv_qmax_allocS;

  N_VDestroyVectorArray(cv_mem->cv_yS, cv_mem->cv_Ns);
  N_VDestroyVectorArray(cv_mem->cv_ewtS, cv_mem->cv_Ns);
  N_VDestroyVectorArray(cv_mem->cv_acorS, cv_mem->cv_Ns);
  N_VDestroyVectorArray(cv_mem->cv_tempvS, cv_mem->cv_Ns);
  N_VDestroyVectorArray(cv_mem->cv_ftempS, cv_mem->cv_Ns);
  
  for (j=0; j<=maxord; j++)
    N_VDestroyVectorArray(cv_mem->cv_znS[j], cv_mem->cv_Ns);  

  free(cv_mem->cv_pbar); cv_mem->cv_pbar = NULL;
  free(cv_mem->cv_plist); cv_mem->cv_plist = NULL;

  cv_mem->cv_lrw -= (maxord + 6)*cv_mem->cv_Ns*cv_mem->cv_lrw1 + cv_mem->cv_Ns;
  cv_mem->cv_liw -= (maxord + 6)*cv_mem->cv_Ns*cv_mem->cv_liw1 + cv_mem->cv_Ns;

  if (cv_mem->cv_VabstolSMallocDone) {
    N_VDestroyVectorArray(cv_mem->cv_VabstolS, cv_mem->cv_Ns);
    cv_mem->cv_lrw -= cv_mem->cv_Ns*cv_mem->cv_lrw1;
    cv_mem->cv_liw -= cv_mem->cv_Ns*cv_mem->cv_liw1;
  }
  if (cv_mem->cv_SabstolSMallocDone) {
    free(cv_mem->cv_SabstolS); cv_mem->cv_SabstolS = NULL;
    cv_mem->cv_lrw -= cv_mem->cv_Ns;
  }
  cv_mem->cv_VabstolSMallocDone = SUNFALSE;
  cv_mem->cv_SabstolSMallocDone = SUNFALSE;
}

/*
 * cvQuadSensAllocVectors
 *
 * Create (through duplication) N_Vectors used for quadrature sensitivity analysis, 
 * using the N_Vector 'tmpl' as a template.
 */

static booleantype cvQuadSensAllocVectors(CVodeMem cv_mem, N_Vector tmpl) 
{
  int i, j;

  /* Allocate ftempQ */
  cv_mem->cv_ftempQ = N_VClone(tmpl);
  if (cv_mem->cv_ftempQ == NULL) {
    return(SUNFALSE);
  }

  /* Allocate yQS */
  cv_mem->cv_yQS = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
  if (cv_mem->cv_yQS == NULL) {
    N_VDestroy(cv_mem->cv_ftempQ);
    return(SUNFALSE);
  }

  /* Allocate ewtQS */
  cv_mem->cv_ewtQS = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
  if (cv_mem->cv_ewtQS == NULL) {
    N_VDestroy(cv_mem->cv_ftempQ);
    N_VDestroyVectorArray(cv_mem->cv_yQS, cv_mem->cv_Ns);
    return(SUNFALSE);
  }

  /* Allocate acorQS */
  cv_mem->cv_acorQS = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
  if (cv_mem->cv_acorQS == NULL) {
    N_VDestroy(cv_mem->cv_ftempQ);
    N_VDestroyVectorArray(cv_mem->cv_yQS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_ewtQS, cv_mem->cv_Ns);
    return(SUNFALSE);
  }
  
  /* Allocate tempvQS */
  cv_mem->cv_tempvQS = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
  if (cv_mem->cv_tempvQS == NULL) {
    N_VDestroy(cv_mem->cv_ftempQ);
    N_VDestroyVectorArray(cv_mem->cv_yQS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_ewtQS, cv_mem->cv_Ns);
    N_VDestroyVectorArray(cv_mem->cv_acorQS, cv_mem->cv_Ns);
    return(SUNFALSE);
  }

  /* Allocate znQS */
  for (j=0; j<=cv_mem->cv_qmax; j++) {
    cv_mem->cv_znQS[j] = N_VCloneVectorArray(cv_mem->cv_Ns, tmpl);
    if (cv_mem->cv_znQS[j] == NULL) {
      N_VDestroy(cv_mem->cv_ftempQ);
      N_VDestroyVectorArray(cv_mem->cv_yQS, cv_mem->cv_Ns);
      N_VDestroyVectorArray(cv_mem->cv_ewtQS, cv_mem->cv_Ns);
      N_VDestroyVectorArray(cv_mem->cv_acorQS, cv_mem->cv_Ns);
      N_VDestroyVectorArray(cv_mem->cv_tempvQS, cv_mem->cv_Ns);
      for (i=0; i<j; i++)
        N_VDestroyVectorArray(cv_mem->cv_znQS[i], cv_mem->cv_Ns);
      return(SUNFALSE);
    }
  }

  /* Update solver workspace lengths */
  cv_mem->cv_lrw += (cv_mem->cv_qmax + 5)*cv_mem->cv_Ns*cv_mem->cv_lrw1Q;
  cv_mem->cv_liw += (cv_mem->cv_qmax + 5)*cv_mem->cv_Ns*cv_mem->cv_liw1Q;

  /* Store the value of qmax used here */
  cv_mem->cv_qmax_allocQS = cv_mem->cv_qmax;

  return(SUNTRUE);
}

/*
 * cvQuadSensFreeVectors
 *
 * This routine frees the CVODES vectors allocated in cvQuadSensAllocVectors.
 */

static void cvQuadSensFreeVectors(CVodeMem cv_mem)
{
  int j, maxord;
  
  maxord = cv_mem->cv_qmax_allocQS;

  N_VDestroy(cv_mem->cv_ftempQ);

  N_VDestroyVectorArray(cv_mem->cv_yQS, cv_mem->cv_Ns);
  N_VDestroyVectorArray(cv_mem->cv_ewtQS, cv_mem->cv_Ns);
  N_VDestroyVectorArray(cv_mem->cv_acorQS, cv_mem->cv_Ns);
  N_VDestroyVectorArray(cv_mem->cv_tempvQS, cv_mem->cv_Ns);
  
  for (j=0; j<=maxord; j++)
    N_VDestroyVectorArray(cv_mem->cv_znQS[j], cv_mem->cv_Ns);  

  cv_mem->cv_lrw -= (maxord + 5)*cv_mem->cv_Ns*cv_mem->cv_lrw1Q;
  cv_mem->cv_liw -= (maxord + 5)*cv_mem->cv_Ns*cv_mem->cv_liw1Q;

  if (cv_mem->cv_VabstolQSMallocDone) {
    N_VDestroyVectorArray(cv_mem->cv_VabstolQS, cv_mem->cv_Ns);
    cv_mem->cv_lrw -= cv_mem->cv_Ns*cv_mem->cv_lrw1Q;
    cv_mem->cv_liw -= cv_mem->cv_Ns*cv_mem->cv_liw1Q;
  }
  if (cv_mem->cv_SabstolQSMallocDone) {
    free(cv_mem->cv_SabstolQS); cv_mem->cv_SabstolQS = NULL;
    cv_mem->cv_lrw -= cv_mem->cv_Ns;
  }
  cv_mem->cv_VabstolQSMallocDone = SUNFALSE;
  cv_mem->cv_SabstolQSMallocDone = SUNFALSE;

}


/* 
 * -----------------------------------------------------------------
 * Initial stepsize calculation
 * -----------------------------------------------------------------
 */

/*
 * cvHin
 *
 * This routine computes a tentative initial step size h0. 
 * If tout is too close to tn (= t0), then cvHin returns CV_TOO_CLOSE
 * and h remains uninitialized. Note that here tout is either the value
 * passed to CVode at the first call or the value of tstop (if tstop is 
 * enabled and it is closer to t0=tn than tout).
 * If any RHS function fails unrecoverably, cvHin returns CV_*RHSFUNC_FAIL.
 * If any RHS function fails recoverably too many times and recovery is
 * not possible, cvHin returns CV_REPTD_*RHSFUNC_ERR.
 * Otherwise, cvHin sets h to the chosen value h0 and returns CV_SUCCESS.
 *
 * The algorithm used seeks to find h0 as a solution of
 *       (WRMS norm of (h0^2 ydd / 2)) = 1, 
 * where ydd = estimated second derivative of y. Here, y includes
 * all variables considered in the error test.
 *
 * We start with an initial estimate equal to the geometric mean of the
 * lower and upper bounds on the step size.
 *
 * Loop up to MAX_ITERS times to find h0.
 * Stop if new and previous values differ by a factor < 2.
 * Stop if hnew/hg > 2 after one iteration, as this probably means
 * that the ydd value is bad because of cancellation error.        
 *  
 * For each new proposed hg, we allow MAX_ITERS attempts to
 * resolve a possible recoverable failure from f() by reducing
 * the proposed stepsize by a factor of 0.2. If a legal stepsize
 * still cannot be found, fall back on a previous value if possible,
 * or else return CV_REPTD_RHSFUNC_ERR.
 *
 * Finally, we apply a bias (0.5) and verify that h0 is within bounds.
 */

static int cvHin(CVodeMem cv_mem, realtype tout)
{
  int retval, sign, count1, count2;
  realtype tdiff, tdist, tround, hlb, hub;
  realtype hg, hgs, hs, hnew, hrat, h0, yddnrm;
  booleantype hgOK;

  /* If tout is too close to tn, give up */
  
  if ((tdiff = tout-cv_mem->cv_tn) == ZERO) return(CV_TOO_CLOSE);
  
  sign = (tdiff > ZERO) ? 1 : -1;
  tdist = SUNRabs(tdiff);
  tround = cv_mem->cv_uround * SUNMAX(SUNRabs(cv_mem->cv_tn), SUNRabs(tout));

  if (tdist < TWO*tround) return(CV_TOO_CLOSE);
  
  /* 
     Set lower and upper bounds on h0, and take geometric mean 
     as first trial value.
     Exit with this value if the bounds cross each other.
  */

  hlb = HLB_FACTOR * tround;
  hub = cvUpperBoundH0(cv_mem, tdist);

  hg  = SUNRsqrt(hlb*hub);

  if (hub < hlb) {
    if (sign == -1) cv_mem->cv_h = -hg;
    else            cv_mem->cv_h =  hg;
    return(CV_SUCCESS);
  }
  
  /* Outer loop */

  hs = hg;         /* safeguard against 'uninitialized variable' warning */

  for(count1 = 1; count1 <= MAX_ITERS; count1++) {

    /* Attempts to estimate ydd */

    hgOK = SUNFALSE;

    for (count2 = 1; count2 <= MAX_ITERS; count2++) {
      hgs = hg*sign;
      retval = cvYddNorm(cv_mem, hgs, &yddnrm);
      /* If a RHS function failed unrecoverably, give up */
      if (retval < 0) return(retval);
      /* If successful, we can use ydd */
      if (retval == CV_SUCCESS) {hgOK = SUNTRUE; break;}
      /* A RHS function failed recoverably; cut step size and test it again */
      hg *= POINT2;
    }

    /* If a RHS function failed recoverably MAX_ITERS times */

    if (!hgOK) {
      /* Exit if this is the first or second pass. No recovery possible */
      if (count1 <= 2) {
        if (retval == RHSFUNC_RECVR)  return(CV_REPTD_RHSFUNC_ERR);
        if (retval == QRHSFUNC_RECVR) return(CV_REPTD_QRHSFUNC_ERR);
        if (retval == SRHSFUNC_RECVR) return(CV_REPTD_SRHSFUNC_ERR);
      }
      /* We have a fall-back option. The value hs is a previous hnew which
         passed through f(). Use it and break */
      hnew = hs;
      break;
    }

    /* The proposed step size is feasible. Save it. */
    hs = hg;

    /* Propose new step size */
    hnew = (yddnrm*hub*hub > TWO) ? SUNRsqrt(TWO/yddnrm) : SUNRsqrt(hg*hub);
    
    /* If last pass, stop now with hnew */
    if (count1 == MAX_ITERS) break;
    
    hrat = hnew/hg;
    
    /* Accept hnew if it does not differ from hg by more than a factor of 2 */
    if ((hrat > HALF) && (hrat < TWO)) break;

    /* After one pass, if ydd seems to be bad, use fall-back value. */
    if ((count1 > 1) && (hrat > TWO)) {
      hnew = hg;
      break;
    }

    /* Send this value back through f() */
    hg = hnew;

  }

  /* Apply bounds, bias factor, and attach sign */

  h0 = H_BIAS*hnew;
  if (h0 < hlb) h0 = hlb;
  if (h0 > hub) h0 = hub;
  if (sign == -1) h0 = -h0;
  cv_mem->cv_h = h0;

  return(CV_SUCCESS);
}

/*
 * cvUpperBoundH0
 *
 * This routine sets an upper bound on abs(h0) based on
 * tdist = tn - t0 and the values of y[i]/y'[i].
 */

static realtype cvUpperBoundH0(CVodeMem cv_mem, realtype tdist)
{
  realtype hub_inv, hubQ_inv, hubS_inv, hubQS_inv, hub;
  N_Vector temp1, temp2;
  N_Vector tempQ1, tempQ2;
  N_Vector *tempS1;
  N_Vector *tempQS1;
  int is;

  /* 
   * Bound based on |y|/|y'| -- allow at most an increase of
   * HUB_FACTOR in y0 (based on a forward Euler step). The weight 
   * factor is used as a safeguard against zero components in y0. 
   */

  temp1 = cv_mem->cv_tempv;
  temp2 = cv_mem->cv_acor;

  N_VAbs(cv_mem->cv_zn[0], temp2);
  cv_mem->cv_efun(cv_mem->cv_zn[0], temp1, cv_mem->cv_e_data);
  N_VInv(temp1, temp1);
  N_VLinearSum(HUB_FACTOR, temp2, ONE, temp1, temp1);

  N_VAbs(cv_mem->cv_zn[1], temp2);

  N_VDiv(temp2, temp1, temp1);
  hub_inv = N_VMaxNorm(temp1);

  /* Bound based on |yQ|/|yQ'| */
  
  if (cv_mem->cv_quadr && cv_mem->cv_errconQ) {

    tempQ1 = cv_mem->cv_tempvQ;
    tempQ2 = cv_mem->cv_acorQ;

    N_VAbs(cv_mem->cv_znQ[0], tempQ2);
    cvQuadEwtSet(cv_mem, cv_mem->cv_znQ[0], tempQ1);
    N_VInv(tempQ1, tempQ1);
    N_VLinearSum(HUB_FACTOR, tempQ2, ONE, tempQ1, tempQ1);
    
    N_VAbs(cv_mem->cv_znQ[1], tempQ2);
    
    N_VDiv(tempQ2, tempQ1, tempQ1);
    hubQ_inv = N_VMaxNorm(tempQ1);

    if (hubQ_inv > hub_inv) hub_inv = hubQ_inv;

  }

  /* Bound based on |yS|/|yS'| */

  if (cv_mem->cv_sensi && cv_mem->cv_errconS) {

    tempS1 = cv_mem->cv_acorS;
    cvSensEwtSet(cv_mem, cv_mem->cv_znS[0], tempS1);

    for (is=0; is<cv_mem->cv_Ns; is++) {

      N_VAbs(cv_mem->cv_znS[0][is], temp2);
      N_VInv(tempS1[is], temp1);
      N_VLinearSum(HUB_FACTOR, temp2, ONE, temp1, temp1);
      
      N_VAbs(cv_mem->cv_znS[1][is], temp2);
      
      N_VDiv(temp2, temp1, temp1);
      hubS_inv = N_VMaxNorm(temp1);

      if (hubS_inv > hub_inv) hub_inv = hubS_inv;

    }

  }

  /* Bound based on |yQS|/|yQS'| */

  if (cv_mem->cv_quadr_sensi && cv_mem->cv_errconQS) {

    tempQ1 = cv_mem->cv_tempvQ;
    tempQ2 = cv_mem->cv_acorQ;

    tempQS1 = cv_mem->cv_acorQS;
    cvQuadSensEwtSet(cv_mem, cv_mem->cv_znQS[0], tempQS1);

    for (is=0; is<cv_mem->cv_Ns; is++) {

      N_VAbs(cv_mem->cv_znQS[0][is], tempQ2);
      N_VInv(tempQS1[is], tempQ1);
      N_VLinearSum(HUB_FACTOR, tempQ2, ONE, tempQ1, tempQ1);
      
      N_VAbs(cv_mem->cv_znQS[1][is], tempQ2);
      
      N_VDiv(tempQ2, tempQ1, tempQ1);
      hubQS_inv = N_VMaxNorm(tempQ1);

      if (hubQS_inv > hub_inv) hub_inv = hubQS_inv;

    }

  }


  /*
   * bound based on tdist -- allow at most a step of magnitude
   * HUB_FACTOR * tdist
   */
  
  hub = HUB_FACTOR*tdist;

  /* Use the smaler of the two */

  if (hub*hub_inv > ONE) hub = ONE/hub_inv;

  return(hub);
}

/*
 * cvYddNorm
 *
 * This routine computes an estimate of the second derivative of Y
 * using a difference quotient, and returns its WRMS norm.
 *
 * Y contains all variables included in the error test. 
 */

static int cvYddNorm(CVodeMem cv_mem, realtype hg, realtype *yddnrm)
{
  int retval;
  N_Vector wrk1, wrk2;
  
  /* y <- h*y'(t) + y(t) */
  
  N_VLinearSum(hg, cv_mem->cv_zn[1], ONE, cv_mem->cv_zn[0], cv_mem->cv_y);
  
  if (cv_mem->cv_sensi && cv_mem->cv_errconS) {
    retval = N_VLinearSumVectorArray(cv_mem->cv_Ns,
                                     hg,  cv_mem->cv_znS[1],
                                     ONE, cv_mem->cv_znS[0],
                                     cv_mem->cv_yS);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
  }
  
  /* tempv <- f(t+h, h*y'(t)+y(t)) */

  retval = cv_mem->cv_f(cv_mem->cv_tn+hg, cv_mem->cv_y,
                        cv_mem->cv_tempv, cv_mem->cv_user_data);
  cv_mem->cv_nfe++;
  if (retval < 0) return(CV_RHSFUNC_FAIL);
  if (retval > 0) return(RHSFUNC_RECVR);

  if (cv_mem->cv_quadr && cv_mem->cv_errconQ) {
    retval = cv_mem->cv_fQ(cv_mem->cv_tn+hg, cv_mem->cv_y,
                           cv_mem->cv_tempvQ, cv_mem->cv_user_data);
    cv_mem->cv_nfQe++;
    if (retval < 0) return(CV_QRHSFUNC_FAIL);
    if (retval > 0) return(QRHSFUNC_RECVR);
  }

  if (cv_mem->cv_sensi && cv_mem->cv_errconS) {
    wrk1 = cv_mem->cv_ftemp;
    wrk2 = cv_mem->cv_acor;
    retval = cvSensRhsWrapper(cv_mem, cv_mem->cv_tn+hg, cv_mem->cv_y,
                              cv_mem->cv_tempv, cv_mem->cv_yS,
                              cv_mem->cv_tempvS, wrk1, wrk2);
    if (retval < 0) return(CV_SRHSFUNC_FAIL);
    if (retval > 0) return(SRHSFUNC_RECVR);
  }  

  if (cv_mem->cv_quadr_sensi && cv_mem->cv_errconQS) {
    wrk1 = cv_mem->cv_ftemp;
    wrk2 = cv_mem->cv_acorQ;
    retval = cv_mem->cv_fQS(cv_mem->cv_Ns, cv_mem->cv_tn+hg,
                            cv_mem->cv_y, cv_mem->cv_yS,
                            cv_mem->cv_tempvQ, cv_mem->cv_tempvQS,
                            cv_mem->cv_fQS_data, wrk1, wrk2);

    cv_mem->cv_nfQSe++;
    if (retval < 0) return(CV_QSRHSFUNC_FAIL);
    if (retval > 0) return(QSRHSFUNC_RECVR);
  } 

  /* Load estimate of ||y''|| into tempv:
   * tempv <-  (1/h) * f(t+h, h*y'(t)+y(t)) - y'(t) */
  
  N_VLinearSum(ONE/hg, cv_mem->cv_tempv, -ONE/hg, cv_mem->cv_zn[1], cv_mem->cv_tempv);

  *yddnrm = N_VWrmsNorm(cv_mem->cv_tempv, cv_mem->cv_ewt);

  if (cv_mem->cv_quadr && cv_mem->cv_errconQ) {
    N_VLinearSum(ONE/hg, cv_mem->cv_tempvQ, -ONE/hg, cv_mem->cv_znQ[1],
                 cv_mem->cv_tempvQ);

    *yddnrm = cvQuadUpdateNorm(cv_mem, *yddnrm, cv_mem->cv_tempvQ,
                               cv_mem->cv_ewtQ);
  }

  if (cv_mem->cv_sensi && cv_mem->cv_errconS) {
    retval = N_VLinearSumVectorArray(cv_mem->cv_Ns,
                                     ONE/hg,  cv_mem->cv_tempvS,
                                     -ONE/hg, cv_mem->cv_znS[1],
                                     cv_mem->cv_tempvS);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

    *yddnrm = cvSensUpdateNorm(cv_mem, *yddnrm, cv_mem->cv_tempvS,
                               cv_mem->cv_ewtS);
  }

  if (cv_mem->cv_quadr_sensi && cv_mem->cv_errconQS) {
    retval = N_VLinearSumVectorArray(cv_mem->cv_Ns,
                                     ONE/hg,  cv_mem->cv_tempvQS,
                                     -ONE/hg, cv_mem->cv_znQS[1],
                                     cv_mem->cv_tempvQS);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

    *yddnrm = cvQuadSensUpdateNorm(cv_mem, *yddnrm, cv_mem->cv_tempvQS,
                                   cv_mem->cv_ewtQS);
  }

  return(CV_SUCCESS);
}

/* 
 * -----------------------------------------------------------------
 * Initial setup
 * -----------------------------------------------------------------
 */

/*  
 * cvInitialSetup
 *
 * This routine performs input consistency checks at the first step.
 * If needed, it also checks the linear solver module and calls the
 * linear solver initialization routine.
 */

static int cvInitialSetup(CVodeMem cv_mem)
{
  int ier;
  booleantype conOK;

  /* Did the user specify tolerances? */
  if (cv_mem->cv_itol == CV_NN) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                   MSGCV_NO_TOL);
    return(CV_ILL_INPUT);
  }

  /* Set data for efun */
  if (cv_mem->cv_user_efun) cv_mem->cv_e_data = cv_mem->cv_user_data;
  else                      cv_mem->cv_e_data = cv_mem;

  /* Check to see if y0 satisfies constraints */
  if (cv_mem->cv_constraintsSet) {

    if (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_SIMULTANEOUS)) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup", MSGCV_BAD_ISM_CONSTR);
      return(CV_ILL_INPUT);
    }

    conOK = N_VConstrMask(cv_mem->cv_constraints, cv_mem->cv_zn[0], cv_mem->cv_tempv);
    if (!conOK) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup", MSGCV_Y0_FAIL_CONSTR);
      return(CV_ILL_INPUT);
    }
  }

  /* Load initial error weights */
  ier = cv_mem->cv_efun(cv_mem->cv_zn[0], cv_mem->cv_ewt,
                        cv_mem->cv_e_data);
  if (ier != 0) {
    if (cv_mem->cv_itol == CV_WF) 
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                     MSGCV_EWT_FAIL);
    else
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                     MSGCV_BAD_EWT);
    return(CV_ILL_INPUT);
  }
  
  /* Quadrature initial setup */

  if (cv_mem->cv_quadr && cv_mem->cv_errconQ) {

    /* Did the user specify tolerances? */
    if (cv_mem->cv_itolQ == CV_NN) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                     MSGCV_NO_TOLQ);
      return(CV_ILL_INPUT);
    }

    /* Load ewtQ */
    ier = cvQuadEwtSet(cv_mem, cv_mem->cv_znQ[0], cv_mem->cv_ewtQ);
    if (ier != 0) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                     MSGCV_BAD_EWTQ);
      return(CV_ILL_INPUT);
    }

  }

  if (!cv_mem->cv_quadr) cv_mem->cv_errconQ = SUNFALSE;

  /* Forward sensitivity initial setup */

  if (cv_mem->cv_sensi) {

    /* Did the user specify tolerances? */
    if (cv_mem->cv_itolS == CV_NN) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                     MSGCV_NO_TOLS);
      return(CV_ILL_INPUT);
    }

    /* If using the internal DQ functions, we must have access to the problem parameters */
    if(cv_mem->cv_fSDQ && (cv_mem->cv_p == NULL)) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                     MSGCV_NULL_P);
      return(CV_ILL_INPUT);
    }

    /* Load ewtS */
    ier = cvSensEwtSet(cv_mem, cv_mem->cv_znS[0], cv_mem->cv_ewtS);
    if (ier != 0) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                     MSGCV_BAD_EWTS);
      return(CV_ILL_INPUT);
    }

  }

  /* FSA of quadrature variables */

  if (cv_mem->cv_quadr_sensi) {

    /* If using the internal DQ functions, we must have access to fQ
     * (i.e. quadrature integration must be enabled) and to the problem parameters */

    if (cv_mem->cv_fQSDQ) {

      /* Test if quadratures are defined, so we can use fQ */
      if (!cv_mem->cv_quadr) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                       MSGCV_NULL_FQ);
        return(CV_ILL_INPUT);
      }

      /* Test if we have the problem parameters */
      if(cv_mem->cv_p == NULL) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                       MSGCV_NULL_P);
        return(CV_ILL_INPUT);
      }

    }

    if (cv_mem->cv_errconQS) {
      
      /* Did the user specify tolerances? */
      if (cv_mem->cv_itolQS == CV_NN) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                       MSGCV_NO_TOLQS);
        return(CV_ILL_INPUT);
      }

      /* If needed, did the user provide quadrature tolerances? */
      if ( (cv_mem->cv_itolQS == CV_EE) && (cv_mem->cv_itolQ == CV_NN) ) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                       MSGCV_NO_TOLQ);
        return(CV_ILL_INPUT);
      }

      /* Load ewtQS */
      ier = cvQuadSensEwtSet(cv_mem, cv_mem->cv_znQS[0], cv_mem->cv_ewtQS);
      if (ier != 0) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "cvInitialSetup",
                       MSGCV_BAD_EWTQS);
        return(CV_ILL_INPUT);
      }

    }

  } else {

    cv_mem->cv_errconQS = SUNFALSE;

  }

  /* Call linit function (if it exists) */
  if (cv_mem->cv_linit != NULL) {
    ier = cv_mem->cv_linit(cv_mem);
    if (ier != 0) {
      cvProcessError(cv_mem, CV_LINIT_FAIL, "CVODES", "cvInitialSetup",
                     MSGCV_LINIT_FAIL);
      return(CV_LINIT_FAIL);
    }
  }

  /* Initialize the nonlinear solver (must occur after linear solver is
     initialized) so that lsetup and lsolve pointer have been set */

  /* always initialize the ODE NLS in case the user disables sensitivities */
  ier = cvNlsInit(cv_mem);
  if (ier != 0) {
    cvProcessError(cv_mem, CV_NLS_INIT_FAIL, "CVODES",
                   "cvInitialSetup", MSGCV_NLS_INIT_FAIL);
    return(CV_NLS_INIT_FAIL);
  }

  if (cv_mem->NLSsim != NULL) {
    ier = cvNlsInitSensSim(cv_mem);
    if (ier != 0) {
      cvProcessError(cv_mem, CV_NLS_INIT_FAIL, "CVODES",
                     "cvInitialSetup", MSGCV_NLS_INIT_FAIL);
      return(CV_NLS_INIT_FAIL);
    }
  }

  if (cv_mem->NLSstg != NULL) {
    ier = cvNlsInitSensStg(cv_mem);
    if (ier != 0) {
      cvProcessError(cv_mem, CV_NLS_INIT_FAIL, "CVODES",
                     "cvInitialSetup", MSGCV_NLS_INIT_FAIL);
      return(CV_NLS_INIT_FAIL);
    }
  }

  if (cv_mem->NLSstg1 != NULL) {
    ier = cvNlsInitSensStg1(cv_mem);
    if (ier != 0) {
      cvProcessError(cv_mem, CV_NLS_INIT_FAIL, "CVODES",
                     "cvInitialSetup", MSGCV_NLS_INIT_FAIL);
      return(CV_NLS_INIT_FAIL);
    }
  }

  return(CV_SUCCESS);
}

/*
 * cvEwtSet
 *
 * This routine is responsible for setting the error weight vector ewt,
 * according to tol_type, as follows:
 *
 * (1) ewt[i] = 1 / (reltol * SUNRabs(ycur[i]) + *abstol), i=0,...,neq-1
 *     if tol_type = CV_SS
 * (2) ewt[i] = 1 / (reltol * SUNRabs(ycur[i]) + abstol[i]), i=0,...,neq-1
 *     if tol_type = CV_SV
 *
 * cvEwtSet returns 0 if ewt is successfully set as above to a
 * positive vector and -1 otherwise. In the latter case, ewt is
 * considered undefined.
 *
 * All the real work is done in the routines cvEwtSetSS, cvEwtSetSV.
 */

int cvEwtSet(N_Vector ycur, N_Vector weight, void *data)
{
  CVodeMem cv_mem;
  int flag = 0;

  /* data points to cv_mem here */

  cv_mem = (CVodeMem) data;

  switch(cv_mem->cv_itol) {
  case CV_SS:
    flag = cvEwtSetSS(cv_mem, ycur, weight);
    break;
  case CV_SV:
    flag = cvEwtSetSV(cv_mem, ycur, weight);
    break;
  }

  return(flag);
}

/*
 * cvEwtSetSS
 *
 * This routine sets ewt as decribed above in the case tol_type = CV_SS.
 * It tests for non-positive components before inverting. cvEwtSetSS
 * returns 0 if ewt is successfully set to a positive vector
 * and -1 otherwise. In the latter case, ewt is considered undefined.
 */

static int cvEwtSetSS(CVodeMem cv_mem, N_Vector ycur, N_Vector weight)
{
  N_VAbs(ycur, cv_mem->cv_tempv);
  N_VScale(cv_mem->cv_reltol, cv_mem->cv_tempv, cv_mem->cv_tempv);
  N_VAddConst(cv_mem->cv_tempv, cv_mem->cv_Sabstol, cv_mem->cv_tempv);
  if (N_VMin(cv_mem->cv_tempv) <= ZERO) return(-1);
  N_VInv(cv_mem->cv_tempv, weight);

  return(0);
}

/*
 * cvEwtSetSV
 *
 * This routine sets ewt as decribed above in the case tol_type = CV_SV.
 * It tests for non-positive components before inverting. cvEwtSetSV
 * returns 0 if ewt is successfully set to a positive vector
 * and -1 otherwise. In the latter case, ewt is considered undefined.
 */

static int cvEwtSetSV(CVodeMem cv_mem, N_Vector ycur, N_Vector weight)
{
  N_VAbs(ycur, cv_mem->cv_tempv);
  N_VLinearSum(cv_mem->cv_reltol, cv_mem->cv_tempv, ONE,
               cv_mem->cv_Vabstol, cv_mem->cv_tempv);
  if (N_VMin(cv_mem->cv_tempv) <= ZERO) return(-1);
  N_VInv(cv_mem->cv_tempv, weight);
  return(0);
}

/*
 * cvQuadEwtSet
 *
 */

static int cvQuadEwtSet(CVodeMem cv_mem, N_Vector qcur, N_Vector weightQ)
{
  int flag=0;

  switch (cv_mem->cv_itolQ) {
  case CV_SS:
    flag = cvQuadEwtSetSS(cv_mem, qcur, weightQ);
    break;
  case CV_SV:
    flag = cvQuadEwtSetSV(cv_mem, qcur, weightQ);
    break;
  }

  return(flag);

}

/*
 * cvQuadEwtSetSS
 *
 */

static int cvQuadEwtSetSS(CVodeMem cv_mem, N_Vector qcur, N_Vector weightQ)
{
  N_VAbs(qcur, cv_mem->cv_tempvQ);
  N_VScale(cv_mem->cv_reltolQ, cv_mem->cv_tempvQ, cv_mem->cv_tempvQ);
  N_VAddConst(cv_mem->cv_tempvQ, cv_mem->cv_SabstolQ, cv_mem->cv_tempvQ);
  if (N_VMin(cv_mem->cv_tempvQ) <= ZERO) return(-1);
  N_VInv(cv_mem->cv_tempvQ, weightQ);

  return(0);
}

/*
 * cvQuadEwtSetSV
 *
 */

static int cvQuadEwtSetSV(CVodeMem cv_mem, N_Vector qcur, N_Vector weightQ)
{
  N_VAbs(qcur, cv_mem->cv_tempvQ);
  N_VLinearSum(cv_mem->cv_reltolQ, cv_mem->cv_tempvQ, ONE,
               cv_mem->cv_VabstolQ, cv_mem->cv_tempvQ);
  if (N_VMin(cv_mem->cv_tempvQ) <= ZERO) return(-1);
  N_VInv(cv_mem->cv_tempvQ, weightQ);

  return(0);
}

/*
 * cvSensEwtSet
 *
 */

static int cvSensEwtSet(CVodeMem cv_mem, N_Vector *yScur, N_Vector *weightS)
{
  int flag=0;

  switch (cv_mem->cv_itolS) {
  case CV_EE:
    flag = cvSensEwtSetEE(cv_mem, yScur, weightS);
    break;
  case CV_SS:
    flag = cvSensEwtSetSS(cv_mem, yScur, weightS);
    break;
  case CV_SV:
    flag = cvSensEwtSetSV(cv_mem, yScur, weightS);
    break;
  }

  return(flag);
}

/*
 * cvSensEwtSetEE
 *
 * In this case, the error weight vector for the i-th sensitivity is set to
 *
 * ewtS_i = pbar_i * efun(pbar_i*yS_i)
 *
 * In other words, the scaled sensitivity pbar_i * yS_i has the same error
 * weight vector calculation as the solution vector.
 *
 */

static int cvSensEwtSetEE(CVodeMem cv_mem, N_Vector *yScur, N_Vector *weightS)
{
  int is;
  N_Vector pyS;
  int flag;

  /* Use tempvS[0] as temporary storage for the scaled sensitivity */
  pyS = cv_mem->cv_tempvS[0];

  for (is=0; is<cv_mem->cv_Ns; is++) {
    N_VScale(cv_mem->cv_pbar[is], yScur[is], pyS);
    flag = cv_mem->cv_efun(pyS, weightS[is], cv_mem->cv_e_data);
    if (flag != 0) return(-1);
    N_VScale(cv_mem->cv_pbar[is], weightS[is], weightS[is]);
  }

  return(0);
}

/*
 * cvSensEwtSetSS
 *
 */

static int cvSensEwtSetSS(CVodeMem cv_mem, N_Vector *yScur, N_Vector *weightS)
{
  int is;
  
  for (is=0; is<cv_mem->cv_Ns; is++) {
    N_VAbs(yScur[is], cv_mem->cv_tempv);
    N_VScale(cv_mem->cv_reltolS, cv_mem->cv_tempv, cv_mem->cv_tempv);
    N_VAddConst(cv_mem->cv_tempv, cv_mem->cv_SabstolS[is], cv_mem->cv_tempv);
    if (N_VMin(cv_mem->cv_tempv) <= ZERO) return(-1);
    N_VInv(cv_mem->cv_tempv, weightS[is]);
  }
  return(0);
}

/*
 * cvSensEwtSetSV
 *
 */

static int cvSensEwtSetSV(CVodeMem cv_mem, N_Vector *yScur, N_Vector *weightS)
{
  int is;
  
  for (is=0; is<cv_mem->cv_Ns; is++) {
    N_VAbs(yScur[is], cv_mem->cv_tempv);
    N_VLinearSum(cv_mem->cv_reltolS, cv_mem->cv_tempv, ONE,
                 cv_mem->cv_VabstolS[is], cv_mem->cv_tempv);
    if (N_VMin(cv_mem->cv_tempv) <= ZERO) return(-1);
    N_VInv(cv_mem->cv_tempv, weightS[is]);
  }

  return(0);
}

/*
 * cvQuadSensEwtSet
 *
 */

static int cvQuadSensEwtSet(CVodeMem cv_mem, N_Vector *yQScur, N_Vector *weightQS)
{
  int flag=0;

  switch (cv_mem->cv_itolQS) {
  case CV_EE:
    flag = cvQuadSensEwtSetEE(cv_mem, yQScur, weightQS);
    break;
  case CV_SS:
    flag = cvQuadSensEwtSetSS(cv_mem, yQScur, weightQS);
    break;
  case CV_SV:
    flag = cvQuadSensEwtSetSV(cv_mem, yQScur, weightQS);
    break;
  }

  return(flag);
}

/*
 * cvQuadSensEwtSetEE
 *
 * In this case, the error weight vector for the i-th quadrature sensitivity
 * is set to
 *
 * ewtQS_i = pbar_i * cvQuadEwtSet(pbar_i*yQS_i)
 *
 * In other words, the scaled sensitivity pbar_i * yQS_i has the same error
 * weight vector calculation as the quadrature vector.
 *
 */
static int cvQuadSensEwtSetEE(CVodeMem cv_mem, N_Vector *yQScur, N_Vector *weightQS)
{
  int is;
  N_Vector pyS;
  int flag;

  /* Use tempvQS[0] as temporary storage for the scaled sensitivity */
  pyS = cv_mem->cv_tempvQS[0];

  for (is=0; is<cv_mem->cv_Ns; is++) {
    N_VScale(cv_mem->cv_pbar[is], yQScur[is], pyS);
    flag = cvQuadEwtSet(cv_mem, pyS, weightQS[is]);
    if (flag != 0) return(-1);
    N_VScale(cv_mem->cv_pbar[is], weightQS[is], weightQS[is]);
  }

  return(0);
}

static int cvQuadSensEwtSetSS(CVodeMem cv_mem, N_Vector *yQScur, N_Vector *weightQS)
{
  int is;

  for (is=0; is<cv_mem->cv_Ns; is++) {
    N_VAbs(yQScur[is], cv_mem->cv_tempvQ);
    N_VScale(cv_mem->cv_reltolQS, cv_mem->cv_tempvQ, cv_mem->cv_tempvQ);
    N_VAddConst(cv_mem->cv_tempvQ, cv_mem->cv_SabstolQS[is], cv_mem->cv_tempvQ);
    if (N_VMin(cv_mem->cv_tempvQ) <= ZERO) return(-1);
    N_VInv(cv_mem->cv_tempvQ, weightQS[is]);
  }

  return(0);
}

static int cvQuadSensEwtSetSV(CVodeMem cv_mem, N_Vector *yQScur, N_Vector *weightQS)
{
  int is;
  
  for (is=0; is<cv_mem->cv_Ns; is++) {
    N_VAbs(yQScur[is], cv_mem->cv_tempvQ);
    N_VLinearSum(cv_mem->cv_reltolQS, cv_mem->cv_tempvQ, ONE,
                 cv_mem->cv_VabstolQS[is], cv_mem->cv_tempvQ);
    if (N_VMin(cv_mem->cv_tempvQ) <= ZERO) return(-1);
    N_VInv(cv_mem->cv_tempvQ, weightQS[is]);
  }

  return(0);
}



/* 
 * -----------------------------------------------------------------
 * Main cvStep function
 * -----------------------------------------------------------------
 */

/* 
 * cvStep
 *
 * This routine performs one internal cvode step, from tn to tn + h.
 * It calls other routines to do all the work.
 *
 * The main operations done here are as follows:
 * - preliminary adjustments if a new step size was chosen;
 * - prediction of the Nordsieck history array zn at tn + h;
 * - setting of multistep method coefficients and test quantities;
 * - solution of the nonlinear system;
 * - testing the local error;
 * - updating zn and other state data if successful;
 * - resetting stepsize and order for the next step.
 * - if SLDET is on, check for stability, reduce order if necessary.
 * On a failure in the nonlinear system solution or error test, the
 * step may be reattempted, depending on the nature of the failure.
 */

static int cvStep(CVodeMem cv_mem)
{
  realtype saved_t, dsm, dsmQ, dsmS, dsmQS;
  booleantype do_sensi_stg, do_sensi_stg1;
  int ncf, ncfS;
  int nef, nefQ, nefS, nefQS;
  int nflag, kflag, eflag;
  int retval, is;

  /* Are we computing sensitivities with a staggered approach? */

  do_sensi_stg  = (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_STAGGERED));
  do_sensi_stg1 = (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_STAGGERED1));

  /* Initialize local counters for convergence and error test failures */

  ncf  = nef  = 0;
  nefQ = nefQS = 0;
  ncfS = nefS = 0;
  if (do_sensi_stg1) {
    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_ncfS1[is] = 0;
  }

  /* If needed, adjust method parameters */

  if ((cv_mem->cv_nst > 0) && (cv_mem->cv_hprime != cv_mem->cv_h))
    cvAdjustParams(cv_mem);

  /* Looping point for attempts to take a step */

  saved_t = cv_mem->cv_tn;
  nflag = FIRST_CALL;

  for(;;) {  

    cvPredict(cv_mem);  
    cvSet(cv_mem);

    /* ------ Correct state variables ------ */
    
    nflag = cvNls(cv_mem, nflag);
    kflag = cvHandleNFlag(cv_mem, &nflag, saved_t, &ncf, &(cv_mem->cv_ncfn));

    /* Go back in loop if we need to predict again (nflag=PREV_CONV_FAIL) */
    if (kflag == PREDICT_AGAIN) continue;

    /* Return if nonlinear solve failed and recovery not possible. */
    if (kflag != DO_ERROR_TEST) return(kflag);

    /* Perform error test (nflag=CV_SUCCESS) */
    eflag = cvDoErrorTest(cv_mem, &nflag, saved_t, cv_mem->cv_acnrm,
                          &nef, &(cv_mem->cv_netf), &dsm);

    /* Go back in loop if we need to predict again (nflag=PREV_ERR_FAIL) */
    if (eflag == TRY_AGAIN) continue;

    /* Return if error test failed and recovery not possible. */
    if (eflag != CV_SUCCESS) return(eflag);

    /* Error test passed (eflag=CV_SUCCESS, nflag=CV_SUCCESS), go on */

    /* ------ Correct the quadrature variables ------ */

    if (cv_mem->cv_quadr) {

      ncf = nef = 0; /* reset counters for states */

      nflag = cvQuadNls(cv_mem);
      kflag = cvHandleNFlag(cv_mem, &nflag, saved_t, &ncf, &(cv_mem->cv_ncfn));

      if (kflag == PREDICT_AGAIN) continue;
      if (kflag != DO_ERROR_TEST) return(kflag);

      /* Error test on quadratures */
      if (cv_mem->cv_errconQ) {
        cv_mem->cv_acnrmQ = N_VWrmsNorm(cv_mem->cv_acorQ, cv_mem->cv_ewtQ);
        eflag = cvDoErrorTest(cv_mem, &nflag, saved_t, cv_mem->cv_acnrmQ,
                              &nefQ, &(cv_mem->cv_netfQ), &dsmQ);

        if (eflag == TRY_AGAIN) continue;
        if (eflag != CV_SUCCESS) return(eflag);

        /* Set dsm = max(dsm, dsmQ) to be used in cvPrepareNextStep */
        if (dsmQ > dsm) dsm = dsmQ;
      }

    }

    /* ------ Correct the sensitivity variables (STAGGERED or STAGGERED1) ------- */

    if (do_sensi_stg || do_sensi_stg1) {

      ncf = nef = 0;        /* reset counters for states     */
      if (cv_mem->cv_quadr) nefQ = 0;  /* reset counter for quadratures */

      /* Evaluate f at converged y, needed for future evaluations of sens. RHS 
       * If f() fails recoverably, treat it as a convergence failure and 
       * attempt the step again */

      retval = cv_mem->cv_f(cv_mem->cv_tn, cv_mem->cv_y,
                            cv_mem->cv_ftemp, cv_mem->cv_user_data);
      cv_mem->cv_nfe++;
      if (retval < 0) return(CV_RHSFUNC_FAIL);
      if (retval > 0) {
        nflag = PREV_CONV_FAIL;
        continue;
      }

      if (do_sensi_stg) {
        /* Nonlinear solve for sensitivities (all-at-once) */
        nflag = cvStgrNls(cv_mem);
        kflag = cvHandleNFlag(cv_mem, &nflag, saved_t, &ncfS,
                              &(cv_mem->cv_ncfnS));
      } else {
        /* Nonlinear solve for sensitivities (one-by-one) */
        for (is=0; is<cv_mem->cv_Ns; is++) { 
          cv_mem->sens_solve_idx = is;
          nflag = cvStgr1Nls(cv_mem, is); 
          kflag = cvHandleNFlag(cv_mem, &nflag, saved_t,
                                &(cv_mem->cv_ncfS1[is]),
                                &(cv_mem->cv_ncfnS1[is]));
          if (kflag != DO_ERROR_TEST) break; 
        }
      }

      if (kflag == PREDICT_AGAIN) continue;
      if (kflag != DO_ERROR_TEST) return(kflag);

      /* Error test on sensitivities */
      if (cv_mem->cv_errconS) {

        if (do_sensi_stg1)
          cv_mem->cv_acnrmS = cvSensNorm(cv_mem, cv_mem->cv_acorS, cv_mem->cv_ewtS);

        eflag = cvDoErrorTest(cv_mem, &nflag, saved_t, cv_mem->cv_acnrmS,
                              &nefS, &(cv_mem->cv_netfS), &dsmS);

        if (eflag == TRY_AGAIN)  continue;
        if (eflag != CV_SUCCESS) return(eflag);

        /* Set dsm = max(dsm, dsmS) to be used in cvPrepareNextStep */
        if (dsmS > dsm) dsm = dsmS;

      }

    }

    /* ------ Correct the quadrature sensitivity variables ------ */

    if (cv_mem->cv_quadr_sensi) {

      /* Reset local convergence and error test failure counters */
      ncf = nef = 0;
      if (cv_mem->cv_quadr) nefQ = 0;
      if (do_sensi_stg) ncfS = nefS = 0;
      if (do_sensi_stg1) {
        for (is=0; is<cv_mem->cv_Ns; is++)
          cv_mem->cv_ncfS1[is] = 0;
        nefS = 0;
      }

      /* Note that ftempQ contains yQdot evaluated at the converged y
       * (stored in cvQuadNls) and can be used in evaluating fQS */

      nflag = cvQuadSensNls(cv_mem);
      kflag = cvHandleNFlag(cv_mem, &nflag, saved_t, &ncf, &(cv_mem->cv_ncfn));

      if (kflag == PREDICT_AGAIN) continue;
      if (kflag != DO_ERROR_TEST) return(kflag);

      /* Error test on quadrature sensitivities */
      if (cv_mem->cv_errconQS) {
        cv_mem->cv_acnrmQS = cvQuadSensNorm(cv_mem, cv_mem->cv_acorQS,
                                            cv_mem->cv_ewtQS);
        eflag = cvDoErrorTest(cv_mem, &nflag, saved_t, cv_mem->cv_acnrmQS,
                              &nefQS, &(cv_mem->cv_netfQS), &dsmQS);

        if (eflag == TRY_AGAIN) continue;
        if (eflag != CV_SUCCESS) return(eflag);

        /* Set dsm = max(dsm, dsmQS) to be used in cvPrepareNextStep */
        if (dsmQS > dsm) dsm = dsmQS;
      }


    }


    /* Everything went fine; exit loop */ 
    break;

  }

  /* Nonlinear system solve and error test were both successful.
     Update data, and consider change of step and/or order.       */

  cvCompleteStep(cv_mem); 

  cvPrepareNextStep(cv_mem, dsm); 

  /* If Stablilty Limit Detection is turned on, call stability limit
     detection routine for possible order reduction. */

  if (cv_mem->cv_sldeton) cvBDFStab(cv_mem);

  cv_mem->cv_etamax = (cv_mem->cv_nst <= SMALL_NST) ? ETAMX2 : ETAMX3;

  /*  Finally, we rescale the acor array to be the 
      estimated local error vector. */

  N_VScale(cv_mem->cv_tq[2], cv_mem->cv_acor, cv_mem->cv_acor);

  if (cv_mem->cv_quadr)
    N_VScale(cv_mem->cv_tq[2], cv_mem->cv_acorQ, cv_mem->cv_acorQ);

  if (cv_mem->cv_sensi) {
    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = cv_mem->cv_tq[2];

    retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                 cv_mem->cv_acorS, cv_mem->cv_acorS);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
  }

  if (cv_mem->cv_quadr_sensi) {
    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = cv_mem->cv_tq[2];

    retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                 cv_mem->cv_acorQS, cv_mem->cv_acorQS);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
  }

  return(CV_SUCCESS);
      
}

/* 
 * -----------------------------------------------------------------
 * Function called at beginning of step
 * -----------------------------------------------------------------
 */

/*
 * cvAdjustParams
 *
 * This routine is called when a change in step size was decided upon,
 * and it handles the required adjustments to the history array zn.
 * If there is to be a change in order, we call cvAdjustOrder and reset
 * q, L = q+1, and qwait.  Then in any case, we call cvRescale, which
 * resets h and rescales the Nordsieck array.
 */

static void cvAdjustParams(CVodeMem cv_mem)
{
  if (cv_mem->cv_qprime != cv_mem->cv_q) {
    cvAdjustOrder(cv_mem, cv_mem->cv_qprime-cv_mem->cv_q);
    cv_mem->cv_q = cv_mem->cv_qprime;
    cv_mem->cv_L = cv_mem->cv_q+1;
    cv_mem->cv_qwait = cv_mem->cv_L;
  }
  cvRescale(cv_mem);
}

/*
 * cvAdjustOrder
 *
 * This routine is a high level routine which handles an order
 * change by an amount deltaq (= +1 or -1). If a decrease in order
 * is requested and q==2, then the routine returns immediately.
 * Otherwise cvAdjustAdams or cvAdjustBDF is called to handle the
 * order change (depending on the value of lmm).
 */

static void cvAdjustOrder(CVodeMem cv_mem, int deltaq)
{
  if ((cv_mem->cv_q==2) && (deltaq != 1)) return;
  
  switch(cv_mem->cv_lmm){
  case CV_ADAMS:
    cvAdjustAdams(cv_mem, deltaq);
    break;
  case CV_BDF:
    cvAdjustBDF(cv_mem, deltaq);
    break;
  }
}

/*
 * cvAdjustAdams
 *
 * This routine adjusts the history array on a change of order q by
 * deltaq, in the case that lmm == CV_ADAMS.
 */

static void cvAdjustAdams(CVodeMem cv_mem, int deltaq)
{
  int i, j;
  realtype xi, hsum;

  /* On an order increase, set new column of zn to zero and return */
  
  if (deltaq==1) {
    N_VConst(ZERO, cv_mem->cv_zn[cv_mem->cv_L]);
    if (cv_mem->cv_quadr)
      N_VConst(ZERO, cv_mem->cv_znQ[cv_mem->cv_L]);
    if (cv_mem->cv_sensi)
      (void) N_VConstVectorArray(cv_mem->cv_Ns, ZERO,
                                 cv_mem->cv_znS[cv_mem->cv_L]);
    return;
  }

  /*
   * On an order decrease, each zn[j] is adjusted by a multiple of zn[q].
   * The coeffs. in the adjustment are the coeffs. of the polynomial:
   *        x
   * q * INT { u * ( u + xi_1 ) * ... * ( u + xi_{q-2} ) } du 
   *        0
   * where xi_j = [t_n - t_(n-j)]/h => xi_0 = 0
   */

  for (i=0; i <= cv_mem->cv_qmax; i++) cv_mem->cv_l[i] = ZERO;
  cv_mem->cv_l[1] = ONE;
  hsum = ZERO;
  for (j=1; j <= cv_mem->cv_q-2; j++) {
    hsum += cv_mem->cv_tau[j];
    xi = hsum / cv_mem->cv_hscale;
    for (i=j+1; i >= 1; i--)
      cv_mem->cv_l[i] = cv_mem->cv_l[i]*xi + cv_mem->cv_l[i-1];
  }
  
  for (j=1; j <= cv_mem->cv_q-2; j++)
    cv_mem->cv_l[j+1] = cv_mem->cv_q * (cv_mem->cv_l[j] / (j+1));
  
  if (cv_mem->cv_q > 2) {

    for (j=2; j < cv_mem->cv_q; j++)
      cv_mem->cv_cvals[j-2] = -cv_mem->cv_l[j];

    (void) N_VScaleAddMulti(cv_mem->cv_q-2, cv_mem->cv_cvals,
                            cv_mem->cv_zn[cv_mem->cv_q],
                            cv_mem->cv_zn+2, cv_mem->cv_zn+2);

    if (cv_mem->cv_quadr)
      (void) N_VScaleAddMulti(cv_mem->cv_q-2, cv_mem->cv_cvals,
                              cv_mem->cv_znQ[cv_mem->cv_q],
                              cv_mem->cv_znQ+2, cv_mem->cv_znQ+2);

    if (cv_mem->cv_sensi)
      (void) N_VScaleAddMultiVectorArray(cv_mem->cv_Ns, cv_mem->cv_q-2,
                                         cv_mem->cv_cvals,
                                         cv_mem->cv_znS[cv_mem->cv_q],
                                         cv_mem->cv_znS+2,
                                         cv_mem->cv_znS+2);
  }

}

/*
 * cvAdjustBDF
 *
 * This is a high level routine which handles adjustments to the
 * history array on a change of order by deltaq in the case that 
 * lmm == CV_BDF.  cvAdjustBDF calls cvIncreaseBDF if deltaq = +1 and 
 * cvDecreaseBDF if deltaq = -1 to do the actual work.
 */

static void cvAdjustBDF(CVodeMem cv_mem, int deltaq)
{
  switch(deltaq) {
  case 1: 
    cvIncreaseBDF(cv_mem);
    return;
  case -1: 
    cvDecreaseBDF(cv_mem);
    return;
  }
}

/*
 * cvIncreaseBDF
 *
 * This routine adjusts the history array on an increase in the 
 * order q in the case that lmm == CV_BDF.  
 * A new column zn[q+1] is set equal to a multiple of the saved 
 * vector (= acor) in zn[indx_acor].  Then each zn[j] is adjusted by
 * a multiple of zn[q+1].  The coefficients in the adjustment are the 
 * coefficients of the polynomial x*x*(x+xi_1)*...*(x+xi_j),
 * where xi_j = [t_n - t_(n-j)]/h.
 */

static void cvIncreaseBDF(CVodeMem cv_mem)
{
  realtype alpha0, alpha1, prod, xi, xiold, hsum, A1;
  int i, j;
  int is;

  for (i=0; i <= cv_mem->cv_qmax; i++)
    cv_mem->cv_l[i] = ZERO;
  cv_mem->cv_l[2] = alpha1 = prod = xiold = ONE;
  alpha0 = -ONE;
  hsum = cv_mem->cv_hscale;
  if (cv_mem->cv_q > 1) {
    for (j=1; j < cv_mem->cv_q; j++) {
      hsum += cv_mem->cv_tau[j+1];
      xi = hsum / cv_mem->cv_hscale;
      prod *= xi;
      alpha0 -= ONE / (j+1);
      alpha1 += ONE / xi;
      for (i=j+2; i >= 2; i--)
        cv_mem->cv_l[i] = cv_mem->cv_l[i]*xiold + cv_mem->cv_l[i-1];
      xiold = xi;
    }
  }
  A1 = (-alpha0 - alpha1) / prod;

  /* 
     zn[indx_acor] contains the value Delta_n = y_n - y_n(0) 
     This value was stored there at the previous successful
     step (in cvCompleteStep) 
     
     A1 contains dbar = (1/xi* - 1/xi_q)/prod(xi_j)
  */
  
  N_VScale(A1, cv_mem->cv_zn[cv_mem->cv_indx_acor],
           cv_mem->cv_zn[cv_mem->cv_L]);

  /* for (j=2; j <= cv_mem->cv_q; j++) */
  if (cv_mem->cv_q > 1)
    (void) N_VScaleAddMulti(cv_mem->cv_q-1, cv_mem->cv_l+2,
                            cv_mem->cv_zn[cv_mem->cv_L],
                            cv_mem->cv_zn+2, cv_mem->cv_zn+2);

  if (cv_mem->cv_quadr) {
    N_VScale(A1, cv_mem->cv_znQ[cv_mem->cv_indx_acor],
             cv_mem->cv_znQ[cv_mem->cv_L]);

    /* for (j=2; j <= cv_mem->cv_q; j++) */
    if (cv_mem->cv_q > 1)
      (void) N_VScaleAddMulti(cv_mem->cv_q-1, cv_mem->cv_l+2,
                              cv_mem->cv_znQ[cv_mem->cv_L],
                              cv_mem->cv_znQ+2, cv_mem->cv_znQ+2);
  }

  if (cv_mem->cv_sensi) {

    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = A1;

    (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                               cv_mem->cv_znS[cv_mem->cv_indx_acor],
                               cv_mem->cv_znS[cv_mem->cv_L]);

    /* for (j=2; j <= cv_mem->cv_q; j++) */
    if (cv_mem->cv_q > 1)
      (void) N_VScaleAddMultiVectorArray(cv_mem->cv_Ns, cv_mem->cv_q-1,
                                         cv_mem->cv_l+2,
                                         cv_mem->cv_znS[cv_mem->cv_L],
                                         cv_mem->cv_znS+2,
                                         cv_mem->cv_znS+2);
  }

  if (cv_mem->cv_quadr_sensi) {

    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = A1;

    (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                               cv_mem->cv_znQS[cv_mem->cv_indx_acor],
                               cv_mem->cv_znQS[cv_mem->cv_L]);

    /* for (j=2; j <= cv_mem->cv_q; j++) */
    if (cv_mem->cv_q > 1)
      (void) N_VScaleAddMultiVectorArray(cv_mem->cv_Ns, cv_mem->cv_q-1,
                                         cv_mem->cv_l+2,
                                         cv_mem->cv_znQS[cv_mem->cv_L],
                                         cv_mem->cv_znQS+2,
                                         cv_mem->cv_znQS+2);
  }

}

/*
 * cvDecreaseBDF
 *
 * This routine adjusts the history array on a decrease in the 
 * order q in the case that lmm == CV_BDF.  
 * Each zn[j] is adjusted by a multiple of zn[q].  The coefficients
 * in the adjustment are the coefficients of the polynomial
 *   x*x*(x+xi_1)*...*(x+xi_j), where xi_j = [t_n - t_(n-j)]/h.
 */

static void cvDecreaseBDF(CVodeMem cv_mem)
{
  realtype hsum, xi;
  int i, j;
  
  for (i=0; i <= cv_mem->cv_qmax; i++)
    cv_mem->cv_l[i] = ZERO;
  cv_mem->cv_l[2] = ONE;
  hsum = ZERO;
  for (j=1; j <= cv_mem->cv_q-2; j++) {
    hsum += cv_mem->cv_tau[j];
    xi = hsum / cv_mem->cv_hscale;
    for (i=j+2; i >= 2; i--)
      cv_mem->cv_l[i] = cv_mem->cv_l[i]*xi + cv_mem->cv_l[i-1];
  }
  
  if (cv_mem->cv_q > 2) {

    for (j=2; j < cv_mem->cv_q; j++)
      cv_mem->cv_cvals[j-2] = -cv_mem->cv_l[j];

    (void) N_VScaleAddMulti(cv_mem->cv_q-2, cv_mem->cv_cvals,
                            cv_mem->cv_zn[cv_mem->cv_q],
                            cv_mem->cv_zn+2, cv_mem->cv_zn+2);

    if (cv_mem->cv_quadr)
      (void) N_VScaleAddMulti(cv_mem->cv_q-2, cv_mem->cv_cvals,
                              cv_mem->cv_znQ[cv_mem->cv_q],
                              cv_mem->cv_znQ+2, cv_mem->cv_znQ+2);

    if (cv_mem->cv_sensi)
      (void) N_VScaleAddMultiVectorArray(cv_mem->cv_Ns, cv_mem->cv_q-2,
                                         cv_mem->cv_cvals,
                                         cv_mem->cv_znS[cv_mem->cv_q],
                                         cv_mem->cv_znS+2,
                                         cv_mem->cv_znS+2);

    if (cv_mem->cv_quadr_sensi)
      (void) N_VScaleAddMultiVectorArray(cv_mem->cv_Ns, cv_mem->cv_q-2,
                                         cv_mem->cv_cvals,
                                         cv_mem->cv_znQS[cv_mem->cv_q],
                                         cv_mem->cv_znQS+2,
                                         cv_mem->cv_znQS+2);
  }

}


/*
 * cvRescale
 *
 * This routine rescales the Nordsieck array by multiplying the
 * jth column zn[j] by eta^j, j = 1, ..., q.  Then the value of
 * h is rescaled by eta, and hscale is reset to h.
 */

static void cvRescale(CVodeMem cv_mem)
{
  int j;
  int is;

  /* compute scaling factors */
  cv_mem->cv_cvals[0] = cv_mem->cv_eta;
  for (j=1; j < cv_mem->cv_q; j++)
    cv_mem->cv_cvals[j] = cv_mem->cv_eta * cv_mem->cv_cvals[j-1];

  (void) N_VScaleVectorArray(cv_mem->cv_q, cv_mem->cv_cvals,
                             cv_mem->cv_zn+1, cv_mem->cv_zn+1);

  if (cv_mem->cv_quadr)
    (void) N_VScaleVectorArray(cv_mem->cv_q, cv_mem->cv_cvals,
                               cv_mem->cv_znQ+1, cv_mem->cv_znQ+1);

  /* compute sensi scaling factors */
  if (cv_mem->cv_sensi || cv_mem->cv_quadr_sensi) {
    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = cv_mem->cv_eta;
    for (j=1; j < cv_mem->cv_q; j++)
      for (is=0; is<cv_mem->cv_Ns; is++)
        cv_mem->cv_cvals[j*cv_mem->cv_Ns+is] =
          cv_mem->cv_eta * cv_mem->cv_cvals[(j-1)*cv_mem->cv_Ns+is];
  }

  if (cv_mem->cv_sensi) {
    for (j=1; j <= cv_mem->cv_q; j++)
      for (is=0; is<cv_mem->cv_Ns; is++)
        cv_mem->cv_Xvecs[(j-1)*cv_mem->cv_Ns+is] = cv_mem->cv_znS[j][is];
      
    (void) N_VScaleVectorArray(cv_mem->cv_q*cv_mem->cv_Ns, cv_mem->cv_cvals,
                               cv_mem->cv_Xvecs, cv_mem->cv_Xvecs);
  }

  if (cv_mem->cv_quadr_sensi) {
    for (j=1; j <= cv_mem->cv_q; j++)
      for (is=0; is<cv_mem->cv_Ns; is++)
        cv_mem->cv_Xvecs[(j-1)*cv_mem->cv_Ns+is] = cv_mem->cv_znQS[j][is];
      
    (void) N_VScaleVectorArray(cv_mem->cv_q*cv_mem->cv_Ns, cv_mem->cv_cvals,
                               cv_mem->cv_Xvecs, cv_mem->cv_Xvecs);
  }

  cv_mem->cv_h = cv_mem->cv_hscale * cv_mem->cv_eta;
  cv_mem->cv_next_h = cv_mem->cv_h;
  cv_mem->cv_hscale = cv_mem->cv_h;
  cv_mem->cv_nscon = 0;

}

/*
 * cvPredict
 *
 * This routine advances tn by the tentative step size h, and computes
 * the predicted array z_n(0), which is overwritten on zn.  The
 * prediction of zn is done by repeated additions.
 * If tstop is enabled, it is possible for tn + h to be past tstop by roundoff,
 * and in that case, we reset tn (after incrementing by h) to tstop.
 */

static void cvPredict(CVodeMem cv_mem)
{
  int j, k;

  cv_mem->cv_tn += cv_mem->cv_h;
  if (cv_mem->cv_tstopset) {
    if ((cv_mem->cv_tn - cv_mem->cv_tstop)*cv_mem->cv_h > ZERO)
      cv_mem->cv_tn = cv_mem->cv_tstop;
  }

  for (k = 1; k <= cv_mem->cv_q; k++)
    for (j = cv_mem->cv_q; j >= k; j--) 
      N_VLinearSum(ONE, cv_mem->cv_zn[j-1], ONE,
                   cv_mem->cv_zn[j], cv_mem->cv_zn[j-1]); 

  if (cv_mem->cv_quadr) {
    for (k = 1; k <= cv_mem->cv_q; k++)
      for (j = cv_mem->cv_q; j >= k; j--) 
        N_VLinearSum(ONE, cv_mem->cv_znQ[j-1], ONE,
                     cv_mem->cv_znQ[j], cv_mem->cv_znQ[j-1]);
  }

  if (cv_mem->cv_sensi) {
    for (k = 1; k <= cv_mem->cv_q; k++)
      for (j = cv_mem->cv_q; j >= k; j--)
        (void) N_VLinearSumVectorArray(cv_mem->cv_Ns,
                                       ONE, cv_mem->cv_znS[j-1],
                                       ONE, cv_mem->cv_znS[j],
                                       cv_mem->cv_znS[j-1]);
  }

  if (cv_mem->cv_quadr_sensi) {
    for (k = 1; k <= cv_mem->cv_q; k++)
      for (j = cv_mem->cv_q; j >= k; j--) 
        (void) N_VLinearSumVectorArray(cv_mem->cv_Ns, 
                                       ONE, cv_mem->cv_znQS[j-1],
                                       ONE, cv_mem->cv_znQS[j],
                                       cv_mem->cv_znQS[j-1]);
  }

}

/*
 * cvSet
 *
 * This routine is a high level routine which calls cvSetAdams or
 * cvSetBDF to set the polynomial l, the test quantity array tq, 
 * and the related variables  rl1, gamma, and gamrat.
 *
 * The array tq is loaded with constants used in the control of estimated
 * local errors and in the nonlinear convergence test.  Specifically, while
 * running at order q, the components of tq are as follows:
 *   tq[1] = a coefficient used to get the est. local error at order q-1
 *   tq[2] = a coefficient used to get the est. local error at order q
 *   tq[3] = a coefficient used to get the est. local error at order q+1
 *   tq[4] = constant used in nonlinear iteration convergence test
 *   tq[5] = coefficient used to get the order q+2 derivative vector used in
 *           the est. local error at order q+1
 */

static void cvSet(CVodeMem cv_mem)
{
  switch(cv_mem->cv_lmm) {
  case CV_ADAMS:
    cvSetAdams(cv_mem);
    break;
  case CV_BDF:
    cvSetBDF(cv_mem);
    break;
  }
  cv_mem->cv_rl1 = ONE / cv_mem->cv_l[1];
  cv_mem->cv_gamma = cv_mem->cv_h * cv_mem->cv_rl1;
  if (cv_mem->cv_nst == 0) cv_mem->cv_gammap = cv_mem->cv_gamma;
  cv_mem->cv_gamrat = (cv_mem->cv_nst > 0) ?
    cv_mem->cv_gamma / cv_mem->cv_gammap : ONE;  /* protect x / x != 1.0 */
}

/*
 * cvSetAdams
 *
 * This routine handles the computation of l and tq for the
 * case lmm == CV_ADAMS.
 *
 * The components of the array l are the coefficients of a
 * polynomial Lambda(x) = l_0 + l_1 x + ... + l_q x^q, given by
 *                          q-1
 * (d/dx) Lambda(x) = c * PRODUCT (1 + x / xi_i) , where
 *                          i=1
 *  Lambda(-1) = 0, Lambda(0) = 1, and c is a normalization factor.
 * Here xi_i = [t_n - t_(n-i)] / h.
 *
 * The array tq is set to test quantities used in the convergence
 * test, the error test, and the selection of h at a new order.
 */

static void cvSetAdams(CVodeMem cv_mem)
{
  realtype m[L_MAX], M[3], hsum;
  
  if (cv_mem->cv_q == 1) {
    cv_mem->cv_l[0] = cv_mem->cv_l[1] = cv_mem->cv_tq[1] = cv_mem->cv_tq[5] = ONE;
    cv_mem->cv_tq[2] = HALF;
    cv_mem->cv_tq[3] = ONE/TWELVE;
    cv_mem->cv_tq[4] = cv_mem->cv_nlscoef / cv_mem->cv_tq[2];       /* = 0.1 / tq[2] */
    return;
  }
  
  hsum = cvAdamsStart(cv_mem, m);
  
  M[0] = cvAltSum(cv_mem->cv_q-1, m, 1);
  M[1] = cvAltSum(cv_mem->cv_q-1, m, 2);
  
  cvAdamsFinish(cv_mem, m, M, hsum);
}

/*
 * cvAdamsStart
 *
 * This routine generates in m[] the coefficients of the product
 * polynomial needed for the Adams l and tq coefficients for q > 1.
 */

static realtype cvAdamsStart(CVodeMem cv_mem, realtype m[])
{
  realtype hsum, xi_inv, sum;
  int i, j;
  
  hsum = cv_mem->cv_h;
  m[0] = ONE;
  for (i=1; i <= cv_mem->cv_q; i++) m[i] = ZERO;
  for (j=1; j < cv_mem->cv_q; j++) {
    if ((j==cv_mem->cv_q-1) && (cv_mem->cv_qwait == 1)) {
      sum = cvAltSum(cv_mem->cv_q-2, m, 2);
      cv_mem->cv_tq[1] = cv_mem->cv_q * sum / m[cv_mem->cv_q-2];
    }
    xi_inv = cv_mem->cv_h / hsum;
    for (i=j; i >= 1; i--)
      m[i] += m[i-1] * xi_inv;
    hsum += cv_mem->cv_tau[j];
    /* The m[i] are coefficients of product(1 to j) (1 + x/xi_i) */
  }
  return(hsum);
}

/*
 * cvAdamsFinish
 *
 * This routine completes the calculation of the Adams l and tq.
 */

static void cvAdamsFinish(CVodeMem cv_mem, realtype m[], realtype M[], realtype hsum)
{
  int i;
  realtype M0_inv, xi, xi_inv;
  
  M0_inv = ONE / M[0];
  
  cv_mem->cv_l[0] = ONE;
  for (i=1; i <= cv_mem->cv_q; i++)
    cv_mem->cv_l[i] = M0_inv * (m[i-1] / i);
  xi = hsum / cv_mem->cv_h;
  xi_inv = ONE / xi;
  
  cv_mem->cv_tq[2] = M[1] * M0_inv / xi;
  cv_mem->cv_tq[5] = xi / cv_mem->cv_l[cv_mem->cv_q];

  if (cv_mem->cv_qwait == 1) {
    for (i=cv_mem->cv_q; i >= 1; i--)
      m[i] += m[i-1] * xi_inv;
    M[2] = cvAltSum(cv_mem->cv_q, m, 2);
    cv_mem->cv_tq[3] = M[2] * M0_inv / cv_mem->cv_L;
  }

  cv_mem->cv_tq[4] = cv_mem->cv_nlscoef / cv_mem->cv_tq[2];
}

/*  
 * cvAltSum
 *
 * cvAltSum returns the value of the alternating sum
 *   sum (i= 0 ... iend) [ (-1)^i * (a[i] / (i + k)) ].
 * If iend < 0 then cvAltSum returns 0.
 * This operation is needed to compute the integral, from -1 to 0,
 * of a polynomial x^(k-1) M(x) given the coefficients of M(x).
 */

static realtype cvAltSum(int iend, realtype a[], int k)
{
  int i, sign;
  realtype sum;
  
  if (iend < 0) return(ZERO);
  
  sum = ZERO;
  sign = 1;
  for (i=0; i <= iend; i++) {
    sum += sign * (a[i] / (i+k));
    sign = -sign;
  }
  return(sum);
}

/*
 * cvSetBDF
 *
 * This routine computes the coefficients l and tq in the case
 * lmm == CV_BDF.  cvSetBDF calls cvSetTqBDF to set the test
 * quantity array tq. 
 * 
 * The components of the array l are the coefficients of a
 * polynomial Lambda(x) = l_0 + l_1 x + ... + l_q x^q, given by
 *                                 q-1
 * Lambda(x) = (1 + x / xi*_q) * PRODUCT (1 + x / xi_i) , where
 *                                 i=1
 *  xi_i = [t_n - t_(n-i)] / h.
 *
 * The array tq is set to test quantities used in the convergence
 * test, the error test, and the selection of h at a new order.
 */

static void cvSetBDF(CVodeMem cv_mem)
{
  realtype alpha0, alpha0_hat, xi_inv, xistar_inv, hsum;
  int i,j;
  
  cv_mem->cv_l[0] = cv_mem->cv_l[1] = xi_inv = xistar_inv = ONE;
  for (i=2; i <= cv_mem->cv_q; i++) cv_mem->cv_l[i] = ZERO;
  alpha0 = alpha0_hat = -ONE;
  hsum = cv_mem->cv_h;
  if (cv_mem->cv_q > 1) {
    for (j=2; j < cv_mem->cv_q; j++) {
      hsum += cv_mem->cv_tau[j-1];
      xi_inv = cv_mem->cv_h / hsum;
      alpha0 -= ONE / j;
      for (i=j; i >= 1; i--)
        cv_mem->cv_l[i] += cv_mem->cv_l[i-1]*xi_inv;
      /* The l[i] are coefficients of product(1 to j) (1 + x/xi_i) */
    }
    
    /* j = q */
    alpha0 -= ONE / cv_mem->cv_q;
    xistar_inv = -cv_mem->cv_l[1] - alpha0;
    hsum += cv_mem->cv_tau[cv_mem->cv_q-1];
    xi_inv = cv_mem->cv_h / hsum;
    alpha0_hat = -cv_mem->cv_l[1] - xi_inv;
    for (i=cv_mem->cv_q; i >= 1; i--)
      cv_mem->cv_l[i] += cv_mem->cv_l[i-1]*xistar_inv;
  }

  cvSetTqBDF(cv_mem, hsum, alpha0, alpha0_hat, xi_inv, xistar_inv);
}

/*
 * cvSetTqBDF
 *
 * This routine sets the test quantity array tq in the case
 * lmm == CV_BDF.
 */

static void cvSetTqBDF(CVodeMem cv_mem, realtype hsum, realtype alpha0,
                       realtype alpha0_hat, realtype xi_inv, realtype xistar_inv)
{
  realtype A1, A2, A3, A4, A5, A6;
  realtype C, Cpinv, Cppinv;
  
  A1 = ONE - alpha0_hat + alpha0;
  A2 = ONE + cv_mem->cv_q * A1;
  cv_mem->cv_tq[2] = SUNRabs(A1 / (alpha0 * A2));
  cv_mem->cv_tq[5] = SUNRabs(A2 * xistar_inv / (cv_mem->cv_l[cv_mem->cv_q] * xi_inv));
  if (cv_mem->cv_qwait == 1) {
    if (cv_mem->cv_q > 1) {
      C = xistar_inv / cv_mem->cv_l[cv_mem->cv_q];
      A3 = alpha0 + ONE / cv_mem->cv_q;
      A4 = alpha0_hat + xi_inv;
      Cpinv = (ONE - A4 + A3) / A3;
      cv_mem->cv_tq[1] = SUNRabs(C * Cpinv);
    }
    else cv_mem->cv_tq[1] = ONE;
    hsum += cv_mem->cv_tau[cv_mem->cv_q];
    xi_inv = cv_mem->cv_h / hsum;
    A5 = alpha0 - (ONE / (cv_mem->cv_q+1));
    A6 = alpha0_hat - xi_inv;
    Cppinv = (ONE - A6 + A5) / A2;
    cv_mem->cv_tq[3] = SUNRabs(Cppinv / (xi_inv * (cv_mem->cv_q+2) * A5));
  }
  cv_mem->cv_tq[4] = cv_mem->cv_nlscoef / cv_mem->cv_tq[2];
}

/* 
 * -----------------------------------------------------------------
 * Nonlinear solver functions
 * -----------------------------------------------------------------
 */

/*
 * cvNls
 *
 * This routine attempts to solve the nonlinear system associated
 * with a single implicit step of the linear multistep method.
 */

static int cvNls(CVodeMem cv_mem, int nflag)
{
  int flag = CV_SUCCESS;
  booleantype callSetup;
  booleantype do_sensi_sim;

  /* Are we computing sensitivities with the CV_SIMULTANEOUS approach? */
  do_sensi_sim = (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_SIMULTANEOUS));

  /* Decide whether or not to call setup routine (if one exists) and */
  /* set flag convfail (input to lsetup for its evaluation decision) */
  if (cv_mem->cv_lsetup) {
    cv_mem->convfail = ((nflag == FIRST_CALL) || (nflag == PREV_ERR_FAIL)) ?
      CV_NO_FAILURES : CV_FAIL_OTHER;

    callSetup = (nflag == PREV_CONV_FAIL) || (nflag == PREV_ERR_FAIL) ||
      (cv_mem->cv_nst == 0) ||
      (cv_mem->cv_nst >= cv_mem->cv_nstlp + MSBP) ||
      (SUNRabs(cv_mem->cv_gamrat-ONE) > DGMAX);

    /* Decide whether to force a call to setup */
    if (cv_mem->cv_forceSetup) {
      callSetup = SUNTRUE;
      cv_mem->convfail = CV_FAIL_OTHER;
    }
  } else {
    cv_mem->cv_crate  = ONE;
    cv_mem->cv_crateS = ONE;  /* if NO lsetup all conv. rates are set to ONE */
    callSetup = SUNFALSE;
  }

  /* initial guess for the correction to the predictor */
  if (do_sensi_sim)
    N_VConst(ZERO, cv_mem->ycor0Sim);
  else
    N_VConst(ZERO, cv_mem->cv_tempv);

  /* call nonlinear solver setup if it exists */
  if ((cv_mem->NLS)->ops->setup) {
    if (do_sensi_sim)
      flag = SUNNonlinSolSetup(cv_mem->NLS, cv_mem->ycor0Sim, cv_mem);
    else
      flag = SUNNonlinSolSetup(cv_mem->NLS, cv_mem->cv_tempv, cv_mem);

    if (flag < 0) return(CV_NLS_SETUP_FAIL);
    if (flag > 0) return(SUN_NLS_CONV_RECVR);
  }

  /* solve the nonlinear system */
  if (do_sensi_sim)
    flag = SUNNonlinSolSolve(cv_mem->NLSsim, cv_mem->ycor0Sim, cv_mem->ycorSim,
                             cv_mem->ewtSim, cv_mem->cv_tq[4], callSetup, cv_mem);
  else
    flag = SUNNonlinSolSolve(cv_mem->NLS, cv_mem->cv_tempv, cv_mem->cv_acor,
                             cv_mem->cv_ewt, cv_mem->cv_tq[4], callSetup, cv_mem);

  /* update the state based on the final correction from the nonlinear solver */
  N_VLinearSum(ONE, cv_mem->cv_zn[0], ONE, cv_mem->cv_acor, cv_mem->cv_y);

  /* update the sensitivities based on the final correction from the nonlinear solver */
  if (do_sensi_sim) {
    N_VLinearSumVectorArray(cv_mem->cv_Ns,
                            ONE, cv_mem->cv_znS[0],
                            ONE, cv_mem->cv_acorS, cv_mem->cv_yS);
  }

  /* if the solve failed return */
  if (flag != CV_SUCCESS) return(flag);

  /* solve successful, update Jacobian status and check constraints */
  cv_mem->cv_jcur = SUNFALSE;

  if (cv_mem->cv_constraintsSet)
    flag = cvCheckConstraints(cv_mem);

  return(flag);

}

/*
 * cvCheckConstraints
 *
 * This routine determines if the constraints of the problem
 * are satisfied by the proposed step
 *
 * Possible return values are:
 *
 *   CV_SUCCESS    ---> allows stepping forward
 *
 *   CONSTR_RECVR  ---> values failed to satisfy constraints
 */

static int cvCheckConstraints(CVodeMem cv_mem)
{
  booleantype constraintsPassed;
  realtype vnorm;
  cv_mem->cv_mm = cv_mem->cv_ftemp;

  /* Get mask vector mm, set where constraints failed */

  constraintsPassed = N_VConstrMask(cv_mem->cv_constraints,
                                    cv_mem->cv_y, cv_mem->cv_mm);
  if (constraintsPassed) return(CV_SUCCESS);
  else {
    N_VCompare(ONEPT5, cv_mem->cv_constraints, cv_mem->cv_tempv);
    /* a, where a[i]=1 when |c[i]|=2; c the vector of constraints */
    N_VProd(cv_mem->cv_tempv, cv_mem->cv_constraints,
            cv_mem->cv_tempv);                        /* a * c */
    N_VDiv(cv_mem->cv_tempv, cv_mem->cv_ewt,
           cv_mem->cv_tempv);                         /* a * c * wt */
    N_VLinearSum(ONE, cv_mem->cv_y, -PT1,
                 cv_mem->cv_tempv, cv_mem->cv_tempv); /* y - 0.1 * a * c * wt */
    N_VProd(cv_mem->cv_tempv, cv_mem->cv_mm,
            cv_mem->cv_tempv);                        /* v = mm*(y-0.1*a*c*wt) */

    vnorm = N_VWrmsNorm(cv_mem->cv_tempv, cv_mem->cv_ewt); /*  ||v||  */

    /* If vector v of constraint corrections is small in
       norm, correct and accept this step */
    if (vnorm <= cv_mem->cv_tq[4]) {
      N_VLinearSum(ONE, cv_mem->cv_acor, -ONE,
                   cv_mem->cv_tempv, cv_mem->cv_acor);    /* acor <- acor - v */
      return(CV_SUCCESS);
    }
    else {
      /* Constraints not met - reduce h by computing eta = h'/h */
      N_VLinearSum(ONE, cv_mem->cv_zn[0], -ONE, cv_mem->cv_y, cv_mem->cv_tempv);
      N_VProd(cv_mem->cv_mm, cv_mem->cv_tempv, cv_mem->cv_tempv);
      cv_mem->cv_eta = PT9*N_VMinQuotient(cv_mem->cv_zn[0], cv_mem->cv_tempv);
      cv_mem->cv_eta = SUNMAX(cv_mem->cv_eta, PT1);
      return(CONSTR_RECVR);
    }
  }
  return(CV_SUCCESS);
}

/*
 * cvQuadNls
 * 
 * This routine solves for the quadrature variables at the new step.
 * It does not solve a nonlinear system, but rather updates the
 * quadrature variables. The name for this function is just for 
 * uniformity purposes.
 *
 * Possible return values (interpreted by cvHandleNFlag)
 *
 *   CV_SUCCESS       -> continue with error test
 *   CV_QRHSFUNC_FAIL -> halt the integration 
 *   QRHSFUNC_RECVR   -> predict again or stop if too many
 *   
 */

static int cvQuadNls(CVodeMem cv_mem)
{
  int retval;

  /* Save quadrature correction in acorQ */
  retval = cv_mem->cv_fQ(cv_mem->cv_tn, cv_mem->cv_y,
                         cv_mem->cv_acorQ, cv_mem->cv_user_data);
  cv_mem->cv_nfQe++;
  if (retval < 0) return(CV_QRHSFUNC_FAIL);
  if (retval > 0) return(QRHSFUNC_RECVR);

  /* If needed, save the value of yQdot = fQ into ftempQ
   * for use in evaluating fQS */
  if (cv_mem->cv_quadr_sensi) {
    N_VScale(ONE, cv_mem->cv_acorQ, cv_mem->cv_ftempQ);
  }

  N_VLinearSum(cv_mem->cv_h, cv_mem->cv_acorQ, -ONE,
               cv_mem->cv_znQ[1], cv_mem->cv_acorQ);
  N_VScale(cv_mem->cv_rl1, cv_mem->cv_acorQ, cv_mem->cv_acorQ);

  /* Apply correction to quadrature variables */
  N_VLinearSum(ONE, cv_mem->cv_znQ[0], ONE, cv_mem->cv_acorQ, cv_mem->cv_yQ);

  return(CV_SUCCESS);
}

/*
 * cvQuadSensNls
 * 
 * This routine solves for the quadrature sensitivity variables
 * at the new step. It does not solve a nonlinear system, but 
 * rather updates the quadrature variables. The name for this
 * function is just for uniformity purposes.
 *
 * Possible return values (interpreted by cvHandleNFlag)
 *
 *   CV_SUCCESS        -> continue with error test
 *   CV_QSRHSFUNC_FAIL -> halt the integration 
 *   QSRHSFUNC_RECVR   -> predict again or stop if too many
 *   
 */

static int cvQuadSensNls(CVodeMem cv_mem)
{
  int is, retval;

  /* Save quadrature correction in acorQ */
  retval = cv_mem->cv_fQS(cv_mem->cv_Ns, cv_mem->cv_tn, cv_mem->cv_y,
                          cv_mem->cv_yS, cv_mem->cv_ftempQ,
                          cv_mem->cv_acorQS, cv_mem->cv_user_data,
                          cv_mem->cv_tempv, cv_mem->cv_tempvQ);
  cv_mem->cv_nfQSe++;
  if (retval < 0) return(CV_QSRHSFUNC_FAIL);
  if (retval > 0) return(QSRHSFUNC_RECVR);


  for (is=0; is<cv_mem->cv_Ns; is++) {
    N_VLinearSum(cv_mem->cv_h, cv_mem->cv_acorQS[is], -ONE,
                 cv_mem->cv_znQS[1][is], cv_mem->cv_acorQS[is]);
    N_VScale(cv_mem->cv_rl1, cv_mem->cv_acorQS[is], cv_mem->cv_acorQS[is]);
    /* Apply correction to quadrature sensitivity variables */
    N_VLinearSum(ONE, cv_mem->cv_znQS[0][is], ONE,
                 cv_mem->cv_acorQS[is], cv_mem->cv_yQS[is]);
  }

  return(CV_SUCCESS);
}


/*
 * cvStgrNls
 *
 * This is a high-level routine that attempts to solve the
 * sensitivity linear systems using the attached nonlinear solver
 * once the states y_n were obtained and passed the error test.
 */

static int cvStgrNls(CVodeMem cv_mem)
{
  booleantype callSetup;
  int flag=CV_SUCCESS;

  cv_mem->sens_solve = SUNTRUE;

  callSetup = SUNFALSE;
  if (cv_mem->cv_lsetup == NULL)
    cv_mem->cv_crateS = ONE;

  /* initial guess for the correction to the predictor */
  N_VConst(ZERO, cv_mem->ycor0Stg);

  /* solve the nonlinear system */
  flag = SUNNonlinSolSolve(cv_mem->NLSstg, cv_mem->ycor0Stg, cv_mem->ycorStg,
                           cv_mem->ewtStg, cv_mem->cv_tq[4], callSetup, cv_mem);

  /* update the sensitivities based on the final correction from the nonlinear solver */
  N_VLinearSumVectorArray(cv_mem->cv_Ns,
                          ONE, cv_mem->cv_znS[0],
                          ONE, cv_mem->cv_acorS, cv_mem->cv_yS);

  /* if the solve is successful, update Jacobian status */
  if (flag == CV_SUCCESS) cv_mem->cv_jcur = SUNFALSE;

  cv_mem->sens_solve = SUNFALSE;

  return(flag);

}

/*
 * cvStgr1Nls
 *
 * This is a high-level routine that attempts to solve the i-th 
 * sensitivity linear system using the attached nonlinear solver
 * once the states y_n were obtained and passed the error test.
 */

static int cvStgr1Nls(CVodeMem cv_mem, int is)
{
  booleantype callSetup;
  long int nni;
  int flag=CV_SUCCESS;

  cv_mem->sens_solve = SUNTRUE;

  callSetup = SUNFALSE;
  if (cv_mem->cv_lsetup == NULL)
    cv_mem->cv_crateS = ONE;

  /* initial guess for the correction to the predictor */
  N_VConst(ZERO, cv_mem->cv_tempvS[is]);

  /* solve the nonlinear system */
  flag = SUNNonlinSolSolve(cv_mem->NLSstg1,
                           cv_mem->cv_tempvS[is], cv_mem->cv_acorS[is],
                           cv_mem->cv_ewtS[is], cv_mem->cv_tq[4], callSetup, cv_mem);

  /* update the sensitivity with the final correction from the nonlinear solver */
  N_VLinearSum(ONE, cv_mem->cv_znS[0][is],
               ONE, cv_mem->cv_acorS[is], cv_mem->cv_yS[is]);

  /* if the solve is successful, update Jacobian status */
  if (flag == CV_SUCCESS) cv_mem->cv_jcur = SUNFALSE;

  /* update nniS iteration count */
  (void) SUNNonlinSolGetNumIters(cv_mem->NLSstg1, &nni);
  cv_mem->cv_nniS1[is] += nni - cv_mem->nnip;
  cv_mem->nnip = nni;

  cv_mem->sens_solve = SUNFALSE;

  return(flag);

}

/*
 * cvHandleNFlag
 *
 * This routine takes action on the return value nflag = *nflagPtr
 * returned by cvNls, as follows:
 *
 * If cvNls succeeded in solving the nonlinear system, then
 * cvHandleNFlag returns the constant DO_ERROR_TEST, which tells cvStep
 * to perform the error test.
 *
 * If the nonlinear system was not solved successfully, then ncfn and
 * ncf = *ncfPtr are incremented and Nordsieck array zn is restored.
 *
 * If the solution of the nonlinear system failed due to an
 * unrecoverable failure by setup, we return the value CV_LSETUP_FAIL.
 * 
 * If it failed due to an unrecoverable failure in solve, then we return
 * the value CV_LSOLVE_FAIL.
 *
 * If it failed due to an unrecoverable failure in rhs, then we return
 * the value CV_RHSFUNC_FAIL.
 *
 * If it failed due to an unrecoverable failure in quad rhs, then we return
 * the value CV_QRHSFUNC_FAIL.
 *
 * If it failed due to an unrecoverable failure in sensi rhs, then we return
 * the value CV_SRHSFUNC_FAIL.
 *
 * Otherwise, a recoverable failure occurred when solving the
 * nonlinear system (cvNls returned nflag = SUN_NLS_CONV_RECVT, RHSFUNC_RECVR,
 * or SRHSFUNC_RECVR).
 * In this case, if ncf is now equal to maxncf or |h| = hmin, 
 * we return the value CV_CONV_FAILURE (if nflag=SUN_NLS_CONV_RECVR), or
 * CV_REPTD_RHSFUNC_ERR (if nflag=RHSFUNC_RECVR), or CV_REPTD_SRHSFUNC_ERR
 * (if nflag=SRHSFUNC_RECVR).
 * If not, we set *nflagPtr = PREV_CONV_FAIL and return the value
 * PREDICT_AGAIN, telling cvStep to reattempt the step.
 *
 */

static int cvHandleNFlag(CVodeMem cv_mem, int *nflagPtr, realtype saved_t,
                         int *ncfPtr, long int *ncfnPtr)
{
  int nflag;
  
  nflag = *nflagPtr;
  
  if (nflag == CV_SUCCESS) return(DO_ERROR_TEST);

  /* The nonlinear soln. failed; increment ncfn and restore zn */
  (*ncfnPtr)++;
  cvRestore(cv_mem, saved_t);
  
  /* Return if failed unrecoverably */
  if (nflag < 0) return(nflag);

  /* At this point, nflag = SUN_NLS_CONV_RECVR, CONSTR_RECVR, RHSFUNC_RECVR,
     or SRHSFUNC_RECVR; increment ncf */

  (*ncfPtr)++;
  cv_mem->cv_etamax = ONE;

  /* If we had maxncf failures or |h| = hmin,
     return CV_CONV_FAILURE, CV_CONSTR_FAIL,
     CV_REPTD_RHSFUNC_ERR, CV_REPTD_QRHSFUNC_ERR,
     CV_REPTD_SRHSFUNC_ERR, or CV_CONSTR_FAIL */

  if ((SUNRabs(cv_mem->cv_h) <= cv_mem->cv_hmin*ONEPSM) ||
      (*ncfPtr == cv_mem->cv_maxncf)) {
    if (nflag == SUN_NLS_CONV_RECVR) return(CV_CONV_FAILURE);
    if (nflag == CONSTR_RECVR)       return(CV_CONSTR_FAIL);
    if (nflag == RHSFUNC_RECVR)      return(CV_REPTD_RHSFUNC_ERR);    
    if (nflag == QRHSFUNC_RECVR)     return(CV_REPTD_QRHSFUNC_ERR);    
    if (nflag == SRHSFUNC_RECVR)     return(CV_REPTD_SRHSFUNC_ERR);    
    if (nflag == QSRHSFUNC_RECVR)    return(CV_REPTD_QSRHSFUNC_ERR);    
  }

  /* Reduce step size; return to reattempt the step
     Note that if nflag=CONSTR_RECVR then eta was already set in CVNls */
  if (nflag != CONSTR_RECVR)
    cv_mem->cv_eta = SUNMAX(ETACF, cv_mem->cv_hmin / SUNRabs(cv_mem->cv_h));
  *nflagPtr = PREV_CONV_FAIL;
  cvRescale(cv_mem);

  return(PREDICT_AGAIN);
}

/*
 * cvRestore
 *
 * This routine restores the value of cv_mem->cv_tn to saved_t and undoes the
 * prediction.  After execution of cvRestore, the Nordsieck array zn has
 * the same values as before the call to cvPredict.
 */

static void cvRestore(CVodeMem cv_mem, realtype saved_t)
{
  int j, k;

  cv_mem->cv_tn = saved_t;
  for (k = 1; k <= cv_mem->cv_q; k++)
    for (j = cv_mem->cv_q; j >= k; j--)
      N_VLinearSum(ONE, cv_mem->cv_zn[j-1], -ONE,
                   cv_mem->cv_zn[j], cv_mem->cv_zn[j-1]);

  if (cv_mem->cv_quadr) {
    for (k = 1; k <= cv_mem->cv_q; k++)
      for (j = cv_mem->cv_q; j >= k; j--)
        N_VLinearSum(ONE, cv_mem->cv_znQ[j-1], -ONE,
                     cv_mem->cv_znQ[j], cv_mem->cv_znQ[j-1]);
  }

  if (cv_mem->cv_sensi) {
    for (k = 1; k <= cv_mem->cv_q; k++)
      for (j = cv_mem->cv_q; j >= k; j--)
        (void) N_VLinearSumVectorArray(cv_mem->cv_Ns,
                                       ONE,  cv_mem->cv_znS[j-1],
                                       -ONE, cv_mem->cv_znS[j],
                                       cv_mem->cv_znS[j-1]);
  }

  if (cv_mem->cv_quadr_sensi) {
    for (k = 1; k <= cv_mem->cv_q; k++)
      for (j = cv_mem->cv_q; j >= k; j--)
        (void) N_VLinearSumVectorArray(cv_mem->cv_Ns,
                                       ONE,  cv_mem->cv_znQS[j-1],
                                       -ONE, cv_mem->cv_znQS[j],
                                       cv_mem->cv_znQS[j-1]);
  }
}

/* 
 * -----------------------------------------------------------------
 * Error Test
 * -----------------------------------------------------------------
 */

/*
 * cvDoErrorTest
 *
 * This routine performs the local error test, for the state, quadrature, 
 * or sensitivity variables. Its last three arguments change depending
 * on which variables the error test is to be performed on.
 * 
 * The weighted local error norm dsm is loaded into *dsmPtr, and 
 * the test dsm ?<= 1 is made.
 *
 * If the test passes, cvDoErrorTest returns CV_SUCCESS. 
 *
 * If the test fails, we undo the step just taken (call cvRestore) and 
 *
 *   - if maxnef error test failures have occurred or if SUNRabs(h) = hmin,
 *     we return CV_ERR_FAILURE.
 *
 *   - if more than MXNEF1 error test failures have occurred, an order
 *     reduction is forced. If already at order 1, restart by reloading 
 *     zn from scratch (also znQ and znS if appropriate).
 *     If f() fails, we return CV_RHSFUNC_FAIL or CV_UNREC_RHSFUNC_ERR;
 *     if fQ() fails, we return CV_QRHSFUNC_FAIL or CV_UNREC_QRHSFUNC_ERR;
 *     if cvSensRhsWrapper() fails, we return CV_SRHSFUNC_FAIL or CV_UNREC_SRHSFUNC_ERR;
 *     (no recovery is possible at this stage).
 *
 *   - otherwise, set *nflagPtr to PREV_ERR_FAIL, and return TRY_AGAIN. 
 *
 */

static int cvDoErrorTest(CVodeMem cv_mem, int *nflagPtr, realtype saved_t, 
                         realtype acor_nrm,
                         int *nefPtr, long int *netfPtr, realtype *dsmPtr)
{
  realtype dsm;
  int retval, is;
  N_Vector wrk1, wrk2;

  dsm = acor_nrm * cv_mem->cv_tq[2];

  /* If est. local error norm dsm passes test, return CV_SUCCESS */  
  *dsmPtr = dsm; 
  if (dsm <= ONE) return(CV_SUCCESS);
  
  /* Test failed; increment counters, set nflag, and restore zn array */
  (*nefPtr)++;
  (*netfPtr)++;
  *nflagPtr = PREV_ERR_FAIL;
  cvRestore(cv_mem, saved_t);

  /* At maxnef failures or |h| = hmin, return CV_ERR_FAILURE */
  if ((SUNRabs(cv_mem->cv_h) <= cv_mem->cv_hmin*ONEPSM) ||
      (*nefPtr == cv_mem->cv_maxnef))
    return(CV_ERR_FAILURE);

  /* Set etamax = 1 to prevent step size increase at end of this step */
  cv_mem->cv_etamax = ONE;

  /* Set h ratio eta from dsm, rescale, and return for retry of step */
  if (*nefPtr <= MXNEF1) {
    cv_mem->cv_eta = ONE / (SUNRpowerR(BIAS2*dsm,ONE/cv_mem->cv_L) + ADDON);
    cv_mem->cv_eta = SUNMAX(ETAMIN, SUNMAX(cv_mem->cv_eta,
                                           cv_mem->cv_hmin / SUNRabs(cv_mem->cv_h)));
    if (*nefPtr >= SMALL_NEF)
      cv_mem->cv_eta = SUNMIN(cv_mem->cv_eta, ETAMXF);
    cvRescale(cv_mem);
    return(TRY_AGAIN);
  }
  
  /* After MXNEF1 failures, force an order reduction and retry step */
  if (cv_mem->cv_q > 1) {
    cv_mem->cv_eta = SUNMAX(ETAMIN, cv_mem->cv_hmin / SUNRabs(cv_mem->cv_h));
    cvAdjustOrder(cv_mem,-1);
    cv_mem->cv_L = cv_mem->cv_q;
    cv_mem->cv_q--;
    cv_mem->cv_qwait = cv_mem->cv_L;
    cvRescale(cv_mem);
    return(TRY_AGAIN);
  }

  /* If already at order 1, restart: reload zn, znQ, znS, znQS from scratch */
  cv_mem->cv_eta = SUNMAX(ETAMIN, cv_mem->cv_hmin / SUNRabs(cv_mem->cv_h));
  cv_mem->cv_h *= cv_mem->cv_eta;
  cv_mem->cv_next_h = cv_mem->cv_h;
  cv_mem->cv_hscale = cv_mem->cv_h;
  cv_mem->cv_qwait = LONG_WAIT;
  cv_mem->cv_nscon = 0;

  retval = cv_mem->cv_f(cv_mem->cv_tn, cv_mem->cv_zn[0],
                        cv_mem->cv_tempv, cv_mem->cv_user_data);
  cv_mem->cv_nfe++;
  if (retval < 0) return(CV_RHSFUNC_FAIL);
  if (retval > 0) return(CV_UNREC_RHSFUNC_ERR);

  N_VScale(cv_mem->cv_h, cv_mem->cv_tempv, cv_mem->cv_zn[1]);

  if (cv_mem->cv_quadr) {

    retval = cv_mem->cv_fQ(cv_mem->cv_tn, cv_mem->cv_zn[0],
                           cv_mem->cv_tempvQ, cv_mem->cv_user_data);
    cv_mem->cv_nfQe++;
    if (retval < 0) return(CV_QRHSFUNC_FAIL);
    if (retval > 0) return(CV_UNREC_QRHSFUNC_ERR);

    N_VScale(cv_mem->cv_h, cv_mem->cv_tempvQ, cv_mem->cv_znQ[1]);

  }

  if (cv_mem->cv_sensi) {

    wrk1 = cv_mem->cv_ftemp;
    wrk2 = cv_mem->cv_ftempS[0];

    retval = cvSensRhsWrapper(cv_mem, cv_mem->cv_tn, cv_mem->cv_zn[0], 
                              cv_mem->cv_tempv, cv_mem->cv_znS[0],
                              cv_mem->cv_tempvS, wrk1, wrk2);
    if (retval < 0) return(CV_SRHSFUNC_FAIL);
    if (retval > 0) return(CV_UNREC_SRHSFUNC_ERR);

    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = cv_mem->cv_h;
      
    retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                 cv_mem->cv_tempvS, cv_mem->cv_znS[1]);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
  }

  if (cv_mem->cv_quadr_sensi) {

    wrk1 = cv_mem->cv_ftemp;
    wrk2 = cv_mem->cv_ftempQ;

    retval = cv_mem->cv_fQS(cv_mem->cv_Ns, cv_mem->cv_tn,
                            cv_mem->cv_zn[0], cv_mem->cv_znS[0], 
                            cv_mem->cv_tempvQ, cv_mem->cv_tempvQS,
                            cv_mem->cv_fQS_data, wrk1, wrk2);
    cv_mem->cv_nfQSe++;
    if (retval < 0) return(CV_QSRHSFUNC_FAIL);
    if (retval > 0) return(CV_UNREC_QSRHSFUNC_ERR);

    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = cv_mem->cv_h;

    retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                 cv_mem->cv_tempvQS, cv_mem->cv_znQS[1]);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
  }
  
  return(TRY_AGAIN);
}

/* 
 * -----------------------------------------------------------------
 * Functions called after a successful step
 * -----------------------------------------------------------------
 */

/*
 * cvCompleteStep
 *
 * This routine performs various update operations when the solution
 * to the nonlinear system has passed the local error test. 
 * We increment the step counter nst, record the values hu and qu,
 * update the tau array, and apply the corrections to the zn array.
 * The tau[i] are the last q values of h, with tau[1] the most recent.
 * The counter qwait is decremented, and if qwait == 1 (and q < qmax)
 * we save acor and tq[5] for a possible order increase.
 */

static void cvCompleteStep(CVodeMem cv_mem)
{
  int i;
  int is;
  
  cv_mem->cv_nst++;
  cv_mem->cv_nscon++;
  cv_mem->cv_hu = cv_mem->cv_h;
  cv_mem->cv_qu = cv_mem->cv_q;

  for (i=cv_mem->cv_q; i >= 2; i--)
    cv_mem->cv_tau[i] = cv_mem->cv_tau[i-1];
  if ((cv_mem->cv_q==1) && (cv_mem->cv_nst > 1))
    cv_mem->cv_tau[2] = cv_mem->cv_tau[1];
  cv_mem->cv_tau[1] = cv_mem->cv_h;

  /* Apply correction to column j of zn: l_j * Delta_n */
  (void) N_VScaleAddMulti(cv_mem->cv_q+1, cv_mem->cv_l, cv_mem->cv_acor,
                          cv_mem->cv_zn, cv_mem->cv_zn);

  if (cv_mem->cv_quadr)
    (void) N_VScaleAddMulti(cv_mem->cv_q+1, cv_mem->cv_l, cv_mem->cv_acorQ,
                            cv_mem->cv_znQ, cv_mem->cv_znQ);

  if (cv_mem->cv_sensi)
    (void) N_VScaleAddMultiVectorArray(cv_mem->cv_Ns, cv_mem->cv_q+1,
                                       cv_mem->cv_l, cv_mem->cv_acorS,
                                       cv_mem->cv_znS, cv_mem->cv_znS);

  if (cv_mem->cv_quadr_sensi)
    (void) N_VScaleAddMultiVectorArray(cv_mem->cv_Ns, cv_mem->cv_q+1,
                                       cv_mem->cv_l, cv_mem->cv_acorQS,
                                       cv_mem->cv_znQS, cv_mem->cv_znQS);

  /* If necessary, store Delta_n in zn[qmax] to be used in order increase.
   * This actually will be Delta_{n-1} in the ELTE at q+1 since it happens at
   * the next to last step of order q before a possible one at order q+1
   */

  cv_mem->cv_qwait--;
  if ((cv_mem->cv_qwait == 1) && (cv_mem->cv_q != cv_mem->cv_qmax)) {
    
    N_VScale(ONE, cv_mem->cv_acor, cv_mem->cv_zn[cv_mem->cv_qmax]);
    
    if (cv_mem->cv_quadr)
      N_VScale(ONE, cv_mem->cv_acorQ, cv_mem->cv_znQ[cv_mem->cv_qmax]);

    if (cv_mem->cv_sensi) {
      for (is=0; is<cv_mem->cv_Ns; is++)
        cv_mem->cv_cvals[is] = ONE;
        
      (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                 cv_mem->cv_acorS, cv_mem->cv_znS[cv_mem->cv_qmax]);
    }
    
    if (cv_mem->cv_quadr_sensi) {
      for (is=0; is<cv_mem->cv_Ns; is++)
        cv_mem->cv_cvals[is] = ONE;

      (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                 cv_mem->cv_acorQS, cv_mem->cv_znQS[cv_mem->cv_qmax]);
    }

    cv_mem->cv_saved_tq5 = cv_mem->cv_tq[5];
    cv_mem->cv_indx_acor = cv_mem->cv_qmax;
  }

}

/*
 * cvPrepareNextStep
 *
 * This routine handles the setting of stepsize and order for the
 * next step -- hprime and qprime.  Along with hprime, it sets the
 * ratio eta = hprime/h.  It also updates other state variables 
 * related to a change of step size or order. 
 */

static void cvPrepareNextStep(CVodeMem cv_mem, realtype dsm)
{
  /* If etamax = 1, defer step size or order changes */
  if (cv_mem->cv_etamax == ONE) {
    cv_mem->cv_qwait = SUNMAX(cv_mem->cv_qwait, 2);
    cv_mem->cv_qprime = cv_mem->cv_q;
    cv_mem->cv_hprime = cv_mem->cv_h;
    cv_mem->cv_eta = ONE;
    return;
  }

  /* etaq is the ratio of new to old h at the current order */  
  cv_mem->cv_etaq = ONE /(SUNRpowerR(BIAS2*dsm,ONE/cv_mem->cv_L) + ADDON);
  
  /* If no order change, adjust eta and acor in cvSetEta and return */
  if (cv_mem->cv_qwait != 0) {
    cv_mem->cv_eta = cv_mem->cv_etaq;
    cv_mem->cv_qprime = cv_mem->cv_q;
    cvSetEta(cv_mem);
    return;
  }
  
  /* If qwait = 0, consider an order change.   etaqm1 and etaqp1 are 
     the ratios of new to old h at orders q-1 and q+1, respectively.
     cvChooseEta selects the largest; cvSetEta adjusts eta and acor */
  cv_mem->cv_qwait = 2;
  cv_mem->cv_etaqm1 = cvComputeEtaqm1(cv_mem);
  cv_mem->cv_etaqp1 = cvComputeEtaqp1(cv_mem);  
  cvChooseEta(cv_mem); 
  cvSetEta(cv_mem);
}

/*
 * cvSetEta
 *
 * This routine adjusts the value of eta according to the various
 * heuristic limits and the optional input hmax.
 */

static void cvSetEta(CVodeMem cv_mem)
{

  /* If eta below the threshhold THRESH, reject a change of step size */
  if (cv_mem->cv_eta < THRESH) {
    cv_mem->cv_eta = ONE;
    cv_mem->cv_hprime = cv_mem->cv_h;
  } else {
    /* Limit eta by etamax and hmax, then set hprime */
    cv_mem->cv_eta = SUNMIN(cv_mem->cv_eta, cv_mem->cv_etamax);
    cv_mem->cv_eta /= SUNMAX(ONE, SUNRabs(cv_mem->cv_h) *
                             cv_mem->cv_hmax_inv*cv_mem->cv_eta);
    cv_mem->cv_hprime = cv_mem->cv_h * cv_mem->cv_eta;
    if (cv_mem->cv_qprime < cv_mem->cv_q) cv_mem->cv_nscon = 0;
  }
}

/*
 * cvComputeEtaqm1
 *
 * This routine computes and returns the value of etaqm1 for a
 * possible decrease in order by 1.
 */

static realtype cvComputeEtaqm1(CVodeMem cv_mem)
{
  realtype ddn;
  
  cv_mem->cv_etaqm1 = ZERO;

  if (cv_mem->cv_q > 1) {

    ddn = N_VWrmsNorm(cv_mem->cv_zn[cv_mem->cv_q], cv_mem->cv_ewt);

    if ( cv_mem->cv_quadr && cv_mem->cv_errconQ )
      ddn = cvQuadUpdateNorm(cv_mem, ddn, cv_mem->cv_znQ[cv_mem->cv_q],
                             cv_mem->cv_ewtQ);

    if ( cv_mem->cv_sensi && cv_mem->cv_errconS )
      ddn = cvSensUpdateNorm(cv_mem, ddn, cv_mem->cv_znS[cv_mem->cv_q],
                             cv_mem->cv_ewtS);

    if ( cv_mem->cv_quadr_sensi && cv_mem->cv_errconQS )
      ddn = cvQuadSensUpdateNorm(cv_mem, ddn, cv_mem->cv_znQS[cv_mem->cv_q],
                                 cv_mem->cv_ewtQS);

    ddn = ddn * cv_mem->cv_tq[1];
    cv_mem->cv_etaqm1 = ONE/(SUNRpowerR(BIAS1*ddn, ONE/cv_mem->cv_q) + ADDON);
  }

  return(cv_mem->cv_etaqm1);
}

/*
 * cvComputeEtaqp1
 *
 * This routine computes and returns the value of etaqp1 for a
 * possible increase in order by 1.
 */

static realtype cvComputeEtaqp1(CVodeMem cv_mem)
{
  realtype dup, cquot;
  
  cv_mem->cv_etaqp1 = ZERO;

  if (cv_mem->cv_q != cv_mem->cv_qmax) {

    if (cv_mem->cv_saved_tq5 == ZERO) return(cv_mem->cv_etaqp1);

    cquot = (cv_mem->cv_tq[5] / cv_mem->cv_saved_tq5) *
      SUNRpowerI(cv_mem->cv_h/cv_mem->cv_tau[2], cv_mem->cv_L);
    N_VLinearSum(-cquot, cv_mem->cv_zn[cv_mem->cv_qmax], ONE,
                 cv_mem->cv_acor, cv_mem->cv_tempv);
    dup = N_VWrmsNorm(cv_mem->cv_tempv, cv_mem->cv_ewt);

    if ( cv_mem->cv_quadr && cv_mem->cv_errconQ ) {
      N_VLinearSum(-cquot, cv_mem->cv_znQ[cv_mem->cv_qmax], ONE,
                   cv_mem->cv_acorQ, cv_mem->cv_tempvQ);
      dup = cvQuadUpdateNorm(cv_mem, dup, cv_mem->cv_tempvQ, cv_mem->cv_ewtQ);
    }

    if ( cv_mem->cv_sensi && cv_mem->cv_errconS ) {
      (void) N_VLinearSumVectorArray(cv_mem->cv_Ns,
                                     -cquot, cv_mem->cv_znS[cv_mem->cv_qmax],
                                     ONE,    cv_mem->cv_acorS,
                                     cv_mem->cv_tempvS);

      dup = cvSensUpdateNorm(cv_mem, dup, cv_mem->cv_tempvS, cv_mem->cv_ewtS);
    }

    if ( cv_mem->cv_quadr_sensi && cv_mem->cv_errconQS ) {
      (void) N_VLinearSumVectorArray(cv_mem->cv_Ns,
                                     -cquot, cv_mem->cv_znQS[cv_mem->cv_qmax],
                                     ONE,    cv_mem->cv_acorQS,
                                     cv_mem->cv_tempvQS);

      dup = cvSensUpdateNorm(cv_mem, dup, cv_mem->cv_tempvQS, cv_mem->cv_ewtQS);
    }

    dup = dup * cv_mem->cv_tq[3];
    cv_mem->cv_etaqp1 = ONE / (SUNRpowerR(BIAS3*dup, ONE/(cv_mem->cv_L+1)) + ADDON);
  }

  return(cv_mem->cv_etaqp1);
}

/*
 * cvChooseEta
 * Given etaqm1, etaq, etaqp1 (the values of eta for qprime =
 * q - 1, q, or q + 1, respectively), this routine chooses the 
 * maximum eta value, sets eta to that value, and sets qprime to the
 * corresponding value of q.  If there is a tie, the preference
 * order is to (1) keep the same order, then (2) decrease the order,
 * and finally (3) increase the order.  If the maximum eta value
 * is below the threshhold THRESH, the order is kept unchanged and
 * eta is set to 1.
 */

static void cvChooseEta(CVodeMem cv_mem)
{
  realtype etam;
  int is;
  
  etam = SUNMAX(cv_mem->cv_etaqm1, SUNMAX(cv_mem->cv_etaq, cv_mem->cv_etaqp1));
  
  if (etam < THRESH) {
    cv_mem->cv_eta = ONE;
    cv_mem->cv_qprime = cv_mem->cv_q;
    return;
  }

  if (etam == cv_mem->cv_etaq) {

    cv_mem->cv_eta = cv_mem->cv_etaq;
    cv_mem->cv_qprime = cv_mem->cv_q;

  } else if (etam == cv_mem->cv_etaqm1) {

    cv_mem->cv_eta = cv_mem->cv_etaqm1;
    cv_mem->cv_qprime = cv_mem->cv_q - 1;

  } else {

    cv_mem->cv_eta = cv_mem->cv_etaqp1;
    cv_mem->cv_qprime = cv_mem->cv_q + 1;

    if (cv_mem->cv_lmm == CV_BDF) {

      /* 
       * Store Delta_n in zn[qmax] to be used in order increase 
       *
       * This happens at the last step of order q before an increase
       * to order q+1, so it represents Delta_n in the ELTE at q+1
       */
      
      N_VScale(ONE, cv_mem->cv_acor, cv_mem->cv_zn[cv_mem->cv_qmax]);
      
      if (cv_mem->cv_quadr && cv_mem->cv_errconQ)
        N_VScale(ONE, cv_mem->cv_acorQ, cv_mem->cv_znQ[cv_mem->cv_qmax]);
      
      if (cv_mem->cv_sensi && cv_mem->cv_errconS) {
        for (is=0; is<cv_mem->cv_Ns; is++)
          cv_mem->cv_cvals[is] = ONE;

        (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                   cv_mem->cv_acorS, cv_mem->cv_znS[cv_mem->cv_qmax]);
      }

      if (cv_mem->cv_quadr_sensi && cv_mem->cv_errconQS) {
        for (is=0; is<cv_mem->cv_Ns; is++)
          cv_mem->cv_cvals[is] = ONE;

        (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                   cv_mem->cv_acorQS, cv_mem->cv_znQS[cv_mem->cv_qmax]);
      }

    }
  }
}

/* 
 * -----------------------------------------------------------------
 * Function to handle failures
 * -----------------------------------------------------------------
 */

/*
 * cvHandleFailure
 *
 * This routine prints error messages for all cases of failure by
 * cvHin or cvStep. 
 * It returns to CVode the value that CVode is to return to the user.
 */

static int cvHandleFailure(CVodeMem cv_mem, int flag)
{

  /* Set vector of  absolute weighted local errors */
  /*
  N_VProd(acor, ewt, tempv);
  N_VAbs(tempv, tempv);
  */

  /* Depending on flag, print error message and return error flag */
  switch (flag) {
  case CV_ERR_FAILURE:
    cvProcessError(cv_mem, CV_ERR_FAILURE, "CVODES", "CVode",
                   MSGCV_ERR_FAILS, cv_mem->cv_tn, cv_mem->cv_h);
    break;
  case CV_CONV_FAILURE:
    cvProcessError(cv_mem, CV_CONV_FAILURE, "CVODES", "CVode",
                   MSGCV_CONV_FAILS, cv_mem->cv_tn, cv_mem->cv_h);
    break;
  case CV_LSETUP_FAIL:
    cvProcessError(cv_mem, CV_LSETUP_FAIL, "CVODES", "CVode",
                   MSGCV_SETUP_FAILED, cv_mem->cv_tn);
    break;
  case CV_LSOLVE_FAIL:
    cvProcessError(cv_mem, CV_LSOLVE_FAIL, "CVODES", "CVode",
                   MSGCV_SOLVE_FAILED, cv_mem->cv_tn);
    break;
  case CV_RHSFUNC_FAIL:
    cvProcessError(cv_mem, CV_RHSFUNC_FAIL, "CVODES", "CVode",
                   MSGCV_RHSFUNC_FAILED, cv_mem->cv_tn);
    break;
  case CV_UNREC_RHSFUNC_ERR:
    cvProcessError(cv_mem, CV_UNREC_RHSFUNC_ERR, "CVODES", "CVode",
                   MSGCV_RHSFUNC_UNREC, cv_mem->cv_tn);
    break;
  case CV_REPTD_RHSFUNC_ERR:
    cvProcessError(cv_mem, CV_REPTD_RHSFUNC_ERR, "CVODES", "CVode",
                   MSGCV_RHSFUNC_REPTD, cv_mem->cv_tn);
    break;
  case CV_RTFUNC_FAIL:
    cvProcessError(cv_mem, CV_RTFUNC_FAIL, "CVODES", "CVode",
                   MSGCV_RTFUNC_FAILED, cv_mem->cv_tn);
    break;
  case CV_QRHSFUNC_FAIL:
    cvProcessError(cv_mem, CV_QRHSFUNC_FAIL, "CVODES", "CVode",
                   MSGCV_QRHSFUNC_FAILED, cv_mem->cv_tn);
    break;
  case CV_UNREC_QRHSFUNC_ERR:
    cvProcessError(cv_mem, CV_UNREC_QRHSFUNC_ERR, "CVODES", "CVode",
                   MSGCV_QRHSFUNC_UNREC, cv_mem->cv_tn);
    break;
  case CV_REPTD_QRHSFUNC_ERR:
    cvProcessError(cv_mem, CV_REPTD_QRHSFUNC_ERR, "CVODES", "CVode",
                   MSGCV_QRHSFUNC_REPTD, cv_mem->cv_tn);
    break;
  case CV_SRHSFUNC_FAIL:
    cvProcessError(cv_mem, CV_SRHSFUNC_FAIL, "CVODES", "CVode",
                   MSGCV_SRHSFUNC_FAILED, cv_mem->cv_tn);
    break;
  case CV_UNREC_SRHSFUNC_ERR:
    cvProcessError(cv_mem, CV_UNREC_SRHSFUNC_ERR, "CVODES", "CVode",
                   MSGCV_SRHSFUNC_UNREC, cv_mem->cv_tn);
    break;
  case CV_REPTD_SRHSFUNC_ERR:
    cvProcessError(cv_mem, CV_REPTD_SRHSFUNC_ERR, "CVODES", "CVode",
                   MSGCV_SRHSFUNC_REPTD, cv_mem->cv_tn);
    break;
  case CV_QSRHSFUNC_FAIL:
    cvProcessError(cv_mem, CV_QSRHSFUNC_FAIL, "CVODES", "CVode",
                   MSGCV_QSRHSFUNC_FAILED, cv_mem->cv_tn);
    break;
  case CV_UNREC_QSRHSFUNC_ERR:
    cvProcessError(cv_mem, CV_UNREC_QSRHSFUNC_ERR, "CVODES", "CVode",
                   MSGCV_QSRHSFUNC_UNREC, cv_mem->cv_tn);
    break;
  case CV_REPTD_QSRHSFUNC_ERR:
    cvProcessError(cv_mem, CV_REPTD_QSRHSFUNC_ERR, "CVODES", "CVode",
                   MSGCV_QSRHSFUNC_REPTD, cv_mem->cv_tn);
    break;
  case CV_TOO_CLOSE:
    cvProcessError(cv_mem, CV_TOO_CLOSE, "CVODES", "CVode",
                   MSGCV_TOO_CLOSE);
    break;
  case CV_MEM_NULL:
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVode", MSGCV_NO_MEM);
    break;
  case SUN_NLS_MEM_NULL:
    cvProcessError(cv_mem, CV_MEM_NULL, "CVODES", "CVode", MSGCV_NLS_INPUT_NULL,
                   cv_mem->cv_tn);
    break;
  case CV_NLS_SETUP_FAIL:
    cvProcessError(cv_mem, CV_NLS_SETUP_FAIL, "CVODES", "CVode", MSGCV_NLS_SETUP_FAILED,
                   cv_mem->cv_tn);
    break;
  case CV_CONSTR_FAIL:
    cvProcessError(cv_mem, CV_CONSTR_FAIL, "CVODES", "CVode",
                   MSGCV_FAILED_CONSTR, cv_mem->cv_tn);
  default:
    return(CV_SUCCESS);
  }

  return(flag);
}

/* 
 * -----------------------------------------------------------------
 * Functions for BDF Stability Limit Detection   
 * -----------------------------------------------------------------
 */

/*
 * cvBDFStab
 *
 * This routine handles the BDF Stability Limit Detection Algorithm
 * STALD.  It is called if lmm = CV_BDF and the SLDET option is on.
 * If the order is 3 or more, the required norm data is saved.
 * If a decision to reduce order has not already been made, and
 * enough data has been saved, cvSLdet is called.  If it signals
 * a stability limit violation, the order is reduced, and the step
 * size is reset accordingly.
 */

static void cvBDFStab(CVodeMem cv_mem)
{
  int i,k, ldflag, factorial;
  realtype sq, sqm1, sqm2;
      
  /* If order is 3 or greater, then save scaled derivative data,
     push old data down in i, then add current values to top.    */

  if (cv_mem->cv_q >= 3) {
    for (k = 1; k <= 3; k++)
      for (i = 5; i >= 2; i--)
        cv_mem->cv_ssdat[i][k] = cv_mem->cv_ssdat[i-1][k];
    factorial = 1;
    for (i = 1; i <= cv_mem->cv_q-1; i++) factorial *= i;
    sq = factorial * cv_mem->cv_q * (cv_mem->cv_q+1) *
      cv_mem->cv_acnrm / SUNMAX(cv_mem->cv_tq[5],TINY);
    sqm1 = factorial * cv_mem->cv_q *
      N_VWrmsNorm(cv_mem->cv_zn[cv_mem->cv_q], cv_mem->cv_ewt);
    sqm2 = factorial *
      N_VWrmsNorm(cv_mem->cv_zn[cv_mem->cv_q-1], cv_mem->cv_ewt);
    cv_mem->cv_ssdat[1][1] = sqm2*sqm2;
    cv_mem->cv_ssdat[1][2] = sqm1*sqm1;
    cv_mem->cv_ssdat[1][3] = sq*sq;
  }  

  if (cv_mem->cv_qprime >= cv_mem->cv_q) {

    /* If order is 3 or greater, and enough ssdat has been saved,
       nscon >= q+5, then call stability limit detection routine.  */

    if ( (cv_mem->cv_q >= 3) && (cv_mem->cv_nscon >= cv_mem->cv_q+5) ) {
      ldflag = cvSLdet(cv_mem);
      if (ldflag > 3) {
        /* A stability limit violation is indicated by
           a return flag of 4, 5, or 6.
           Reduce new order.                     */
        cv_mem->cv_qprime = cv_mem->cv_q-1;
        cv_mem->cv_eta = cv_mem->cv_etaqm1; 
        cv_mem->cv_eta = SUNMIN(cv_mem->cv_eta,cv_mem->cv_etamax);
        cv_mem->cv_eta = cv_mem->cv_eta /
          SUNMAX(ONE,SUNRabs(cv_mem->cv_h)*cv_mem->cv_hmax_inv*cv_mem->cv_eta);
        cv_mem->cv_hprime = cv_mem->cv_h * cv_mem->cv_eta;
        cv_mem->cv_nor = cv_mem->cv_nor + 1;
      }
    }
  }
  else {
    /* Otherwise, let order increase happen, and 
       reset stability limit counter, nscon.     */
    cv_mem->cv_nscon = 0;
  }
}

/*
 * cvSLdet
 *
 * This routine detects stability limitation using stored scaled 
 * derivatives data. cvSLdet returns the magnitude of the
 * dominate characteristic root, rr. The presence of a stability
 * limit is indicated by rr > "something a little less then 1.0",  
 * and a positive kflag. This routine should only be called if
 * order is greater than or equal to 3, and data has been collected
 * for 5 time steps. 
 * 
 * Returned values:
 *    kflag = 1 -> Found stable characteristic root, normal matrix case
 *    kflag = 2 -> Found stable characteristic root, quartic solution
 *    kflag = 3 -> Found stable characteristic root, quartic solution,
 *                 with Newton correction
 *    kflag = 4 -> Found stability violation, normal matrix case
 *    kflag = 5 -> Found stability violation, quartic solution
 *    kflag = 6 -> Found stability violation, quartic solution,
 *                 with Newton correction
 *
 *    kflag < 0 -> No stability limitation, 
 *                 or could not compute limitation.
 *
 *    kflag = -1 -> Min/max ratio of ssdat too small.
 *    kflag = -2 -> For normal matrix case, vmax > vrrt2*vrrt2
 *    kflag = -3 -> For normal matrix case, The three ratios
 *                  are inconsistent.
 *    kflag = -4 -> Small coefficient prevents elimination of quartics.  
 *    kflag = -5 -> R value from quartics not consistent.
 *    kflag = -6 -> No corrected root passes test on qk values
 *    kflag = -7 -> Trouble solving for sigsq.
 *    kflag = -8 -> Trouble solving for B, or R via B.
 *    kflag = -9 -> R via sigsq[k] disagrees with R from data.
 */

static int cvSLdet(CVodeMem cv_mem)
{
  int i, k, j, it, kmin = 0, kflag = 0;
  realtype rat[5][4], rav[4], qkr[4], sigsq[4], smax[4], ssmax[4];
  realtype drr[4], rrc[4],sqmx[4], qjk[4][4], vrat[5], qc[6][4], qco[6][4];
  realtype rr, rrcut, vrrtol, vrrt2, sqtol, rrtol;
  realtype smink, smaxk, sumrat, sumrsq, vmin, vmax, drrmax, adrr;
  realtype tem, sqmax, saqk, qp, s, sqmaxk, saqj, sqmin;
  realtype rsa, rsb, rsc, rsd, rd1a, rd1b, rd1c;
  realtype rd2a, rd2b, rd3a, cest1, corr1; 
  realtype ratp, ratm, qfac1, qfac2, bb, rrb;

  /* The following are cutoffs and tolerances used by this routine */

  rrcut  = RCONST(0.98);
  vrrtol = RCONST(1.0e-4);
  vrrt2  = RCONST(5.0e-4);
  sqtol  = RCONST(1.0e-3);
  rrtol  = RCONST(1.0e-2);

  rr = ZERO;

  /*  Index k corresponds to the degree of the interpolating polynomial. */
  /*      k = 1 -> q-1          */
  /*      k = 2 -> q            */
  /*      k = 3 -> q+1          */

  /*  Index i is a backward-in-time index, i = 1 -> current time, */
  /*      i = 2 -> previous step, etc    */

  /* get maxima, minima, and variances, and form quartic coefficients  */

  for (k=1; k<=3; k++) {
    smink = cv_mem->cv_ssdat[1][k];
    smaxk = ZERO;
    
    for (i=1; i<=5; i++) {
      smink = SUNMIN(smink,cv_mem->cv_ssdat[i][k]);
      smaxk = SUNMAX(smaxk,cv_mem->cv_ssdat[i][k]);
    }
    
    if (smink < TINY*smaxk) {
      kflag = -1;  
      return(kflag);
    }
    smax[k] = smaxk;
    ssmax[k] = smaxk*smaxk;

    sumrat = ZERO;
    sumrsq = ZERO;
    for (i=1; i<=4; i++) {
      rat[i][k] = cv_mem->cv_ssdat[i][k] / cv_mem->cv_ssdat[i+1][k];
      sumrat = sumrat + rat[i][k];
      sumrsq = sumrsq + rat[i][k]*rat[i][k];
    } 
    rav[k] = FOURTH*sumrat;
    vrat[k] = SUNRabs(FOURTH*sumrsq - rav[k]*rav[k]);

    qc[5][k] = cv_mem->cv_ssdat[1][k] * cv_mem->cv_ssdat[3][k] -
      cv_mem->cv_ssdat[2][k] * cv_mem->cv_ssdat[2][k];
    qc[4][k] = cv_mem->cv_ssdat[2][k] * cv_mem->cv_ssdat[3][k] -
      cv_mem->cv_ssdat[1][k] * cv_mem->cv_ssdat[4][k];
    qc[3][k] = ZERO;
    qc[2][k] = cv_mem->cv_ssdat[2][k] * cv_mem->cv_ssdat[5][k] -
      cv_mem->cv_ssdat[3][k] * cv_mem->cv_ssdat[4][k];
    qc[1][k] = cv_mem->cv_ssdat[4][k] * cv_mem->cv_ssdat[4][k] -
      cv_mem->cv_ssdat[3][k] * cv_mem->cv_ssdat[5][k];

    for (i=1; i<=5; i++) {
      qco[i][k] = qc[i][k];
    }
  }                            /* End of k loop */
  
  /* Isolate normal or nearly-normal matrix case. The three quartics will
     have a common or nearly-common root in this case.
     Return a kflag = 1 if this procedure works. If the three roots
     differ more than vrrt2, return error kflag = -3.    */
  
  vmin = SUNMIN(vrat[1],SUNMIN(vrat[2],vrat[3]));
  vmax = SUNMAX(vrat[1],SUNMAX(vrat[2],vrat[3]));
  
  if (vmin < vrrtol*vrrtol) {

    if (vmax > vrrt2*vrrt2) {
      kflag = -2;  
      return(kflag);
    } else {
      rr = (rav[1] + rav[2] + rav[3])/THREE;
      drrmax = ZERO;
      for (k = 1;k<=3;k++) {
        adrr = SUNRabs(rav[k] - rr);
        drrmax = SUNMAX(drrmax, adrr);
      }
      if (drrmax > vrrt2) { kflag = -3; return(kflag); }
    
      kflag = 1;

      /*  can compute charactistic root, drop to next section   */
    }

  } else {

    /* use the quartics to get rr. */

    if (SUNRabs(qco[1][1]) < TINY*ssmax[1]) {
      kflag = -4;    
      return(kflag);
    }

    tem = qco[1][2]/qco[1][1];
    for (i=2; i<=5; i++) {
      qco[i][2] = qco[i][2] - tem*qco[i][1];
    }

    qco[1][2] = ZERO;
    tem = qco[1][3]/qco[1][1];
    for (i=2; i<=5; i++) {
      qco[i][3] = qco[i][3] - tem*qco[i][1];
    }
    qco[1][3] = ZERO;

    if (SUNRabs(qco[2][2]) < TINY*ssmax[2]) {
      kflag = -4;    
      return(kflag);
    }

    tem = qco[2][3]/qco[2][2];
    for (i=3; i<=5; i++) {
      qco[i][3] = qco[i][3] - tem*qco[i][2];
    }

    if (SUNRabs(qco[4][3]) < TINY*ssmax[3]) {
      kflag = -4;    
      return(kflag);
    }

    rr = -qco[5][3]/qco[4][3];

    if (rr < TINY || rr > HUNDRED) {
      kflag = -5;   
      return(kflag);
    }

    for (k=1; k<=3; k++)
      qkr[k] = qc[5][k] + rr*(qc[4][k] + rr*rr*(qc[2][k] + rr*qc[1][k]));

    sqmax = ZERO;
    for (k=1; k<=3; k++) {
      saqk = SUNRabs(qkr[k])/ssmax[k];
      if (saqk > sqmax) sqmax = saqk;
    } 

    if (sqmax < sqtol) {
      kflag = 2;

      /*  can compute charactistic root, drop to "given rr,etc"   */

    } else {

      /* do Newton corrections to improve rr.  */

      for (it=1; it<=3; it++) {
        for (k=1; k<=3; k++) {
          qp = qc[4][k] + rr*rr*(THREE*qc[2][k] + rr*FOUR*qc[1][k]);
          drr[k] = ZERO;
          if (SUNRabs(qp) > TINY*ssmax[k]) drr[k] = -qkr[k]/qp;
          rrc[k] = rr + drr[k];
        } 

        for (k=1; k<=3; k++) {
          s = rrc[k];
          sqmaxk = ZERO;
          for (j=1; j<=3; j++) {
            qjk[j][k] = qc[5][j] + s*(qc[4][j] + s*s*(qc[2][j] + s*qc[1][j]));
            saqj = SUNRabs(qjk[j][k])/ssmax[j];
            if (saqj > sqmaxk) sqmaxk = saqj;
          } 
          sqmx[k] = sqmaxk;
        }

        sqmin = sqmx[1] + ONE;
        for (k=1; k<=3; k++) {
          if (sqmx[k] < sqmin) {
            kmin = k;
            sqmin = sqmx[k];
          }
        } 
        rr = rrc[kmin];

        if (sqmin < sqtol) {
          kflag = 3;
          /*  can compute charactistic root   */
          /*  break out of Newton correction loop and drop to "given rr,etc" */ 
          break;
        } else {
          for (j=1; j<=3; j++) {
            qkr[j] = qjk[j][kmin];
          }
        }     
      } /*  end of Newton correction loop  */ 

      if (sqmin > sqtol) {
        kflag = -6;
        return(kflag);
      }
    } /*  end of if (sqmax < sqtol) else   */
  } /*  end of if (vmin < vrrtol*vrrtol) else, quartics to get rr. */

  /* given rr, find sigsq[k] and verify rr.  */
  /* All positive kflag drop to this section  */
  
  for (k=1; k<=3; k++) {
    rsa = cv_mem->cv_ssdat[1][k];
    rsb = cv_mem->cv_ssdat[2][k]*rr;
    rsc = cv_mem->cv_ssdat[3][k]*rr*rr;
    rsd = cv_mem->cv_ssdat[4][k]*rr*rr*rr;
    rd1a = rsa - rsb;
    rd1b = rsb - rsc;
    rd1c = rsc - rsd;
    rd2a = rd1a - rd1b;
    rd2b = rd1b - rd1c;
    rd3a = rd2a - rd2b;
    
    if (SUNRabs(rd1b) < TINY*smax[k]) {
      kflag = -7;
      return(kflag);
    }
    
    cest1 = -rd3a/rd1b;
    if (cest1 < TINY || cest1 > FOUR) {
      kflag = -7;
      return(kflag);
    }
    corr1 = (rd2b/cest1)/(rr*rr);
    sigsq[k] = cv_mem->cv_ssdat[3][k] + corr1;
  }
  
  if (sigsq[2] < TINY) {
    kflag = -8;
    return(kflag);
  }
  
  ratp = sigsq[3]/sigsq[2];
  ratm = sigsq[1]/sigsq[2];
  qfac1 = FOURTH*(cv_mem->cv_q*cv_mem->cv_q - ONE);
  qfac2 = TWO/(cv_mem->cv_q - ONE);
  bb = ratp*ratm - ONE - qfac1*ratp;
  tem = ONE - qfac2*bb;
  
  if (SUNRabs(tem) < TINY) {
    kflag = -8;
    return(kflag);
  }
  
  rrb = ONE/tem;
  
  if (SUNRabs(rrb - rr) > rrtol) {
    kflag = -9;
    return(kflag);
  }
  
  /* Check to see if rr is above cutoff rrcut  */
  if (rr > rrcut) {
    if (kflag == 1) kflag = 4;
    if (kflag == 2) kflag = 5;
    if (kflag == 3) kflag = 6;
  }
  
  /* All positive kflag returned at this point  */
  
  return(kflag);
  
}

/* 
 * -----------------------------------------------------------------
 * Functions for rootfinding
 * -----------------------------------------------------------------
 */

/* 
 * cvRcheck1
 *
 * This routine completes the initialization of rootfinding memory
 * information, and checks whether g has a zero both at and very near
 * the initial point of the IVP.
 *
 * This routine returns an int equal to:
 *  CV_RTFUNC_FAIL < 0 if the g function failed, or
 *  CV_SUCCESS     = 0 otherwise.
 */

static int cvRcheck1(CVodeMem cv_mem)
{
  int i, retval;
  realtype smallh, hratio, tplus;
  booleantype zroot;

  for (i = 0; i < cv_mem->cv_nrtfn; i++)
    cv_mem->cv_iroots[i] = 0;
  cv_mem->cv_tlo = cv_mem->cv_tn;
  cv_mem->cv_ttol = (SUNRabs(cv_mem->cv_tn) + SUNRabs(cv_mem->cv_h)) *
    cv_mem->cv_uround*HUNDRED;

  /* Evaluate g at initial t and check for zero values. */
  retval = cv_mem->cv_gfun(cv_mem->cv_tlo, cv_mem->cv_zn[0],
                           cv_mem->cv_glo, cv_mem->cv_user_data);
  cv_mem->cv_nge = 1;
  if (retval != 0) return(CV_RTFUNC_FAIL);

  zroot = SUNFALSE;
  for (i = 0; i < cv_mem->cv_nrtfn; i++) {
    if (SUNRabs(cv_mem->cv_glo[i]) == ZERO) {
      zroot = SUNTRUE;
      cv_mem->cv_gactive[i] = SUNFALSE;
    }
  }
  if (!zroot) return(CV_SUCCESS);

  /* Some g_i is zero at t0; look at g at t0+(small increment). */
  hratio = SUNMAX(cv_mem->cv_ttol/SUNRabs(cv_mem->cv_h), PT1);
  smallh = hratio*cv_mem->cv_h;
  tplus = cv_mem->cv_tlo + smallh;
  N_VLinearSum(ONE, cv_mem->cv_zn[0], hratio, cv_mem->cv_zn[1], cv_mem->cv_y);
  retval = cv_mem->cv_gfun(tplus, cv_mem->cv_y,
                           cv_mem->cv_ghi, cv_mem->cv_user_data);
  cv_mem->cv_nge++;
  if (retval != 0) return(CV_RTFUNC_FAIL);

  /* We check now only the components of g which were exactly 0.0 at t0
   * to see if we can 'activate' them. */
  for (i = 0; i < cv_mem->cv_nrtfn; i++) {
    if (!cv_mem->cv_gactive[i] && SUNRabs(cv_mem->cv_ghi[i]) != ZERO) {
      cv_mem->cv_gactive[i] = SUNTRUE;
      cv_mem->cv_glo[i] = cv_mem->cv_ghi[i];
    }
  }
  return(CV_SUCCESS);
}

/*
 * cvRcheck2
 *
 * This routine checks for exact zeros of g at the last root found,
 * if the last return was a root.  It then checks for a close pair of
 * zeros (an error condition), and for a new root at a nearby point.
 * The array glo = g(tlo) at the left endpoint of the search interval
 * is adjusted if necessary to assure that all g_i are nonzero
 * there, before returning to do a root search in the interval.
 *
 * On entry, tlo = tretlast is the last value of tret returned by
 * CVode.  This may be the previous tn, the previous tout value,
 * or the last root location.
 *
 * This routine returns an int equal to:
 *     CV_RTFUNC_FAIL  < 0 if the g function failed, or
 *     CLOSERT         = 3 if a close pair of zeros was found, or
 *     RTFOUND         = 1 if a new zero of g was found near tlo, or
 *     CV_SUCCESS      = 0 otherwise.
 */

static int cvRcheck2(CVodeMem cv_mem)
{
  int i, retval;
  realtype smallh, hratio, tplus;
  booleantype zroot;

  if (cv_mem->cv_irfnd == 0) return(CV_SUCCESS);

  (void) CVodeGetDky(cv_mem, cv_mem->cv_tlo, 0, cv_mem->cv_y);
  retval = cv_mem->cv_gfun(cv_mem->cv_tlo, cv_mem->cv_y,
                           cv_mem->cv_glo, cv_mem->cv_user_data);
  cv_mem->cv_nge++;
  if (retval != 0) return(CV_RTFUNC_FAIL);

  zroot = SUNFALSE;
  for (i = 0; i < cv_mem->cv_nrtfn; i++)
    cv_mem->cv_iroots[i] = 0;
  for (i = 0; i < cv_mem->cv_nrtfn; i++) {
    if (!cv_mem->cv_gactive[i]) continue;
    if (SUNRabs(cv_mem->cv_glo[i]) == ZERO) {
      zroot = SUNTRUE;
      cv_mem->cv_iroots[i] = 1;
    }
  }
  if (!zroot) return(CV_SUCCESS);

  /* One or more g_i has a zero at tlo.  Check g at tlo+smallh. */
  cv_mem->cv_ttol = (SUNRabs(cv_mem->cv_tn) + SUNRabs(cv_mem->cv_h)) *
    cv_mem->cv_uround*HUNDRED;
  smallh = (cv_mem->cv_h > ZERO) ? cv_mem->cv_ttol : -cv_mem->cv_ttol;
  tplus = cv_mem->cv_tlo + smallh;
  if ( (tplus - cv_mem->cv_tn)*cv_mem->cv_h >= ZERO) {
    hratio = smallh/cv_mem->cv_h;
    N_VLinearSum(ONE, cv_mem->cv_y, hratio, cv_mem->cv_zn[1], cv_mem->cv_y);
  } else {
    (void) CVodeGetDky(cv_mem, tplus, 0, cv_mem->cv_y);
  }
  retval = cv_mem->cv_gfun(tplus, cv_mem->cv_y,
                           cv_mem->cv_ghi, cv_mem->cv_user_data);
  cv_mem->cv_nge++;
  if (retval != 0) return(CV_RTFUNC_FAIL);

  /* Check for close roots (error return), for a new zero at tlo+smallh,
  and for a g_i that changed from zero to nonzero. */
  zroot = SUNFALSE;
  for (i = 0; i < cv_mem->cv_nrtfn; i++) {
    if (!cv_mem->cv_gactive[i]) continue;
    if (SUNRabs(cv_mem->cv_ghi[i]) == ZERO) {
      if (cv_mem->cv_iroots[i] == 1) return(CLOSERT);
      zroot = SUNTRUE;
      cv_mem->cv_iroots[i] = 1;
    } else {
      if (cv_mem->cv_iroots[i] == 1)
        cv_mem->cv_glo[i] = cv_mem->cv_ghi[i];
    }
  }
  if (zroot) return(RTFOUND);
  return(CV_SUCCESS);
}

/*
 * cvRcheck3
 *
 * This routine interfaces to cvRootfind to look for a root of g
 * between tlo and either tn or tout, whichever comes first.
 * Only roots beyond tlo in the direction of integration are sought.
 *
 * This routine returns an int equal to:
 *     CV_RTFUNC_FAIL  < 0 if the g function failed, or
 *     RTFOUND         = 1 if a root of g was found, or
 *     CV_SUCCESS      = 0 otherwise.
 */

static int cvRcheck3(CVodeMem cv_mem)
{
  int i, ier, retval;

  /* Set thi = tn or tout, whichever comes first; set y = y(thi). */
  if (cv_mem->cv_taskc == CV_ONE_STEP) {
    cv_mem->cv_thi = cv_mem->cv_tn;
    N_VScale(ONE, cv_mem->cv_zn[0], cv_mem->cv_y);
  }
  if (cv_mem->cv_taskc == CV_NORMAL) {
    if ( (cv_mem->cv_toutc - cv_mem->cv_tn)*cv_mem->cv_h >= ZERO) {
      cv_mem->cv_thi = cv_mem->cv_tn; 
      N_VScale(ONE, cv_mem->cv_zn[0], cv_mem->cv_y);
    } else {
      cv_mem->cv_thi = cv_mem->cv_toutc;
      (void) CVodeGetDky(cv_mem, cv_mem->cv_thi, 0, cv_mem->cv_y);
    }
  }

  /* Set ghi = g(thi) and call cvRootfind to search (tlo,thi) for roots. */
  retval = cv_mem->cv_gfun(cv_mem->cv_thi, cv_mem->cv_y,
                           cv_mem->cv_ghi, cv_mem->cv_user_data);
  cv_mem->cv_nge++;
  if (retval != 0) return(CV_RTFUNC_FAIL);

  cv_mem->cv_ttol = (SUNRabs(cv_mem->cv_tn) + SUNRabs(cv_mem->cv_h)) *
    cv_mem->cv_uround*HUNDRED;
  ier = cvRootfind(cv_mem);
  if (ier == CV_RTFUNC_FAIL) return(CV_RTFUNC_FAIL);
  for(i=0; i<cv_mem->cv_nrtfn; i++) {
    if(!cv_mem->cv_gactive[i] && cv_mem->cv_grout[i] != ZERO)
      cv_mem->cv_gactive[i] = SUNTRUE;
  }
  cv_mem->cv_tlo = cv_mem->cv_trout;
  for (i = 0; i < cv_mem->cv_nrtfn; i++)
    cv_mem->cv_glo[i] = cv_mem->cv_grout[i];

  /* If no root found, return CV_SUCCESS. */  
  if (ier == CV_SUCCESS) return(CV_SUCCESS);

  /* If a root was found, interpolate to get y(trout) and return.  */
  (void) CVodeGetDky(cv_mem, cv_mem->cv_trout, 0, cv_mem->cv_y);
  return(RTFOUND);
}

/*
 * cvRootfind
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
 *            (void) gfun(t, y, gt, user_data)
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
 *      CV_RTFUNC_FAIL  < 0 if the g function failed, or
 *      RTFOUND         = 1 if a root of g was found, or
 *      CV_SUCCESS      = 0 otherwise.
 */

static int cvRootfind(CVodeMem cv_mem)
{
  realtype alph, tmid, gfrac, maxfrac, fracint, fracsub;
  int i, retval, imax, side, sideprev;
  booleantype zroot, sgnchg;

  imax = 0;

  /* First check for change in sign in ghi or for a zero in ghi. */
  maxfrac = ZERO;
  zroot = SUNFALSE;
  sgnchg = SUNFALSE;
  for (i = 0;  i < cv_mem->cv_nrtfn; i++) {
    if(!cv_mem->cv_gactive[i]) continue;
    if (SUNRabs(cv_mem->cv_ghi[i]) == ZERO) {
      if(cv_mem->cv_rootdir[i]*cv_mem->cv_glo[i] <= ZERO) {
        zroot = SUNTRUE;
      }
    } else {
      if ( (cv_mem->cv_glo[i]*cv_mem->cv_ghi[i] < ZERO) &&
           (cv_mem->cv_rootdir[i]*cv_mem->cv_glo[i] <= ZERO) ) {
        gfrac = SUNRabs(cv_mem->cv_ghi[i]/(cv_mem->cv_ghi[i] - cv_mem->cv_glo[i]));
        if (gfrac > maxfrac) {
          sgnchg = SUNTRUE;
          maxfrac = gfrac;
          imax = i;
        }
      }
    }
  }

  /* If no sign change was found, reset trout and grout.  Then return
     CV_SUCCESS if no zero was found, or set iroots and return RTFOUND.  */ 
  if (!sgnchg) {
    cv_mem->cv_trout = cv_mem->cv_thi;
    for (i = 0; i < cv_mem->cv_nrtfn; i++)
      cv_mem->cv_grout[i] = cv_mem->cv_ghi[i];
    if (!zroot) return(CV_SUCCESS);
    for (i = 0; i < cv_mem->cv_nrtfn; i++) {
      cv_mem->cv_iroots[i] = 0;
      if(!cv_mem->cv_gactive[i]) continue;
      if ( (SUNRabs(cv_mem->cv_ghi[i]) == ZERO) &&
           (cv_mem->cv_rootdir[i]*cv_mem->cv_glo[i] <= ZERO) )
        cv_mem->cv_iroots[i] = cv_mem->cv_glo[i] > 0 ? -1:1;
    }
    return(RTFOUND);
  }

  /* Initialize alph to avoid compiler warning */
  alph = ONE;

  /* A sign change was found.  Loop to locate nearest root. */

  side = 0;  sideprev = -1;
  for(;;) {                                    /* Looping point */

    /* If interval size is already less than tolerance ttol, break. */
      if (SUNRabs(cv_mem->cv_thi - cv_mem->cv_tlo) <= cv_mem->cv_ttol) break;

    /* Set weight alph.
       On the first two passes, set alph = 1.  Thereafter, reset alph
       according to the side (low vs high) of the subinterval in which
       the sign change was found in the previous two passes.
       If the sides were opposite, set alph = 1.
       If the sides were the same, then double alph (if high side),
       or halve alph (if low side).
       The next guess tmid is the secant method value if alph = 1, but
       is closer to cv_mem->cv_tlo if alph < 1, and closer to thi if alph > 1.    */

    if (sideprev == side) {
      alph = (side == 2) ? alph*TWO : alph*HALF;
    } else {
      alph = ONE;
    }

    /* Set next root approximation tmid and get g(tmid).
       If tmid is too close to tlo or thi, adjust it inward,
       by a fractional distance that is between 0.1 and 0.5.  */
    tmid = cv_mem->cv_thi - (cv_mem->cv_thi - cv_mem->cv_tlo) *
      cv_mem->cv_ghi[imax] / (cv_mem->cv_ghi[imax] - alph*cv_mem->cv_glo[imax]);
    if (SUNRabs(tmid - cv_mem->cv_tlo) < HALF*cv_mem->cv_ttol) {
      fracint = SUNRabs(cv_mem->cv_thi - cv_mem->cv_tlo)/cv_mem->cv_ttol;
      fracsub = (fracint > FIVE) ? PT1 : HALF/fracint;
      tmid = cv_mem->cv_tlo + fracsub*(cv_mem->cv_thi - cv_mem->cv_tlo);
    }
    if (SUNRabs(cv_mem->cv_thi - tmid) < HALF*cv_mem->cv_ttol) {
      fracint = SUNRabs(cv_mem->cv_thi - cv_mem->cv_tlo)/cv_mem->cv_ttol;
      fracsub = (fracint > FIVE) ? PT1 : HALF/fracint;
      tmid = cv_mem->cv_thi - fracsub*(cv_mem->cv_thi - cv_mem->cv_tlo);
    }

    (void) CVodeGetDky(cv_mem, tmid, 0, cv_mem->cv_y);
    retval = cv_mem->cv_gfun(tmid, cv_mem->cv_y, cv_mem->cv_grout,
                             cv_mem->cv_user_data);
    cv_mem->cv_nge++;
    if (retval != 0) return(CV_RTFUNC_FAIL);

    /* Check to see in which subinterval g changes sign, and reset imax.
       Set side = 1 if sign change is on low side, or 2 if on high side.  */  
    maxfrac = ZERO;
    zroot = SUNFALSE;
    sgnchg = SUNFALSE;
    sideprev = side;
    for (i = 0;  i < cv_mem->cv_nrtfn; i++) {
      if(!cv_mem->cv_gactive[i]) continue;
      if (SUNRabs(cv_mem->cv_grout[i]) == ZERO) {
        if(cv_mem->cv_rootdir[i]*cv_mem->cv_glo[i] <= ZERO) zroot = SUNTRUE;
      } else {
        if ( (cv_mem->cv_glo[i]*cv_mem->cv_grout[i] < ZERO) &&
             (cv_mem->cv_rootdir[i]*cv_mem->cv_glo[i] <= ZERO) ) {
          gfrac = SUNRabs(cv_mem->cv_grout[i] /
                          (cv_mem->cv_grout[i] - cv_mem->cv_glo[i]));
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
      cv_mem->cv_thi = tmid;
      for (i = 0; i < cv_mem->cv_nrtfn; i++)
        cv_mem->cv_ghi[i] = cv_mem->cv_grout[i];
      side = 1;
      /* Stop at root thi if converged; otherwise loop. */
      if (SUNRabs(cv_mem->cv_thi - cv_mem->cv_tlo) <= cv_mem->cv_ttol) break;
      continue;  /* Return to looping point. */
    }

    if (zroot) {
      /* No sign change in (tlo,tmid), but g = 0 at tmid; return root tmid. */
      cv_mem->cv_thi = tmid;
      for (i = 0; i < cv_mem->cv_nrtfn; i++)
        cv_mem->cv_ghi[i] = cv_mem->cv_grout[i];
      break;
    }

    /* No sign change in (tlo,tmid), and no zero at tmid.
       Sign change must be in (tmid,thi).  Replace tlo with tmid. */
    cv_mem->cv_tlo = tmid;
    for (i = 0; i < cv_mem->cv_nrtfn; i++)
      cv_mem->cv_glo[i] = cv_mem->cv_grout[i];
    side = 2;
    /* Stop at root thi if converged; otherwise loop back. */
    if (SUNRabs(cv_mem->cv_thi - cv_mem->cv_tlo) <= cv_mem->cv_ttol) break;

  } /* End of root-search loop */

  /* Reset trout and grout, set iroots, and return RTFOUND. */
  cv_mem->cv_trout = cv_mem->cv_thi;
  for (i = 0; i < cv_mem->cv_nrtfn; i++) {
    cv_mem->cv_grout[i] = cv_mem->cv_ghi[i];
    cv_mem->cv_iroots[i] = 0;
    if(!cv_mem->cv_gactive[i]) continue;
    if ( (SUNRabs(cv_mem->cv_ghi[i]) == ZERO) &&
         (cv_mem->cv_rootdir[i]*cv_mem->cv_glo[i] <= ZERO) )
      cv_mem->cv_iroots[i] = cv_mem->cv_glo[i] > 0 ? -1:1;
    if ( (cv_mem->cv_glo[i]*cv_mem->cv_ghi[i] < ZERO) &&
         (cv_mem->cv_rootdir[i]*cv_mem->cv_glo[i] <= ZERO) ) 
      cv_mem->cv_iroots[i] = cv_mem->cv_glo[i] > 0 ? -1:1;
  }
  return(RTFOUND);
}

/* 
 * -----------------------------------------------------------------
 * Functions for combined norms
 * -----------------------------------------------------------------
 */

/*
 * cvQuadUpdateNorm
 *
 * Updates the norm old_nrm to account for all quadratures.
 */

static realtype cvQuadUpdateNorm(CVodeMem cv_mem, realtype old_nrm,
                                 N_Vector xQ, N_Vector wQ)
{
  realtype qnrm;

  qnrm = N_VWrmsNorm(xQ, wQ);
  if (old_nrm > qnrm) return(old_nrm);
  else                return(qnrm);
}

/*
 * cvSensNorm
 *
 * This routine returns the maximum over the weighted root mean 
 * square norm of xS with weight vectors wS:
 *
 *  max { wrms(xS[0],wS[0]) ... wrms(xS[Ns-1],wS[Ns-1]) }    
 *
 * Called by cvSensUpdateNorm or directly in the CV_STAGGERED approach 
 * during the NLS solution and before the error test.
 */

realtype cvSensNorm(CVodeMem cv_mem, N_Vector *xS, N_Vector *wS)
{
  int is;
  realtype nrm;

  (void) N_VWrmsNormVectorArray(cv_mem->cv_Ns, xS, wS, cv_mem->cv_cvals);
  
  nrm = cv_mem->cv_cvals[0];
  for (is=1; is<cv_mem->cv_Ns; is++)
    if ( cv_mem->cv_cvals[is] > nrm ) nrm = cv_mem->cv_cvals[is];

  return(nrm);
}

/*
 * cvSensUpdateNorm
 *
 * Updates the norm old_nrm to account for all sensitivities.
 */

realtype cvSensUpdateNorm(CVodeMem cv_mem, realtype old_nrm,
                          N_Vector *xS, N_Vector *wS)
{
  realtype snrm;
  
  snrm = cvSensNorm(cv_mem, xS, wS);
  if (old_nrm > snrm) return(old_nrm);
  else                return(snrm);
}

/*
 * cvQuadSensNorm
 *
 * This routine returns the maximum over the weighted root mean 
 * square norm of xQS with weight vectors wQS:
 *
 *  max { wrms(xQS[0],wS[0]) ... wrms(xQS[Ns-1],wS[Ns-1]) }    
 *
 * Called by cvQuadSensUpdateNorm.
 */

static realtype cvQuadSensNorm(CVodeMem cv_mem, N_Vector *xQS, N_Vector *wQS)
{
  int is;
  realtype nrm;

  (void) N_VWrmsNormVectorArray(cv_mem->cv_Ns, xQS, wQS, cv_mem->cv_cvals);
  
  nrm = cv_mem->cv_cvals[0];
  for (is=1; is<cv_mem->cv_Ns; is++)
    if ( cv_mem->cv_cvals[is] > nrm ) nrm = cv_mem->cv_cvals[is];

  return(nrm);
}

/*
 * cvSensUpdateNorm
 *
 * Updates the norm old_nrm to account for all quadrature sensitivities.
 */

static realtype cvQuadSensUpdateNorm(CVodeMem cv_mem, realtype old_nrm,
                                     N_Vector *xQS, N_Vector *wQS)
{
  realtype snrm;
  
  snrm = cvQuadSensNorm(cv_mem, xQS, wQS);
  if (old_nrm > snrm) return(old_nrm);
  else                return(snrm);
}

/* 
 * -----------------------------------------------------------------
 * Wrappers for sensitivity RHS
 * -----------------------------------------------------------------
 */

/*
 * cvSensRhsWrapper
 *
 * CVSensRhs is a high level routine that returns right hand side 
 * of sensitivity equations. Depending on the 'ifS' flag, it either 
 * calls directly the fS routine (ifS=CV_ALLSENS) or (if ifS=CV_ONESENS) 
 * calls the fS1 routine in a loop over all sensitivities.
 *
 * CVSensRhs is called:
 *  (*) by CVode at the first step
 *  (*) by cvYddNorm if errcon=SUNTRUE
 *  (*) by the nonlinear solver if ism=CV_SIMULTANEOUS
 *  (*) by cvDoErrorTest when restarting from scratch
 *  (*) in the corrector loop if ism=CV_STAGGERED
 *  (*) by cvStgrDoErrorTest when restarting from scratch 
 *
 * The return value is that of the sensitivity RHS function fS,
 *
 */

int cvSensRhsWrapper(CVodeMem cv_mem, realtype time, 
                     N_Vector ycur, N_Vector fcur, 
                     N_Vector *yScur, N_Vector *fScur,
                     N_Vector temp1, N_Vector temp2)
{
  int retval=0, is;

  if (cv_mem->cv_ifS==CV_ALLSENS) {
    retval = cv_mem->cv_fS(cv_mem->cv_Ns, time, ycur, fcur, yScur, 
                           fScur, cv_mem->cv_fS_data, temp1, temp2);
    cv_mem->cv_nfSe++;
  } else {
    for (is=0; is<cv_mem->cv_Ns; is++) {
      retval = cv_mem->cv_fS1(cv_mem->cv_Ns, time, ycur, fcur, is, yScur[is], 
                              fScur[is], cv_mem->cv_fS_data, temp1, temp2);
      cv_mem->cv_nfSe++;
      if (retval != 0) break;
    }
  }

  return(retval);
}

/*
 * cvSensRhs1Wrapper
 *
 * cvSensRhs1Wrapper is a high level routine that returns right-hand
 * side of the is-th sensitivity equation. 
 *
 * cvSensRhs1Wrapper is called only during the CV_STAGGERED1 corrector loop
 * (ifS must be CV_ONESENS, otherwise CVodeSensInit would have 
 * issued an error message).
 *
 * The return value is that of the sensitivity RHS function fS1,
 */

int cvSensRhs1Wrapper(CVodeMem cv_mem, realtype time, 
                      N_Vector ycur, N_Vector fcur, 
                      int is, N_Vector yScur, N_Vector fScur,
                      N_Vector temp1, N_Vector temp2)
{
  int retval;

  retval = cv_mem->cv_fS1(cv_mem->cv_Ns, time, ycur, fcur, is, yScur, 
                          fScur, cv_mem->cv_fS_data, temp1, temp2);
  cv_mem->cv_nfSe++;

  return(retval);
}

/* 
 * -----------------------------------------------------------------
 * Internal DQ approximations for sensitivity RHS
 * -----------------------------------------------------------------
 */

/* Undefine Readibility Constants */

#undef y

/*
 * cvSensRhsInternalDQ   - internal CVSensRhsFn
 *
 * cvSensRhsInternalDQ computes right hand side of all sensitivity equations
 * by finite differences
 */

int cvSensRhsInternalDQ(int Ns, realtype t, 
                        N_Vector y, N_Vector ydot, 
                        N_Vector *yS, N_Vector *ySdot, 
                        void *cvode_mem,  
                        N_Vector ytemp, N_Vector ftemp)
{
  int is, retval;
  
  for (is=0; is<Ns; is++) {
    retval = cvSensRhs1InternalDQ(Ns, t, y, ydot, is, yS[is], 
                                  ySdot[is], cvode_mem, ytemp, ftemp);
    if (retval!=0) return(retval);
  }

  return(0);
}

/*
 * cvSensRhs1InternalDQ   - internal CVSensRhs1Fn
 *
 * cvSensRhs1InternalDQ computes the right hand side of the is-th sensitivity 
 * equation by finite differences
 *
 * cvSensRhs1InternalDQ returns 0 if successful. Otherwise it returns the 
 * non-zero return value from f().
 */

int cvSensRhs1InternalDQ(int Ns, realtype t, 
                         N_Vector y, N_Vector ydot, 
                         int is, N_Vector yS, N_Vector ySdot, 
                         void *cvode_mem,
                         N_Vector ytemp, N_Vector ftemp)
{
  CVodeMem cv_mem;
  int retval, method;
  int nfel = 0, which;
  realtype psave, pbari;
  realtype delta , rdelta;
  realtype Deltap, rDeltap, r2Deltap;
  realtype Deltay, rDeltay, r2Deltay;
  realtype Delta , rDelta , r2Delta ;
  realtype norms, ratio;

  /* local variables for fused vector operations */
  realtype cvals[3];
  N_Vector Xvecs[3];
  
  /* cvode_mem is passed here as user data */
  cv_mem = (CVodeMem) cvode_mem;

  delta = SUNRsqrt(SUNMAX(cv_mem->cv_reltol, cv_mem->cv_uround));
  rdelta = ONE/delta;
  
  pbari = cv_mem->cv_pbar[is];

  which = cv_mem->cv_plist[is];

  psave = cv_mem->cv_p[which];
  
  Deltap  = pbari * delta;
  rDeltap = ONE/Deltap;
  norms   = N_VWrmsNorm(yS, cv_mem->cv_ewt) * pbari;
  rDeltay = SUNMAX(norms, rdelta) / pbari;
  Deltay  = ONE/rDeltay;
  
  if (cv_mem->cv_DQrhomax == ZERO) {
    /* No switching */
    method = (cv_mem->cv_DQtype==CV_CENTERED) ? CENTERED1 : FORWARD1;
  } else {
    /* switch between simultaneous/separate DQ */
    ratio = Deltay * rDeltap;
    if ( SUNMAX(ONE/ratio, ratio) <= cv_mem->cv_DQrhomax )
      method = (cv_mem->cv_DQtype==CV_CENTERED) ? CENTERED1 : FORWARD1;
    else
      method = (cv_mem->cv_DQtype==CV_CENTERED) ? CENTERED2 : FORWARD2;
  }

  switch(method) {
    
  case CENTERED1:
    
    Delta = SUNMIN(Deltay, Deltap);
    r2Delta = HALF/Delta;
    
    N_VLinearSum(ONE,y,Delta,yS,ytemp);
    cv_mem->cv_p[which] = psave + Delta;

    retval = cv_mem->cv_f(t, ytemp, ySdot, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);
    
    N_VLinearSum(ONE,y,-Delta,yS,ytemp);
    cv_mem->cv_p[which] = psave - Delta;

    retval = cv_mem->cv_f(t, ytemp, ftemp, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);

    N_VLinearSum(r2Delta,ySdot,-r2Delta,ftemp,ySdot);
    
    break;
    
  case CENTERED2:
    
    r2Deltap = HALF/Deltap;
    r2Deltay = HALF/Deltay;
    
    N_VLinearSum(ONE,y,Deltay,yS,ytemp);

    retval = cv_mem->cv_f(t, ytemp, ySdot, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);

    N_VLinearSum(ONE,y,-Deltay,yS,ytemp);

    retval = cv_mem->cv_f(t, ytemp, ftemp, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);

    N_VLinearSum(r2Deltay, ySdot, -r2Deltay, ftemp, ySdot);
    
    cv_mem->cv_p[which] = psave + Deltap;
    retval = cv_mem->cv_f(t, y, ytemp, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);

    cv_mem->cv_p[which] = psave - Deltap;
    retval = cv_mem->cv_f(t, y, ftemp, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);
    
    /* ySdot = ySdot + r2Deltap * ytemp - r2Deltap * ftemp */
    cvals[0] = ONE;        Xvecs[0] = ySdot;
    cvals[1] = r2Deltap;   Xvecs[1] = ytemp;
    cvals[2] = -r2Deltap;  Xvecs[2] = ftemp;

    retval = N_VLinearCombination(3, cvals, Xvecs, ySdot);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

    break;

  case FORWARD1:
    
    Delta = SUNMIN(Deltay, Deltap);
    rDelta = ONE/Delta;
    
    N_VLinearSum(ONE,y,Delta,yS,ytemp);
    cv_mem->cv_p[which] = psave + Delta;

    retval = cv_mem->cv_f(t, ytemp, ySdot, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);
    
    N_VLinearSum(rDelta,ySdot,-rDelta,ydot,ySdot);
    
    break;
    
  case FORWARD2:
    
    N_VLinearSum(ONE,y,Deltay,yS,ytemp);

    retval = cv_mem->cv_f(t, ytemp, ySdot, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);

    N_VLinearSum(rDeltay, ySdot, -rDeltay, ydot, ySdot);
    
    cv_mem->cv_p[which] = psave + Deltap;
    retval = cv_mem->cv_f(t, y, ytemp, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);

    /* ySdot = ySdot + rDeltap * ytemp - rDeltap * ydot */
    cvals[0] = ONE;       Xvecs[0] = ySdot;
    cvals[1] = rDeltap;   Xvecs[1] = ytemp;
    cvals[2] = -rDeltap;  Xvecs[2] = ydot;

    retval = N_VLinearCombination(3, cvals, Xvecs, ySdot);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

    break;

  }
  
  cv_mem->cv_p[which] = psave;
  
  /* Increment counter nfeS */
  cv_mem->cv_nfeS += nfel;
  
  return(0);
}


/*
 * cvQuadSensRhsInternalDQ   - internal CVQuadSensRhsFn
 *
 * cvQuadSensRhsInternalDQ computes right hand side of all quadrature
 * sensitivity equations by finite differences. All work is actually
 * done in cvQuadSensRhs1InternalDQ.
 */

static int cvQuadSensRhsInternalDQ(int Ns, realtype t, 
                                   N_Vector y, N_Vector *yS,
                                   N_Vector yQdot, N_Vector *yQSdot,
                                   void *cvode_mem,  
                                   N_Vector tmp, N_Vector tmpQ)
{
  CVodeMem cv_mem;
  int is, retval;
  
  /* cvode_mem is passed here as user data */
  cv_mem = (CVodeMem) cvode_mem;

  for (is=0; is<Ns; is++) {
    retval = cvQuadSensRhs1InternalDQ(cv_mem, is, t,
                                      y, yS[is], 
                                      yQdot, yQSdot[is],
                                      tmp, tmpQ);
    if (retval!=0) return(retval);
  }

  return(0);
}

static int cvQuadSensRhs1InternalDQ(CVodeMem cv_mem, int is, realtype t, 
                                    N_Vector y, N_Vector yS,
                                    N_Vector yQdot, N_Vector yQSdot, 
                                    N_Vector tmp, N_Vector tmpQ)
{
  int retval, method;
  int nfel = 0, which;
  realtype psave, pbari;
  realtype delta , rdelta;
  realtype Deltap;
  realtype Deltay, rDeltay;
  realtype Delta , rDelta , r2Delta ;
  realtype norms;

  delta = SUNRsqrt(SUNMAX(cv_mem->cv_reltol, cv_mem->cv_uround));
  rdelta = ONE/delta;
  
  pbari = cv_mem->cv_pbar[is];

  which = cv_mem->cv_plist[is];

  psave = cv_mem->cv_p[which];
  
  Deltap  = pbari * delta;
  norms   = N_VWrmsNorm(yS, cv_mem->cv_ewt) * pbari;
  rDeltay = SUNMAX(norms, rdelta) / pbari;
  Deltay  = ONE/rDeltay;
  
  method = (cv_mem->cv_DQtype==CV_CENTERED) ? CENTERED1 : FORWARD1;

  switch(method) {

  case CENTERED1:
    
    Delta = SUNMIN(Deltay, Deltap);
    r2Delta = HALF/Delta;
    
    N_VLinearSum(ONE, y, Delta, yS, tmp);
    cv_mem->cv_p[which] = psave + Delta;

    retval = cv_mem->cv_fQ(t, tmp, yQSdot, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);
    
    N_VLinearSum(ONE, y, -Delta, yS, tmp);
    cv_mem->cv_p[which] = psave - Delta;

    retval = cv_mem->cv_fQ(t, tmp, tmpQ, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);

    N_VLinearSum(r2Delta, yQSdot, -r2Delta, tmpQ, yQSdot);
    
    break;

  case FORWARD1:
    
    Delta = SUNMIN(Deltay, Deltap);
    rDelta = ONE/Delta;
    
    N_VLinearSum(ONE, y, Delta, yS, tmp);
    cv_mem->cv_p[which] = psave + Delta;

    retval = cv_mem->cv_fQ(t, tmp, yQSdot, cv_mem->cv_user_data);
    nfel++;
    if (retval != 0) return(retval);
    
    N_VLinearSum(rDelta, yQSdot, -rDelta, yQdot, yQSdot);
    
    break;

  }

  cv_mem->cv_p[which] = psave;
  
  /* Increment counter nfQeS */
  cv_mem->cv_nfQeS += nfel;
  
  return(0);
}



/* 
 * -----------------------------------------------------------------
 * Error message handling functions
 * -----------------------------------------------------------------
 */

/*
 * cvProcessError is a high level error handling function.
 * - If cv_mem==NULL it prints the error message to stderr.
 * - Otherwise, it sets up and calls the error handling function 
 *   pointed to by cv_ehfun.
 */

void cvProcessError(CVodeMem cv_mem, 
                    int error_code, const char *module, const char *fname, 
                    const char *msgfmt, ...)
{
  va_list ap;
  char msg[256];

  /* Initialize the argument pointer variable 
     (msgfmt is the last required argument to cvProcessError) */

  va_start(ap, msgfmt);

  /* Compose the message */

  vsprintf(msg, msgfmt, ap);

  if (cv_mem == NULL) {    /* We write to stderr */
#ifndef NO_FPRINTF_OUTPUT
    fprintf(stderr, "\n[%s ERROR]  %s\n  ", module, fname);
    fprintf(stderr, "%s\n\n", msg);
#endif

  } else {                 /* We can call ehfun */
    cv_mem->cv_ehfun(error_code, module, fname, msg, cv_mem->cv_eh_data);
  }

  /* Finalize argument processing */
  va_end(ap);

  return;
}

/* 
 * cvErrHandler is the default error handling function.
 * It sends the error message to the stream pointed to by cv_errfp.
 */

void cvErrHandler(int error_code, const char *module,
                  const char *function, char *msg, void *data)
{
  CVodeMem cv_mem;
  char err_type[10];

  /* data points to cv_mem here */

  cv_mem = (CVodeMem) data;

  if (error_code == CV_WARNING)
    sprintf(err_type,"WARNING");
  else
    sprintf(err_type,"ERROR");

#ifndef NO_FPRINTF_OUTPUT
  if (cv_mem->cv_errfp!=NULL) {
    fprintf(cv_mem->cv_errfp,"\n[%s %s]  %s\n",module,err_type,function);
    fprintf(cv_mem->cv_errfp,"  %s\n\n",msg);
  }
#endif

  return;
}
