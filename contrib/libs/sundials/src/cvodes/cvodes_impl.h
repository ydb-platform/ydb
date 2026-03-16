/*
 * -----------------------------------------------------------------
 * $Revision$
 * $Date$
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
 * Implementation header file for the main CVODES integrator.
 * -----------------------------------------------------------------
 */

#ifndef _CVODES_IMPL_H
#define _CVODES_IMPL_H

#include <stdarg.h>

#include <cvodes/cvodes.h>
#include <sundials/sundials_nvector.h>
#include <sundials/sundials_types.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/* 
 * =================================================================
 *   I N T E R N A L   C V O D E S   C O N S T A N T S
 * =================================================================
 */

/* Basic CVODES constants */

#define ADAMS_Q_MAX 12      /* max value of q for lmm == ADAMS    */
#define BDF_Q_MAX    5      /* max value of q for lmm == BDF      */
#define Q_MAX  ADAMS_Q_MAX  /* max value of q for either lmm      */
#define L_MAX  (Q_MAX+1)    /* max value of L for either lmm      */
#define NUM_TESTS    5      /* number of error test quantities    */

#define HMIN_DEFAULT     RCONST(0.0)    /* hmin default value     */
#define HMAX_INV_DEFAULT RCONST(0.0)    /* hmax_inv default value */
#define MXHNIL_DEFAULT   10             /* mxhnil default value   */
#define MXSTEP_DEFAULT   500            /* mxstep default value   */

/* Return values for lower level routines used by CVode and functions
   provided to the nonlinear solver */

#define RHSFUNC_RECVR    +9
#define SRHSFUNC_RECVR   +12

/* nonlinear solver constants
   NLS_MAXCOR  maximum no. of corrector iterations for the nonlinear solver
   CRDOWN      constant used in the estimation of the convergence rate (crate)
               of the iterates for the nonlinear equation
   RDIV        declare divergence if ratio del/delp > RDIV
*/
#define NLS_MAXCOR 3
#define CRDOWN     RCONST(0.3)
#define RDIV       RCONST(2.0)

/* 
 * =================================================================
 *   F O R W A R D   P O I N T E R   R E F E R E N C E S
 * =================================================================
 */

typedef struct CVadjMemRec  *CVadjMem;
typedef struct CkpntMemRec  *CkpntMem;
typedef struct DtpntMemRec  *DtpntMem;
typedef struct CVodeBMemRec *CVodeBMem;

/* 
 * =================================================================
 *   M A I N    I N T E G R A T O R    M E M O R Y    B L O C K
 * =================================================================
 */


/*
 * -----------------------------------------------------------------
 * Types: struct CVodeMemRec, CVodeMem
 * -----------------------------------------------------------------
 * The type CVodeMem is type pointer to struct CVodeMemRec.
 * This structure contains fields to keep track of problem state.
 * -----------------------------------------------------------------
 */
  
typedef struct CVodeMemRec {
    
  realtype cv_uround;         /* machine unit roundoff                         */   

  /*-------------------------- 
    Problem Specification Data 
    --------------------------*/

  CVRhsFn cv_f;               /* y' = f(t,y(t))                                */
  void *cv_user_data;         /* user pointer passed to f                      */

  int cv_lmm;                 /* lmm = ADAMS or BDF                            */

  int cv_itol;                /* itol = CV_SS, CV_SV, or CV_WF, or CV_NN       */
  realtype cv_reltol;         /* relative tolerance                            */
  realtype cv_Sabstol;        /* scalar absolute tolerance                     */
  N_Vector cv_Vabstol;        /* vector absolute tolerance                     */
  booleantype cv_user_efun;   /* SUNTRUE if user sets efun                     */
  CVEwtFn cv_efun;            /* function to set ewt                           */
  void *cv_e_data;            /* user pointer passed to efun                   */

  booleantype cv_constraintsSet; /* constraints vector present:
                                    do constraints calc                        */

  /*-----------------------
    Quadrature Related Data 
    -----------------------*/

  booleantype cv_quadr;       /* SUNTRUE if integrating quadratures            */

  CVQuadRhsFn cv_fQ;          /* q' = fQ(t, y(t))                              */

  booleantype cv_errconQ;     /* SUNTRUE if quadrs. are included in error test */

  int cv_itolQ;               /* itolQ = CV_SS or CV_SV                        */
  realtype cv_reltolQ;        /* relative tolerance for quadratures            */
  realtype cv_SabstolQ;       /* scalar absolute tolerance for quadratures     */
  N_Vector cv_VabstolQ;       /* vector absolute tolerance for quadratures     */

  /*------------------------
    Sensitivity Related Data 
    ------------------------*/

  booleantype cv_sensi;       /* SUNTRUE if computing sensitivities           */

  int cv_Ns;                  /* Number of sensitivities                      */

  int cv_ism;                 /* ism = SIMULTANEOUS or STAGGERED              */

  CVSensRhsFn cv_fS;          /* fS = (df/dy)*yS + (df/dp)                    */
  CVSensRhs1Fn cv_fS1;        /* fS1 = (df/dy)*yS_i + (df/dp)                 */
  void *cv_fS_data;           /* data pointer passed to fS                    */
  booleantype cv_fSDQ;        /* SUNTRUE if using internal DQ functions       */
  int cv_ifS;                 /* ifS = ALLSENS or ONESENS                     */

  realtype *cv_p;             /* parameters in f(t,y,p)                       */
  realtype *cv_pbar;          /* scale factors for parameters                 */
  int *cv_plist;              /* list of sensitivities                        */
  int cv_DQtype;              /* central/forward finite differences           */
  realtype cv_DQrhomax;       /* cut-off value for separate/simultaneous FD   */

  booleantype cv_errconS;     /* SUNTRUE if yS are considered in err. control */

  int cv_itolS;
  realtype cv_reltolS;        /* relative tolerance for sensitivities         */
  realtype *cv_SabstolS;      /* scalar absolute tolerances for sensi.        */
  N_Vector *cv_VabstolS;      /* vector absolute tolerances for sensi.        */

  /*-----------------------------------
    Quadrature Sensitivity Related Data 
    -----------------------------------*/

  booleantype cv_quadr_sensi; /* SUNTRUE if computing sensitivties of quadrs. */

  CVQuadSensRhsFn cv_fQS;     /* fQS = (dfQ/dy)*yS + (dfQ/dp)                 */
  void *cv_fQS_data;          /* data pointer passed to fQS                   */
  booleantype cv_fQSDQ;       /* SUNTRUE if using internal DQ functions       */

  booleantype cv_errconQS;    /* SUNTRUE if yQS are considered in err. con.   */

  int cv_itolQS;
  realtype cv_reltolQS;       /* relative tolerance for yQS                   */
  realtype *cv_SabstolQS;     /* scalar absolute tolerances for yQS           */
  N_Vector *cv_VabstolQS;     /* vector absolute tolerances for yQS           */

  /*-----------------------
    Nordsieck History Array 
    -----------------------*/

  N_Vector cv_zn[L_MAX];      /* Nordsieck array, of size N x (q+1).
                                 zn[j] is a vector of length N (j=0,...,q)
                                 zn[j] = [1/factorial(j)] * h^j * 
                                 (jth derivative of the interpolating poly.)  */

  /*-------------------
    Vectors of length N 
    -------------------*/

  N_Vector cv_ewt;            /* error weight vector                          */
  N_Vector cv_y;              /* y is used as temporary storage by the solver.
                                 The memory is provided by the user to CVode 
                                 where the vector is named yout.              */
  N_Vector cv_acor;           /* In the context of the solution of the
                                 nonlinear equation, acor = y_n(m) - y_n(0).
                                 On return, this vector is scaled to give
                                 the estimated local error in y.              */
  N_Vector cv_tempv;          /* temporary storage vector                     */
  N_Vector cv_ftemp;          /* temporary storage vector                     */
  N_Vector cv_vtemp1;         /* temporary storage vector                     */
  N_Vector cv_vtemp2;         /* temporary storage vector                     */
  N_Vector cv_vtemp3;         /* temporary storage vector                     */

  N_Vector cv_mm;             /* mask vector in constraints tests             */
  N_Vector cv_constraints;    /* vector of inequality constraint options      */

  /*--------------------------
    Quadrature Related Vectors 
    --------------------------*/

  N_Vector cv_znQ[L_MAX];     /* Nordsieck arrays for quadratures             */
  N_Vector cv_ewtQ;           /* error weight vector for quadratures          */
  N_Vector cv_yQ;             /* Unlike y, yQ is not allocated by the user    */
  N_Vector cv_acorQ;          /* acorQ = yQ_n(m) - yQ_n(0)                    */
  N_Vector cv_tempvQ;         /* temporary storage vector (~ tempv)           */

  /*---------------------------
    Sensitivity Related Vectors 
    ---------------------------*/

  N_Vector *cv_znS[L_MAX];    /* Nordsieck arrays for sensitivities           */
  N_Vector *cv_ewtS;          /* error weight vectors for sensitivities       */
  N_Vector *cv_yS;            /* yS=yS0 (allocated by the user)               */
  N_Vector *cv_acorS;         /* acorS = yS_n(m) - yS_n(0)                    */
  N_Vector *cv_tempvS;        /* temporary storage vector (~ tempv)           */
  N_Vector *cv_ftempS;        /* temporary storage vector (~ ftemp)           */

  booleantype cv_stgr1alloc;  /* Did we allocate ncfS1, ncfnS1, and nniS1?    */

  /*--------------------------------------
    Quadrature Sensitivity Related Vectors 
    --------------------------------------*/

  N_Vector *cv_znQS[L_MAX];   /* Nordsieck arrays for quadr. sensitivities    */
  N_Vector *cv_ewtQS;         /* error weight vectors for sensitivities       */
  N_Vector *cv_yQS;           /* Unlike yS, yQS is not allocated by the user  */
  N_Vector *cv_acorQS;        /* acorQS = yQS_n(m) - yQS_n(0)                 */
  N_Vector *cv_tempvQS;       /* temporary storage vector (~ tempv)           */
  N_Vector cv_ftempQ;         /* temporary storage vector (~ ftemp)           */
  
  /*-----------------
    Tstop information
    -----------------*/

  booleantype cv_tstopset;
  realtype cv_tstop;

  /*---------
    Step Data 
    ---------*/

  int cv_q;                    /* current order                               */
  int cv_qprime;               /* order to be used on the next step
                                * qprime = q-1, q, or q+1                     */
  int cv_next_q;               /* order to be used on the next step           */
  int cv_qwait;                /* number of internal steps to wait before
                                * considering a change in q                   */
  int cv_L;                    /* L = q + 1                                   */

  realtype cv_hin;
  realtype cv_h;               /* current step size                           */
  realtype cv_hprime;          /* step size to be used on the next step       */ 
  realtype cv_next_h;          /* step size to be used on the next step       */ 
  realtype cv_eta;             /* eta = hprime / h                            */
  realtype cv_hscale;          /* value of h used in zn                       */
  realtype cv_tn;              /* current internal value of t                 */
  realtype cv_tretlast;        /* last value of t returned                    */

  realtype cv_tau[L_MAX+1];    /* array of previous q+1 successful step
                                * sizes indexed from 1 to q+1                 */
  realtype cv_tq[NUM_TESTS+1]; /* array of test quantities indexed from
                                * 1 to NUM_TESTS(=5)                          */
  realtype cv_l[L_MAX];        /* coefficients of l(x) (degree q poly)        */

  realtype cv_rl1;             /* the scalar 1/l[1]                           */
  realtype cv_gamma;           /* gamma = h * rl1                             */
  realtype cv_gammap;          /* gamma at the last setup call                */
  realtype cv_gamrat;          /* gamma / gammap                              */

  realtype cv_crate;           /* est. corrector conv. rate in Nls            */
  realtype cv_crateS;          /* est. corrector conv. rate in NlsStgr        */
  realtype cv_delp;            /* norm of previous nonlinear solver update    */
  realtype cv_acnrm;           /* | acor |                                    */
  realtype cv_acnrmQ;          /* | acorQ |                                   */
  realtype cv_acnrmS;          /* | acorS |                                   */
  realtype cv_acnrmQS;         /* | acorQS |                                  */
  realtype cv_nlscoef;         /* coeficient in nonlinear convergence test    */
  int  *cv_ncfS1;              /* Array of Ns local counters for conv.  
                                * failures (used in CVStep for STAGGERED1)    */

  /*------
    Limits 
    ------*/

  int cv_qmax;             /* q <= qmax                                       */
  long int cv_mxstep;      /* maximum number of internal steps for one 
			      user call                                       */
  int cv_mxhnil;           /* max. number of warning messages issued to the
			      user that t + h == t for the next internal step */
  int cv_maxnef;           /* maximum number of error test failures           */
  int cv_maxncf;           /* maximum number of nonlinear conv. failures      */
  
  realtype cv_hmin;        /* |h| >= hmin                                     */
  realtype cv_hmax_inv;    /* |h| <= 1/hmax_inv                               */
  realtype cv_etamax;      /* eta <= etamax                                   */

  /*----------
    Counters 
    ----------*/

  long int cv_nst;         /* number of internal steps taken                  */

  long int cv_nfe;         /* number of f calls                               */
  long int cv_nfQe;        /* number of fQ calls                              */
  long int cv_nfSe;        /* number of fS calls                              */
  long int cv_nfeS;        /* number of f calls from sensi DQ                 */
  long int cv_nfQSe;       /* number of fQS calls                             */
  long int cv_nfQeS;       /* number of fQ calls from sensi DQ                */


  long int cv_ncfn;        /* number of corrector convergence failures        */
  long int cv_ncfnS;       /* number of total sensi. corr. conv. failures     */
  long int *cv_ncfnS1;     /* number of sensi. corrector conv. failures       */

  long int cv_nni;         /* number of nonlinear iterations performed        */
  long int cv_nniS;        /* number of total sensi. nonlinear iterations     */
  long int *cv_nniS1;      /* number of sensi. nonlinear iterations           */

  long int cv_netf;        /* number of error test failures                   */
  long int cv_netfQ;       /* number of quadr. error test failures            */
  long int cv_netfS;       /* number of sensi. error test failures            */
  long int cv_netfQS;      /* number of quadr. sensi. error test failures     */

  long int cv_nsetups;     /* number of setup calls                           */
  long int cv_nsetupsS;    /* number of setup calls due to sensitivities      */

  int cv_nhnil;            /* number of messages issued to the user that
			      t + h == t for the next iternal step            */

  /*-----------------------------
    Space requirements for CVODES 
    -----------------------------*/

  sunindextype cv_lrw1;        /* no. of realtype words in 1 N_Vector y           */ 
  sunindextype cv_liw1;        /* no. of integer words in 1 N_Vector y            */ 
  sunindextype cv_lrw1Q;       /* no. of realtype words in 1 N_Vector yQ          */ 
  sunindextype cv_liw1Q;       /* no. of integer words in 1 N_Vector yQ           */ 
  long int cv_lrw;             /* no. of realtype words in CVODES work vectors    */
  long int cv_liw;             /* no. of integer words in CVODES work vectors     */

  /*----------------
    Step size ratios
    ----------------*/

  realtype cv_etaqm1;      /* ratio of new to old h for order q-1             */
  realtype cv_etaq;        /* ratio of new to old h for order q               */
  realtype cv_etaqp1;      /* ratio of new to old h for order q+1             */

  /*---------------------
    Nonlinear Solver Data
    ---------------------*/

  SUNNonlinearSolver NLS;      /* nonlinear solver object for ODE solves */
  booleantype ownNLS;          /* flag indicating NLS ownership          */

  SUNNonlinearSolver NLSsim;   /* NLS object for the simultaneous corrector */
  booleantype ownNLSsim;       /* flag indicating NLS ownership             */

  SUNNonlinearSolver NLSstg;   /* NLS object for the staggered corrector */
  booleantype ownNLSstg;       /* flag indicating NLS ownership          */

  SUNNonlinearSolver NLSstg1;  /* NLS object for the staggered1 corrector */
  booleantype ownNLSstg1;      /* flag indicating NLS ownership           */
  int sens_solve_idx;          /* index of the current staggered1 solve   */
  long int nnip;               /* previous total number of iterations     */

  booleantype sens_solve;      /* flag indicating if the current solve is a
                                  staggered or staggered1 sensitivity solve */
  int convfail;                /* flag to indicate when a Jacobian update may
                                  be needed */

  /* The following vectors are NVector wrappers for use with the simultaneous
     and staggered corrector methods:

       Simultaneous: ycor0Sim = [ida_delta, ida_deltaS]
                     ycorSim  = [ida_ee,    ida_eeS]
                     ewtSim   = [ida_ewt,   ida_ewtS]

       Staggered: ycor0Stg = ida_deltaS
                  ycorStg  = ida_eeS
                  ewtStg   = ida_ewtS
  */
  N_Vector ycor0Sim, ycorSim, ewtSim;
  N_Vector ycor0Stg, ycorStg, ewtStg;

  /* flags indicating if vector wrappers for the simultaneous and staggered
     correctors have been allocated */
  booleantype simMallocDone;
  booleantype stgMallocDone;


  /*------------------
    Linear Solver Data
    ------------------*/

  /* Linear Solver functions to be called */

  int (*cv_linit)(struct CVodeMemRec *cv_mem);

  int (*cv_lsetup)(struct CVodeMemRec *cv_mem, int convfail, 
		   N_Vector ypred, N_Vector fpred, booleantype *jcurPtr, 
		   N_Vector vtemp1, N_Vector vtemp2, N_Vector vtemp3); 

  int (*cv_lsolve)(struct CVodeMemRec *cv_mem, N_Vector b, N_Vector weight,
		   N_Vector ycur, N_Vector fcur);

  int (*cv_lfree)(struct CVodeMemRec *cv_mem);

  /* Linear Solver specific memory */

  void *cv_lmem;           

  /* Flag to request a call to the setup routine */

  booleantype cv_forceSetup;

  /*------------
    Saved Values
    ------------*/

  int cv_qu;                   /* last successful q value used                */
  long int cv_nstlp;           /* step number of last setup call              */
  realtype cv_h0u;             /* actual initial stepsize                     */
  realtype cv_hu;              /* last successful h value used                */
  realtype cv_saved_tq5;       /* saved value of tq[5]                        */
  booleantype cv_jcur;         /* is Jacobian info for linear solver current? */
  int cv_convfail;             /* flag storing previous solver failure mode   */
  realtype cv_tolsf;           /* tolerance scale factor                      */
  int cv_qmax_alloc;           /* qmax used when allocating mem               */
  int cv_qmax_allocQ;          /* qmax used when allocating quad. mem         */
  int cv_qmax_allocS;          /* qmax used when allocating sensi. mem        */
  int cv_qmax_allocQS;         /* qmax used when allocating quad. sensi. mem  */
  int cv_indx_acor;            /* index of zn vector in which acor is saved   */

  /*--------------------------------------------------------------------
    Flags turned ON by CVodeInit, CVodeSensMalloc, and CVodeQuadMalloc 
    and read by CVodeReInit, CVodeSensReInit, and CVodeQuadReInit
    --------------------------------------------------------------------*/

  booleantype cv_VabstolMallocDone;
  booleantype cv_MallocDone;
  booleantype cv_constraintsMallocDone;

  booleantype cv_VabstolQMallocDone;
  booleantype cv_QuadMallocDone;

  booleantype cv_VabstolSMallocDone;
  booleantype cv_SabstolSMallocDone;
  booleantype cv_SensMallocDone;

  booleantype cv_VabstolQSMallocDone;
  booleantype cv_SabstolQSMallocDone;
  booleantype cv_QuadSensMallocDone;

  /*-------------------------------------------
    Error handler function and error ouput file 
    -------------------------------------------*/

  CVErrHandlerFn cv_ehfun;    /* Error messages are handled by ehfun          */
  void *cv_eh_data;           /* dats pointer passed to ehfun                 */
  FILE *cv_errfp;             /* CVODES error messages are sent to errfp      */    

  /*-------------------------
    Stability Limit Detection
    -------------------------*/

  booleantype cv_sldeton;     /* Is Stability Limit Detection on?             */
  realtype cv_ssdat[6][4];    /* scaled data array for STALD                  */
  int cv_nscon;               /* counter for STALD method                     */
  long int cv_nor;            /* counter for number of order reductions       */

  /*----------------
    Rootfinding Data
    ----------------*/

  CVRootFn cv_gfun;        /* Function g for roots sought                     */
  int cv_nrtfn;            /* number of components of g                       */
  int *cv_iroots;          /* array for root information                      */
  int *cv_rootdir;         /* array specifying direction of zero-crossing     */
  realtype cv_tlo;         /* nearest endpoint of interval in root search     */
  realtype cv_thi;         /* farthest endpoint of interval in root search    */
  realtype cv_trout;       /* t value returned by rootfinding routine         */
  realtype *cv_glo;        /* saved array of g values at t = tlo              */
  realtype *cv_ghi;        /* saved array of g values at t = thi              */
  realtype *cv_grout;      /* array of g values at t = trout                  */
  realtype cv_toutc;       /* copy of tout (if NORMAL mode)                   */
  realtype cv_ttol;        /* tolerance on root location trout                */
  int cv_taskc;            /* copy of parameter itask                         */
  int cv_irfnd;            /* flag showing whether last step had a root       */
  long int cv_nge;         /* counter for g evaluations                       */
  booleantype *cv_gactive; /* array with active/inactive event functions      */
  int cv_mxgnull;          /* number of warning messages about possible g==0  */

  /*-----------------------
    Fused Vector Operations
    -----------------------*/

  realtype* cv_cvals; /* array of scalars */
  N_Vector* cv_Xvecs; /* array of vectors */
  N_Vector* cv_Zvecs; /* array of vectors */

  /*------------------------
    Adjoint sensitivity data
    ------------------------*/

  booleantype cv_adj;             /* SUNTRUE if performing ASA                */

  struct CVadjMemRec *cv_adj_mem; /* Pointer to adjoint memory structure      */

  booleantype cv_adjMallocDone;

} *CVodeMem;


/* 
 * =================================================================
 *   A D J O I N T   M O D U L E    M E M O R Y    B L O C K
 * =================================================================
 */

/*
 * -----------------------------------------------------------------
 * Types : struct CkpntMemRec, CkpntMem
 * -----------------------------------------------------------------
 * The type CkpntMem is type pointer to struct CkpntMemRec.
 * This structure contains fields to store all information at a
 * check point that is needed to 'hot' start cvodes.
 * -----------------------------------------------------------------
 */

struct CkpntMemRec {

  /* Integration limits */
  realtype ck_t0;
  realtype ck_t1;
    
  /* Nordsieck History Array */
  N_Vector ck_zn[L_MAX];
    
  /* Do we need to carry quadratures? */
  booleantype ck_quadr;
    
  /* Nordsieck History Array for quadratures */
  N_Vector ck_znQ[L_MAX];
    
  /* Do we need to carry sensitivities? */
  booleantype ck_sensi;

  /* number of sensitivities */
  int ck_Ns;

  /* Nordsieck History Array for sensitivities */
  N_Vector *ck_znS[L_MAX];

  /* Do we need to carry quadrature sensitivities? */
  booleantype ck_quadr_sensi;

  /* Nordsieck History Array for quadrature sensitivities */
  N_Vector *ck_znQS[L_MAX];
    
  /* Was ck_zn[qmax] allocated?
     ck_zqm = 0    - no
     ck_zqm = qmax - yes      */
  int ck_zqm;
    
  /* Step data */
  long int ck_nst;
  realtype ck_tretlast;
  int      ck_q;
  int      ck_qprime;
  int      ck_qwait;
  int      ck_L;
  realtype ck_gammap;
  realtype ck_h;
  realtype ck_hprime;
  realtype ck_hscale;
  realtype ck_eta;
  realtype ck_etamax;
  realtype ck_tau[L_MAX+1];
  realtype ck_tq[NUM_TESTS+1];
  realtype ck_l[L_MAX];
    
  /* Saved values */
  realtype ck_saved_tq5;
    
  /* Pointer to next structure in list */
  struct CkpntMemRec *ck_next;
    
};
  
/*
 * -----------------------------------------------------------------
 * Types for functions provided by an interpolation module
 * -----------------------------------------------------------------
 * cvaIMMallocFn: Type for a function that initializes the content
 *                field of the structures in the dt array
 * cvaIMFreeFn:   Type for a function that deallocates the content
 *                field of the structures in the dt array
 * cvaIMGetYFn:   Type for a function that returns the 
 *                interpolated forward solution.
 * cvaIMStorePnt: Type for a function that stores a new
 *                point in the structure d
 * -----------------------------------------------------------------
 */

typedef booleantype (*cvaIMMallocFn)(CVodeMem cv_mem);
typedef void (*cvaIMFreeFn)(CVodeMem cv_mem);
typedef int (*cvaIMGetYFn)(CVodeMem cv_mem, realtype t, N_Vector y, N_Vector *yS);
typedef int (*cvaIMStorePntFn)(CVodeMem cv_mem, DtpntMem d);

/*
 * -----------------------------------------------------------------
 * Type : struct DtpntMemRec
 * -----------------------------------------------------------------
 * This structure contains fields to store all information at a
 * data point that is needed to interpolate solution of forward
 * simulations. Its content field depends on IMtype.
 * -----------------------------------------------------------------
 */
  
struct DtpntMemRec {
  realtype t;    /* time */
  void *content; /* IMtype-dependent content */
};

/* Data for cubic Hermite interpolation */
typedef struct HermiteDataMemRec {
  N_Vector y;
  N_Vector yd;
  N_Vector *yS;
  N_Vector *ySd;
} *HermiteDataMem;

/* Data for polynomial interpolation */
typedef struct PolynomialDataMemRec {
  N_Vector y;
  N_Vector *yS;
  int order;
} *PolynomialDataMem;


/*
 * -----------------------------------------------------------------
 * Type : struct CVodeBMemRec
 * -----------------------------------------------------------------
 * The type CVodeBMem is a pointer to a structure which stores all
 * information for ONE backward problem.
 * The CVadjMem structure contains a linked list of CVodeBMem pointers
 * -----------------------------------------------------------------
 */

struct CVodeBMemRec {

  /* Index of this backward problem */
  int cv_index;

  /* Time at which the backward problem is initialized */
  realtype cv_t0;
  
  /* CVODES memory for this backward problem */
  CVodeMem cv_mem;

  /* Flags to indicate that this backward problem's RHS or quad RHS
   * require forward sensitivities */
  booleantype cv_f_withSensi;
  booleantype cv_fQ_withSensi;

  /* Right hand side function for backward run */
  CVRhsFnB cv_f;
  CVRhsFnBS cv_fs;

  /* Right hand side quadrature function for backward run */
  CVQuadRhsFnB cv_fQ;
  CVQuadRhsFnBS cv_fQs;

  /* User user_data */
  void *cv_user_data;
    
  /* Memory block for a linear solver's interface to CVODEA */
  void *cv_lmem;

  /* Function to free any memory allocated by the linear solver */
  int (*cv_lfree)(CVodeBMem cvB_mem);

  /* Memory block for a preconditioner's module interface to CVODEA */ 
  void *cv_pmem;

  /* Function to free any memory allocated by the preconditioner module */
  int (*cv_pfree)(CVodeBMem cvB_mem);

  /* Time at which to extract solution / quadratures */
  realtype cv_tout;
  
  /* Workspace Nvector */
  N_Vector cv_y;

  /* Pointer to next structure in list */
  struct CVodeBMemRec *cv_next;

};

/*
 * -----------------------------------------------------------------
 * Type : struct CVadjMemRec
 * -----------------------------------------------------------------
 * The type CVadjMem is type pointer to struct CVadjMemRec.
 * This structure contins fields to store all information
 * necessary for adjoint sensitivity analysis.
 * -----------------------------------------------------------------
 */

struct CVadjMemRec {
    
  /* --------------------
   * Forward problem data
   * -------------------- */

  /* Integration interval */
  realtype ca_tinitial, ca_tfinal;

  /* Flag for first call to CVodeF */
  booleantype ca_firstCVodeFcall;

  /* Flag if CVodeF was called with TSTOP */
  booleantype ca_tstopCVodeFcall;
  realtype ca_tstopCVodeF;
    
  /* ----------------------
   * Backward problems data
   * ---------------------- */

  /* Storage for backward problems */
  struct CVodeBMemRec *cvB_mem;

  /* Number of backward problems */
  int ca_nbckpbs;

  /* Address of current backward problem */
  struct CVodeBMemRec *ca_bckpbCrt;

  /* Flag for first call to CVodeB */
  booleantype ca_firstCVodeBcall;
    
  /* ----------------
   * Check point data
   * ---------------- */

  /* Storage for check point information */
  struct CkpntMemRec *ck_mem;

  /* Number of check points */
  int ca_nckpnts;

  /* address of the check point structure for which data is available */
  struct CkpntMemRec *ca_ckpntData;
    
  /* ------------------
   * Interpolation data
   * ------------------ */

  /* Number of steps between 2 check points */
  long int ca_nsteps;

  /* Last index used in CVAfindIndex */
  long int ca_ilast;
  
  /* Storage for data from forward runs */
  struct DtpntMemRec **dt_mem;

  /* Actual number of data points in dt_mem (typically np=nsteps+1) */
  long int ca_np;
    
  /* Interpolation type */
  int ca_IMtype;

  /* Functions set by the interpolation module */
  cvaIMMallocFn   ca_IMmalloc; 
  cvaIMFreeFn     ca_IMfree;
  cvaIMStorePntFn ca_IMstore; /* store a new interpolation point */
  cvaIMGetYFn     ca_IMget;   /* interpolate forward solution    */

  /* Flags controlling the interpolation module */
  booleantype ca_IMmallocDone;   /* IM initialized? */
  booleantype ca_IMnewData;      /* new data available in dt_mem?*/
  booleantype ca_IMstoreSensi;   /* store sensitivities? */
  booleantype ca_IMinterpSensi;  /* interpolate sensitivities? */

  /* Workspace for the interpolation module */
  N_Vector ca_Y[L_MAX];     /* pointers to zn[i] */
  N_Vector *ca_YS[L_MAX];   /* pointers to znS[i] */
  realtype ca_T[L_MAX];

  /* -------------------------------
   * Workspace for wrapper functions
   * ------------------------------- */

  N_Vector ca_ytmp;

  N_Vector *ca_yStmp;
    
};
  

/*
 * =================================================================
 *     I N T E R F A C E   T O    L I N E A R   S O L V E R S
 * =================================================================
 */

/*
 * -----------------------------------------------------------------
 * Communication between CVODE and a CVODE Linear Solver
 * -----------------------------------------------------------------
 * convfail (input to cv_lsetup)
 *
 * CV_NO_FAILURES : Either this is the first cv_setup call for this
 *                  step, or the local error test failed on the
 *                  previous attempt at this step (but the nonlinear
 *                  solver iteration converged).
 *
 * CV_FAIL_BAD_J  : This value is passed to cv_lsetup if
 *
 *                  (a) The previous nonlinear solver corrector iteration
 *                      did not converge and the linear solver's
 *                      setup routine indicated that its Jacobian-
 *                      related data is not current
 *                                   or
 *                  (b) During the previous nonlinear solver corrector
 *                      iteration, the linear solver's solve routine
 *                      failed in a recoverable manner and the
 *                      linear solver's setup routine indicated that
 *                      its Jacobian-related data is not current.
 *
 * CV_FAIL_OTHER  : During the current internal step try, the
 *                  previous nonlinear solver iteration failed to converge
 *                  even though the linear solver was using current
 *                  Jacobian-related data.
 * -----------------------------------------------------------------
 */

/* Constants for convfail (input to cv_lsetup) */

#define CV_NO_FAILURES 0
#define CV_FAIL_BAD_J  1
#define CV_FAIL_OTHER  2

/*
 * -----------------------------------------------------------------
 * int (*cv_linit)(CVodeMem cv_mem);
 * -----------------------------------------------------------------
 * The purpose of cv_linit is to complete initializations for a
 * specific linear solver, such as counters and statistics.
 * An LInitFn should return 0 if it has successfully initialized the
 * CVODE linear solver and a negative value otherwise.
 * If an error does occur, an appropriate message should be sent to
 * the error handler function.
 * -----------------------------------------------------------------
 */

/*
 * -----------------------------------------------------------------
 * int (*cv_lsetup)(CVodeMem cv_mem, int convfail, N_Vector ypred,
 *                 N_Vector fpred, booleantype *jcurPtr,
 *                 N_Vector vtemp1, N_Vector vtemp2,
 *                 N_Vector vtemp3);
 * -----------------------------------------------------------------
 * The job of cv_lsetup is to prepare the linear solver for
 * subsequent calls to cv_lsolve. It may recompute Jacobian-
 * related data is it deems necessary. Its parameters are as
 * follows:
 *
 * cv_mem - problem memory pointer of type CVodeMem. See the
 *          typedef earlier in this file.
 *
 * convfail - a flag to indicate any problem that occurred during
 *            the solution of the nonlinear equation on the
 *            current time step for which the linear solver is
 *            being used. This flag can be used to help decide
 *            whether the Jacobian data kept by a CVODE linear
 *            solver needs to be updated or not.
 *            Its possible values have been documented above.
 *
 * ypred - the predicted y vector for the current CVODE internal
 *         step.
 *
 * fpred - f(tn, ypred).
 *
 * jcurPtr - a pointer to a boolean to be filled in by cv_lsetup.
 *           The function should set *jcurPtr=SUNTRUE if its Jacobian
 *           data is current after the call and should set
 *           *jcurPtr=SUNFALSE if its Jacobian data is not current.
 *           Note: If cv_lsetup calls for re-evaluation of
 *           Jacobian data (based on convfail and CVODE state
 *           data), it should return *jcurPtr=SUNTRUE always;
 *           otherwise an infinite loop can result.
 *
 * vtemp1 - temporary N_Vector provided for use by cv_lsetup.
 *
 * vtemp3 - temporary N_Vector provided for use by cv_lsetup.
 *
 * vtemp3 - temporary N_Vector provided for use by cv_lsetup.
 *
 * The cv_lsetup routine should return 0 if successful, a positive
 * value for a recoverable error, and a negative value for an
 * unrecoverable error.
 * -----------------------------------------------------------------
 */

/*
 * -----------------------------------------------------------------
 * int (*cv_lsolve)(CVodeMem cv_mem, N_Vector b, N_Vector weight,
 *                  N_Vector ycur, N_Vector fcur);
 * -----------------------------------------------------------------
 * cv_lsolve must solve the linear equation P x = b, where
 * P is some approximation to (I - gamma J), J = (df/dy)(tn,ycur)
 * and the RHS vector b is input. The N-vector ycur contains
 * the solver's current approximation to y(tn) and the vector
 * fcur contains the N_Vector f(tn,ycur). The solution is to be
 * returned in the vector b. cv_lsolve returns a positive value
 * for a recoverable error and a negative value for an
 * unrecoverable error. Success is indicated by a 0 return value.
 * -----------------------------------------------------------------
 */

/*
 * -----------------------------------------------------------------
 * int (*cv_lfree)(CVodeMem cv_mem);
 * -----------------------------------------------------------------
 * cv_lfree should free up any memory allocated by the linear
 * solver. This routine is called once a problem has been
 * completed and the linear solver is no longer needed.  It should 
 * return 0 upon success, nonzero on failure.
 * -----------------------------------------------------------------
 */

/*
 * =================================================================
 *   C V O D E S    I N T E R N A L   F U N C T I O N S
 * =================================================================
 */

/* Norm functions */

realtype cvSensNorm(CVodeMem cv_mem, N_Vector *xS, N_Vector *wS);

realtype cvSensUpdateNorm(CVodeMem cv_mem, realtype old_nrm,
                          N_Vector *xS, N_Vector *wS);


/* Prototype of internal ewtSet function */

int cvEwtSet(N_Vector ycur, N_Vector weight, void *data);

/* High level error handler */

void cvProcessError(CVodeMem cv_mem, 
		    int error_code, const char *module, const char *fname, 
		    const char *msgfmt, ...);

/* Prototype of internal errHandler function */

void cvErrHandler(int error_code, const char *module, const char *function, 
		  char *msg, void *data);

/* Prototypes for internal sensitivity rhs wrappers */

int cvSensRhsWrapper(CVodeMem cv_mem, realtype time, 
                     N_Vector ycur, N_Vector fcur, 
                     N_Vector *yScur, N_Vector *fScur,
                     N_Vector temp1, N_Vector temp2);

int cvSensRhs1Wrapper(CVodeMem cv_mem, realtype time, 
                      N_Vector ycur, N_Vector fcur, 
                      int is, N_Vector yScur, N_Vector fScur,
                      N_Vector temp1, N_Vector temp2);

/* Prototypes for internal sensitivity rhs DQ functions */

int cvSensRhsInternalDQ(int Ns, realtype t, 
                        N_Vector y, N_Vector ydot, 
                        N_Vector *yS, N_Vector *ySdot, 
                        void *fS_data,  
                        N_Vector tempv, N_Vector ftemp);

int cvSensRhs1InternalDQ(int Ns, realtype t, 
                         N_Vector y, N_Vector ydot, 
                         int is, N_Vector yS, N_Vector ySdot, 
                         void *fS_data,
                         N_Vector tempv, N_Vector ftemp);

/* Nonlinear solver functions */
int cvNlsInit(CVodeMem cv_mem);
int cvNlsInitSensSim(CVodeMem cv_mem);
int cvNlsInitSensStg(CVodeMem cv_mem);
int cvNlsInitSensStg1(CVodeMem cv_mem);

/* 
 * =================================================================
 *   C V O D E S    E R R O R    M E S S A G E S
 * =================================================================
 */

#if defined(SUNDIALS_EXTENDED_PRECISION)

#define MSG_TIME        "t = %Lg"
#define MSG_TIME_H      "t = %Lg and h = %Lg"
#define MSG_TIME_INT    "t = %Lg is not between tcur - hu = %Lg and tcur = %Lg."
#define MSG_TIME_TOUT   "tout = %Lg"
#define MSG_TIME_TSTOP  "tstop = %Lg"

#elif defined(SUNDIALS_DOUBLE_PRECISION)

#define MSG_TIME        "t = %lg"
#define MSG_TIME_H      "t = %lg and h = %lg"
#define MSG_TIME_INT    "t = %lg is not between tcur - hu = %lg and tcur = %lg."
#define MSG_TIME_TOUT   "tout = %lg"
#define MSG_TIME_TSTOP  "tstop = %lg"

#else

#define MSG_TIME        "t = %g"
#define MSG_TIME_H      "t = %g and h = %g"
#define MSG_TIME_INT    "t = %g is not between tcur - hu = %g and tcur = %g."
#define MSG_TIME_TOUT   "tout = %g"
#define MSG_TIME_TSTOP  "tstop = %g"

#endif


/* Initialization and I/O error messages */

#define MSGCV_NO_MEM "cvode_mem = NULL illegal."
#define MSGCV_CVMEM_FAIL "Allocation of cvode_mem failed."
#define MSGCV_MEM_FAIL "A memory request failed."
#define MSGCV_BAD_LMM  "Illegal value for lmm. The legal values are CV_ADAMS and CV_BDF."
#define MSGCV_NO_MALLOC "Attempt to call before CVodeInit."
#define MSGCV_NEG_MAXORD "maxord <= 0 illegal."
#define MSGCV_BAD_MAXORD  "Illegal attempt to increase maximum method order."
#define MSGCV_SET_SLDET  "Attempt to use stability limit detection with the CV_ADAMS method illegal."
#define MSGCV_NEG_HMIN "hmin < 0 illegal."
#define MSGCV_NEG_HMAX "hmax < 0 illegal."
#define MSGCV_BAD_HMIN_HMAX "Inconsistent step size limits: hmin > hmax."
#define MSGCV_BAD_RELTOL "reltol < 0 illegal."
#define MSGCV_BAD_ABSTOL "abstol has negative component(s) (illegal)."
#define MSGCV_NULL_ABSTOL "abstol = NULL illegal."
#define MSGCV_NULL_Y0 "y0 = NULL illegal."
#define MSGCV_Y0_FAIL_CONSTR "y0 fails to satisfy constraints."
#define MSGCV_BAD_ISM_CONSTR "Constraints can not be enforced while forward sensitivity is used with simultaneous method"
#define MSGCV_NULL_F "f = NULL illegal."
#define MSGCV_NULL_G "g = NULL illegal."
#define MSGCV_BAD_NVECTOR "A required vector operation is not implemented."
#define MSGCV_BAD_CONSTR "Illegal values in constraints vector."
#define MSGCV_BAD_K "Illegal value for k."
#define MSGCV_NULL_DKY "dky = NULL illegal."
#define MSGCV_BAD_T "Illegal value for t." MSG_TIME_INT
#define MSGCV_NO_ROOT "Rootfinding was not initialized."
#define MSGCV_NLS_INIT_FAIL "The nonlinear solver's init routine failed."

#define MSGCV_NO_QUAD  "Quadrature integration not activated."
#define MSGCV_BAD_ITOLQ "Illegal value for itolQ. The legal values are CV_SS and CV_SV."
#define MSGCV_NULL_ABSTOLQ "abstolQ = NULL illegal."
#define MSGCV_BAD_RELTOLQ "reltolQ < 0 illegal."
#define MSGCV_BAD_ABSTOLQ "abstolQ has negative component(s) (illegal)."  

#define MSGCV_SENSINIT_2 "Sensitivity analysis already initialized."
#define MSGCV_NO_SENSI  "Forward sensitivity analysis not activated."
#define MSGCV_BAD_ITOLS "Illegal value for itolS. The legal values are CV_SS, CV_SV, and CV_EE."
#define MSGCV_NULL_ABSTOLS "abstolS = NULL illegal."
#define MSGCV_BAD_RELTOLS "reltolS < 0 illegal."
#define MSGCV_BAD_ABSTOLS "abstolS has negative component(s) (illegal)."  
#define MSGCV_BAD_PBAR "pbar has zero component(s) (illegal)."
#define MSGCV_BAD_PLIST "plist has negative component(s) (illegal)."
#define MSGCV_BAD_NS "NS <= 0 illegal."
#define MSGCV_NULL_YS0 "yS0 = NULL illegal."
#define MSGCV_BAD_ISM "Illegal value for ism. Legal values are: CV_SIMULTANEOUS, CV_STAGGERED and CV_STAGGERED1."
#define MSGCV_BAD_IFS "Illegal value for ifS. Legal values are: CV_ALLSENS and CV_ONESENS."
#define MSGCV_BAD_ISM_IFS "Illegal ism = CV_STAGGERED1 for CVodeSensInit."
#define MSGCV_BAD_IS "Illegal value for is."
#define MSGCV_NULL_DKYA "dkyA = NULL illegal."
#define MSGCV_BAD_DQTYPE "Illegal value for DQtype. Legal values are: CV_CENTERED and CV_FORWARD."
#define MSGCV_BAD_DQRHO "DQrhomax < 0 illegal."

#define MSGCV_BAD_ITOLQS "Illegal value for itolQS. The legal values are CV_SS, CV_SV, and CV_EE."
#define MSGCV_NULL_ABSTOLQS "abstolQS = NULL illegal."
#define MSGCV_BAD_RELTOLQS "reltolQS < 0 illegal."
#define MSGCV_BAD_ABSTOLQS "abstolQS has negative component(s) (illegal)."  
#define MSGCV_NO_QUADSENSI  "Forward sensitivity analysis for quadrature variables not activated."
#define MSGCV_NULL_YQS0 "yQS0 = NULL illegal."

/* CVode Error Messages */

#define MSGCV_NO_TOL "No integration tolerances have been specified."
#define MSGCV_LSOLVE_NULL "The linear solver's solve routine is NULL."
#define MSGCV_YOUT_NULL "yout = NULL illegal."
#define MSGCV_TRET_NULL "tret = NULL illegal."
#define MSGCV_BAD_EWT "Initial ewt has component(s) equal to zero (illegal)."
#define MSGCV_EWT_NOW_BAD "At " MSG_TIME ", a component of ewt has become <= 0."
#define MSGCV_BAD_ITASK "Illegal value for itask."
#define MSGCV_BAD_H0 "h0 and tout - t0 inconsistent."
#define MSGCV_BAD_TOUT "Trouble interpolating at " MSG_TIME_TOUT ". tout too far back in direction of integration"
#define MSGCV_EWT_FAIL "The user-provide EwtSet function failed."
#define MSGCV_EWT_NOW_FAIL "At " MSG_TIME ", the user-provide EwtSet function failed."
#define MSGCV_LINIT_FAIL "The linear solver's init routine failed."
#define MSGCV_HNIL_DONE "The above warning has been issued mxhnil times and will not be issued again for this problem."
#define MSGCV_TOO_CLOSE "tout too close to t0 to start integration."
#define MSGCV_MAX_STEPS "At " MSG_TIME ", mxstep steps taken before reaching tout."
#define MSGCV_TOO_MUCH_ACC "At " MSG_TIME ", too much accuracy requested."
#define MSGCV_HNIL "Internal " MSG_TIME_H " are such that t + h = t on the next step. The solver will continue anyway."
#define MSGCV_ERR_FAILS "At " MSG_TIME_H ", the error test failed repeatedly or with |h| = hmin."
#define MSGCV_CONV_FAILS "At " MSG_TIME_H ", the corrector convergence test failed repeatedly or with |h| = hmin."
#define MSGCV_SETUP_FAILED "At " MSG_TIME ", the setup routine failed in an unrecoverable manner."
#define MSGCV_SOLVE_FAILED "At " MSG_TIME ", the solve routine failed in an unrecoverable manner."
#define MSGCV_FAILED_CONSTR "At " MSG_TIME ", unable to satisfy inequality constraints."
#define MSGCV_RHSFUNC_FAILED "At " MSG_TIME ", the right-hand side routine failed in an unrecoverable manner."
#define MSGCV_RHSFUNC_UNREC "At " MSG_TIME ", the right-hand side failed in a recoverable manner, but no recovery is possible."
#define MSGCV_RHSFUNC_REPTD "At " MSG_TIME " repeated recoverable right-hand side function errors."
#define MSGCV_RHSFUNC_FIRST "The right-hand side routine failed at the first call."
#define MSGCV_RTFUNC_FAILED "At " MSG_TIME ", the rootfinding routine failed in an unrecoverable manner."
#define MSGCV_CLOSE_ROOTS "Root found at and very near " MSG_TIME "."
#define MSGCV_BAD_TSTOP "The value " MSG_TIME_TSTOP " is behind current " MSG_TIME " in the direction of integration."
#define MSGCV_INACTIVE_ROOTS "At the end of the first step, there are still some root functions identically 0. This warning will not be issued again."
#define MSGCV_NLS_SETUP_FAILED "At " MSG_TIME "the nonlinear solver setup failed unrecoverably."
#define MSGCV_NLS_INPUT_NULL "At " MSG_TIME "the nonlinear solver was passed a NULL input."


#define MSGCV_NO_TOLQ "No integration tolerances for quadrature variables have been specified."
#define MSGCV_BAD_EWTQ "Initial ewtQ has component(s) equal to zero (illegal)."
#define MSGCV_EWTQ_NOW_BAD "At " MSG_TIME ", a component of ewtQ has become <= 0."
#define MSGCV_QRHSFUNC_FAILED "At " MSG_TIME ", the quadrature right-hand side routine failed in an unrecoverable manner."
#define MSGCV_QRHSFUNC_UNREC "At " MSG_TIME ", the quadrature right-hand side failed in a recoverable manner, but no recovery is possible."
#define MSGCV_QRHSFUNC_REPTD "At " MSG_TIME " repeated recoverable quadrature right-hand side function errors."
#define MSGCV_QRHSFUNC_FIRST "The quadrature right-hand side routine failed at the first call."

#define MSGCV_NO_TOLS "No integration tolerances for sensitivity variables have been specified."
#define MSGCV_NULL_P "p = NULL when using internal DQ for sensitivity RHS illegal."
#define MSGCV_BAD_EWTS "Initial ewtS has component(s) equal to zero (illegal)."
#define MSGCV_EWTS_NOW_BAD "At " MSG_TIME ", a component of ewtS has become <= 0."
#define MSGCV_SRHSFUNC_FAILED "At " MSG_TIME ", the sensitivity right-hand side routine failed in an unrecoverable manner."
#define MSGCV_SRHSFUNC_UNREC "At " MSG_TIME ", the sensitivity right-hand side failed in a recoverable manner, but no recovery is possible."
#define MSGCV_SRHSFUNC_REPTD "At " MSG_TIME " repeated recoverable sensitivity right-hand side function errors."
#define MSGCV_SRHSFUNC_FIRST "The sensitivity right-hand side routine failed at the first call."

#define MSGCV_NULL_FQ "CVODES is expected to use DQ to evaluate the RHS of quad. sensi., but quadratures were not initialized."
#define MSGCV_NO_TOLQS "No integration tolerances for quadrature sensitivity variables have been specified."
#define MSGCV_BAD_EWTQS "Initial ewtQS has component(s) equal to zero (illegal)."
#define MSGCV_EWTQS_NOW_BAD "At " MSG_TIME ", a component of ewtQS has become <= 0."
#define MSGCV_QSRHSFUNC_FAILED "At " MSG_TIME ", the quadrature sensitivity right-hand side routine failed in an unrecoverable manner."
#define MSGCV_QSRHSFUNC_UNREC "At " MSG_TIME ", the quadrature sensitivity right-hand side failed in a recoverable manner, but no recovery is possible."
#define MSGCV_QSRHSFUNC_REPTD "At " MSG_TIME " repeated recoverable quadrature sensitivity right-hand side function errors."
#define MSGCV_QSRHSFUNC_FIRST "The quadrature sensitivity right-hand side routine failed at the first call."

/* 
 * =================================================================
 *   C V O D E A    E R R O R    M E S S A G E S
 * =================================================================
 */

#define MSGCV_NO_ADJ      "Illegal attempt to call before calling CVodeAdjMalloc."
#define MSGCV_BAD_STEPS   "Steps nonpositive illegal."
#define MSGCV_BAD_INTERP  "Illegal value for interp."
#define MSGCV_BAD_WHICH   "Illegal value for which."
#define MSGCV_NO_BCK      "No backward problems have been defined yet."
#define MSGCV_NO_FWD      "Illegal attempt to call before calling CVodeF."
#define MSGCV_BAD_TB0     "The initial time tB0 for problem %d is outside the interval over which the forward problem was solved."
#define MSGCV_BAD_SENSI   "At least one backward problem requires sensitivities, but they were not stored for interpolation."
#define MSGCV_BAD_ITASKB  "Illegal value for itaskB. Legal values are CV_NORMAL and CV_ONE_STEP."
#define MSGCV_BAD_TBOUT   "The final time tBout is outside the interval over which the forward problem was solved."
#define MSGCV_BACK_ERROR  "Error occured while integrating backward problem # %d" 
#define MSGCV_BAD_TINTERP "Bad t = %g for interpolation."
#define MSGCV_WRONG_INTERP "This function cannot be called for the specified interp type."

#ifdef __cplusplus
}
#endif

#endif
