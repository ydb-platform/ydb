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
 * This is the header file (private version) for the main IDAS solver.
 * -----------------------------------------------------------------
 */

#ifndef _IDAS_IMPL_H
#define _IDAS_IMPL_H

#include <stdarg.h>

#include <idas/idas.h>
#include <sundials/sundials_nvector.h>
#include <sundials/sundials_types.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/* 
 * =================================================================
 *   M A I N    I N T E G R A T O R    M E M O R Y    B L O C K
 * =================================================================
 */


/* Basic IDA constants */
  
#define HMAX_INV_DEFAULT RCONST(0.0) /* hmax_inv default value          */
#define MAXORD_DEFAULT   5           /* maxord default value            */
#define MXORDP1          6           /* max. number of N_Vectors in phi */
#define MXSTEP_DEFAULT   500         /* mxstep default value            */

/* Return values for lower level routines used by IDASolve and functions
   provided to the nonlinear solver */

#define IDA_RES_RECVR       +1
#define IDA_LSETUP_RECVR    +2
#define IDA_LSOLVE_RECVR    +3
#define IDA_CONSTR_RECVR    +5
#define IDA_NLS_SETUP_RECVR +6

#define IDA_QRHS_RECVR   +10
#define IDA_SRES_RECVR   +11
#define IDA_QSRHS_RECVR  +12

/* itol */
#define IDA_NN               0
#define IDA_SS               1
#define IDA_SV               2
#define IDA_WF               3
#define IDA_EE               4 

/*
 * -----------------------------------------------------------------
 * Types: struct IDAMemRec, IDAMem
 * -----------------------------------------------------------------
 * The type IDAMem is type pointer to struct IDAMemRec.
 * This structure contains fields to keep track of problem state.
 * -----------------------------------------------------------------
 */

typedef struct IDAMemRec {

  realtype ida_uround;    /* machine unit roundoff */

  /*--------------------------
    Problem Specification Data 
    --------------------------*/

  IDAResFn       ida_res;            /* F(t,y(t),y'(t))=0; the function F     */
  void          *ida_user_data;      /* user pointer passed to res            */

  int            ida_itol;           /* itol = IDA_SS, IDA_SV, IDA_WF, IDA_NN */
  realtype       ida_rtol;           /* relative tolerance                    */
  realtype       ida_Satol;          /* scalar absolute tolerance             */  
  N_Vector       ida_Vatol;          /* vector absolute tolerance             */  
  booleantype    ida_user_efun;      /* SUNTRUE if user provides efun         */
  IDAEwtFn       ida_efun;           /* function to set ewt                   */
  void          *ida_edata;          /* user pointer passed to efun           */
  
  /*-----------------------
    Quadrature Related Data 
    -----------------------*/

  booleantype    ida_quadr;

  IDAQuadRhsFn   ida_rhsQ;
  void          *ida_user_dataQ;

  booleantype    ida_errconQ;

  int            ida_itolQ;
  realtype       ida_rtolQ;
  realtype       ida_SatolQ;    /* scalar absolute tolerance for quadratures  */
  N_Vector       ida_VatolQ;    /* vector absolute tolerance for quadratures  */

  /*------------------------
    Sensitivity Related Data
    ------------------------*/

  booleantype    ida_sensi;
  int            ida_Ns;
  int            ida_ism;

  IDASensResFn   ida_resS;
  void          *ida_user_dataS;
  booleantype    ida_resSDQ;

  realtype      *ida_p;
  realtype      *ida_pbar;
  int           *ida_plist;
  int            ida_DQtype;
  realtype       ida_DQrhomax;

  booleantype    ida_errconS;       /* SUNTRUE if sensitivities in err. control  */

  int            ida_itolS;
  realtype       ida_rtolS;         /* relative tolerance for sensitivities   */
  realtype       *ida_SatolS;       /* scalar absolute tolerances for sensi.  */
  N_Vector       *ida_VatolS;       /* vector absolute tolerances for sensi.  */

  /*-----------------------------------
    Quadrature Sensitivity Related Data 
    -----------------------------------*/

  booleantype ida_quadr_sensi;   /* SUNTRUE if computing sensitivities of quadrs. */

  IDAQuadSensRhsFn ida_rhsQS;    /* fQS = (dfQ/dy)*yS + (dfQ/dp)                  */
  void *ida_user_dataQS;         /* data pointer passed to fQS                    */
  booleantype ida_rhsQSDQ;       /* SUNTRUE if using internal DQ functions        */

  booleantype ida_errconQS;      /* SUNTRUE if yQS are considered in err. con.    */

  int ida_itolQS;
  realtype ida_rtolQS;           /* relative tolerance for yQS                */
  realtype *ida_SatolQS;         /* scalar absolute tolerances for yQS        */
  N_Vector *ida_VatolQS;         /* vector absolute tolerances for yQS        */

  /*-----------------------------------------------
    Divided differences array and associated arrays
    -----------------------------------------------*/

  N_Vector ida_phi[MXORDP1];   /* phi = (maxord+1) arrays of divided differences */

  realtype ida_psi[MXORDP1];   /* differences in t (sums of recent step sizes)   */
  realtype ida_alpha[MXORDP1]; /* ratios of current stepsize to psi values       */
  realtype ida_beta[MXORDP1];  /* ratios of current to previous product of psi's */
  realtype ida_sigma[MXORDP1]; /* product successive alpha values and factorial  */
  realtype ida_gamma[MXORDP1]; /* sum of reciprocals of psi values               */

  /*-------------------------
    N_Vectors for integration
    -------------------------*/

  N_Vector ida_ewt;          /* error weight vector                           */
  N_Vector ida_yy;           /* work space for y vector (= user's yret)       */
  N_Vector ida_yp;           /* work space for y' vector (= user's ypret)     */
  N_Vector ida_yypredict;    /* predicted y vector                            */
  N_Vector ida_yppredict;    /* predicted y' vector                           */
  N_Vector ida_delta;        /* residual vector                               */
  N_Vector ida_id;           /* bit vector for diff./algebraic components     */
  N_Vector ida_constraints;  /* vector of inequality constraint options       */
  N_Vector ida_savres;       /* saved residual vector                         */
  N_Vector ida_ee;           /* accumulated corrections to y vector, but
                                set equal to estimated local errors upon
                                successful return                             */
  N_Vector ida_mm;           /* mask vector in constraints tests (= tempv2)   */
  N_Vector ida_tempv1;       /* work space vector                             */
  N_Vector ida_tempv2;       /* work space vector                             */
  N_Vector ida_tempv3;       /* work space vector                             */
  N_Vector ida_ynew;         /* work vector for y in IDACalcIC (= tempv2)     */
  N_Vector ida_ypnew;        /* work vector for yp in IDACalcIC (= ee)        */
  N_Vector ida_delnew;       /* work vector for delta in IDACalcIC (= phi[2]) */
  N_Vector ida_dtemp;        /* work vector in IDACalcIC (= phi[3])           */


  /*----------------------------
    Quadrature Related N_Vectors 
    ----------------------------*/

  N_Vector ida_phiQ[MXORDP1];
  N_Vector ida_yyQ;
  N_Vector ida_ypQ;
  N_Vector ida_ewtQ;
  N_Vector ida_eeQ;

  /*---------------------------
    Sensitivity Related Vectors 
    ---------------------------*/

  N_Vector *ida_phiS[MXORDP1];
  N_Vector *ida_ewtS;

  N_Vector *ida_eeS;         /* cumulative sensitivity corrections            */

  N_Vector *ida_yyS;         /* allocated and used for:                       */
  N_Vector *ida_ypS;         /*                 ism = SIMULTANEOUS            */
  N_Vector *ida_yySpredict;  /*                 ism = STAGGERED               */
  N_Vector *ida_ypSpredict;
  N_Vector *ida_deltaS;

  N_Vector ida_tmpS1;        /* work space vectors  | tmpS1 = tempv1          */
  N_Vector ida_tmpS2;        /* for resS            | tmpS2 = tempv2          */
  N_Vector ida_tmpS3;        /*                     | tmpS3 = allocated       */    

  N_Vector *ida_savresS;     /* work vector in IDACalcIC for stg (= phiS[2])  */ 
  N_Vector *ida_delnewS;     /* work vector in IDACalcIC for stg (= phiS[3])  */ 
 
  N_Vector *ida_yyS0;        /* initial yS, ypS vectors allocated and         */
  N_Vector *ida_ypS0;        /* deallocated in IDACalcIC function             */

  N_Vector *ida_yyS0new;     /* work vector in IDASensLineSrch   (= phiS[4])  */
  N_Vector *ida_ypS0new;     /* work vector in IDASensLineSrch   (= eeS)      */

  /*--------------------------------------
    Quadrature Sensitivity Related Vectors 
    --------------------------------------*/

  N_Vector *ida_phiQS[MXORDP1];/* Mod. div. diffs. for quadr. sensitivities   */
  N_Vector *ida_ewtQS;         /* error weight vectors for sensitivities      */

  N_Vector *ida_eeQS;          /* cumulative quadr.sensi.corrections          */

  N_Vector *ida_yyQS;          /* Unlike yS, yQS is not allocated by the user */
  N_Vector *ida_tempvQS;       /* temporary storage vector (~ tempv)          */
  N_Vector ida_savrhsQ;        /* saved quadr. rhs (needed for rhsQS calls)   */

  /*------------------------------ 
    Variables for use by IDACalcIC
    ------------------------------*/

  realtype ida_t0;          /* initial t                                      */
  N_Vector ida_yy0;         /* initial y vector (user-supplied).              */
  N_Vector ida_yp0;         /* initial y' vector (user-supplied).             */

  int ida_icopt;            /* IC calculation user option                     */
  booleantype ida_lsoff;    /* IC calculation linesearch turnoff option       */
  int ida_maxnh;            /* max. number of h tries in IC calculation       */
  int ida_maxnj;            /* max. number of J tries in IC calculation       */
  int ida_maxnit;           /* max. number of Netwon iterations in IC calc.   */
  int ida_nbacktr;          /* number of IC linesearch backtrack operations   */
  int ida_sysindex;         /* computed system index (0 or 1)                 */
  int ida_maxbacks;         /* max backtracks per Newton step                 */
  realtype ida_epiccon;     /* IC nonlinear convergence test constant         */
  realtype ida_steptol;     /* minimum Newton step size in IC calculation     */
  realtype ida_tscale;      /* time scale factor = abs(tout1 - t0)            */

  /* Tstop information */

  booleantype ida_tstopset;
  realtype ida_tstop;

  /* Step Data */

  int ida_kk;        /* current BDF method order                              */
  int ida_knew;      /* order for next step from order decrease decision      */
  int ida_phase;     /* flag to trigger step doubling in first few steps      */
  int ida_ns;        /* counts steps at fixed stepsize and order              */

  realtype ida_hin;      /* initial step                                      */
  realtype ida_hh;       /* current step size h                               */
  realtype ida_rr;       /* rr = hnext / hused                                */
  realtype ida_tn;       /* current internal value of t                       */
  realtype ida_tretlast; /* value of tret previously returned by IDASolve     */
  realtype ida_cj;       /* current value of scalar (-alphas/hh) in Jacobian  */
  realtype ida_cjlast;   /* cj value saved from last successful step          */
  realtype ida_cjold;    /* cj value saved from last call to lsetup           */
  realtype ida_cjratio;  /* ratio of cj values: cj/cjold                      */
  realtype ida_ss;       /* scalar used in Newton iteration convergence test  */
  realtype ida_oldnrm;   /* norm of previous nonlinear solver update          */
  realtype ida_epsNewt;  /* test constant in Newton convergence test          */
  realtype ida_epcon;    /* coeficient of the Newton covergence test          */
  realtype ida_toldel;   /* tolerance in direct test on Newton corrections    */

  realtype ida_ssS;      /* scalar ss for staggered sensitivities             */

  /*------
    Limits
    ------*/

  int ida_maxncf;        /* max numer of convergence failures                 */
  int ida_maxcor;        /* max number of Newton corrections                  */
  int ida_maxnef;        /* max number of error test failures                 */

  int ida_maxord;        /* max value of method order k:                      */
  int ida_maxord_alloc;  /* value of maxord used when allocating memory       */
  long int ida_mxstep;   /* max number of internal steps for one user call    */
  realtype ida_hmax_inv; /* inverse of max. step size hmax (default = 0.0)    */

  int ida_maxcorS;       /* max number of Newton corrections for sensitivity
			    systems (staggered method)                        */

  /*--------
    Counters
    --------*/

  long int ida_nst;      /* number of internal steps taken                    */

  long int ida_nre;      /* number of function (res) calls                    */
  long int ida_nrQe;
  long int ida_nrSe;
  long int ida_nrQSe;    /* number of fQS calls                               */
  long int ida_nreS;
  long int ida_nrQeS;    /* number of fQ calls from sensi DQ                  */


  long int ida_ncfn;     /* number of corrector convergence failures          */
  long int ida_ncfnQ;
  long int ida_ncfnS;

  long int ida_netf;     /* number of error test failures                     */
  long int ida_netfQ;
  long int ida_netfS;
  long int ida_netfQS;   /* number of quadr. sensi. error test failures  */

  long int ida_nni;      /* number of Newton iterations performed             */
  long int ida_nniS;

  long int ida_nsetups;  /* number of lsetup calls                            */
  long int ida_nsetupsS;
  
  /*---------------------------
    Space requirements for IDAS
    ---------------------------*/

  sunindextype ida_lrw1; /* no. of realtype words in 1 N_Vector               */
  sunindextype ida_liw1; /* no. of integer words in 1 N_Vector                */
  sunindextype ida_lrw1Q;
  sunindextype ida_liw1Q;
  long int     ida_lrw;  /* number of realtype words in IDA work vectors      */
  long int     ida_liw;  /* no. of integer words in IDA work vectors          */


  /*-------------------------------------------
    Error handler function and error ouput file 
    -------------------------------------------*/

  IDAErrHandlerFn ida_ehfun;  /* Error messages are handled by ehfun           */
  void *ida_eh_data;          /* dats pointer passed to ehfun                  */
  FILE *ida_errfp;            /* IDA error messages are sent to errfp          */

  /* Flags to verify correct calling sequence */
    
  booleantype ida_SetupDone;     /* set to SUNFALSE by IDAInit and IDAReInit
				    set to SUNTRUE by IDACalcIC or IDASolve    */

  booleantype ida_VatolMallocDone;
  booleantype ida_constraintsMallocDone;
  booleantype ida_idMallocDone;

  booleantype ida_MallocDone;    /* set to SUNFALSE by IDACreate
				    set to SUNTRUE by IDAInit
				    tested by IDAReInit and IDASolve           */

  booleantype ida_VatolQMallocDone;
  booleantype ida_quadMallocDone;

  booleantype ida_VatolSMallocDone;
  booleantype ida_SatolSMallocDone;
  booleantype ida_sensMallocDone;

  booleantype ida_VatolQSMallocDone;
  booleantype ida_SatolQSMallocDone;
  booleantype ida_quadSensMallocDone;

  /*---------------------
    Nonlinear Solver Data
    ---------------------*/

  SUNNonlinearSolver NLS;    /* nonlinear solver object for DAE solves */
  booleantype ownNLS;        /* flag indicating NLS ownership */

  SUNNonlinearSolver NLSsim; /* nonlinear solver object for DAE+Sens solves
                                with the simultaneous corrector option */
  booleantype ownNLSsim;     /* flag indicating NLS ownership */

  SUNNonlinearSolver NLSstg; /* nonlinear solver object for DAE+Sens solves
                                with the staggered corrector option */
  booleantype ownNLSstg;     /* flag indicating NLS ownership */

  /* The following vectors are NVector wrappers for use with the simultaneous
     and staggered corrector methods:

       Simult:  ycor0Sim = [ida_delta, ida_deltaS]
                ycorSim  = [ida_ee,    ida_eeS]
                ewtSim   = [ida_ewt,   ida_ewtS]

       Stagger: ycor0Stg = ida_deltaS
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

  int (*ida_linit)(struct IDAMemRec *idamem);

  int (*ida_lsetup)(struct IDAMemRec *idamem, N_Vector yyp, 
		    N_Vector ypp, N_Vector resp, 
		    N_Vector tempv1, N_Vector tempv2, N_Vector tempv3); 

  int (*ida_lsolve)(struct IDAMemRec *idamem, N_Vector b, N_Vector weight,
		    N_Vector ycur, N_Vector ypcur, N_Vector rescur);

  int (*ida_lperf)(struct IDAMemRec *idamem, int perftask);

  int (*ida_lfree)(struct IDAMemRec *idamem);

  /* Linear Solver specific memory */

  void *ida_lmem;           

  /* Flag to request a call to the setup routine */
  
  booleantype ida_forceSetup;

  /* Flag to indicate successful ida_linit call */

  booleantype ida_linitOK;

  /*------------
    Saved Values
    ------------*/

  booleantype    ida_constraintsSet; /* constraints vector present             */
  booleantype    ida_suppressalg;    /* SUNTRUE if suppressing algebraic vars.
					in local error tests                   */
  int ida_kused;         /* method order used on last successful step          */
  realtype ida_h0u;      /* actual initial stepsize                            */
  realtype ida_hused;    /* step size used on last successful step             */
  realtype ida_tolsf;    /* tolerance scale factor (saved value)               */

  /*----------------
    Rootfinding Data
    ----------------*/

  IDARootFn ida_gfun;    /* Function g for roots sought                       */
  int ida_nrtfn;         /* number of components of g                         */
  int *ida_iroots;       /* array for root information                        */
  int *ida_rootdir;      /* array specifying direction of zero-crossing       */
  realtype ida_tlo;      /* nearest endpoint of interval in root search       */
  realtype ida_thi;      /* farthest endpoint of interval in root search      */
  realtype ida_trout;    /* t return value from rootfinder routine            */
  realtype *ida_glo;     /* saved array of g values at t = tlo                */
  realtype *ida_ghi;     /* saved array of g values at t = thi                */
  realtype *ida_grout;   /* array of g values at t = trout                    */
  realtype ida_toutc;    /* copy of tout (if NORMAL mode)                     */
  realtype ida_ttol;     /* tolerance on root location                        */
  int ida_taskc;         /* copy of parameter itask                           */
  int ida_irfnd;         /* flag showing whether last step had a root         */
  long int ida_nge;      /* counter for g evaluations                         */
  booleantype *ida_gactive; /* array with active/inactive event functions     */
  int ida_mxgnull;       /* number of warning messages about possible g==0    */

  /* Arrays for Fused Vector Operations */

  /* scalar arrays */
  realtype* ida_cvals;
  realtype  ida_dvals[MAXORD_DEFAULT];

  /* vector  arrays */
  N_Vector* ida_Xvecs;
  N_Vector* ida_Zvecs;

  /*------------------------
    Adjoint sensitivity data
    ------------------------*/

  booleantype ida_adj;              /* SUNTRUE if performing ASA              */

  struct IDAadjMemRec *ida_adj_mem; /* Pointer to adjoint memory structure    */

  booleantype ida_adjMallocDone;

} *IDAMem;

/* 
 * =================================================================
 *   A D J O I N T   M O D U L E    M E M O R Y    B L O C K
 * =================================================================
 */

/*
 * -----------------------------------------------------------------
 * Forward references for pointers to various structures 
 * -----------------------------------------------------------------
 */

typedef struct IDAadjMemRec *IDAadjMem;
typedef struct CkpntMemRec *CkpntMem;
typedef struct DtpntMemRec *DtpntMem;
typedef struct IDABMemRec *IDABMem;

/*
 * -----------------------------------------------------------------
 * Types for functions provided by an interpolation module
 * -----------------------------------------------------------------
 * IDAAMMallocFn: Type for a function that initializes the content
 *                field of the structures in the dt array
 * IDAAMFreeFn:   Type for a function that deallocates the content
 *                field of the structures in the dt array
 * IDAAGetYFn:    Function type for a function that returns the 
 *                interpolated forward solution.
 * IDAAStorePnt:  Function type for a function that stores a new
 *                point in the structure d
 * -----------------------------------------------------------------
 */

typedef booleantype (*IDAAMMallocFn)(IDAMem IDA_mem);
typedef void (*IDAAMFreeFn)(IDAMem IDA_mem);
typedef int (*IDAAGetYFn)(IDAMem IDA_mem, realtype t, 
                          N_Vector yy, N_Vector yp, 
                          N_Vector *yyS, N_Vector *ypS);
typedef int (*IDAAStorePntFn)(IDAMem IDA_mem, DtpntMem d);

/*
 * -----------------------------------------------------------------
 * Types : struct CkpntMemRec, CkpntMem
 * -----------------------------------------------------------------
 * The type CkpntMem is type pointer to struct CkpntMemRec.
 * This structure contains fields to store all information at a
 * check point that is needed to 'hot' start IDAS.
 * -----------------------------------------------------------------
 */

struct CkpntMemRec {

  /* Integration limits */
  realtype ck_t0;
  realtype ck_t1;
    
  /* Modified divided difference array */
  N_Vector ck_phi[MXORDP1];

  /* Do we need to carry quadratures? */
  booleantype ck_quadr;

  /* Modified divided difference array for quadratures */
  N_Vector ck_phiQ[MXORDP1];
    
  /* Do we need to carry sensitivities? */
  booleantype ck_sensi;

  /* number of sensitivities */
  int ck_Ns;

  /* Modified divided difference array for sensitivities */
  N_Vector *ck_phiS[MXORDP1];

  /* Do we need to carry quadrature sensitivities? */
  booleantype ck_quadr_sensi;

  /* Modified divided difference array for quadrature sensitivities */
  N_Vector *ck_phiQS[MXORDP1];


  /* Step data */
  long int     ck_nst;
  realtype     ck_tretlast; 
  long int     ck_ns;
  int          ck_kk;
  int          ck_kused;
  int          ck_knew;
  int          ck_phase;
    
  realtype     ck_hh;
  realtype     ck_hused;
  realtype     ck_rr;
  realtype     ck_cj;
  realtype     ck_cjlast;
  realtype     ck_cjold;
  realtype     ck_cjratio;
  realtype     ck_ss;
  realtype     ck_ssS;

  realtype     ck_psi[MXORDP1];
  realtype     ck_alpha[MXORDP1];
  realtype     ck_beta[MXORDP1];
  realtype     ck_sigma[MXORDP1];
  realtype     ck_gamma[MXORDP1];

  /* How many phi, phiS, phiQ and phiQS were allocated? */
  int          ck_phi_alloc;
        
  /* Pointer to next structure in list */
  struct CkpntMemRec *ck_next;    
};

/*
 * -----------------------------------------------------------------
 * Type : struct DtpntMemRec
 * -----------------------------------------------------------------
 * This structure contains fields to store all information at a
 * data point that is needed to interpolate solution of forward
 * simulations. Its content field is interpType-dependent.
 * -----------------------------------------------------------------
 */
  
struct DtpntMemRec {
  realtype t;    /* time */
  void *content; /* interpType-dependent content */
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

  /* yd and ySd store the derivative(s) only for the first dt 
     point. NULL otherwise. */
  N_Vector yd;   
  N_Vector *ySd;
  int order;
} *PolynomialDataMem;

/*
 * -----------------------------------------------------------------
 * Type : struct IDABMemRec
 * -----------------------------------------------------------------
 * The type IDABMemRec is a pointer to a structure which stores all
 * information for ONE backward problem.
 * The IDAadjMem struct contains a linked list of IDABMem pointers
 * -----------------------------------------------------------------
 */
struct IDABMemRec {

  /* Index of this backward problem */
  int ida_index;

  /* Time at which the backward problem is initialized. */
  realtype ida_t0;

  /* Memory for this backward problem */
  IDAMem IDA_mem;

  /* Flags to indicate that this backward problem's RHS or quad RHS
   * require forward sensitivities */
  booleantype ida_res_withSensi;
  booleantype ida_rhsQ_withSensi;

  /* Residual function for backward run */
  IDAResFnB   ida_res;
  IDAResFnBS  ida_resS;

  /* Right hand side quadrature function (fQB) for backward run */
  IDAQuadRhsFnB   ida_rhsQ;
  IDAQuadRhsFnBS  ida_rhsQS;
  
  /* User user_data */
  void *ida_user_data;
    
  /* Linear solver's data and functions */

  /* Memory block for a linear solver's interface to IDAA */
  void *ida_lmem;

  /* Function to free any memory allocated by the linear solver */
  int (*ida_lfree)(IDABMem IDAB_mem);

  /* Memory block for a preconditioner's module interface to IDAA */ 
  void *ida_pmem;

  /* Function to free any memory allocated by the preconditioner module */
  int (*ida_pfree)(IDABMem IDAB_mem);

  /* Time at which to extract solution / quadratures */
  realtype ida_tout;

  /* Workspace Nvectors */
  N_Vector ida_yy;
  N_Vector ida_yp;

  /* Link to next structure in list. */
  struct IDABMemRec *ida_next;
};


/*
 * -----------------------------------------------------------------
 * Type : struct IDAadjMemRec
 * -----------------------------------------------------------------
 * The type IDAadjMem is type pointer to struct IDAadjMemRec.
 * This structure contins fields to store all information
 * necessary for adjoint sensitivity analysis.
 * -----------------------------------------------------------------
 */

struct IDAadjMemRec {
    
  /* --------------------
   * Forward problem data
   * -------------------- */
  
  /* Integration interval */
  realtype ia_tinitial, ia_tfinal;

  /* Flag for first call to IDASolveF */
  booleantype ia_firstIDAFcall;

  /* Flag if IDASolveF was called with TSTOP */
  booleantype ia_tstopIDAFcall;
  realtype ia_tstopIDAF;

  /* ----------------------
   * Backward problems data
   * ---------------------- */

  /* Storage for backward problems */
  struct IDABMemRec *IDAB_mem;

  /* Number of backward problems. */
  int ia_nbckpbs;

  /* Address of current backward problem (iterator). */
  struct IDABMemRec *ia_bckpbCrt; 

  /* Flag for first call to IDASolveB */
  booleantype ia_firstIDABcall;

  /* ----------------
   * Check point data
   * ---------------- */
    
  /* Storage for check point information */
  struct CkpntMemRec *ck_mem;

  /* address of the check point structure for which data is available */
  struct CkpntMemRec *ia_ckpntData;

  /* Number of checkpoints. */
  int ia_nckpnts;

  /* ------------------
   * Interpolation data
   * ------------------ */

  /* Number of steps between 2 check points */
  long int ia_nsteps;
    
  /* Last index used in IDAAfindIndex */
  long int ia_ilast;

  /* Storage for data from forward runs */
  struct DtpntMemRec **dt_mem;
    
  /* Actual number of data points saved in current dt_mem */
  /* Commonly, np = nsteps+1                              */
  long int ia_np;

  /* Interpolation type */
  int ia_interpType;


  /* Functions set by the interpolation module */
  IDAAStorePntFn ia_storePnt; /* store a new interpolation point */
  IDAAGetYFn     ia_getY;     /* interpolate forward solution    */
  IDAAMMallocFn  ia_malloc;   /* allocate new data point         */
  IDAAMFreeFn    ia_free;     /* destroys data point             */

  /* Flags controlling the interpolation module */
  booleantype ia_mallocDone;   /* IM initialized?                */
  booleantype ia_newData;      /* new data available in dt_mem?  */
  booleantype ia_storeSensi;   /* store sensitivities?           */
  booleantype ia_interpSensi;  /* interpolate sensitivities?     */

  booleantype ia_noInterp;     /* interpolations are temporarly  */
                               /* disabled ( IDACalcICB )        */

  /* Workspace for polynomial interpolation */
  N_Vector ia_Y[MXORDP1];      /* pointers  phi[i]               */
  N_Vector *ia_YS[MXORDP1];    /* pointers phiS[i]               */
  realtype ia_T[MXORDP1];

  /* Workspace for wrapper functions */
  N_Vector ia_yyTmp, ia_ypTmp;
  N_Vector *ia_yySTmp, *ia_ypSTmp;
    
};


/*
 * =================================================================
 *     I N T E R F A C E   T O    L I N E A R   S O L V E R S     
 * =================================================================
 */

/*
 * -----------------------------------------------------------------
 * int (*ida_linit)(IDAMem IDA_mem);                               
 * -----------------------------------------------------------------
 * The purpose of ida_linit is to allocate memory for the          
 * solver-specific fields in the structure *(idamem->ida_lmem) and 
 * perform any needed initializations of solver-specific memory,   
 * such as counters/statistics. An (*ida_linit) should return      
 * 0 if it has successfully initialized the IDA linear solver and 
 * a non-zero value otherwise. If an error does occur, an 
 * appropriate message should be issued.
 * ----------------------------------------------------------------
 */                                                                 

/*
 * -----------------------------------------------------------------
 * int (*ida_lsetup)(IDAMem IDA_mem, N_Vector yyp, N_Vector ypp,   
 *                  N_Vector resp, N_Vector tempv1, 
 *                  N_Vector tempv2, N_Vector tempv3);  
 * -----------------------------------------------------------------
 * The job of ida_lsetup is to prepare the linear solver for       
 * subsequent calls to ida_lsolve. Its parameters are as follows:  
 *                                                                 
 * idamem - problem memory pointer of type IDAMem. See the big     
 *          typedef earlier in this file.                          
 *                                                                 
 * yyp   - the predicted y vector for the current IDA internal     
 *         step.                                                   
 *                                                                 
 * ypp   - the predicted y' vector for the current IDA internal    
 *         step.                                                   
 *                                                                 
 * resp  - F(tn, yyp, ypp).                                        
 *                                                                 
 * tempv1, tempv2, tempv3 - temporary N_Vectors provided for use   
 *         by ida_lsetup.                                          
 *                                                                 
 * The ida_lsetup routine should return 0 if successful,
 * a positive value for a recoverable error, and a negative value 
 * for an unrecoverable error.
 * -----------------------------------------------------------------
 */                                                                 

/*
 * -----------------------------------------------------------------
 * int (*ida_lsolve)(IDAMem IDA_mem, N_Vector b, N_Vector weight,  
 *               N_Vector ycur, N_Vector ypcur, N_Vector rescur);  
 * -----------------------------------------------------------------
 * ida_lsolve must solve the linear equation P x = b, where        
 * P is some approximation to the system Jacobian                  
 *                  J = (dF/dy) + cj (dF/dy')                      
 * evaluated at (tn,ycur,ypcur) and the RHS vector b is input.     
 * The N-vector ycur contains the solver's current approximation   
 * to y(tn), ypcur contains that for y'(tn), and the vector rescur 
 * contains the N-vector residual F(tn,ycur,ypcur).                
 * The solution is to be returned in the vector b. 
 *                                                                 
 * The ida_lsolve routine should return 0 if successful,
 * a positive value for a recoverable error, and a negative value 
 * for an unrecoverable error.
 * -----------------------------------------------------------------
 */                                                                 

/*
 * -----------------------------------------------------------------
 * int (*ida_lperf)(IDAMem IDA_mem, int perftask);                 
 * -----------------------------------------------------------------
 * ida_lperf is called two places in IDAS where linear solver       
 * performance data is required by IDAS. For perftask = 0, an       
 * initialization of performance variables is performed, while for 
 * perftask = 1, the performance is evaluated.                     
 * -----------------------------------------------------------------
 */                                                                 

/*
 * -----------------------------------------------------------------
 * int (*ida_lfree)(IDAMem IDA_mem);                               
 * -----------------------------------------------------------------
 * ida_lfree should free up any memory allocated by the linear     
 * solver. This routine is called once a problem has been          
 * completed and the linear solver is no longer needed.  It should 
 * return 0 upon success, nonzero on failure.
 * -----------------------------------------------------------------
 */
  
/*
 * =================================================================
 *   I D A S    I N T E R N A L   F U N C T I O N S
 * =================================================================
 */

/* Prototype of internal ewtSet function */

int IDAEwtSet(N_Vector ycur, N_Vector weight, void *data);

/* High level error handler */

void IDAProcessError(IDAMem IDA_mem, 
		     int error_code, const char *module, const char *fname, 
		     const char *msgfmt, ...);

/* Prototype of internal errHandler function */

void IDAErrHandler(int error_code, const char *module, const char *function, 
		   char *msg, void *data);

/* Norm functions. Also used for IC, so they are global.*/

realtype IDAWrmsNorm(IDAMem IDA_mem, N_Vector x, N_Vector w,
                     booleantype mask);

realtype IDASensWrmsNorm(IDAMem IDA_mem, N_Vector *xS, N_Vector *wS,
                         booleantype mask);

realtype IDASensWrmsNormUpdate(IDAMem IDA_mem, realtype old_nrm,
                                      N_Vector *xS, N_Vector *wS,
                                      booleantype mask);

/* Nonlinear solver functions */
int idaNlsInit(IDAMem IDA_mem);
int idaNlsInitSensSim(IDAMem IDA_mem);
int idaNlsInitSensStg(IDAMem IDA_mem);

/* Prototype for internal sensitivity residual DQ function */

int IDASensResDQ(int Ns, realtype t, 
		 N_Vector yy, N_Vector yp, N_Vector resval,
		 N_Vector *yyS, N_Vector *ypS, N_Vector *resvalS,
		 void *user_dataS,
		 N_Vector ytemp, N_Vector yptemp, N_Vector restemp);

/*
 * =================================================================
 *    I D A S    E R R O R    M E S S A G E S
 * =================================================================
 */

#if defined(SUNDIALS_EXTENDED_PRECISION)

#define MSG_TIME "t = %Lg, "
#define MSG_TIME_H "t = %Lg and h = %Lg, "
#define MSG_TIME_INT "t = %Lg is not between tcur - hu = %Lg and tcur = %Lg."
#define MSG_TIME_TOUT "tout = %Lg"
#define MSG_TIME_TSTOP "tstop = %Lg"

#elif defined(SUNDIALS_DOUBLE_PRECISION)

#define MSG_TIME "t = %lg, "
#define MSG_TIME_H "t = %lg and h = %lg, "
#define MSG_TIME_INT "t = %lg is not between tcur - hu = %lg and tcur = %lg."
#define MSG_TIME_TOUT "tout = %lg"
#define MSG_TIME_TSTOP "tstop = %lg"

#else

#define MSG_TIME "t = %g, "
#define MSG_TIME_H "t = %g and h = %g, "
#define MSG_TIME_INT "t = %g is not between tcur - hu = %g and tcur = %g."
#define MSG_TIME_TOUT "tout = %g"
#define MSG_TIME_TSTOP "tstop = %g"

#endif

/* General errors */

#define MSG_MEM_FAIL       "A memory request failed."
#define MSG_NO_MEM         "ida_mem = NULL illegal."
#define MSG_NO_MALLOC      "Attempt to call before IDAMalloc."
#define MSG_BAD_NVECTOR    "A required vector operation is not implemented."

/* Initialization errors */

#define MSG_Y0_NULL        "y0 = NULL illegal."
#define MSG_YP0_NULL       "yp0 = NULL illegal."
#define MSG_BAD_ITOL       "Illegal value for itol. The legal values are IDA_SS, IDA_SV, and IDA_WF."
#define MSG_RES_NULL       "res = NULL illegal."
#define MSG_BAD_RTOL       "rtol < 0 illegal."
#define MSG_ATOL_NULL      "atol = NULL illegal."
#define MSG_BAD_ATOL       "Some atol component < 0.0 illegal."
#define MSG_ROOT_FUNC_NULL "g = NULL illegal."

#define MSG_MISSING_ID     "id = NULL but suppressalg option on."
#define MSG_NO_TOLS        "No integration tolerances have been specified."
#define MSG_FAIL_EWT       "The user-provide EwtSet function failed."
#define MSG_BAD_EWT        "Some initial ewt component = 0.0 illegal."
#define MSG_Y0_FAIL_CONSTR "y0 fails to satisfy constraints."
#define MSG_BAD_ISM_CONSTR "Constraints can not be enforced while forward sensitivity is used with simultaneous method."
#define MSG_LSOLVE_NULL    "The linear solver's solve routine is NULL."
#define MSG_LINIT_FAIL     "The linear solver's init routine failed."
#define MSG_NLS_INIT_FAIL  "The nonlinear solver's init routine failed."

#define MSG_NO_QUAD        "Illegal attempt to call before calling IDAQuadInit."
#define MSG_BAD_EWTQ       "Initial ewtQ has component(s) equal to zero (illegal)."
#define MSG_BAD_ITOLQ      "Illegal value for itolQ. The legal values are IDA_SS and IDA_SV."
#define MSG_NO_TOLQ        "No integration tolerances for quadrature variables have been specified."
#define MSG_NULL_ATOLQ     "atolQ = NULL illegal."
#define MSG_BAD_RTOLQ      "rtolQ < 0 illegal."
#define MSG_BAD_ATOLQ      "atolQ has negative component(s) (illegal)."  

#define MSG_NO_SENSI       "Illegal attempt to call before calling IDASensInit."
#define MSG_BAD_EWTS       "Initial ewtS has component(s) equal to zero (illegal)."
#define MSG_BAD_ITOLS      "Illegal value for itolS. The legal values are IDA_SS, IDA_SV, and IDA_EE."
#define MSG_NULL_ATOLS     "atolS = NULL illegal."
#define MSG_BAD_RTOLS      "rtolS < 0 illegal."
#define MSG_BAD_ATOLS      "atolS has negative component(s) (illegal)."  
#define MSG_BAD_PBAR       "pbar has zero component(s) (illegal)."
#define MSG_BAD_PLIST      "plist has negative component(s) (illegal)."
#define MSG_BAD_NS         "NS <= 0 illegal."
#define MSG_NULL_YYS0      "yyS0 = NULL illegal."
#define MSG_NULL_YPS0      "ypS0 = NULL illegal."
#define MSG_BAD_ISM        "Illegal value for ism. Legal values are: IDA_SIMULTANEOUS and IDA_STAGGERED."
#define MSG_BAD_IS         "Illegal value for is."
#define MSG_NULL_DKYA      "dkyA = NULL illegal."
#define MSG_BAD_DQTYPE     "Illegal value for DQtype. Legal values are: IDA_CENTERED and IDA_FORWARD."
#define MSG_BAD_DQRHO      "DQrhomax < 0 illegal."

#define MSG_NULL_ABSTOLQS  "abstolQS = NULL illegal parameter."
#define MSG_BAD_RELTOLQS   "reltolQS < 0 illegal parameter."
#define MSG_BAD_ABSTOLQS   "abstolQS has negative component(s) (illegal)."  
#define MSG_NO_QUADSENSI   "Forward sensitivity analysis for quadrature variables was not activated."
#define MSG_NULL_YQS0      "yQS0 = NULL illegal parameter."


/* IDACalcIC error messages */

#define MSG_IC_BAD_ICOPT   "icopt has an illegal value."
#define MSG_IC_BAD_MAXBACKS "maxbacks <= 0 illegal."
#define MSG_IC_MISSING_ID  "id = NULL conflicts with icopt."
#define MSG_IC_TOO_CLOSE   "tout1 too close to t0 to attempt initial condition calculation."
#define MSG_IC_BAD_ID      "id has illegal values."
#define MSG_IC_BAD_EWT     "Some initial ewt component = 0.0 illegal."
#define MSG_IC_RES_NONREC  "The residual function failed unrecoverably. "
#define MSG_IC_RES_FAIL    "The residual function failed at the first call. "
#define MSG_IC_SETUP_FAIL  "The linear solver setup failed unrecoverably."
#define MSG_IC_SOLVE_FAIL  "The linear solver solve failed unrecoverably."
#define MSG_IC_NO_RECOVERY "The residual routine or the linear setup or solve routine had a recoverable error, but IDACalcIC was unable to recover."
#define MSG_IC_FAIL_CONSTR "Unable to satisfy the inequality constraints."
#define MSG_IC_FAILED_LINS "The linesearch algorithm failed: step too small or too many backtracks."
#define MSG_IC_CONV_FAILED "Newton/Linesearch algorithm failed to converge."

/* IDASolve error messages */

#define MSG_YRET_NULL      "yret = NULL illegal."
#define MSG_YPRET_NULL     "ypret = NULL illegal."
#define MSG_TRET_NULL      "tret = NULL illegal."
#define MSG_BAD_ITASK      "itask has an illegal value."
#define MSG_TOO_CLOSE      "tout too close to t0 to start integration."
#define MSG_BAD_HINIT      "Initial step is not towards tout."
#define MSG_BAD_TSTOP      "The value " MSG_TIME_TSTOP " is behind current " MSG_TIME "in the direction of integration."
#define MSG_CLOSE_ROOTS    "Root found at and very near " MSG_TIME "."
#define MSG_MAX_STEPS      "At " MSG_TIME ", mxstep steps taken before reaching tout." 
#define MSG_EWT_NOW_FAIL   "At " MSG_TIME "the user-provide EwtSet function failed."
#define MSG_EWT_NOW_BAD    "At " MSG_TIME "some ewt component has become <= 0.0."
#define MSG_TOO_MUCH_ACC   "At " MSG_TIME "too much accuracy requested."

#define MSG_BAD_T          "Illegal value for t. " MSG_TIME_INT
#define MSG_BAD_TOUT       "Trouble interpolating at " MSG_TIME_TOUT ". tout too far back in direction of integration."

#define MSG_BAD_K        "Illegal value for k."
#define MSG_NULL_DKY     "dky = NULL illegal."
#define MSG_NULL_DKYP    "dkyp = NULL illegal."

#define MSG_ERR_FAILS      "At " MSG_TIME_H "the error test failed repeatedly or with |h| = hmin."
#define MSG_CONV_FAILS     "At " MSG_TIME_H "the corrector convergence failed repeatedly or with |h| = hmin."
#define MSG_SETUP_FAILED   "At " MSG_TIME "the linear solver setup failed unrecoverably."
#define MSG_SOLVE_FAILED   "At " MSG_TIME "the linear solver solve failed unrecoverably."
#define MSG_REP_RES_ERR    "At " MSG_TIME "repeated recoverable residual errors."
#define MSG_RES_NONRECOV   "At " MSG_TIME "the residual function failed unrecoverably."
#define MSG_FAILED_CONSTR  "At " MSG_TIME "unable to satisfy inequality constraints."
#define MSG_RTFUNC_FAILED  "At " MSG_TIME ", the rootfinding routine failed in an unrecoverable manner."
#define MSG_NO_ROOT        "Rootfinding was not initialized."
#define MSG_INACTIVE_ROOTS "At the end of the first step, there are still some root functions identically 0. This warning will not be issued again."
#define MSG_NLS_INPUT_NULL "At " MSG_TIME "the nonlinear solver was passed a NULL input."
#define MSG_NLS_SETUP_FAILED "At " MSG_TIME "the nonlinear solver setup failed unrecoverably."

#define MSG_EWTQ_NOW_BAD "At " MSG_TIME ", a component of ewtQ has become <= 0."
#define MSG_QRHSFUNC_FAILED "At " MSG_TIME ", the quadrature right-hand side routine failed in an unrecoverable manner."
#define MSG_QRHSFUNC_UNREC "At " MSG_TIME ", the quadrature right-hand side failed in a recoverable manner, but no recovery is possible."
#define MSG_QRHSFUNC_REPTD "At " MSG_TIME "repeated recoverable quadrature right-hand side function errors."
#define MSG_QRHSFUNC_FIRST "The quadrature right-hand side routine failed at the first call."

#define MSG_NULL_P "p = NULL when using internal DQ for sensitivity residual is illegal."
#define MSG_EWTS_NOW_BAD "At " MSG_TIME ", a component of ewtS has become <= 0."
#define MSG_SRHSFUNC_FAILED "At " MSG_TIME ", the sensitivity residual routine failed in an unrecoverable manner."
#define MSG_SRHSFUNC_UNREC "At " MSG_TIME ", the sensitivity residual failed in a recoverable manner, but no recovery is possible."
#define MSG_SRHSFUNC_REPTD "At " MSG_TIME "repeated recoverable sensitivity residual function errors."

#define MSG_NO_TOLQS  "No integration tolerances for quadrature sensitivity variables have been specified."
#define MSG_NULL_RHSQ "IDAS is expected to use DQ to evaluate the RHS of quad. sensi., but quadratures were not initialized."
#define MSG_BAD_EWTQS "Initial ewtQS has component(s) equal to zero (illegal)."
#define MSG_EWTQS_NOW_BAD "At " MSG_TIME ", a component of ewtQS has become <= 0."
#define MSG_QSRHSFUNC_FAILED "At " MSG_TIME ", the sensitivity quadrature right-hand side routine failed in an unrecoverable manner."
#define MSG_QSRHSFUNC_FIRST "The quadrature right-hand side routine failed at the first call."

/* IDASet* / IDAGet* error messages */
#define MSG_NEG_MAXORD     "maxord<=0 illegal."
#define MSG_BAD_MAXORD     "Illegal attempt to increase maximum order."
#define MSG_NEG_HMAX       "hmax < 0 illegal."
#define MSG_NEG_EPCON      "epcon <= 0.0 illegal."
#define MSG_BAD_CONSTR     "Illegal values in constraints vector."
#define MSG_BAD_EPICCON    "epiccon <= 0.0 illegal."
#define MSG_BAD_MAXNH      "maxnh <= 0 illegal."
#define MSG_BAD_MAXNJ      "maxnj <= 0 illegal."
#define MSG_BAD_MAXNIT     "maxnit <= 0 illegal."
#define MSG_BAD_STEPTOL    "steptol <= 0.0 illegal."

#define MSG_TOO_LATE       "IDAGetConsistentIC can only be called before IDASolve."

/*
 * =================================================================
 *    I D A A    E R R O R    M E S S A G E S
 * =================================================================
 */

#define MSGAM_NULL_IDAMEM  "ida_mem = NULL illegal."
#define MSGAM_NO_ADJ       "Illegal attempt to call before calling IDAadjInit."
#define MSGAM_BAD_INTERP   "Illegal value for interp."
#define MSGAM_BAD_STEPS    "Steps nonpositive illegal."
#define MSGAM_BAD_WHICH    "Illegal value for which."
#define MSGAM_NO_BCK       "No backward problems have been defined yet."
#define MSGAM_NO_FWD       "Illegal attempt to call before calling IDASolveF."
#define MSGAM_BAD_TB0      "The initial time tB0 is outside the interval over which the forward problem was solved."
#define MSGAM_BAD_SENSI    "At least one backward problem requires sensitivities, but they were not stored for interpolation."
#define MSGAM_BAD_ITASKB   "Illegal value for itaskB. Legal values are IDA_NORMAL and IDA_ONE_STEP."
#define MSGAM_BAD_TBOUT    "The final time tBout is outside the interval over which the forward problem was solved."
#define MSGAM_BACK_ERROR   "Error occured while integrating backward problem # %d" 
#define MSGAM_BAD_TINTERP  "Bad t = %g for interpolation."
#define MSGAM_BAD_T        "Bad t for interpolation."
#define MSGAM_WRONG_INTERP "This function cannot be called for the specified interp type."
#define MSGAM_MEM_FAIL     "A memory request failed."
#define MSGAM_NO_INITBS    "Illegal attempt to call before calling IDAInitBS."

#ifdef __cplusplus
}
#endif

#endif
