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
 * This is the implementation file for the IDAA adjoint integrator.
 * -----------------------------------------------------------------
 */

/*=================================================================*/
/*                  Import Header Files                            */
/*=================================================================*/

#include <stdio.h>
#include <stdlib.h>

#include "idas_impl.h"
#include <sundials/sundials_math.h>

/*=================================================================*/
/*                 IDAA Private Constants                          */
/*=================================================================*/

#define ZERO        RCONST(0.0)    /* real   0.0 */
#define ONE         RCONST(1.0)    /* real   1.0 */
#define TWO         RCONST(2.0)    /* real   2.0 */
#define HUNDRED     RCONST(100.0)  /* real 100.0 */
#define FUZZ_FACTOR RCONST(1000000.0)  /* fuzz factor for IDAAgetY */


/*=================================================================*/
/*               Private Functions Prototypes                      */
/*=================================================================*/

static CkpntMem IDAAckpntInit(IDAMem IDA_mem);
static CkpntMem IDAAckpntNew(IDAMem IDA_mem);
static void IDAAckpntCopyVectors(IDAMem IDA_mem, CkpntMem ck_mem);
static booleantype IDAAckpntAllocVectors(IDAMem IDA_mem, CkpntMem ck_mem);
static void IDAAckpntDelete(CkpntMem *ck_memPtr);

static void IDAAbckpbDelete(IDABMem *IDAB_memPtr);

static booleantype IDAAdataMalloc(IDAMem IDA_mem);
static void IDAAdataFree(IDAMem IDA_mem);
static int  IDAAdataStore(IDAMem IDA_mem, CkpntMem ck_mem);

static int  IDAAckpntGet(IDAMem IDA_mem, CkpntMem ck_mem); 

static booleantype IDAAhermiteMalloc(IDAMem IDA_mem);
static void        IDAAhermiteFree(IDAMem IDA_mem);
static int         IDAAhermiteStorePnt(IDAMem IDA_mem, DtpntMem d);
static int         IDAAhermiteGetY(IDAMem IDA_mem, realtype t, 
                                   N_Vector yy, N_Vector yp,
                                   N_Vector *yyS, N_Vector *ypS);

static booleantype IDAApolynomialMalloc(IDAMem IDA_mem);
static void        IDAApolynomialFree(IDAMem IDA_mem);
static int         IDAApolynomialStorePnt(IDAMem IDA_mem, DtpntMem d);
static int         IDAApolynomialGetY(IDAMem IDA_mem, realtype t, 
                                      N_Vector yy, N_Vector yp,
                                      N_Vector *yyS, N_Vector *ypS);

static int IDAAfindIndex(IDAMem ida_mem, realtype t, 
                         long int *indx, booleantype *newpoint);                         

static int IDAAres(realtype tt, 
                   N_Vector yyB, N_Vector ypB, 
                   N_Vector resvalB,  void *ida_mem);

static int IDAArhsQ(realtype tt, 
                     N_Vector yyB, N_Vector ypB,
                     N_Vector rrQB, void *ida_mem);

static int IDAAGettnSolutionYp(IDAMem IDA_mem, N_Vector yp);
static int IDAAGettnSolutionYpS(IDAMem IDA_mem, N_Vector *ypS);

extern int IDAGetSolution(void *ida_mem, realtype t, N_Vector yret, N_Vector ypret);


/*=================================================================*/
/*                  Exported Functions                             */
/*=================================================================*/

/*
 * IDAAdjInit 
 *
 * This routine allocates space for the global IDAA memory
 * structure.
 */


int IDAAdjInit(void *ida_mem, long int steps, int interp)
{
  IDAadjMem IDAADJ_mem;
  IDAMem IDA_mem;

  /* Check arguments */

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAAdjInit", MSGAM_NULL_IDAMEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem)ida_mem;

  if (steps <= 0) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAAdjInit", MSGAM_BAD_STEPS);
    return(IDA_ILL_INPUT);
  }

  if ( (interp != IDA_HERMITE) && (interp != IDA_POLYNOMIAL) ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAAdjInit", MSGAM_BAD_INTERP);
    return(IDA_ILL_INPUT);
  } 

  /* Allocate memory block for IDAadjMem. */
  IDAADJ_mem = (IDAadjMem) malloc(sizeof(struct IDAadjMemRec));
  if (IDAADJ_mem == NULL) {
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAA", "IDAAdjInit", MSGAM_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  /* Attach IDAS memory for forward runs */
  IDA_mem->ida_adj_mem = IDAADJ_mem;

  /* Initialization of check points. */
  IDAADJ_mem->ck_mem = NULL;
  IDAADJ_mem->ia_nckpnts = 0;
  IDAADJ_mem->ia_ckpntData = NULL;


  /* Initialization of interpolation data. */
  IDAADJ_mem->ia_interpType = interp;
  IDAADJ_mem->ia_nsteps = steps;

  /* Last index used in IDAAfindIndex, initailize to invalid value */
  IDAADJ_mem->ia_ilast = -1;

  /* Allocate space for the array of Data Point structures. */
  if (IDAAdataMalloc(IDA_mem) == SUNFALSE) {
    free(IDAADJ_mem); IDAADJ_mem = NULL;
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAA", "IDAAdjInit", MSGAM_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  /* Attach functions for the appropriate interpolation module */
  switch(interp) {

  case IDA_HERMITE:
    IDAADJ_mem->ia_malloc    = IDAAhermiteMalloc;
    IDAADJ_mem->ia_free      = IDAAhermiteFree;
    IDAADJ_mem->ia_getY      = IDAAhermiteGetY;
    IDAADJ_mem->ia_storePnt  = IDAAhermiteStorePnt;
    break;
    
    case IDA_POLYNOMIAL:
    
    IDAADJ_mem->ia_malloc    = IDAApolynomialMalloc;
    IDAADJ_mem->ia_free      = IDAApolynomialFree;
    IDAADJ_mem->ia_getY      = IDAApolynomialGetY;
    IDAADJ_mem->ia_storePnt  = IDAApolynomialStorePnt;
    break;
  }

 /* The interpolation module has not been initialized yet */
  IDAADJ_mem->ia_mallocDone = SUNFALSE;

  /* By default we will store but not interpolate sensitivities
   *  - storeSensi will be set in IDASolveF to SUNFALSE if FSA is not enabled
   *    or if the user forced this through IDAAdjSetNoSensi 
   *  - interpSensi will be set in IDASolveB to SUNTRUE if storeSensi is SUNTRUE 
   *    and if at least one backward problem requires sensitivities 
   *  - noInterp will be set in IDACalcICB to SUNTRUE before the call to
   *    IDACalcIC and SUNFALSE after.*/

  IDAADJ_mem->ia_storeSensi  = SUNTRUE;
  IDAADJ_mem->ia_interpSensi = SUNFALSE;
  IDAADJ_mem->ia_noInterp    = SUNFALSE;

  /* Initialize backward problems. */
  IDAADJ_mem->IDAB_mem = NULL;
  IDAADJ_mem->ia_bckpbCrt = NULL;
  IDAADJ_mem->ia_nbckpbs = 0;

  /* Flags for tracking the first calls to IDASolveF and IDASolveF. */
  IDAADJ_mem->ia_firstIDAFcall = SUNTRUE;
  IDAADJ_mem->ia_tstopIDAFcall = SUNFALSE;
  IDAADJ_mem->ia_firstIDABcall = SUNTRUE;

  /* Adjoint module initialized and allocated. */
  IDA_mem->ida_adj = SUNTRUE;
  IDA_mem->ida_adjMallocDone = SUNTRUE;

  return(IDA_SUCCESS);
} 

/*
 * IDAAdjReInit
 *
 * IDAAdjReInit reinitializes the IDAS memory structure for ASA
 */

int IDAAdjReInit(void *ida_mem)
{
  IDAadjMem IDAADJ_mem;
  IDAMem IDA_mem;

  /* Check arguments */

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAAdjReInit", MSGAM_NULL_IDAMEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem)ida_mem;

  /* Was ASA previously initialized? */
  if(IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAAdjReInit",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }

  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Free all stored  checkpoints. */
  while (IDAADJ_mem->ck_mem != NULL) 
      IDAAckpntDelete(&(IDAADJ_mem->ck_mem));

  IDAADJ_mem->ck_mem = NULL;
  IDAADJ_mem->ia_nckpnts = 0;
  IDAADJ_mem->ia_ckpntData = NULL;

  /* Flags for tracking the first calls to IDASolveF and IDASolveF. */
  IDAADJ_mem->ia_firstIDAFcall = SUNTRUE;
  IDAADJ_mem->ia_tstopIDAFcall = SUNFALSE;
  IDAADJ_mem->ia_firstIDABcall = SUNTRUE;

  return(IDA_SUCCESS);
} 

/*
 * IDAAdjFree
 *
 * IDAAdjFree routine frees the memory allocated by IDAAdjInit.
*/


void IDAAdjFree(void *ida_mem)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;

  if (ida_mem == NULL) return;
  IDA_mem = (IDAMem) ida_mem;

  if(IDA_mem->ida_adjMallocDone) {

    /* Data for adjoint. */
    IDAADJ_mem = IDA_mem->ida_adj_mem;
    
    /* Delete check points one by one */
    while (IDAADJ_mem->ck_mem != NULL) {
      IDAAckpntDelete(&(IDAADJ_mem->ck_mem));
    }

    IDAAdataFree(IDA_mem);

    /* Free all backward problems. */
    while (IDAADJ_mem->IDAB_mem != NULL)
      IDAAbckpbDelete( &(IDAADJ_mem->IDAB_mem) );

    /* Free IDAA memory. */
    free(IDAADJ_mem);

    IDA_mem->ida_adj_mem = NULL;
  }
}

/* 
 * =================================================================
 * PRIVATE FUNCTIONS FOR BACKWARD PROBLEMS
 * =================================================================
 */

static void IDAAbckpbDelete(IDABMem *IDAB_memPtr)
{
  IDABMem IDAB_mem = (*IDAB_memPtr);
  void * ida_mem;

  if (IDAB_mem == NULL) return;

  /* Move head to the next element in list. */
  *IDAB_memPtr = IDAB_mem->ida_next;

  /* IDAB_mem is going to be deallocated. */

  /* Free IDAS memory for this backward problem. */
  ida_mem = (void *)IDAB_mem->IDA_mem;
  IDAFree(&ida_mem);

  /* Free linear solver memory. */
  if (IDAB_mem->ida_lfree != NULL) IDAB_mem->ida_lfree(IDAB_mem);

  /* Free preconditioner memory. */
  if (IDAB_mem->ida_pfree != NULL) IDAB_mem->ida_pfree(IDAB_mem);

  /* Free any workspace vectors. */
  N_VDestroy(IDAB_mem->ida_yy);
  N_VDestroy(IDAB_mem->ida_yp);

  /* Free the node itself. */
  free(IDAB_mem);
  IDAB_mem = NULL;
}

/*=================================================================*/
/*                    Wrappers for IDAA                            */
/*=================================================================*/

/*
 *                      IDASolveF 
 *
 * This routine integrates to tout and returns solution into yout.
 * In the same time, it stores check point data every 'steps' steps. 
 *  
 * IDASolveF can be called repeatedly by the user. The last tout
 *  will be used as the starting time for the backward integration.
 * 
 *  ncheckPtr points to the number of check points stored so far.
*/

int IDASolveF(void *ida_mem, realtype tout, realtype *tret,
              N_Vector yret, N_Vector ypret, int itask, int *ncheckPtr)
{
  IDAadjMem IDAADJ_mem;
  IDAMem IDA_mem;
  CkpntMem tmp;
  DtpntMem *dt_mem;
  int flag, i;
  booleantype /* iret, */ allocOK;

  /* Is the mem OK? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDASolveF", MSGAM_NULL_IDAMEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized ? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDASolveF",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check for yret != NULL */
  if (yret == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDASolveF", MSG_YRET_NULL);
    return(IDA_ILL_INPUT);
  }
  
  /* Check for ypret != NULL */
  if (ypret == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDASolveF", MSG_YPRET_NULL);
    return(IDA_ILL_INPUT);
  }
  /* Check for tret != NULL */
  if (tret == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDASolveF", MSG_TRET_NULL);
    return(IDA_ILL_INPUT);
  }
  
  /* Check for valid itask */
  if ( (itask != IDA_NORMAL) && (itask != IDA_ONE_STEP) ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDASolveF", MSG_BAD_ITASK);
    return(IDA_ILL_INPUT);
  }
  
  /* All memory checks done, proceed ... */
  
  dt_mem = IDAADJ_mem->dt_mem;

  /* If tstop is enabled, store some info */
  if (IDA_mem->ida_tstopset) {
    IDAADJ_mem->ia_tstopIDAFcall = SUNTRUE;
    IDAADJ_mem->ia_tstopIDAF = IDA_mem->ida_tstop;
  }
  
  /* We will call IDASolve in IDA_ONE_STEP mode, regardless
     of what itask is, so flag if we need to return */
/*   if (itask == IDA_ONE_STEP) iret = SUNTRUE;
 *   else                       iret = SUNFALSE;
 */

  /* On the first step:
   *   - set tinitial
   *   - initialize list of check points
   *   - if needed, initialize the interpolation module
   *   - load dt_mem[0]
   * On subsequent steps, test if taking a new step is necessary. 
   */
  if ( IDAADJ_mem->ia_firstIDAFcall ) {
    
    IDAADJ_mem->ia_tinitial = IDA_mem->ida_tn;
    IDAADJ_mem->ck_mem = IDAAckpntInit(IDA_mem);
    if (IDAADJ_mem->ck_mem == NULL) {
      IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAA", "IDASolveF", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    if (!IDAADJ_mem->ia_mallocDone) {
      /* Do we need to store sensitivities? */
      if (!IDA_mem->ida_sensi) IDAADJ_mem->ia_storeSensi = SUNFALSE;

      /* Allocate space for interpolation data */
      allocOK = IDAADJ_mem->ia_malloc(IDA_mem);
      if (!allocOK) {
        IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAA", "IDASolveF", MSG_MEM_FAIL);
        return(IDA_MEM_FAIL);
      }

      /* Rename phi and, if needed, phiS for use in interpolation */
      for (i=0;i<MXORDP1;i++) IDAADJ_mem->ia_Y[i] = IDA_mem->ida_phi[i];
      if (IDAADJ_mem->ia_storeSensi) {
        for (i=0;i<MXORDP1;i++)
          IDAADJ_mem->ia_YS[i] = IDA_mem->ida_phiS[i];
      }

      IDAADJ_mem->ia_mallocDone = SUNTRUE;
    }

    dt_mem[0]->t = IDAADJ_mem->ck_mem->ck_t0;
    IDAADJ_mem->ia_storePnt(IDA_mem, dt_mem[0]);

    IDAADJ_mem->ia_firstIDAFcall = SUNFALSE;

  } else if ( (IDA_mem->ida_tn-tout)*IDA_mem->ida_hh >= ZERO ) {

    /* If tout was passed, return interpolated solution. 
       No changes to ck_mem or dt_mem are needed. */
    *tret = tout;
    flag = IDAGetSolution(IDA_mem, tout, yret, ypret);
    *ncheckPtr = IDAADJ_mem->ia_nckpnts;
    IDAADJ_mem->ia_newData = SUNTRUE;
    IDAADJ_mem->ia_ckpntData = IDAADJ_mem->ck_mem;
    IDAADJ_mem->ia_np = IDA_mem->ida_nst % IDAADJ_mem->ia_nsteps + 1;

    return(flag);
  }
  /* Integrate to tout while loading check points */
  for(;;) {

    /* Perform one step of the integration */

    flag = IDASolve(IDA_mem, tout, tret, yret, ypret, IDA_ONE_STEP);

    if (flag < 0) break;

    /* Test if a new check point is needed */

    if ( IDA_mem->ida_nst % IDAADJ_mem->ia_nsteps == 0 ) {

      IDAADJ_mem->ck_mem->ck_t1 = *tret;

      /* Create a new check point, load it, and append it to the list */
      tmp = IDAAckpntNew(IDA_mem);
      if (tmp == NULL) {
        flag = IDA_MEM_FAIL;
        break;
      }

      tmp->ck_next = IDAADJ_mem->ck_mem;
      IDAADJ_mem->ck_mem = tmp;
      IDAADJ_mem->ia_nckpnts++;
      
      IDA_mem->ida_forceSetup = SUNTRUE;
      
      /* Reset i=0 and load dt_mem[0] */
      dt_mem[0]->t = IDAADJ_mem->ck_mem->ck_t0;
      IDAADJ_mem->ia_storePnt(IDA_mem, dt_mem[0]);

    } else {
      
      /* Load next point in dt_mem */
      dt_mem[IDA_mem->ida_nst%IDAADJ_mem->ia_nsteps]->t = *tret;
      IDAADJ_mem->ia_storePnt(IDA_mem, dt_mem[IDA_mem->ida_nst % IDAADJ_mem->ia_nsteps]);
    }

    /* Set t1 field of the current ckeck point structure
       for the case in which there will be no future
       check points */
    IDAADJ_mem->ck_mem->ck_t1 = *tret;

    /* tfinal is now set to *t */
    IDAADJ_mem->ia_tfinal = *tret;

    /* In IDA_ONE_STEP mode break from loop */
    if (itask == IDA_ONE_STEP) break;

    /* Return if tout reached */
    if ( (*tret - tout)*IDA_mem->ida_hh >= ZERO ) {
      *tret = tout;
      IDAGetSolution(IDA_mem, tout, yret, ypret);
      /* Reset tretlast in IDA_mem so that IDAGetQuad and IDAGetSens 
       * evaluate quadratures and/or sensitivities at the proper time */
      IDA_mem->ida_tretlast = tout;
      break;
    }    
  }

  /* Get ncheck from IDAADJ_mem */ 
  *ncheckPtr = IDAADJ_mem->ia_nckpnts;

  /* Data is available for the last interval */
  IDAADJ_mem->ia_newData = SUNTRUE;
  IDAADJ_mem->ia_ckpntData = IDAADJ_mem->ck_mem;
  IDAADJ_mem->ia_np = IDA_mem->ida_nst % IDAADJ_mem->ia_nsteps + 1;

  return(flag);
}




/* 
 * =================================================================
 * FUNCTIONS FOR BACKWARD PROBLEMS
 * =================================================================
 */

int IDACreateB(void *ida_mem, int *which)
{
  IDAMem IDA_mem;
  void* ida_memB;
  IDABMem new_IDAB_mem;
  IDAadjMem IDAADJ_mem;
  
  /* Is the mem OK? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDACreateB", MSGAM_NULL_IDAMEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized ? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDACreateB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Allocate a new IDABMem struct. */
  new_IDAB_mem = (IDABMem) malloc( sizeof( struct IDABMemRec ) );
  if (new_IDAB_mem == NULL) {
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAA", "IDACreateB",  MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }
  
  /* Allocate the IDAMem struct needed by this backward problem. */
  ida_memB = IDACreate();
  if (ida_memB == NULL) {
    IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAA", "IDACreateB",  MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  /* Save ida_mem in ida_memB as user data. */
  IDASetUserData(ida_memB, ida_mem);
  
  /* Set same error output and handler for ida_memB. */
  IDASetErrHandlerFn(ida_memB, IDA_mem->ida_ehfun, IDA_mem->ida_eh_data);
  IDASetErrFile(ida_memB, IDA_mem->ida_errfp);

  /* Initialize fields in the IDABMem struct. */
  new_IDAB_mem->ida_index   = IDAADJ_mem->ia_nbckpbs;
  new_IDAB_mem->IDA_mem     = (IDAMem) ida_memB;

  new_IDAB_mem->ida_res      = NULL;
  new_IDAB_mem->ida_resS     = NULL;
  new_IDAB_mem->ida_rhsQ     = NULL;
  new_IDAB_mem->ida_rhsQS    = NULL;


  new_IDAB_mem->ida_user_data = NULL;

  new_IDAB_mem->ida_lmem     = NULL;
  new_IDAB_mem->ida_lfree    = NULL;
  new_IDAB_mem->ida_pmem     = NULL;
  new_IDAB_mem->ida_pfree    = NULL;

  new_IDAB_mem->ida_yy       = NULL;
  new_IDAB_mem->ida_yp       = NULL;

  new_IDAB_mem->ida_res_withSensi = SUNFALSE;
  new_IDAB_mem->ida_rhsQ_withSensi = SUNFALSE;
  
  /* Attach the new object to the beginning of the linked list IDAADJ_mem->IDAB_mem. */
  new_IDAB_mem->ida_next = IDAADJ_mem->IDAB_mem;
  IDAADJ_mem->IDAB_mem = new_IDAB_mem;

  /* Return the assigned index. This id is used as identificator and has to be passed 
     to IDAInitB and other ***B functions that set the optional inputs for  this 
     backward problem. */
  *which = IDAADJ_mem->ia_nbckpbs;

  /*Increase the counter of the backward problems stored. */
  IDAADJ_mem->ia_nbckpbs++;

  return(IDA_SUCCESS);

}

int IDAInitB(void *ida_mem, int which, IDAResFnB resB,
             realtype tB0, N_Vector yyB0, N_Vector ypB0)
{
  IDAadjMem IDAADJ_mem;
  IDAMem IDA_mem;
  IDABMem IDAB_mem;
  void * ida_memB;
  int flag;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAInitB", MSGAM_NULL_IDAMEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized ? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAInitB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the initial time for this backward problem against the adjoint data. */
  if ( (tB0 < IDAADJ_mem->ia_tinitial) || (tB0 > IDAADJ_mem->ia_tfinal) ) {
    IDAProcessError(IDA_mem, IDA_BAD_TB0, "IDAA", "IDAInitB", MSGAM_BAD_TB0);
    return(IDA_BAD_TB0);
  }

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAInitB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }

  /* Get the IDAMem corresponding to this backward problem. */
  ida_memB = (void*) IDAB_mem->IDA_mem;

  /* Call the IDAInit for this backward problem. */
  flag = IDAInit(ida_memB, IDAAres, tB0, yyB0, ypB0);
  if (IDA_SUCCESS != flag) return(flag);

  /* Copy residual function in IDAB_mem. */
  IDAB_mem->ida_res = resB;
  IDAB_mem->ida_res_withSensi = SUNFALSE;

  /* Initialized the initial time field. */
  IDAB_mem->ida_t0 = tB0;

  /* Allocate and initialize space workspace vectors. */
  IDAB_mem->ida_yy = N_VClone(yyB0);
  IDAB_mem->ida_yp = N_VClone(yyB0);
  N_VScale(ONE, yyB0, IDAB_mem->ida_yy);
  N_VScale(ONE, ypB0, IDAB_mem->ida_yp);

  return(flag);

}

int IDAInitBS(void *ida_mem, int which, IDAResFnBS resS,
                realtype tB0, N_Vector yyB0, N_Vector ypB0)
{
  IDAadjMem IDAADJ_mem;
  IDAMem IDA_mem;
  IDABMem IDAB_mem;
  void * ida_memB;
  int flag;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAInitBS", MSGAM_NULL_IDAMEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized ? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAInitBS",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the initial time for this backward problem against the adjoint data. */
  if ( (tB0 < IDAADJ_mem->ia_tinitial) || (tB0 > IDAADJ_mem->ia_tfinal) ) {
    IDAProcessError(IDA_mem, IDA_BAD_TB0, "IDAA", "IDAInitBS", MSGAM_BAD_TB0);
    return(IDA_BAD_TB0);
  }

  /* Were sensitivities active during the forward integration? */
  if (!IDAADJ_mem->ia_storeSensi) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAInitBS", MSGAM_BAD_SENSI);
    return(IDA_ILL_INPUT);
  }

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAInitBS", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }

  /* Get the IDAMem corresponding to this backward problem. */
  ida_memB = (void*) IDAB_mem->IDA_mem;
  
  /* Allocate and set the IDAS object */
  flag = IDAInit(ida_memB, IDAAres, tB0, yyB0, ypB0);

  if (flag != IDA_SUCCESS) return(flag);

  /* Copy residual function pointer in IDAB_mem. */
  IDAB_mem->ida_res_withSensi = SUNTRUE;
  IDAB_mem->ida_resS = resS;

  /* Allocate space and initialize the yy and yp vectors. */
  IDAB_mem->ida_t0 = tB0;
  IDAB_mem->ida_yy = N_VClone(yyB0);
  IDAB_mem->ida_yp = N_VClone(ypB0);
  N_VScale(ONE, yyB0, IDAB_mem->ida_yy);
  N_VScale(ONE, ypB0, IDAB_mem->ida_yp);

  return(IDA_SUCCESS);
}


int IDAReInitB(void *ida_mem, int which,
               realtype tB0, N_Vector yyB0, N_Vector ypB0)
{

  IDAadjMem IDAADJ_mem;
  IDAMem IDA_mem;
  IDABMem IDAB_mem;
  void * ida_memB;
  int flag;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAReInitB", MSGAM_NULL_IDAMEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized ? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAReInitB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the initial time for this backward problem against the adjoint data. */
  if ( (tB0 < IDAADJ_mem->ia_tinitial) || (tB0 > IDAADJ_mem->ia_tfinal) ) {
    IDAProcessError(IDA_mem, IDA_BAD_TB0, "IDAA", "IDAReInitB", MSGAM_BAD_TB0);
    return(IDA_BAD_TB0);
  }

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAReInitB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }

  /* Get the IDAMem corresponding to this backward problem. */
  ida_memB = (void*) IDAB_mem->IDA_mem;


  /* Call the IDAReInit for this backward problem. */
  flag = IDAReInit(ida_memB, tB0, yyB0, ypB0);
  return(flag);
}

int IDASStolerancesB(void *ida_mem, int which, 
                     realtype relTolB, realtype absTolB)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  void *ida_memB;
  
  /* Is ida_mem valid? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDASStolerancesB", MSGAM_NULL_IDAMEM);
    return IDA_MEM_NULL;
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDASStolerancesB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDASStolerancesB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }

  /* Get the IDAMem corresponding to this backward problem. */
  ida_memB = (void*) IDAB_mem->IDA_mem;

  /* Set tolerances and return. */
  return IDASStolerances(ida_memB, relTolB, absTolB);
  
}
int IDASVtolerancesB(void *ida_mem, int which, 
                     realtype relTolB, N_Vector absTolB)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  void *ida_memB;
  
  /* Is ida_mem valid? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDASVtolerancesB", MSGAM_NULL_IDAMEM);
    return IDA_MEM_NULL;
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDASVtolerancesB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDASVtolerancesB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }

  /* Get the IDAMem corresponding to this backward problem. */
  ida_memB = (void*) IDAB_mem->IDA_mem;

  /* Set tolerances and return. */
  return IDASVtolerances(ida_memB, relTolB, absTolB);
}

int IDAQuadSStolerancesB(void *ida_mem, int which,
                         realtype reltolQB, realtype abstolQB)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  void *ida_memB;
  
  /* Is ida_mem valid? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAQuadSStolerancesB", MSGAM_NULL_IDAMEM);
    return IDA_MEM_NULL;
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAQuadSStolerancesB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAQuadSStolerancesB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }
  ida_memB = (void *) IDAB_mem->IDA_mem;
  
  return IDAQuadSStolerances(ida_memB, reltolQB, abstolQB);
}


int IDAQuadSVtolerancesB(void *ida_mem, int which,
                         realtype reltolQB, N_Vector abstolQB)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  void *ida_memB;
  
  /* Is ida_mem valid? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAQuadSVtolerancesB", MSGAM_NULL_IDAMEM);
    return IDA_MEM_NULL;
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAQuadSVtolerancesB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAQuadSVtolerancesB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }
  ida_memB = (void *) IDAB_mem->IDA_mem;
  
  return IDAQuadSVtolerances(ida_memB, reltolQB, abstolQB);
}


int IDAQuadInitB(void *ida_mem, int which, IDAQuadRhsFnB rhsQB, N_Vector yQB0)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  void *ida_memB;
  int flag;
  
  /* Is ida_mem valid? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAQuadInitB", MSGAM_NULL_IDAMEM);
    return IDA_MEM_NULL;
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAQuadInitB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAQuadInitB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }
  ida_memB = (void *) IDAB_mem->IDA_mem;

  flag = IDAQuadInit(ida_memB, IDAArhsQ, yQB0);
  if (IDA_SUCCESS != flag) return flag;

  IDAB_mem->ida_rhsQ_withSensi = SUNFALSE;
  IDAB_mem->ida_rhsQ = rhsQB;

  return(flag);
}


int IDAQuadInitBS(void *ida_mem, int which, 
                  IDAQuadRhsFnBS rhsQS, N_Vector yQB0)
{
  IDAadjMem IDAADJ_mem;
  IDAMem IDA_mem;
  IDABMem IDAB_mem;
  void * ida_memB;
  int flag;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAQuadInitBS", MSGAM_NULL_IDAMEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized ? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAQuadInitBS",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAQuadInitBS", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }

  /* Get the IDAMem corresponding to this backward problem. */
  ida_memB = (void*) IDAB_mem->IDA_mem;
  
  /* Allocate and set the IDAS object */
  flag = IDAQuadInit(ida_memB, IDAArhsQ, yQB0);

  if (flag != IDA_SUCCESS) return(flag);

  /* Copy RHS function pointer in IDAB_mem and enable quad sensitivities. */
  IDAB_mem->ida_rhsQ_withSensi = SUNTRUE;
  IDAB_mem->ida_rhsQS = rhsQS;

  return(IDA_SUCCESS);
}


int IDAQuadReInitB(void *ida_mem, int which, N_Vector yQB0)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  
  /* Is ida_mem valid? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAQuadInitB", MSGAM_NULL_IDAMEM);
    return IDA_MEM_NULL;
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAQuadInitB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAQuadInitB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }

  return IDAQuadReInit(ida_mem, yQB0);
}


/*
 * ----------------------------------------------------------------
 * Function : IDACalcICB                                         
 * ----------------------------------------------------------------
 * IDACalcIC calculates corrected initial conditions for a DAE  
 * backward system (index-one in semi-implicit form).
 * It uses Newton iteration combined with a Linesearch algorithm. 
 * Calling IDACalcICB is optional. It is only necessary when the   
 * initial conditions do not solve the given system.  I.e., if    
 * yB0 and ypB0 are known to satisfy the backward problem, then       
 * a call to IDACalcIC is NOT necessary (for index-one problems). 
*/

int IDACalcICB(void *ida_mem, int which, realtype tout1, 
               N_Vector yy0, N_Vector yp0)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  void *ida_memB;
  int flag;
  
  /* Is ida_mem valid? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDACalcICB", MSGAM_NULL_IDAMEM);
    return IDA_MEM_NULL;
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDACalcICB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDACalcICB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }
  ida_memB = (void *) IDAB_mem->IDA_mem;

  /* The wrapper for user supplied res function requires ia_bckpbCrt from
     IDAAdjMem to be set to curent problem. */
  IDAADJ_mem->ia_bckpbCrt = IDAB_mem;

  /* Save (y, y') in yyTmp and ypTmp for use in the res wrapper.*/
  /* yyTmp and ypTmp workspaces are safe to use if IDAADataStore is not called.*/
  N_VScale(ONE, yy0, IDAADJ_mem->ia_yyTmp);
  N_VScale(ONE, yp0, IDAADJ_mem->ia_ypTmp);
  
  /* Set noInterp flag to SUNTRUE, so IDAARes will use user provided values for
     y and y' and will not call the interpolation routine(s). */
  IDAADJ_mem->ia_noInterp = SUNTRUE;
  
  flag = IDACalcIC(ida_memB, IDA_YA_YDP_INIT, tout1);

  /* Set interpolation on in IDAARes. */
  IDAADJ_mem->ia_noInterp = SUNFALSE;

  return(flag);
}

/*
 * ----------------------------------------------------------------
 * Function : IDACalcICBS                                        
 * ----------------------------------------------------------------
 * IDACalcIC calculates corrected initial conditions for a DAE  
 * backward system (index-one in semi-implicit form) that also 
 * dependes on the sensivities.
 *
 * It calls IDACalcIC for the 'which' backward problem.
*/

int IDACalcICBS(void *ida_mem, int which, realtype tout1, 
               N_Vector yy0, N_Vector yp0, 
               N_Vector *yyS0, N_Vector *ypS0)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  void *ida_memB;
  int flag, is, retval;
  
  /* Is ida_mem valid? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDACalcICBS", MSGAM_NULL_IDAMEM);
    return IDA_MEM_NULL;
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDACalcICBS",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Were sensitivities active during the forward integration? */
  if (!IDAADJ_mem->ia_storeSensi) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDACalcICBS", MSGAM_BAD_SENSI);
    return(IDA_ILL_INPUT);
  }

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDACalcICBS", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }
  ida_memB = (void *) IDAB_mem->IDA_mem;

  /* Was InitBS called for this problem? */
  if (!IDAB_mem->ida_res_withSensi) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDACalcICBS", MSGAM_NO_INITBS);
    return(IDA_ILL_INPUT);    
  }

  /* The wrapper for user supplied res function requires ia_bckpbCrt from
     IDAAdjMem to be set to curent problem. */
  IDAADJ_mem->ia_bckpbCrt = IDAB_mem;

  /* Save (y, y') and (y_p, y'_p) in yyTmp, ypTmp and yySTmp, ypSTmp.The wrapper 
     for residual will use these values instead of calling interpolation routine.*/

  /* The four workspaces variables are safe to use if IDAADataStore is not called.*/
  N_VScale(ONE, yy0, IDAADJ_mem->ia_yyTmp);
  N_VScale(ONE, yp0, IDAADJ_mem->ia_ypTmp);

  for (is=0; is<IDA_mem->ida_Ns; is++)
    IDA_mem->ida_cvals[is] = ONE;

  retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                               yyS0, IDAADJ_mem->ia_yySTmp);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                               ypS0, IDAADJ_mem->ia_ypSTmp);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
  
  /* Set noInterp flag to SUNTRUE, so IDAARes will use user provided values for
     y and y' and will not call the interpolation routine(s). */
  IDAADJ_mem->ia_noInterp = SUNTRUE;
  
  flag = IDACalcIC(ida_memB, IDA_YA_YDP_INIT, tout1);

  /* Set interpolation on in IDAARes. */
  IDAADJ_mem->ia_noInterp = SUNFALSE;

  return(flag);
}


/*
 * IDASolveB
 *
 * This routine performs the backward integration from tB0 
 * to tinitial through a sequence of forward-backward runs in
 * between consecutive check points. It returns the values of
 * the adjoint variables and any existing quadrature variables
 * at tinitial.
 *
 * On a successful return, IDASolveB returns IDA_SUCCESS.
 *
 * NOTE that IDASolveB DOES NOT return the solution for the 
 * backward problem(s). Use IDAGetB to extract the solution 
 * for any given backward problem.
 *
 * If there are multiple backward problems and multiple check points,
 * IDASolveB may not succeed in getting all problems to take one step
 * when called in ONE_STEP mode.
 */

int IDASolveB(void *ida_mem, realtype tBout, int itaskB)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  CkpntMem ck_mem;
  IDABMem IDAB_mem, tmp_IDAB_mem;
  int flag=0, sign;
  realtype tfuzz, tBret, tBn;
  booleantype gotCkpnt, reachedTBout, isActive;

  /* Is the mem OK? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDASolveB", MSGAM_NULL_IDAMEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized ? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDASolveB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  if ( IDAADJ_mem->ia_nbckpbs == 0 ) {
    IDAProcessError(IDA_mem, IDA_NO_BCK, "IDAA", "IDASolveB", MSGAM_NO_BCK);
    return(IDA_NO_BCK);
  }
  IDAB_mem = IDAADJ_mem->IDAB_mem;

  /* Check whether IDASolveF has been called */
  if ( IDAADJ_mem->ia_firstIDAFcall ) {
    IDAProcessError(IDA_mem, IDA_NO_FWD, "IDAA", "IDASolveB", MSGAM_NO_FWD);
    return(IDA_NO_FWD);
  }
  sign = (IDAADJ_mem->ia_tfinal - IDAADJ_mem->ia_tinitial > ZERO) ? 1 : -1;

  /* If this is the first call, loop over all backward problems and
   *   - check that tB0 is valid
   *   - check that tBout is ahead of tB0 in the backward direction
   *   - check whether we need to interpolate forward sensitivities
   */
  if (IDAADJ_mem->ia_firstIDABcall) {

    /* First IDABMem struct. */
    tmp_IDAB_mem = IDAB_mem;
    
    while (tmp_IDAB_mem != NULL) {

      tBn = tmp_IDAB_mem->IDA_mem->ida_tn;

      if ( (sign*(tBn-IDAADJ_mem->ia_tinitial) < ZERO) || (sign*(IDAADJ_mem->ia_tfinal-tBn) < ZERO) ) {
        IDAProcessError(IDA_mem, IDA_BAD_TB0, "IDAA", "IDASolveB", 
                        MSGAM_BAD_TB0, tmp_IDAB_mem->ida_index);
        return(IDA_BAD_TB0);
      }

      if (sign*(tBn-tBout) <= ZERO) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDASolveB", MSGAM_BAD_TBOUT,
                       tmp_IDAB_mem->ida_index);
        return(IDA_ILL_INPUT);
      }

      if ( tmp_IDAB_mem->ida_res_withSensi || 
           tmp_IDAB_mem->ida_rhsQ_withSensi )
        IDAADJ_mem->ia_interpSensi = SUNTRUE;

      /* Advance in list. */
      tmp_IDAB_mem = tmp_IDAB_mem->ida_next;      
    }

    if ( IDAADJ_mem->ia_interpSensi && !IDAADJ_mem->ia_storeSensi) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDASolveB", MSGAM_BAD_SENSI);
      return(IDA_ILL_INPUT);
    }

    IDAADJ_mem->ia_firstIDABcall = SUNFALSE;
  }

  /* Check for valid itask */
  if ( (itaskB != IDA_NORMAL) && (itaskB != IDA_ONE_STEP) ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDASolveB", MSG_BAD_ITASK);
    return(IDA_ILL_INPUT);
  }

  /* Check if tBout is legal */
  if ( (sign*(tBout-IDAADJ_mem->ia_tinitial) < ZERO) || (sign*(IDAADJ_mem->ia_tfinal-tBout) < ZERO) ) {
    tfuzz = HUNDRED * IDA_mem->ida_uround *
      (SUNRabs(IDAADJ_mem->ia_tinitial) + SUNRabs(IDAADJ_mem->ia_tfinal));
    if ( (sign*(tBout-IDAADJ_mem->ia_tinitial) < ZERO) && (SUNRabs(tBout-IDAADJ_mem->ia_tinitial) < tfuzz) ) {
      tBout = IDAADJ_mem->ia_tinitial;
    } else {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDASolveB", MSGAM_BAD_TBOUT);
      return(IDA_ILL_INPUT);
    }
  }

  /* Loop through the check points and stop as soon as a backward
   * problem has its tn value behind the current check point's t0_
   * value (in the backward direction) */

  ck_mem = IDAADJ_mem->ck_mem;

  gotCkpnt = SUNFALSE;

  for(;;) {
    tmp_IDAB_mem = IDAB_mem;
    while(tmp_IDAB_mem != NULL) {
      tBn = tmp_IDAB_mem->IDA_mem->ida_tn;

      if ( sign*(tBn-ck_mem->ck_t0) > ZERO ) {
        gotCkpnt = SUNTRUE;
        break;
      }

      if ( (itaskB == IDA_NORMAL) && (tBn == ck_mem->ck_t0) && (sign*(tBout-ck_mem->ck_t0) >= ZERO) ) {
        gotCkpnt = SUNTRUE;
        break;
      }

      tmp_IDAB_mem = tmp_IDAB_mem->ida_next;
    }

    if (gotCkpnt) break;

    if (ck_mem->ck_next == NULL) break;

    ck_mem = ck_mem->ck_next;
  }

  /* Loop while propagating backward problems */
  for(;;) {

    /* Store interpolation data if not available.
       This is the 2nd forward integration pass */
    if (ck_mem != IDAADJ_mem->ia_ckpntData) {

      flag = IDAAdataStore(IDA_mem, ck_mem);
      if (flag != IDA_SUCCESS) break;
    }

    /* Starting with the current check point from above, loop over check points
       while propagating backward problems */

    tmp_IDAB_mem = IDAB_mem;
    while (tmp_IDAB_mem != NULL) {

      /* Decide if current backward problem is "active" in this check point */
      isActive = SUNTRUE;

      tBn = tmp_IDAB_mem->IDA_mem->ida_tn;

      if ( (tBn == ck_mem->ck_t0) && (sign*(tBout-ck_mem->ck_t0) < ZERO ) ) isActive = SUNFALSE;
      if ( (tBn == ck_mem->ck_t0) && (itaskB == IDA_ONE_STEP) ) isActive = SUNFALSE;
      if ( sign*(tBn - ck_mem->ck_t0) < ZERO ) isActive = SUNFALSE;

      if ( isActive ) {
        /* Store the address of current backward problem memory 
         * in IDAADJ_mem to be used in the wrapper functions */
        IDAADJ_mem->ia_bckpbCrt = tmp_IDAB_mem;

        /* Integrate current backward problem */
        IDASetStopTime(tmp_IDAB_mem->IDA_mem, ck_mem->ck_t0);
        flag = IDASolve(tmp_IDAB_mem->IDA_mem, tBout, &tBret, 
                        tmp_IDAB_mem->ida_yy, tmp_IDAB_mem->ida_yp, 
                        itaskB);

        /* Set the time at which we will report solution and/or quadratures */
        tmp_IDAB_mem->ida_tout = tBret;

        /* If an error occurred, exit while loop */
        if (flag < 0) break;

      } else {

        flag = IDA_SUCCESS;
        tmp_IDAB_mem->ida_tout = tBn;
      }

      /* Move to next backward problem */
      tmp_IDAB_mem = tmp_IDAB_mem->ida_next;
    } /* End of while: iteration through backward problems. */
    
    /* If an error occurred, return now */
    if (flag <0) {
      IDAProcessError(IDA_mem, flag, "IDAA", "IDASolveB",
                      MSGAM_BACK_ERROR, tmp_IDAB_mem->ida_index);
      return(flag);
    }

    /* If in IDA_ONE_STEP mode, return now (flag = IDA_SUCCESS) */
    if (itaskB == IDA_ONE_STEP) break;

    /* If all backward problems have succesfully reached tBout, return now */
    reachedTBout = SUNTRUE;

    tmp_IDAB_mem = IDAB_mem;
    while(tmp_IDAB_mem != NULL) {
      if ( sign*(tmp_IDAB_mem->ida_tout - tBout) > ZERO ) {
        reachedTBout = SUNFALSE;
        break;
      }
      tmp_IDAB_mem = tmp_IDAB_mem->ida_next;
    }

    if ( reachedTBout ) break;

    /* Move check point in linked list to next one */
    ck_mem = ck_mem->ck_next;

  } /* End of loop. */

  return(flag);
}


/*
 * IDAGetB
 *
 * IDAGetB returns the state variables at the same time (also returned 
 * in tret) as that at which IDASolveBreturned the solution.
 */

SUNDIALS_EXPORT int IDAGetB(void* ida_mem, int which, realtype *tret,
                            N_Vector yy, N_Vector yp)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  
  /* Is ida_mem valid? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAGetB", MSGAM_NULL_IDAMEM);
    return IDA_MEM_NULL;
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAGetB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAGetB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }

  N_VScale(ONE, IDAB_mem->ida_yy, yy);
  N_VScale(ONE, IDAB_mem->ida_yp, yp);
  *tret = IDAB_mem->ida_tout;

  return(IDA_SUCCESS);
}



/*
 * IDAGetQuadB
 *
 * IDAGetQuadB returns the quadrature variables at the same 
 * time (also returned in tret) as that at which IDASolveB 
 * returned the solution.
 */

int IDAGetQuadB(void *ida_mem, int which, realtype *tret, N_Vector qB)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  void *ida_memB;
  int flag;
  long int nstB;
  
  /* Is ida_mem valid? */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAGetQuadB", MSGAM_NULL_IDAMEM);
    return IDA_MEM_NULL;
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_ADJ, "IDAA", "IDAGetQuadB",  MSGAM_NO_ADJ);
    return(IDA_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAA", "IDAGetQuadB", MSGAM_BAD_WHICH);
    return(IDA_ILL_INPUT);
  }
  
  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }
  ida_memB = (void *) IDAB_mem->IDA_mem;

  /* If the integration for this backward problem has not started yet,
   * simply return the current value of qB (i.e. the final conditions) */

  flag = IDAGetNumSteps(ida_memB, &nstB);
  if (IDA_SUCCESS != flag) return(flag);

  if (nstB == 0) {
    N_VScale(ONE, IDAB_mem->IDA_mem->ida_phiQ[0], qB);
    *tret = IDAB_mem->ida_tout;
  } else {
    flag = IDAGetQuad(ida_memB, tret, qB);
  }
  return(flag);
}

/*=================================================================*/
/*                Private Functions Implementation                 */
/*=================================================================*/

/*
 * IDAAckpntInit
 *
 * This routine initializes the check point linked list with 
 * information from the initial time.
*/

static CkpntMem IDAAckpntInit(IDAMem IDA_mem)
{
  CkpntMem ck_mem;

  /* Allocate space for ckdata */
  ck_mem = (CkpntMem) malloc(sizeof(struct CkpntMemRec));
  if (NULL==ck_mem) return(NULL);

  ck_mem->ck_t0    = IDA_mem->ida_tn;
  ck_mem->ck_nst   = 0;
  ck_mem->ck_kk    = 1;
  ck_mem->ck_hh    = ZERO;

  /* Test if we need to carry quadratures */
  ck_mem->ck_quadr = IDA_mem->ida_quadr && IDA_mem->ida_errconQ;

  /* Test if we need to carry sensitivities */
  ck_mem->ck_sensi = IDA_mem->ida_sensi;
  if(ck_mem->ck_sensi) ck_mem->ck_Ns = IDA_mem->ida_Ns;

  /* Test if we need to carry quadrature sensitivities */
  ck_mem->ck_quadr_sensi = IDA_mem->ida_quadr_sensi && IDA_mem->ida_errconQS;

  /* Alloc 3: current order, i.e. 1,  +   2. */
  ck_mem->ck_phi_alloc = 3;
  
  if (!IDAAckpntAllocVectors(IDA_mem, ck_mem)) {
    free(ck_mem); ck_mem = NULL;
    return(NULL);
  }
  /* Save phi* vectors from IDA_mem to ck_mem. */
  IDAAckpntCopyVectors(IDA_mem, ck_mem);

  /* Next in list */
  ck_mem->ck_next  = NULL;

  return(ck_mem);
}

/*
 * IDAAckpntNew
 *
 * This routine allocates space for a new check point and sets 
 * its data from current values in IDA_mem.
*/

static CkpntMem IDAAckpntNew(IDAMem IDA_mem)
{
  CkpntMem ck_mem;
  int j;

  /* Allocate space for ckdata */
  ck_mem = (CkpntMem) malloc(sizeof(struct CkpntMemRec));
  if (ck_mem == NULL) return(NULL);

  ck_mem->ck_nst       = IDA_mem->ida_nst;
  ck_mem->ck_tretlast  = IDA_mem->ida_tretlast;
  ck_mem->ck_kk        = IDA_mem->ida_kk;
  ck_mem->ck_kused     = IDA_mem->ida_kused;
  ck_mem->ck_knew      = IDA_mem->ida_knew;
  ck_mem->ck_phase     = IDA_mem->ida_phase;
  ck_mem->ck_ns        = IDA_mem->ida_ns;
  ck_mem->ck_hh        = IDA_mem->ida_hh;
  ck_mem->ck_hused     = IDA_mem->ida_hused;
  ck_mem->ck_rr        = IDA_mem->ida_rr;
  ck_mem->ck_cj        = IDA_mem->ida_cj;
  ck_mem->ck_cjlast    = IDA_mem->ida_cjlast;
  ck_mem->ck_cjold     = IDA_mem->ida_cjold;
  ck_mem->ck_cjratio   = IDA_mem->ida_cjratio;
  ck_mem->ck_ss        = IDA_mem->ida_ss;
  ck_mem->ck_ssS       = IDA_mem->ida_ssS;
  ck_mem->ck_t0        = IDA_mem->ida_tn;

  for (j=0; j<MXORDP1; j++) {
    ck_mem->ck_psi[j]   = IDA_mem->ida_psi[j];
    ck_mem->ck_alpha[j] = IDA_mem->ida_alpha[j];
    ck_mem->ck_beta[j]  = IDA_mem->ida_beta[j];
    ck_mem->ck_sigma[j] = IDA_mem->ida_sigma[j];
    ck_mem->ck_gamma[j] = IDA_mem->ida_gamma[j];
  }

  /* Test if we need to carry quadratures */
  ck_mem->ck_quadr = IDA_mem->ida_quadr && IDA_mem->ida_errconQ;

  /* Test if we need to carry sensitivities */
  ck_mem->ck_sensi = IDA_mem->ida_sensi;
  if(ck_mem->ck_sensi) ck_mem->ck_Ns = IDA_mem->ida_Ns;

  /* Test if we need to carry quadrature sensitivities */
  ck_mem->ck_quadr_sensi = IDA_mem->ida_quadr_sensi && IDA_mem->ida_errconQS;

  ck_mem->ck_phi_alloc = (IDA_mem->ida_kk+2 < MXORDP1) ?
    IDA_mem->ida_kk+2 : MXORDP1;

  if (!IDAAckpntAllocVectors(IDA_mem, ck_mem)) {
    free(ck_mem); ck_mem = NULL;
    return(NULL);
  }

  /* Save phi* vectors from IDA_mem to ck_mem. */
  IDAAckpntCopyVectors(IDA_mem, ck_mem);

  return(ck_mem);
}

/* IDAAckpntDelete 
 *
 * This routine deletes the first check point in list.
*/

static void IDAAckpntDelete(CkpntMem *ck_memPtr)
{
  CkpntMem tmp;
  int j;

  if (*ck_memPtr != NULL) {
    /* store head of list */
    tmp = *ck_memPtr;
    /* move head of list */
    *ck_memPtr = (*ck_memPtr)->ck_next;

    /* free N_Vectors in tmp */
    for (j=0; j<tmp->ck_phi_alloc; j++) 
      N_VDestroy(tmp->ck_phi[j]);

    /* free N_Vectors for quadratures in tmp */
    if (tmp->ck_quadr) {
      for (j=0; j<tmp->ck_phi_alloc; j++) 
        N_VDestroy(tmp->ck_phiQ[j]);
    }

    /* Free sensitivity related data. */
    if (tmp->ck_sensi) {
      for (j=0; j<tmp->ck_phi_alloc; j++) 
        N_VDestroyVectorArray(tmp->ck_phiS[j], tmp->ck_Ns);
    }
    
    if (tmp->ck_quadr_sensi) {
      for (j=0; j<tmp->ck_phi_alloc; j++) 
        N_VDestroyVectorArray(tmp->ck_phiQS[j], tmp->ck_Ns);
    }

    free(tmp); tmp=NULL;
  }
}

/* 
 * IDAAckpntAllocVectors
 *
 * Allocate checkpoint's phi, phiQ, phiS, phiQS vectors needed to save 
 * current state of IDAMem.
 *
 */
static booleantype IDAAckpntAllocVectors(IDAMem IDA_mem, CkpntMem ck_mem)
{
  int j, jj;

  for (j=0; j<ck_mem->ck_phi_alloc; j++) {
    ck_mem->ck_phi[j] = N_VClone(IDA_mem->ida_tempv1);
    if(ck_mem->ck_phi[j] == NULL) {    
      for(jj=0; jj<j; jj++) N_VDestroy(ck_mem->ck_phi[jj]);
      return(SUNFALSE);
    }
  }

  /* Do we need to carry quadratures? */
  if(ck_mem->ck_quadr) {
    for (j=0; j<ck_mem->ck_phi_alloc; j++) {
      ck_mem->ck_phiQ[j] = N_VClone(IDA_mem->ida_eeQ);
      if(ck_mem->ck_phiQ[j] == NULL)  {        
        for (jj=0; jj<j; jj++) N_VDestroy(ck_mem->ck_phiQ[jj]);

        for(jj=0; jj<ck_mem->ck_phi_alloc; jj++)
          N_VDestroy(ck_mem->ck_phi[jj]);

        return(SUNFALSE);
      }
    }
  }

  /* Do we need to carry sensitivities? */
  if(ck_mem->ck_sensi) {

    for (j=0; j<ck_mem->ck_phi_alloc; j++) {
      ck_mem->ck_phiS[j] = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_tempv1);
      if (ck_mem->ck_phiS[j] == NULL) {
        for (jj=0; jj<j; jj++)
          N_VDestroyVectorArray(ck_mem->ck_phiS[jj], IDA_mem->ida_Ns);

        if (ck_mem->ck_quadr)
          for (jj=0; jj<ck_mem->ck_phi_alloc; jj++)
            N_VDestroy(ck_mem->ck_phiQ[jj]);

        for (jj=0; jj<ck_mem->ck_phi_alloc; jj++)
          N_VDestroy(ck_mem->ck_phi[jj]);

        return(SUNFALSE);
      }
    }
  }

  /* Do we need to carry quadrature sensitivities? */
  if (ck_mem->ck_quadr_sensi) {

    for (j=0; j<ck_mem->ck_phi_alloc; j++) {
      ck_mem->ck_phiQS[j] = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_eeQ);
      if (ck_mem->ck_phiQS[j] == NULL) {

        for (jj=0; jj<j; jj++)
          N_VDestroyVectorArray(ck_mem->ck_phiQS[jj], IDA_mem->ida_Ns);

        for (jj=0; jj<ck_mem->ck_phi_alloc; jj++)
          N_VDestroyVectorArray(ck_mem->ck_phiS[jj], IDA_mem->ida_Ns);

        if (ck_mem->ck_quadr) 
          for (jj=0; jj<ck_mem->ck_phi_alloc; jj++)
            N_VDestroy(ck_mem->ck_phiQ[jj]);

        for (jj=0; jj<ck_mem->ck_phi_alloc; jj++)
          N_VDestroy(ck_mem->ck_phi[jj]);

        return(SUNFALSE);
      }
    }
  }
  return(SUNTRUE);
}

/* 
 * IDAAckpntCopyVectors
 *
 * Copy phi* vectors from IDAMem in the corresponding vectors from checkpoint
 *
 */
static void IDAAckpntCopyVectors(IDAMem IDA_mem, CkpntMem ck_mem)
{
  int j, is;

  /* Save phi* arrays from IDA_mem */

  for (j=0; j<ck_mem->ck_phi_alloc; j++)
    IDA_mem->ida_cvals[j] = ONE;

  (void) N_VScaleVectorArray(ck_mem->ck_phi_alloc, IDA_mem->ida_cvals,
                             IDA_mem->ida_phi, ck_mem->ck_phi);

  if (ck_mem->ck_quadr)
    (void) N_VScaleVectorArray(ck_mem->ck_phi_alloc, IDA_mem->ida_cvals,
                               IDA_mem->ida_phiQ, ck_mem->ck_phiQ);

  if (ck_mem->ck_sensi || ck_mem->ck_quadr_sensi) {
    for (j=0; j<ck_mem->ck_phi_alloc; j++) {
      for (is=0; is<IDA_mem->ida_Ns; is++) {
        IDA_mem->ida_cvals[j*IDA_mem->ida_Ns + is] = ONE;
      }
    }
  }

  if (ck_mem->ck_sensi) {
    for (j=0; j<ck_mem->ck_phi_alloc; j++) {
      for (is=0; is<IDA_mem->ida_Ns; is++) {
        IDA_mem->ida_Xvecs[j*IDA_mem->ida_Ns + is] = IDA_mem->ida_phiS[j][is];
        IDA_mem->ida_Zvecs[j*IDA_mem->ida_Ns + is] = ck_mem->ck_phiS[j][is];
      }
    }

    (void) N_VScaleVectorArray(ck_mem->ck_phi_alloc * IDA_mem->ida_Ns,
                               IDA_mem->ida_cvals,
                               IDA_mem->ida_Xvecs, IDA_mem->ida_Zvecs);
  }

  if(ck_mem->ck_quadr_sensi) {
    for (j=0; j<ck_mem->ck_phi_alloc; j++) {
      for (is=0; is<IDA_mem->ida_Ns; is++) {
        IDA_mem->ida_Xvecs[j*IDA_mem->ida_Ns + is] = IDA_mem->ida_phiQS[j][is];
        IDA_mem->ida_Zvecs[j*IDA_mem->ida_Ns + is] = ck_mem->ck_phiQS[j][is];
      }
    }

    (void) N_VScaleVectorArray(ck_mem->ck_phi_alloc * IDA_mem->ida_Ns,
                               IDA_mem->ida_cvals,
                               IDA_mem->ida_Xvecs, IDA_mem->ida_Zvecs);
  }

}

/*
 * IDAAdataMalloc
 *
 * This routine allocates memory for storing information at all
 * intermediate points between two consecutive check points. 
 * This data is then used to interpolate the forward solution 
 * at any other time.
*/

static booleantype IDAAdataMalloc(IDAMem IDA_mem)
{
  IDAadjMem IDAADJ_mem;
  DtpntMem *dt_mem;
  long int i, j;

  IDAADJ_mem = IDA_mem->ida_adj_mem;
  IDAADJ_mem->dt_mem = NULL;

  dt_mem = (DtpntMem *)malloc((IDAADJ_mem->ia_nsteps+1)*sizeof(struct DtpntMemRec *));
  if (dt_mem==NULL) return(SUNFALSE);

  for (i=0; i<=IDAADJ_mem->ia_nsteps; i++) {
    
    dt_mem[i] = (DtpntMem)malloc(sizeof(struct DtpntMemRec));
    
    /* On failure, free any allocated memory and return NULL. */
    if (dt_mem[i] == NULL) {

      for(j=0; j<i; j++) 
        free(dt_mem[j]);

      free(dt_mem);
      return(SUNFALSE);
    }
    dt_mem[i]->content = NULL;
  }
  /* Attach the allocated dt_mem to IDAADJ_mem. */
  IDAADJ_mem->dt_mem = dt_mem;
  return(SUNTRUE);
}

/*
 * IDAAdataFree
 *
 * This routine frees the memory allocated for data storage.
 */

static void IDAAdataFree(IDAMem IDA_mem)
{
  IDAadjMem IDAADJ_mem;
  long int i;

  IDAADJ_mem = IDA_mem->ida_adj_mem;

  if (IDAADJ_mem == NULL) return;

  /* Destroy data points by calling the interpolation's 'free' routine. */
  IDAADJ_mem->ia_free(IDA_mem);

  for (i=0; i<=IDAADJ_mem->ia_nsteps; i++) {
     free(IDAADJ_mem->dt_mem[i]);
     IDAADJ_mem->dt_mem[i] = NULL;
  }

  free(IDAADJ_mem->dt_mem);
  IDAADJ_mem->dt_mem = NULL;
}


/*
 * IDAAdataStore 
 *
 * This routine integrates the forward model starting at the check
 * point ck_mem and stores y and yprime at all intermediate 
 * steps. 
 *
 * Return values: 
 *   - the flag that IDASolve may return on error
 *   - IDA_REIFWD_FAIL if no check point is available for this hot start
 *   - IDA_SUCCESS
 */

static int IDAAdataStore(IDAMem IDA_mem, CkpntMem ck_mem)
{
  IDAadjMem IDAADJ_mem;
  DtpntMem *dt_mem;
  realtype t;
  long int i;
  int flag, sign;

  IDAADJ_mem = IDA_mem->ida_adj_mem;
  dt_mem = IDAADJ_mem->dt_mem;

  /* Initialize IDA_mem with data from ck_mem. */
  flag = IDAAckpntGet(IDA_mem, ck_mem);
  if (flag != IDA_SUCCESS)
    return(IDA_REIFWD_FAIL);

  /* Set first structure in dt_mem[0] */
  dt_mem[0]->t = ck_mem->ck_t0;
  IDAADJ_mem->ia_storePnt(IDA_mem, dt_mem[0]);

  /* Decide whether TSTOP must be activated */
  if (IDAADJ_mem->ia_tstopIDAFcall) {
    IDASetStopTime(IDA_mem, IDAADJ_mem->ia_tstopIDAF);
  }

  sign = (IDAADJ_mem->ia_tfinal - IDAADJ_mem->ia_tinitial > ZERO) ? 1 : -1;

  /* Run IDASolve in IDA_ONE_STEP mode to set following structures in dt_mem[i]. */
  i = 1;
  do {

    flag = IDASolve(IDA_mem, ck_mem->ck_t1, &t, IDAADJ_mem->ia_yyTmp,
                    IDAADJ_mem->ia_ypTmp, IDA_ONE_STEP);
    if (flag < 0) return(IDA_FWD_FAIL);

    dt_mem[i]->t = t;
    IDAADJ_mem->ia_storePnt(IDA_mem, dt_mem[i]);

    i++;
  } while ( sign*(ck_mem->ck_t1 - t) > ZERO );

  /* New data is now available. */
  IDAADJ_mem->ia_ckpntData = ck_mem;
  IDAADJ_mem->ia_newData = SUNTRUE;
  IDAADJ_mem->ia_np  = i;

  return(IDA_SUCCESS);
}

/*
 * CVAckpntGet
 *
 * This routine prepares IDAS for a hot restart from
 * the check point ck_mem
 */

static int IDAAckpntGet(IDAMem IDA_mem, CkpntMem ck_mem) 
{
  int flag, j, is;

  if (ck_mem->ck_next == NULL) {

    /* In this case, we just call the reinitialization routine,
     * but make sure we use the same initial stepsize as on 
     * the first run. */

    IDASetInitStep(IDA_mem, IDA_mem->ida_h0u);

    flag = IDAReInit(IDA_mem, ck_mem->ck_t0, ck_mem->ck_phi[0], ck_mem->ck_phi[1]);
    if (flag != IDA_SUCCESS) return(flag);

    if (ck_mem->ck_quadr) {
      flag = IDAQuadReInit(IDA_mem, ck_mem->ck_phiQ[0]);
      if (flag != IDA_SUCCESS) return(flag);
    }

    if (ck_mem->ck_sensi) {
      flag = IDASensReInit(IDA_mem, IDA_mem->ida_ism, ck_mem->ck_phiS[0], ck_mem->ck_phiS[1]);
      if (flag != IDA_SUCCESS) return(flag);
    }

    if (ck_mem->ck_quadr_sensi) {
      flag = IDAQuadSensReInit(IDA_mem, ck_mem->ck_phiQS[0]);
      if (flag != IDA_SUCCESS) return(flag);
    }

  } else {

    /* Copy parameters from check point data structure */
    IDA_mem->ida_nst       = ck_mem->ck_nst;
    IDA_mem->ida_tretlast  = ck_mem->ck_tretlast;
    IDA_mem->ida_kk        = ck_mem->ck_kk;
    IDA_mem->ida_kused     = ck_mem->ck_kused;
    IDA_mem->ida_knew      = ck_mem->ck_knew;
    IDA_mem->ida_phase     = ck_mem->ck_phase;
    IDA_mem->ida_ns        = ck_mem->ck_ns;
    IDA_mem->ida_hh        = ck_mem->ck_hh;
    IDA_mem->ida_hused     = ck_mem->ck_hused;
    IDA_mem->ida_rr        = ck_mem->ck_rr;
    IDA_mem->ida_cj        = ck_mem->ck_cj;
    IDA_mem->ida_cjlast    = ck_mem->ck_cjlast;
    IDA_mem->ida_cjold     = ck_mem->ck_cjold;
    IDA_mem->ida_cjratio   = ck_mem->ck_cjratio;
    IDA_mem->ida_tn        = ck_mem->ck_t0;
    IDA_mem->ida_ss        = ck_mem->ck_ss;
    IDA_mem->ida_ssS       = ck_mem->ck_ssS;

    
    /* Copy the arrays from check point data structure */
    for (j=0; j<ck_mem->ck_phi_alloc; j++)
      N_VScale(ONE, ck_mem->ck_phi[j], IDA_mem->ida_phi[j]);

    if(ck_mem->ck_quadr) {
      for (j=0; j<ck_mem->ck_phi_alloc; j++)
        N_VScale(ONE, ck_mem->ck_phiQ[j], IDA_mem->ida_phiQ[j]);
    }

    if (ck_mem->ck_sensi) {
      for (is=0; is<IDA_mem->ida_Ns; is++) {
        for (j=0; j<ck_mem->ck_phi_alloc; j++)
          N_VScale(ONE, ck_mem->ck_phiS[j][is], IDA_mem->ida_phiS[j][is]);
      }
    }

    if (ck_mem->ck_quadr_sensi) {
      for (is=0; is<IDA_mem->ida_Ns; is++) {
        for (j=0; j<ck_mem->ck_phi_alloc; j++)
          N_VScale(ONE, ck_mem->ck_phiQS[j][is], IDA_mem->ida_phiQS[j][is]);
      }
    }

    for (j=0; j<MXORDP1; j++) {
      IDA_mem->ida_psi[j]   = ck_mem->ck_psi[j];
      IDA_mem->ida_alpha[j] = ck_mem->ck_alpha[j];
      IDA_mem->ida_beta[j]  = ck_mem->ck_beta[j];
      IDA_mem->ida_sigma[j] = ck_mem->ck_sigma[j];
      IDA_mem->ida_gamma[j] = ck_mem->ck_gamma[j];
    }

    /* Force a call to setup */
    IDA_mem->ida_forceSetup = SUNTRUE;
  }

  return(IDA_SUCCESS);
}


/* 
 * -----------------------------------------------------------------
 * Functions specific to cubic Hermite interpolation
 * -----------------------------------------------------------------
 */

/*
 * IDAAhermiteMalloc
 *
 * This routine allocates memory for storing information at all
 * intermediate points between two consecutive check points. 
 * This data is then used to interpolate the forward solution 
 * at any other time.
 */

static booleantype IDAAhermiteMalloc(IDAMem IDA_mem)
{
  IDAadjMem IDAADJ_mem;
  DtpntMem *dt_mem;
  HermiteDataMem content;
  long int i, ii=0;
  booleantype allocOK;

  allocOK = SUNTRUE;

  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Allocate space for the vectors yyTmp and ypTmp. */
  IDAADJ_mem->ia_yyTmp = N_VClone(IDA_mem->ida_tempv1);
  if (IDAADJ_mem->ia_yyTmp == NULL) {
    return(SUNFALSE);
  }
  IDAADJ_mem->ia_ypTmp = N_VClone(IDA_mem->ida_tempv1);
  if (IDAADJ_mem->ia_ypTmp == NULL) {
    return(SUNFALSE);
  }

  /* Allocate space for sensitivities temporary vectors. */
  if (IDAADJ_mem->ia_storeSensi) {
    
    IDAADJ_mem->ia_yySTmp = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_tempv1);
    if (IDAADJ_mem->ia_yySTmp == NULL) {
      N_VDestroy(IDAADJ_mem->ia_yyTmp);
      N_VDestroy(IDAADJ_mem->ia_ypTmp);
      return(SUNFALSE);
    }

    IDAADJ_mem->ia_ypSTmp = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_tempv1);
    if (IDAADJ_mem->ia_ypSTmp == NULL) {
      N_VDestroy(IDAADJ_mem->ia_yyTmp);
      N_VDestroy(IDAADJ_mem->ia_ypTmp);
      N_VDestroyVectorArray(IDAADJ_mem->ia_yySTmp, IDA_mem->ida_Ns);
      return(SUNFALSE);

    }
  }

  /* Allocate space for the content field of the dt structures */

  dt_mem = IDAADJ_mem->dt_mem;

  for (i=0; i<=IDAADJ_mem->ia_nsteps; i++) {

    content = NULL;
    content = (HermiteDataMem) malloc(sizeof(struct HermiteDataMemRec));
    if (content == NULL) {
      ii = i;
      allocOK = SUNFALSE;
      break;
    }

    content->y = N_VClone(IDA_mem->ida_tempv1);
    if (content->y == NULL) {
      free(content); content = NULL;
      ii = i;
      allocOK = SUNFALSE;
      break;
    }

    content->yd = N_VClone(IDA_mem->ida_tempv1);
    if (content->yd == NULL) {
      N_VDestroy(content->y);
      free(content); content = NULL;
      ii = i;
      allocOK = SUNFALSE;
      break;
    }

    if (IDAADJ_mem->ia_storeSensi) {
      
      content->yS = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_tempv1);
      if (content->yS == NULL) {
        N_VDestroy(content->y);
        N_VDestroy(content->yd);
        free(content); content = NULL;
        ii = i;
        allocOK = SUNFALSE;
        break;
      }

      content->ySd = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_tempv1);
      if (content->ySd == NULL) {
        N_VDestroy(content->y);
        N_VDestroy(content->yd);
        N_VDestroyVectorArray(content->yS, IDA_mem->ida_Ns);
        free(content); content = NULL;
        ii = i;
        allocOK = SUNFALSE;
        break;
      }
    }
    
    dt_mem[i]->content = content;

  } 

  /* If an error occurred, deallocate and return */

  if (!allocOK) {

    N_VDestroy(IDAADJ_mem->ia_yyTmp);
    N_VDestroy(IDAADJ_mem->ia_ypTmp);  

    if (IDAADJ_mem->ia_storeSensi) {     
      N_VDestroyVectorArray(IDAADJ_mem->ia_yySTmp, IDA_mem->ida_Ns);
      N_VDestroyVectorArray(IDAADJ_mem->ia_ypSTmp, IDA_mem->ida_Ns);
    }

    for (i=0; i<ii; i++) {
      content = (HermiteDataMem) (dt_mem[i]->content);
      N_VDestroy(content->y);
      N_VDestroy(content->yd);

      if (IDAADJ_mem->ia_storeSensi) {
        N_VDestroyVectorArray(content->yS, IDA_mem->ida_Ns);
        N_VDestroyVectorArray(content->ySd, IDA_mem->ida_Ns);        
      }

      free(dt_mem[i]->content); dt_mem[i]->content = NULL;
    }

  }

  return(allocOK);
}

/*
 * IDAAhermiteFree
 *
 * This routine frees the memory allocated for data storage.
 */

static void IDAAhermiteFree(IDAMem IDA_mem)
{  
  IDAadjMem IDAADJ_mem;
  DtpntMem *dt_mem;
  HermiteDataMem content;
  long int i;

  IDAADJ_mem = IDA_mem->ida_adj_mem;

  N_VDestroy(IDAADJ_mem->ia_yyTmp);
  N_VDestroy(IDAADJ_mem->ia_ypTmp);

  if (IDAADJ_mem->ia_storeSensi) {    
    N_VDestroyVectorArray(IDAADJ_mem->ia_yySTmp, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDAADJ_mem->ia_ypSTmp, IDA_mem->ida_Ns);
  }

  dt_mem = IDAADJ_mem->dt_mem;

  for (i=0; i<=IDAADJ_mem->ia_nsteps; i++) {

    content = (HermiteDataMem) (dt_mem[i]->content);
    /* content might be NULL, if IDAAdjInit was called but IDASolveF was not. */
    if(content) {

      N_VDestroy(content->y);
      N_VDestroy(content->yd);

      if (IDAADJ_mem->ia_storeSensi) {
        N_VDestroyVectorArray(content->yS, IDA_mem->ida_Ns);
        N_VDestroyVectorArray(content->ySd, IDA_mem->ida_Ns);      
      }
      free(dt_mem[i]->content); 
      dt_mem[i]->content = NULL;
    }
  }
}

/*
 * IDAAhermiteStorePnt
 *
 * This routine stores a new point (y,yd) in the structure d for use
 * in the cubic Hermite interpolation.
 * Note that the time is already stored.
 */

static int IDAAhermiteStorePnt(IDAMem IDA_mem, DtpntMem d)
{
  IDAadjMem IDAADJ_mem;
  HermiteDataMem content;
  int is, retval;

  IDAADJ_mem = IDA_mem->ida_adj_mem;

  content = (HermiteDataMem) d->content;

  /* Load solution(s) */
  N_VScale(ONE, IDA_mem->ida_phi[0], content->y);
  
  if (IDAADJ_mem->ia_storeSensi) {
    for (is=0; is<IDA_mem->ida_Ns; is++)
      IDA_mem->ida_cvals[is] = ONE;

    retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                                 IDA_mem->ida_phiS[0], content->yS);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
  }

  /* Load derivative(s). */
  IDAAGettnSolutionYp(IDA_mem, content->yd);

  if (IDAADJ_mem->ia_storeSensi) {
    IDAAGettnSolutionYpS(IDA_mem, content->ySd);
  }

  return(0);
}


/*
 * IDAAhermiteGetY
 *
 * This routine uses cubic piece-wise Hermite interpolation for 
 * the forward solution vector. 
 * It is typically called by the wrapper routines before calling
 * user provided routines (fB, djacB, bjacB, jtimesB, psolB) but
 * can be directly called by the user through IDAGetAdjY
 */

static int IDAAhermiteGetY(IDAMem IDA_mem, realtype t,
                           N_Vector yy, N_Vector yp,
                          N_Vector *yyS, N_Vector *ypS)
{
  IDAadjMem IDAADJ_mem;
  DtpntMem *dt_mem;
  HermiteDataMem content0, content1;

  realtype t0, t1, delta;
  realtype factor1, factor2, factor3;

  N_Vector y0, yd0, y1, yd1;
  N_Vector *yS0=NULL, *ySd0=NULL, *yS1, *ySd1;

  int flag, is, NS;
  long int indx;
  booleantype newpoint;

  /* local variables for fused vector oerations */
  int retval;
  realtype  cvals[4];
  N_Vector  Xvecs[4];
  N_Vector* XXvecs[4];
 
  IDAADJ_mem = IDA_mem->ida_adj_mem;
  dt_mem = IDAADJ_mem->dt_mem;
 
  /* Local value of Ns */
  NS = (IDAADJ_mem->ia_interpSensi && (yyS != NULL)) ? IDA_mem->ida_Ns : 0;

  /* Get the index in dt_mem */
  flag = IDAAfindIndex(IDA_mem, t, &indx, &newpoint);
  if (flag != IDA_SUCCESS) return(flag);

  /* If we are beyond the left limit but close enough,
     then return y at the left limit. */

  if (indx == 0) {
    content0 = (HermiteDataMem) (dt_mem[0]->content);
    N_VScale(ONE, content0->y,  yy);
    N_VScale(ONE, content0->yd, yp);

    if (NS > 0) {
      for (is=0; is<NS; is++)
        IDA_mem->ida_cvals[is] = ONE;

      retval = N_VScaleVectorArray(NS, IDA_mem->ida_cvals, content0->yS, yyS);
      if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

      retval = N_VScaleVectorArray(NS, IDA_mem->ida_cvals, content0->ySd, ypS);
      if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
    }

    return(IDA_SUCCESS);
  }

  /* Extract stuff from the appropriate data points */
  t0 = dt_mem[indx-1]->t;
  t1 = dt_mem[indx]->t;
  delta = t1 - t0;

  content0 = (HermiteDataMem) (dt_mem[indx-1]->content);
  y0  = content0->y;
  yd0 = content0->yd;
  if (IDAADJ_mem->ia_interpSensi) {
    yS0  = content0->yS;
    ySd0 = content0->ySd;
  }

  if (newpoint) {
    
    /* Recompute Y0 and Y1 */
    content1 = (HermiteDataMem) (dt_mem[indx]->content);

    y1  = content1->y;
    yd1 = content1->yd;

    /* Y1 = delta (yd1 + yd0) - 2 (y1 - y0) */
    cvals[0] = -TWO;   Xvecs[0] = y1;
    cvals[1] = TWO;    Xvecs[1] = y0;
    cvals[2] = delta;  Xvecs[2] = yd1;
    cvals[3] = delta;  Xvecs[3] = yd0;

    retval = N_VLinearCombination(4, cvals, Xvecs, IDAADJ_mem->ia_Y[1]);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

    /* Y0 = y1 - y0 - delta * yd0 */
    cvals[0] = ONE;     Xvecs[0] = y1;
    cvals[1] = -ONE;    Xvecs[1] = y0;
    cvals[2] = -delta;  Xvecs[2] = yd0;

    retval = N_VLinearCombination(3, cvals, Xvecs, IDAADJ_mem->ia_Y[0]);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

    /* Recompute YS0 and YS1, if needed */

    if (NS > 0) {

      yS1  = content1->yS;
      ySd1 = content1->ySd;

      /* YS1 = delta (ySd1 + ySd0) - 2 (yS1 - yS0) */
      cvals[0] = -TWO;   XXvecs[0] = yS1;
      cvals[1] = TWO;    XXvecs[1] = yS0;
      cvals[2] = delta;  XXvecs[2] = ySd1;
      cvals[3] = delta;  XXvecs[3] = ySd0;

      retval = N_VLinearCombinationVectorArray(NS, 4, cvals, XXvecs, IDAADJ_mem->ia_YS[1]);
      if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

      /* YS0 = yS1 - yS0 - delta * ySd0 */
      cvals[0] = ONE;     XXvecs[0] = yS1;
      cvals[1] = -ONE;    XXvecs[1] = yS0;
      cvals[2] = -delta;  XXvecs[2] = ySd0;

      retval = N_VLinearCombinationVectorArray(NS, 3, cvals, XXvecs, IDAADJ_mem->ia_YS[0]);
      if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

    }

  }

  /* Perform the actual interpolation. */

  /* For y. */
  factor1 = t - t0;

  factor2 = factor1/delta;
  factor2 = factor2*factor2;

  factor3 = factor2*(t-t1)/delta;

  cvals[0] = ONE;
  cvals[1] = factor1;
  cvals[2] = factor2;
  cvals[3] = factor3;

  /* y = y0 + factor1 yd0 + factor2 * Y[0] + factor3 Y[1] */
  Xvecs[0] = y0;
  Xvecs[1] = yd0;
  Xvecs[2] = IDAADJ_mem->ia_Y[0];
  Xvecs[3] = IDAADJ_mem->ia_Y[1];

  retval = N_VLinearCombination(4, cvals, Xvecs, yy);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  /* Sensi Interpolation. */

  /* yS = yS0 + factor1 ySd0 + factor2 * YS[0] + factor3 YS[1], if needed */
  if (NS > 0) {

    XXvecs[0] = yS0;
    XXvecs[1] = ySd0;
    XXvecs[2] = IDAADJ_mem->ia_YS[0];
    XXvecs[3] = IDAADJ_mem->ia_YS[1];

    retval = N_VLinearCombinationVectorArray(NS, 4, cvals, XXvecs, yyS);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  }

  /* For y'. */
  factor1 = factor1/delta/delta;           /* factor1 = 2(t-t0)/(t1-t0)^2             */
  factor2 = factor1*((3*t-2*t1-t0)/delta); /* factor2 = (t-t0)(3*t-2*t1-t0)/(t1-t0)^3 */
  factor1 *= 2;

  cvals[0] = ONE;
  cvals[1] = factor1;
  cvals[2] = factor2;

  /* yp = yd0 + factor1 Y[0] + factor 2 Y[1] */
  Xvecs[0] = yd0;
  Xvecs[1] = IDAADJ_mem->ia_Y[0];
  Xvecs[2] = IDAADJ_mem->ia_Y[1];

  retval = N_VLinearCombination(3, cvals, Xvecs, yp);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
                                            
  /* Sensi interpolation for 1st derivative. */

  /* ypS = ySd0 + factor1 YS[0] + factor 2 YS[1], if needed */
  if (NS > 0) {

    XXvecs[0] = ySd0;
    XXvecs[1] = IDAADJ_mem->ia_YS[0];
    XXvecs[2] = IDAADJ_mem->ia_YS[1];

    retval = N_VLinearCombinationVectorArray(NS, 3, cvals, XXvecs, ypS);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  }

  return(IDA_SUCCESS);
}

/* 
 * -----------------------------------------------------------------
 * Functions specific to Polynomial interpolation
 * -----------------------------------------------------------------
 */

/*
 * IDAApolynomialMalloc 
 *
 * This routine allocates memory for storing information at all
 * intermediate points between two consecutive check points. 
 * This data is then used to interpolate the forward solution 
 * at any other time.
 *
 * Information about the first derivative is stored only for the first
 * data point.
 */

static booleantype IDAApolynomialMalloc(IDAMem IDA_mem)
{
  IDAadjMem IDAADJ_mem;
  DtpntMem *dt_mem;
  PolynomialDataMem content;
  long int i, ii=0;
  booleantype allocOK;

  allocOK = SUNTRUE;

  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Allocate space for the vectors yyTmp and ypTmp */
  IDAADJ_mem->ia_yyTmp = N_VClone(IDA_mem->ida_tempv1);
  if (IDAADJ_mem->ia_yyTmp == NULL) {
    return(SUNFALSE);
  }
  IDAADJ_mem->ia_ypTmp = N_VClone(IDA_mem->ida_tempv1);
  if (IDAADJ_mem->ia_ypTmp == NULL) {
    return(SUNFALSE);
  }

  if (IDAADJ_mem->ia_storeSensi) {
    
    IDAADJ_mem->ia_yySTmp = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_tempv1);
    if (IDAADJ_mem->ia_yySTmp == NULL) {
      N_VDestroy(IDAADJ_mem->ia_yyTmp);
      N_VDestroy(IDAADJ_mem->ia_ypTmp);
      return(SUNFALSE);
    }

    IDAADJ_mem->ia_ypSTmp = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_tempv1);
    if (IDAADJ_mem->ia_ypSTmp == NULL) {
      N_VDestroy(IDAADJ_mem->ia_yyTmp);
      N_VDestroy(IDAADJ_mem->ia_ypTmp);
      N_VDestroyVectorArray(IDAADJ_mem->ia_yySTmp, IDA_mem->ida_Ns);
      return(SUNFALSE);

    }
  }

  /* Allocate space for the content field of the dt structures */
  dt_mem = IDAADJ_mem->dt_mem;

  for (i=0; i<=IDAADJ_mem->ia_nsteps; i++) {

    content = NULL;
    content = (PolynomialDataMem) malloc(sizeof(struct PolynomialDataMemRec));
    if (content == NULL) {
      ii = i;
      allocOK = SUNFALSE;
      break;
    }

    content->y = N_VClone(IDA_mem->ida_tempv1);
    if (content->y == NULL) {
      free(content); content = NULL;
      ii = i;
      allocOK = SUNFALSE;
      break;
    }

    /* Allocate space for yp also. Needed for the most left point interpolation. */
    if (i == 0) {
      content->yd = N_VClone(IDA_mem->ida_tempv1);
      
      /* Memory allocation failure ? */
      if (content->yd == NULL) {
        N_VDestroy(content->y);
        free(content); content = NULL;
        ii = i;
        allocOK = SUNFALSE;
      }
    } else {
      /* Not the first data point. */
      content->yd = NULL;
    }

    if (IDAADJ_mem->ia_storeSensi) {
      
      content->yS = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_tempv1);
      if (content->yS == NULL) {
        N_VDestroy(content->y);
        if (content->yd) N_VDestroy(content->yd);
        free(content); content = NULL;
        ii = i;
        allocOK = SUNFALSE;
        break;
      }
      
      if (i==0) {
        content->ySd = N_VCloneVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_tempv1);
        if (content->ySd == NULL) {
          N_VDestroy(content->y);
          if (content->yd) N_VDestroy(content->yd);
          N_VDestroyVectorArray(content->yS, IDA_mem->ida_Ns);
          free(content); content = NULL;
          ii = i;
          allocOK = SUNFALSE;
        }
      } else {
        content->ySd = NULL;
      }
    }

    dt_mem[i]->content = content;
  } 

  /* If an error occurred, deallocate and return */
  if (!allocOK) {

    N_VDestroy(IDAADJ_mem->ia_yyTmp);
    N_VDestroy(IDAADJ_mem->ia_ypTmp);
    if (IDAADJ_mem->ia_storeSensi) {

        N_VDestroyVectorArray(IDAADJ_mem->ia_yySTmp, IDA_mem->ida_Ns);
        N_VDestroyVectorArray(IDAADJ_mem->ia_ypSTmp, IDA_mem->ida_Ns);      
    }

    for (i=0; i<ii; i++) {
      content = (PolynomialDataMem) (dt_mem[i]->content);
      N_VDestroy(content->y);

      if (content->yd) N_VDestroy(content->yd);

      if (IDAADJ_mem->ia_storeSensi) {
        
          N_VDestroyVectorArray(content->yS, IDA_mem->ida_Ns);
        
          if (content->ySd)
            N_VDestroyVectorArray(content->ySd, IDA_mem->ida_Ns);
      }
      free(dt_mem[i]->content); dt_mem[i]->content = NULL;
    }

  }
  return(allocOK);
}

/*
 * IDAApolynomialFree
 *
 * This routine frees the memory allocated for data storage.
 */

static void IDAApolynomialFree(IDAMem IDA_mem)
{
  IDAadjMem IDAADJ_mem;
  DtpntMem *dt_mem;
  PolynomialDataMem content;
  long int i;

  IDAADJ_mem = IDA_mem->ida_adj_mem;

  N_VDestroy(IDAADJ_mem->ia_yyTmp);
  N_VDestroy(IDAADJ_mem->ia_ypTmp);

  if (IDAADJ_mem->ia_storeSensi) {
    N_VDestroyVectorArray(IDAADJ_mem->ia_yySTmp, IDA_mem->ida_Ns);
    N_VDestroyVectorArray(IDAADJ_mem->ia_ypSTmp, IDA_mem->ida_Ns);
  }

  dt_mem = IDAADJ_mem->dt_mem;

  for (i=0; i<=IDAADJ_mem->ia_nsteps; i++) {

    content = (PolynomialDataMem) (dt_mem[i]->content);

    /* content might be NULL, if IDAAdjInit was called but IDASolveF was not. */
    if(content) {
      N_VDestroy(content->y);

      if (content->yd) N_VDestroy(content->yd);

      if (IDAADJ_mem->ia_storeSensi) {
        
        N_VDestroyVectorArray(content->yS, IDA_mem->ida_Ns);
        
        if (content->ySd)
          N_VDestroyVectorArray(content->ySd, IDA_mem->ida_Ns);
      }
      free(dt_mem[i]->content); dt_mem[i]->content = NULL;
    }
  }
}

/*
 * IDAApolynomialStorePnt
 *
 * This routine stores a new point y in the structure d for use
 * in the Polynomial interpolation.
 *
 * Note that the time is already stored. Information about the 
 * first derivative is available only for the first data point, 
 * in which case content->yp is non-null.
 */

static int IDAApolynomialStorePnt(IDAMem IDA_mem, DtpntMem d)
{
  IDAadjMem IDAADJ_mem;
  PolynomialDataMem content;
  int is, retval;

  IDAADJ_mem = IDA_mem->ida_adj_mem;
  content = (PolynomialDataMem) d->content;

  N_VScale(ONE, IDA_mem->ida_phi[0], content->y);

  /* copy also the derivative for the first data point (in this case
     content->yp is non-null). */
  if (content->yd)
    IDAAGettnSolutionYp(IDA_mem, content->yd);

  if (IDAADJ_mem->ia_storeSensi) {
    
    for (is=0; is<IDA_mem->ida_Ns; is++)
      IDA_mem->ida_cvals[is] = ONE;

    retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                                 IDA_mem->ida_phiS[0], content->yS);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
    
    /* store the derivative if it is the first data point. */
    if(content->ySd)
      IDAAGettnSolutionYpS(IDA_mem, content->ySd);
  }

  content->order = IDA_mem->ida_kused;

  return(0);
}

/*
 * IDAApolynomialGetY
 *
 * This routine uses polynomial interpolation for the forward solution vector. 
 * It is typically called by the wrapper routines before calling
 * user provided routines (fB, djacB, bjacB, jtimesB, psolB)) but
 * can be directly called by the user through CVodeGetAdjY.
 */

static int IDAApolynomialGetY(IDAMem IDA_mem, realtype t,
                              N_Vector yy, N_Vector yp,
                              N_Vector *yyS, N_Vector *ypS)
{
  IDAadjMem IDAADJ_mem;
  DtpntMem *dt_mem;
  PolynomialDataMem content;

  int flag, dir, order, i, j, is, NS, retval;
  long int indx, base;
  booleantype newpoint;
  realtype delt, factor, Psi, Psiprime;

  IDAADJ_mem = IDA_mem->ida_adj_mem;
  dt_mem = IDAADJ_mem->dt_mem;
 
  /* Local value of Ns */
  NS = (IDAADJ_mem->ia_interpSensi && (yyS != NULL)) ? IDA_mem->ida_Ns : 0;

  /* Get the index in dt_mem */
  flag = IDAAfindIndex(IDA_mem, t, &indx, &newpoint);
  if (flag != IDA_SUCCESS) return(flag);

  /* If we are beyond the left limit but close enough,
     then return y at the left limit. */

  if (indx == 0) {
    content = (PolynomialDataMem) (dt_mem[0]->content);
    N_VScale(ONE, content->y,  yy);
    N_VScale(ONE, content->yd, yp);

    if (NS > 0) {
      for (is=0; is<NS; is++)
        IDA_mem->ida_cvals[is] = ONE;

      retval = N_VScaleVectorArray(NS, IDA_mem->ida_cvals, content->yS, yyS);
      if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

      retval = N_VScaleVectorArray(NS, IDA_mem->ida_cvals, content->ySd, ypS);
      if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
    }

    return(IDA_SUCCESS);
  }

  /* Scaling factor */
  delt = SUNRabs(dt_mem[indx]->t - dt_mem[indx-1]->t);

  /* Find the direction of the forward integration */
  dir = (IDAADJ_mem->ia_tfinal - IDAADJ_mem->ia_tinitial > ZERO) ? 1 : -1;

  /* Establish the base point depending on the integration direction.
     Modify the base if there are not enough points for the current order */

  if (dir == 1) {
    base = indx;
    content = (PolynomialDataMem) (dt_mem[base]->content);
    order = content->order;
    if(indx < order) base += order-indx;
  } else {
    base = indx-1;
    content = (PolynomialDataMem) (dt_mem[base]->content);
    order = content->order;
    if (IDAADJ_mem->ia_np-indx > order) base -= indx+order-IDAADJ_mem->ia_np;
  }

  /* Recompute Y (divided differences for Newton polynomial) if needed */

  if (newpoint) {

    /* Store 0-th order DD */
    if (dir == 1) {
      for(j=0;j<=order;j++) {
        IDAADJ_mem->ia_T[j] = dt_mem[base-j]->t;
        content = (PolynomialDataMem) (dt_mem[base-j]->content);
        N_VScale(ONE, content->y, IDAADJ_mem->ia_Y[j]);

        if (NS > 0) {
          for (is=0; is<NS; is++)
            IDA_mem->ida_cvals[is] = ONE;
          retval = N_VScaleVectorArray(NS, IDA_mem->ida_cvals,
                                       content->yS, IDAADJ_mem->ia_YS[j]);
          if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
        }
      }
    } else {
      for(j=0;j<=order;j++) {
        IDAADJ_mem->ia_T[j] = dt_mem[base-1+j]->t;
        content = (PolynomialDataMem) (dt_mem[base-1+j]->content);
        N_VScale(ONE, content->y, IDAADJ_mem->ia_Y[j]);

        if (NS > 0) {
          for (is=0; is<NS; is++)
            IDA_mem->ida_cvals[is] = ONE;
          retval = N_VScaleVectorArray(NS, IDA_mem->ida_cvals, 
                                       content->yS, IDAADJ_mem->ia_YS[j]);
          if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
        }
      }
    }

    /* Compute higher-order DD */
    for(i=1;i<=order;i++) {
      for(j=order;j>=i;j--) {
        factor = delt/(IDAADJ_mem->ia_T[j]-IDAADJ_mem->ia_T[j-i]);
        N_VLinearSum(factor, IDAADJ_mem->ia_Y[j], -factor, IDAADJ_mem->ia_Y[j-1], IDAADJ_mem->ia_Y[j]);

        for (is=0; is<NS; is++) 
          N_VLinearSum(factor, IDAADJ_mem->ia_YS[j][is], -factor, IDAADJ_mem->ia_YS[j-1][is], IDAADJ_mem->ia_YS[j][is]);

      }
    }
  }

  /* Perform the actual interpolation for yy using nested multiplications */

  IDA_mem->ida_cvals[0] = ONE;
  for (i=0; i<order; i++)
    IDA_mem->ida_cvals[i+1] = IDA_mem->ida_cvals[i] * (t-IDAADJ_mem->ia_T[i]) / delt;

  retval = N_VLinearCombination(order+1, IDA_mem->ida_cvals, IDAADJ_mem->ia_Y, yy);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  if (NS > 0) {
    retval = N_VLinearCombinationVectorArray(NS, order+1, IDA_mem->ida_cvals, IDAADJ_mem->ia_YS, yyS);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
  }
  
  /* Perform the actual interpolation for yp.

     Writing p(t) = y0 + (t-t0)*f[t0,t1] + ... + (t-t0)(t-t1)...(t-tn)*f[t0,t1,...tn],
     denote psi_k(t) = (t-t0)(t-t1)...(t-tk).

     The formula used for p'(t) is: 
       - p'(t) = f[t0,t1] + psi_1'(t)*f[t0,t1,t2] + ... + psi_n'(t)*f[t0,t1,...,tn]
     
     We reccursively compute psi_k'(t) from:
       - psi_k'(t) = (t-tk)*psi_{k-1}'(t) + psi_{k-1}

     psi_k is rescaled with 1/delt each time is computed, because the Newton DDs from Y were
     scaled with delt.
  */

  Psi = ONE;
  Psiprime = ZERO; 

  for(i=1; i<=order; i++) {
    factor = (t-IDAADJ_mem->ia_T[i-1])/delt;

    Psiprime = Psi/delt + factor * Psiprime;
    Psi = Psi * factor;

    IDA_mem->ida_cvals[i-1] = Psiprime;
  }

  retval = N_VLinearCombination(order, IDA_mem->ida_cvals, IDAADJ_mem->ia_Y+1, yp);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  if (NS > 0) {
    retval = N_VLinearCombinationVectorArray(NS, order, IDA_mem->ida_cvals, IDAADJ_mem->ia_YS+1, ypS);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);
  }

  return(IDA_SUCCESS);
}

/* 
 * IDAAGettnSolutionYp
 *
 * Evaluates the first derivative of the solution at the last time returned by
 * IDASolve (tretlast).
 * 
 * The function implements the same algorithm as in IDAGetSolution but in the 
 * particular case when  t=tn (i.e. delta=0).
 *
 * This function was implemented to avoid calls to IDAGetSolution which computes 
 * y by doing a loop that is not necessary for this particular situation.
 */

static int IDAAGettnSolutionYp(IDAMem IDA_mem, N_Vector yp)
{
  int j, kord, retval;
  realtype C, D, gam;

  if (IDA_mem->ida_nst==0) {

    /* If no integration was done, return the yp supplied by user.*/
    N_VScale(ONE, IDA_mem->ida_phi[1], yp);

    return(0);
  }

  /* Compute yp as in IDAGetSolution for this particular case when t=tn. */
  
  kord = IDA_mem->ida_kused;
  if(IDA_mem->ida_kused==0) kord=1;
  
  C = ONE; D = ZERO;
  gam = ZERO;
  for (j=1; j <= kord; j++) {
    D = D*gam + C/IDA_mem->ida_psi[j-1];
    C = C*gam;
    gam = IDA_mem->ida_psi[j-1] / IDA_mem->ida_psi[j];

    IDA_mem->ida_dvals[j-1] = D;
  }

  retval = N_VLinearCombination(kord, IDA_mem->ida_dvals,
                                IDA_mem->ida_phi+1, yp);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  return(0);
}


/* 
 * IDAAGettnSolutionYpS
 *
 * Same as IDAAGettnSolutionYp, but for first derivative of the sensitivities.
 *
 */

static int IDAAGettnSolutionYpS(IDAMem IDA_mem, N_Vector *ypS)
{
  int j, kord, is, retval;
  realtype C, D, gam;

  if (IDA_mem->ida_nst==0) {

    /* If no integration was done, return the ypS supplied by user.*/
    for (is=0; is<IDA_mem->ida_Ns; is++)
      IDA_mem->ida_cvals[is] = ONE;

    retval = N_VScaleVectorArray(IDA_mem->ida_Ns, IDA_mem->ida_cvals,
                                 IDA_mem->ida_phiS[1], ypS);
    if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

    return(0);
  }
  
  kord = IDA_mem->ida_kused;
  if(IDA_mem->ida_kused==0) kord=1;
  
  C = ONE; D = ZERO;
  gam = ZERO;
  for (j=1; j <= kord; j++) {
    D = D*gam + C/IDA_mem->ida_psi[j-1];
    C = C*gam;
    gam = IDA_mem->ida_psi[j-1] / IDA_mem->ida_psi[j];
  
    IDA_mem->ida_dvals[j-1] = D;
  }

  retval = N_VLinearCombinationVectorArray(IDA_mem->ida_Ns, kord,
                                           IDA_mem->ida_dvals,
                                           IDA_mem->ida_phiS+1, ypS);
  if (retval != IDA_SUCCESS) return (IDA_VECTOROP_ERR);

  return(0);
}



/*
 * IDAAfindIndex
 *
 * Finds the index in the array of data point strctures such that
 *     dt_mem[indx-1].t <= t < dt_mem[indx].t
 * If indx is changed from the previous invocation, then newpoint = SUNTRUE
 *
 * If t is beyond the leftmost limit, but close enough, indx=0.
 *
 * Returns IDA_SUCCESS if successful and IDA_GETY_BADT if unable to
 * find indx (t is too far beyond limits).
 */

static int IDAAfindIndex(IDAMem ida_mem, realtype t, 
                        long int *indx, booleantype *newpoint)
{
  IDAadjMem IDAADJ_mem;
  IDAMem IDA_mem;
  DtpntMem *dt_mem;
  int sign;
  booleantype to_left, to_right;

  IDA_mem = (IDAMem) ida_mem;
  IDAADJ_mem = IDA_mem->ida_adj_mem;
  dt_mem = IDAADJ_mem->dt_mem;

  *newpoint = SUNFALSE;

  /* Find the direction of integration */
  sign = (IDAADJ_mem->ia_tfinal - IDAADJ_mem->ia_tinitial > ZERO) ? 1 : -1;

  /* If this is the first time we use new data */
  if (IDAADJ_mem->ia_newData) {
    IDAADJ_mem->ia_ilast     = IDAADJ_mem->ia_np-1;
    *newpoint = SUNTRUE;
    IDAADJ_mem->ia_newData   = SUNFALSE;
  }

  /* Search for indx starting from ilast */
  to_left  = ( sign*(t - dt_mem[IDAADJ_mem->ia_ilast-1]->t) < ZERO);
  to_right = ( sign*(t - dt_mem[IDAADJ_mem->ia_ilast]->t)   > ZERO);

  if ( to_left ) {
    /* look for a new indx to the left */

    *newpoint = SUNTRUE;
    
    *indx = IDAADJ_mem->ia_ilast;
    for(;;) {
      if ( *indx == 0 ) break;
      if ( sign*(t - dt_mem[*indx-1]->t) <= ZERO ) (*indx)--;
      else                                         break;
    }

    if ( *indx == 0 )
      IDAADJ_mem->ia_ilast = 1;
    else
      IDAADJ_mem->ia_ilast = *indx;

    if ( *indx == 0 ) {
      /* t is beyond leftmost limit. Is it too far? */  
      if ( SUNRabs(t - dt_mem[0]->t) > FUZZ_FACTOR * IDA_mem->ida_uround ) {
        return(IDA_GETY_BADT);
      }
    }

  } else if ( to_right ) {
    /* look for a new indx to the right */

    *newpoint = SUNTRUE;

    *indx = IDAADJ_mem->ia_ilast;
    for(;;) {
      if ( sign*(t - dt_mem[*indx]->t) > ZERO) (*indx)++;
      else                                     break;
    }

    IDAADJ_mem->ia_ilast = *indx;

  } else {
    /* ilast is still OK */

    *indx = IDAADJ_mem->ia_ilast;

  }
  return(IDA_SUCCESS);
}


/*
 * IDAGetAdjY
 *
 * This routine returns the interpolated forward solution at time t.
 * The user must allocate space for y.
 */

int IDAGetAdjY(void *ida_mem, realtype t, N_Vector yy, N_Vector yp)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  int flag;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAA", "IDAGetAdjY", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;                              
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  flag = IDAADJ_mem->ia_getY(IDA_mem, t, yy, yp, NULL, NULL);

  return(flag);
}

/*=================================================================*/
/*             Wrappers for adjoint system                         */
/*=================================================================*/

/*
 * IDAAres
 *
 * This routine interfaces to the RhsFnB routine provided by
 * the user.
*/

static int IDAAres(realtype tt, 
                   N_Vector yyB, N_Vector ypB, N_Vector rrB, 
                   void *ida_mem)
{
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  IDAMem IDA_mem;
  int flag, retval;

  IDA_mem = (IDAMem) ida_mem;

  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Get the current backward problem. */
  IDAB_mem = IDAADJ_mem->ia_bckpbCrt;

  /* Get forward solution from interpolation. */
  if( IDAADJ_mem->ia_noInterp == SUNFALSE) {
    if (IDAADJ_mem->ia_interpSensi)
      flag = IDAADJ_mem->ia_getY(IDA_mem, tt, IDAADJ_mem->ia_yyTmp, IDAADJ_mem->ia_ypTmp, IDAADJ_mem->ia_yySTmp, IDAADJ_mem->ia_ypSTmp);
    else
      flag = IDAADJ_mem->ia_getY(IDA_mem, tt, IDAADJ_mem->ia_yyTmp, IDAADJ_mem->ia_ypTmp, NULL, NULL);
  
    if (flag != IDA_SUCCESS) {
      IDAProcessError(IDA_mem, -1, "IDAA", "IDAAres", MSGAM_BAD_TINTERP, tt);
      return(-1);
    }
  }

  /* Call the user supplied residual. */
  if(IDAB_mem->ida_res_withSensi) {
    retval = IDAB_mem->ida_resS(tt, IDAADJ_mem->ia_yyTmp, IDAADJ_mem->ia_ypTmp, 
                                IDAADJ_mem->ia_yySTmp, IDAADJ_mem->ia_ypSTmp,
                                yyB, ypB, 
                                rrB, IDAB_mem->ida_user_data);
  }else {
    retval = IDAB_mem->ida_res(tt, IDAADJ_mem->ia_yyTmp, IDAADJ_mem->ia_ypTmp, yyB, ypB, rrB, IDAB_mem->ida_user_data);
  }
  return(retval);
}

/*
 *IDAArhsQ
 *
 * This routine interfaces to the IDAQuadRhsFnB routine provided by
 * the user.
 *
 * It is passed to IDAQuadInit calls for backward problem, so it must
 * be of IDAQuadRhsFn type.
*/

static int IDAArhsQ(realtype tt, 
                    N_Vector yyB, N_Vector ypB,
                    N_Vector resvalQB, void *ida_mem)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  int retval, flag;

  IDA_mem = (IDAMem) ida_mem;
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Get current backward problem. */
  IDAB_mem = IDAADJ_mem->ia_bckpbCrt;

  retval = IDA_SUCCESS;

  /* Get forward solution from interpolation. */
  if (IDAADJ_mem->ia_noInterp == SUNFALSE) {
    if (IDAADJ_mem->ia_interpSensi) {
      flag = IDAADJ_mem->ia_getY(IDA_mem, tt, IDAADJ_mem->ia_yyTmp, IDAADJ_mem->ia_ypTmp, IDAADJ_mem->ia_yySTmp, IDAADJ_mem->ia_ypSTmp);
    } else {
      flag = IDAADJ_mem->ia_getY(IDA_mem, tt, IDAADJ_mem->ia_yyTmp, IDAADJ_mem->ia_ypTmp, NULL, NULL);
    }
    
    if (flag != IDA_SUCCESS) {
      IDAProcessError(IDA_mem, -1, "IDAA", "IDAArhsQ", MSGAM_BAD_TINTERP, tt);
      return(-1);
    }  
  }

  /* Call user's adjoint quadrature RHS routine */
  if (IDAB_mem->ida_rhsQ_withSensi) {
    retval = IDAB_mem->ida_rhsQS(tt, IDAADJ_mem->ia_yyTmp, IDAADJ_mem->ia_ypTmp, IDAADJ_mem->ia_yySTmp, IDAADJ_mem->ia_ypSTmp, 
                                 yyB, ypB, 
                                 resvalQB, IDAB_mem->ida_user_data);
  } else {
    retval = IDAB_mem->ida_rhsQ(tt,
                                IDAADJ_mem->ia_yyTmp, IDAADJ_mem->ia_ypTmp,
                                yyB, ypB,
                                resvalQB, IDAB_mem->ida_user_data);
  }
  return(retval);
}


