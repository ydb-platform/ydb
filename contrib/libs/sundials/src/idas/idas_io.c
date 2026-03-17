/*
 * -----------------------------------------------------------------
 * $Revision$
 * $Date$
 * ----------------------------------------------------------------- 
 * Programmer(s): Radu Serban and Cosmin Petra @ LLNL
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
 * This is the implementation file for the optional inputs and     
 * outputs for the IDAS solver.                                    
 * -----------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>

#include "idas_impl.h"
#include <sundials/sundials_math.h>
#include <sundials/sundials_types.h>

#define ZERO    RCONST(0.0)
#define HALF    RCONST(0.5)
#define ONE     RCONST(1.0)
#define TWOPT5  RCONST(2.5)

/* 
 * =================================================================
 * IDA optional input functions
 * =================================================================
 */

int IDASetErrHandlerFn(void *ida_mem, IDAErrHandlerFn ehfun, void *eh_data)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetErrHandlerFn", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  IDA_mem->ida_ehfun = ehfun;
  IDA_mem->ida_eh_data = eh_data;

  return(IDA_SUCCESS);
}


int IDASetErrFile(void *ida_mem, FILE *errfp)
{
  IDAMem IDA_mem;
  
  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetErrFile", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  IDA_mem->ida_errfp = errfp;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetUserData(void *ida_mem, void *user_data)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetUserData", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  IDA_mem->ida_user_data = user_data;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetMaxOrd(void *ida_mem, int maxord)
{
  IDAMem IDA_mem;
  int maxord_alloc;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetMaxOrd", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (maxord <= 0) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetMaxOrd", MSG_NEG_MAXORD);
    return(IDA_ILL_INPUT);
  }

  /* Cannot increase maximum order beyond the value that
     was used when allocating memory */
  maxord_alloc = IDA_mem->ida_maxord_alloc;

  if (maxord > maxord_alloc) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetMaxOrd", MSG_BAD_MAXORD);
    return(IDA_ILL_INPUT);
  }  

  IDA_mem->ida_maxord = SUNMIN(maxord,MAXORD_DEFAULT);

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetMaxNumSteps(void *ida_mem, long int mxsteps)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetMaxNumSteps", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  /* Passing mxsteps=0 sets the default. Passing mxsteps<0 disables the test. */

  if (mxsteps == 0)
    IDA_mem->ida_mxstep = MXSTEP_DEFAULT;
  else
    IDA_mem->ida_mxstep = mxsteps;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetInitStep(void *ida_mem, realtype hin)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetInitStep", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  IDA_mem->ida_hin = hin;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetMaxStep(void *ida_mem, realtype hmax)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetMaxStep", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (hmax < 0) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetMaxStep", MSG_NEG_HMAX);
    return(IDA_ILL_INPUT);
  }

  /* Passing 0 sets hmax = infinity */
  if (hmax == ZERO) {
    IDA_mem->ida_hmax_inv = HMAX_INV_DEFAULT;
    return(IDA_SUCCESS);
  }

  IDA_mem->ida_hmax_inv = ONE/hmax;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetStopTime(void *ida_mem, realtype tstop)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetStopTime", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  /* If IDASolve was called at least once, test if tstop is legal
   * (i.e. if it was not already passed).
   * If IDASetStopTime is called before the first call to IDASolve,
   * tstop will be checked in IDASolve. */
  if (IDA_mem->ida_nst > 0) {

    if ( (tstop - IDA_mem->ida_tn) * IDA_mem->ida_hh < ZERO ) {
      IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDA", "IDASetStopTime", MSG_BAD_TSTOP, tstop, IDA_mem->ida_tn);
      return(IDA_ILL_INPUT);
    }

  }

  IDA_mem->ida_tstop = tstop;
  IDA_mem->ida_tstopset = SUNTRUE;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetNonlinConvCoef(void *ida_mem, realtype epcon)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetNonlinConvCoef", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (epcon <= ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetNonlinConvCoef", MSG_NEG_EPCON);
    return(IDA_ILL_INPUT);
  }

  IDA_mem->ida_epcon = epcon;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetMaxErrTestFails(void *ida_mem, int maxnef)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetMaxErrTestFails", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  IDA_mem->ida_maxnef = maxnef;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetMaxConvFails(void *ida_mem, int maxncf)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetMaxConvFails", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  IDA_mem->ida_maxncf = maxncf;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetMaxNonlinIters(void *ida_mem, int maxcor)
{
  IDAMem IDA_mem;
  booleantype sensi_sim;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDASetMaxNonlinIters", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  /* Are we computing sensitivities with the simultaneous approach? */
  sensi_sim = (IDA_mem->ida_sensi && (IDA_mem->ida_ism==IDA_SIMULTANEOUS));

  if (sensi_sim) {

    /* check that the NLS is non-NULL */
    if (IDA_mem->NLSsim == NULL) {
      IDAProcessError(NULL, IDA_MEM_FAIL, "IDAS",
                      "IDASetMaxNonlinIters", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    return(SUNNonlinSolSetMaxIters(IDA_mem->NLSsim, maxcor));

  } else {

    /* check that the NLS is non-NULL */
    if (IDA_mem->NLS == NULL) {
      IDAProcessError(NULL, IDA_MEM_FAIL, "IDAS",
                      "IDASetMaxNonlinIters", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    return(SUNNonlinSolSetMaxIters(IDA_mem->NLS, maxcor));
  }
}

/*-----------------------------------------------------------------*/

int IDASetSuppressAlg(void *ida_mem, booleantype suppressalg)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetSuppressAlg", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  IDA_mem->ida_suppressalg = suppressalg;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetId(void *ida_mem, N_Vector id)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetId", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (id == NULL) {
    if (IDA_mem->ida_idMallocDone) {
      N_VDestroy(IDA_mem->ida_id);
      IDA_mem->ida_lrw -= IDA_mem->ida_lrw1;
      IDA_mem->ida_liw -= IDA_mem->ida_liw1;
    }
    IDA_mem->ida_idMallocDone = SUNFALSE;    
    return(IDA_SUCCESS);
  }

  if ( !(IDA_mem->ida_idMallocDone) ) {
    IDA_mem->ida_id = N_VClone(id);
    IDA_mem->ida_lrw += IDA_mem->ida_lrw1;
    IDA_mem->ida_liw += IDA_mem->ida_liw1;
    IDA_mem->ida_idMallocDone = SUNTRUE;
  }

  /* Load the id vector */

  N_VScale(ONE, id, IDA_mem->ida_id);

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetConstraints(void *ida_mem, N_Vector constraints)
{
  IDAMem IDA_mem;
  realtype temptest;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetConstraints", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (constraints == NULL) {
    if (IDA_mem->ida_constraintsMallocDone) {
      N_VDestroy(IDA_mem->ida_constraints);
      IDA_mem->ida_lrw -= IDA_mem->ida_lrw1;
      IDA_mem->ida_liw -= IDA_mem->ida_liw1;
    }
    IDA_mem->ida_constraintsMallocDone = SUNFALSE;
    IDA_mem->ida_constraintsSet = SUNFALSE;
    return(IDA_SUCCESS);
  }

  /* Test if required vector ops. are defined */

  if (constraints->ops->nvdiv         == NULL ||
      constraints->ops->nvmaxnorm     == NULL ||
      constraints->ops->nvcompare     == NULL ||
      constraints->ops->nvconstrmask  == NULL ||
      constraints->ops->nvminquotient == NULL) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetConstraints", MSG_BAD_NVECTOR);
    return(IDA_ILL_INPUT);
  }

  /*  Check the constraints vector */

  temptest = N_VMaxNorm(constraints);
  if((temptest > TWOPT5) || (temptest < HALF)){ 
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetConstraints", MSG_BAD_CONSTR);
    return(IDA_ILL_INPUT); 
  }

  if ( !(IDA_mem->ida_constraintsMallocDone) ) {
    IDA_mem->ida_constraints = N_VClone(constraints);
    IDA_mem->ida_lrw += IDA_mem->ida_lrw1;
    IDA_mem->ida_liw += IDA_mem->ida_liw1;
    IDA_mem->ida_constraintsMallocDone = SUNTRUE;
  }

  /* Load the constraints vector */

  N_VScale(ONE, constraints, IDA_mem->ida_constraints);

  IDA_mem->ida_constraintsSet = SUNTRUE;

  return(IDA_SUCCESS);
}

/* 
 * IDASetRootDirection
 *
 * Specifies the direction of zero-crossings to be monitored.
 * The default is to monitor both crossings.
 */

int IDASetRootDirection(void *ida_mem, int *rootdir)
{
  IDAMem IDA_mem;
  int i, nrt;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetRootDirection", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  nrt = IDA_mem->ida_nrtfn;
  if (nrt==0) {
    IDAProcessError(NULL, IDA_ILL_INPUT, "IDAS", "IDASetRootDirection", MSG_NO_ROOT);
    return(IDA_ILL_INPUT);    
  }

  for(i=0; i<nrt; i++) IDA_mem->ida_rootdir[i] = rootdir[i];

  return(IDA_SUCCESS);
}

/*
 * IDASetNoInactiveRootWarn
 *
 * Disables issuing a warning if some root function appears
 * to be identically zero at the beginning of the integration
 */

int IDASetNoInactiveRootWarn(void *ida_mem)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetNoInactiveRootWarn", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  IDA_mem->ida_mxgnull = 0;
  
  return(IDA_SUCCESS);
}


/* 
 * =================================================================
 * IDA IC optional input functions
 * =================================================================
 */

int IDASetNonlinConvCoefIC(void *ida_mem, realtype epiccon)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetNonlinConvCoefIC", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (epiccon <= ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetNonlinConvCoefIC", MSG_BAD_EPICCON);
    return(IDA_ILL_INPUT);
  }

  IDA_mem->ida_epiccon = epiccon;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetMaxNumStepsIC(void *ida_mem, int maxnh)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetMaxNumStepsIC", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (maxnh <= 0) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetMaxNumStepsIC", MSG_BAD_MAXNH);
    return(IDA_ILL_INPUT);
  }

  IDA_mem->ida_maxnh = maxnh;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetMaxNumJacsIC(void *ida_mem, int maxnj)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetMaxNumJacsIC", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

   if (maxnj <= 0) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetMaxNumJacsIC", MSG_BAD_MAXNJ);
    return(IDA_ILL_INPUT);
  } 

  IDA_mem->ida_maxnj = maxnj;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetMaxNumItersIC(void *ida_mem, int maxnit)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetMaxNumItersIC", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (maxnit <= 0) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetMaxNumItersIC", MSG_BAD_MAXNIT);
    return(IDA_ILL_INPUT);
  }

  IDA_mem->ida_maxnit = maxnit;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetMaxBacksIC(void *ida_mem, int maxbacks)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDA", "IDASetMaxBacksIC", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (maxbacks <= 0) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDA", "IDASetMaxBacksIC", MSG_IC_BAD_MAXBACKS);
    return(IDA_ILL_INPUT);
  }

  IDA_mem->ida_maxbacks = maxbacks;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetLineSearchOffIC(void *ida_mem, booleantype lsoff)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetLineSearchOffIC", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  IDA_mem->ida_lsoff = lsoff;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetStepToleranceIC(void *ida_mem, realtype steptol)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetStepToleranceIC", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (steptol <= ZERO) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetStepToleranceIC", MSG_BAD_STEPTOL);
    return(IDA_ILL_INPUT);
  }

  IDA_mem->ida_steptol = steptol;

  return(IDA_SUCCESS);
}

/* 
 * =================================================================
 * Quadrature optional input functions
 * =================================================================
 */

/*-----------------------------------------------------------------*/

int IDASetQuadErrCon(void *ida_mem, booleantype errconQ)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetQuadErrCon", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }  
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_quadMallocDone == SUNFALSE) {
    IDAProcessError(NULL, IDA_NO_QUAD, "IDAS", "IDASetQuadErrCon", MSG_NO_QUAD);    
    return(IDA_NO_QUAD);
  }

  IDA_mem->ida_errconQ = errconQ;

  return (IDA_SUCCESS);
}

/* 
 * =================================================================
 * FSA optional input functions
 * =================================================================
 */

int IDASetSensDQMethod(void *ida_mem, int DQtype, realtype DQrhomax)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetSensDQMethod", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if ( (DQtype != IDA_CENTERED) && (DQtype != IDA_FORWARD) ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetSensDQMethod", MSG_BAD_DQTYPE);    
    return(IDA_ILL_INPUT);
  }

  if (DQrhomax < ZERO ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetSensDQMethod", MSG_BAD_DQRHO);    
    return(IDA_ILL_INPUT);
  }

  IDA_mem->ida_DQtype = DQtype;
  IDA_mem->ida_DQrhomax = DQrhomax;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetSensErrCon(void *ida_mem, booleantype errconS)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetSensErrCon", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;
  IDA_mem->ida_errconS = errconS;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDASetSensMaxNonlinIters(void *ida_mem, int maxcorS)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDASetSensMaxNonlinIters", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  /* check that the NLS is non-NULL */
  if (IDA_mem->NLSstg == NULL) {
    IDAProcessError(NULL, IDA_MEM_FAIL, "IDAS",
                    "IDASetSensMaxNonlinIters", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  return(SUNNonlinSolSetMaxIters(IDA_mem->NLSstg, maxcorS));
}

/*-----------------------------------------------------------------*/

int IDASetSensParams(void *ida_mem, realtype *p, realtype *pbar, int *plist)
{
  IDAMem IDA_mem;
  int Ns, is;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetSensParams", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  /* Was sensitivity initialized? */

  if (IDA_mem->ida_sensMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDASetSensParams", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  Ns = IDA_mem->ida_Ns;

  /* Parameters */

  IDA_mem->ida_p = p;

  /* pbar */

  if (pbar != NULL)
    for (is=0; is<Ns; is++) {
      if (pbar[is] == ZERO) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetSensParams", MSG_BAD_PBAR);
        return(IDA_ILL_INPUT);
      }
      IDA_mem->ida_pbar[is] = SUNRabs(pbar[is]);
    }
  else
    for (is=0; is<Ns; is++)
      IDA_mem->ida_pbar[is] = ONE;

  /* plist */

  if (plist != NULL)
    for (is=0; is<Ns; is++) {
      if ( plist[is] < 0 ) {
        IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDASetSensParams", MSG_BAD_PLIST);
        return(IDA_ILL_INPUT);
      }
      IDA_mem->ida_plist[is] = plist[is];
    }
  else
    for (is=0; is<Ns; is++)
      IDA_mem->ida_plist[is] = is;

  return(IDA_SUCCESS);
}

/*
 * -----------------------------------------------------------------
 * Function: IDASetQuadSensErrCon 
 * -----------------------------------------------------------------
 * IDASetQuadSensErrCon specifies if quadrature sensitivity variables
 * are considered or not in the error control.
 * -----------------------------------------------------------------
 */
int IDASetQuadSensErrCon(void *ida_mem, booleantype errconQS)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDASetQuadSensErrCon", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Was sensitivity initialized? */
  if (IDA_mem->ida_sensMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDASetQuadSensErrCon", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  /* Was quadrature sensitivity initialized? */
  if (IDA_mem->ida_quadSensMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDASetQuadSensErrCon", MSG_NO_SENSI);
    return(IDA_NO_QUADSENS);
  }

  IDA_mem->ida_errconQS = errconQS;

  return(IDA_SUCCESS);
}

/* 
 * =================================================================
 * IDA optional output functions
 * =================================================================
 */

int IDAGetNumSteps(void *ida_mem, long int *nsteps)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetNumSteps", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *nsteps = IDA_mem->ida_nst;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetNumResEvals(void *ida_mem, long int *nrevals)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetNumResEvals", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *nrevals = IDA_mem->ida_nre;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetNumLinSolvSetups(void *ida_mem, long int *nlinsetups)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetNumLinSolvSetups", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *nlinsetups = IDA_mem->ida_nsetups;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetNumErrTestFails(void *ida_mem, long int *netfails)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetNumErrTestFails", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *netfails = IDA_mem->ida_netf;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetNumBacktrackOps(void *ida_mem, long int *nbacktracks)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetNumBacktrackOps", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *nbacktracks = IDA_mem->ida_nbacktr;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetConsistentIC(void *ida_mem, N_Vector yy0, N_Vector yp0)
{
  IDAMem IDA_mem;
  
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetConsistentIC", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem; 

  if (IDA_mem->ida_kused != 0) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAGetConsistentIC", MSG_TOO_LATE);
    return(IDA_ILL_INPUT);
  }

  if(yy0 != NULL) N_VScale(ONE, IDA_mem->ida_phi[0], yy0);
  if(yp0 != NULL) N_VScale(ONE, IDA_mem->ida_phi[1], yp0);

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetLastOrder(void *ida_mem, int *klast)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetLastOrder", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *klast = IDA_mem->ida_kused;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetCurrentOrder(void *ida_mem, int *kcur)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetCurrentOrder", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *kcur = IDA_mem->ida_kk;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetActualInitStep(void *ida_mem, realtype *hinused)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetActualInitStep", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *hinused = IDA_mem->ida_h0u;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetLastStep(void *ida_mem, realtype *hlast)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetLastStep", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *hlast = IDA_mem->ida_hused;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetCurrentStep(void *ida_mem, realtype *hcur)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetCurrentStep", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *hcur = IDA_mem->ida_hh;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetCurrentTime(void *ida_mem, realtype *tcur)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetCurrentTime", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *tcur = IDA_mem->ida_tn;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetTolScaleFactor(void *ida_mem, realtype *tolsfact)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetTolScaleFactor", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *tolsfact = IDA_mem->ida_tolsf;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetErrWeights(void *ida_mem, N_Vector eweight)
{
  IDAMem IDA_mem;
  
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetErrWeights", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem; 

  N_VScale(ONE, IDA_mem->ida_ewt, eweight);

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetEstLocalErrors(void *ida_mem, N_Vector ele)
{
  IDAMem IDA_mem;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetEstLocalErrors", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  N_VScale(ONE, IDA_mem->ida_ee, ele);

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetWorkSpace(void *ida_mem, long int *lenrw, long int *leniw)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetWorkSpace", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *leniw = IDA_mem->ida_liw;
  *lenrw = IDA_mem->ida_lrw;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetIntegratorStats(void *ida_mem, long int *nsteps, long int *nrevals, 
                          long int *nlinsetups, long int *netfails,
                          int *klast, int *kcur, realtype *hinused, realtype *hlast, 
                          realtype *hcur, realtype *tcur)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetIntegratorStats", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *nsteps     = IDA_mem->ida_nst;
  *nrevals    = IDA_mem->ida_nre;
  *nlinsetups = IDA_mem->ida_nsetups;
  *netfails   = IDA_mem->ida_netf;
  *klast      = IDA_mem->ida_kused;
  *kcur       = IDA_mem->ida_kk;
  *hinused    = IDA_mem->ida_h0u;
  *hlast      = IDA_mem->ida_hused;
  *hcur       = IDA_mem->ida_hh;  
  *tcur       = IDA_mem->ida_tn;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetNumGEvals(void *ida_mem, long int *ngevals)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetNumGEvals", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *ngevals = IDA_mem->ida_nge;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetRootInfo(void *ida_mem, int *rootsfound)
{
  IDAMem IDA_mem;
  int i, nrt;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetRootInfo", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  nrt = IDA_mem->ida_nrtfn;

  for (i=0; i<nrt; i++)
    rootsfound[i] = IDA_mem->ida_iroots[i];

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetNumNonlinSolvIters(void *ida_mem, long int *nniters)
{
  IDAMem IDA_mem;
  long int nls_iters;
  booleantype sensi_sim;
  int retval;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDAGetNumNonlinSolvIters", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  /* get number of iterations for IC calc */
  *nniters = IDA_mem->ida_nni;

  /* are we computing sensitivities with the simultaneous approach? */
  sensi_sim = (IDA_mem->ida_sensi && (IDA_mem->ida_ism==IDA_SIMULTANEOUS));

  /* get number of iterations from the NLS */
  if (sensi_sim) {

    /* check that the NLS is non-NULL */
    if (IDA_mem->NLSsim == NULL) {
      IDAProcessError(NULL, IDA_MEM_FAIL, "IDAS",
                      "IDAGetNumNonlinSolvIters", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    retval = SUNNonlinSolGetNumIters(IDA_mem->NLSsim, &nls_iters);
    if (retval != IDA_SUCCESS) return(retval);

  } else {

    /* check that the NLS is non-NULL */
    if (IDA_mem->NLS == NULL) {
      IDAProcessError(NULL, IDA_MEM_FAIL, "IDAS",
                      "IDAGetNumNonlinSolvIters", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    retval = SUNNonlinSolGetNumIters(IDA_mem->NLS, &nls_iters);
    if (retval != IDA_SUCCESS) return(retval);
  }

  /* update the number of nonlinear iterations */
  *nniters += nls_iters;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetNumNonlinSolvConvFails(void *ida_mem, long int *nncfails)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetNumNonlinSolvConvFails", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *nncfails = IDA_mem->ida_ncfn;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetNonlinSolvStats(void *ida_mem, long int *nniters, long int *nncfails)
{
  IDAMem IDA_mem;
  long int nls_iters;
  booleantype sensi_sim;
  int retval;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDAGetNonlinSolvStats", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  *nniters  = IDA_mem->ida_nni;
  *nncfails = IDA_mem->ida_ncfn;

  /* Are we computing sensitivities with the simultaneous approach? */
  sensi_sim = (IDA_mem->ida_sensi && (IDA_mem->ida_ism==IDA_SIMULTANEOUS));

  /* get number of iterations from NLS */
  if (sensi_sim) {

    if (IDA_mem->NLSsim == NULL) {
      IDAProcessError(NULL, IDA_MEM_FAIL, "IDAS",
                      "IDAGetNonlinSolvStats", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    retval = SUNNonlinSolGetNumIters(IDA_mem->NLSsim, &nls_iters);
    if (retval != IDA_SUCCESS) return(retval);

  } else {

    if (IDA_mem->NLS == NULL) {
      IDAProcessError(NULL, IDA_MEM_FAIL, "IDAS",
                      "IDAGetNonlinSolvStats", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    retval = SUNNonlinSolGetNumIters(IDA_mem->NLS, &nls_iters);
    if (retval != IDA_SUCCESS) return(retval);

  }

  /* update the number of nonlinear iterations */
  *nniters += nls_iters;

  return(IDA_SUCCESS);
}

/* 
 * =================================================================
 * Quadrature optional output functions
 * =================================================================
 */

/*-----------------------------------------------------------------*/

int IDAGetQuadNumRhsEvals(void *ida_mem, long int *nrQevals)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadNumRhsEvals", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_quadr==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUAD, "IDAS", "IDAGetQuadNumRhsEvals", MSG_NO_QUAD); 
    return(IDA_NO_QUAD);
  }

  *nrQevals = IDA_mem->ida_nrQe;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetQuadNumErrTestFails(void *ida_mem, long int *nQetfails)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadNumErrTestFails", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_quadr==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUAD, "IDAS", "IDAGetQuadNumErrTestFails", MSG_NO_QUAD); 
    return(IDA_NO_QUAD);
  }

  *nQetfails = IDA_mem->ida_netfQ;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetQuadErrWeights(void *ida_mem, N_Vector eQweight)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadErrWeights", MSG_NO_MEM); 
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_quadr==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUAD, "IDAS", "IDAGetQuadErrWeights", MSG_NO_QUAD); 
    return(IDA_NO_QUAD);
  }

  if(IDA_mem->ida_errconQ)
    N_VScale(ONE, IDA_mem->ida_ewtQ, eQweight);

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetQuadStats(void *ida_mem, long int *nrQevals, long int *nQetfails)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadStats", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_quadr==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUAD, "IDAS", "IDAGetQuadStats", MSG_NO_QUAD); 
    return(IDA_NO_QUAD);
  }

  *nrQevals = IDA_mem->ida_nrQe;
  *nQetfails = IDA_mem->ida_netfQ;

  return(IDA_SUCCESS);
}


/* 
 * =================================================================
 * Quadrature FSA optional output functions
 * =================================================================
 */

/*-----------------------------------------------------------------*/

int IDAGetQuadSensNumRhsEvals(void *ida_mem, long int *nrhsQSevals)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadSensNumRhsEvals", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_quadr_sensi == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAGetQuadSensNumRhsEvals", MSG_NO_QUADSENSI); 
    return(IDA_NO_QUADSENS);
  }

  *nrhsQSevals = IDA_mem->ida_nrQSe;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetQuadSensNumErrTestFails(void *ida_mem, long int *nQSetfails)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadSensNumErrTestFails", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_quadr_sensi == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAGetQuadSensNumErrTestFails", MSG_NO_QUADSENSI); 
    return(IDA_NO_QUADSENS);
  }

  *nQSetfails = IDA_mem->ida_netfQS;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetQuadSensErrWeights(void *ida_mem, N_Vector *eQSweight)
{
  IDAMem IDA_mem;
  int is, Ns;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadSensErrWeights", MSG_NO_MEM); 
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_quadr_sensi == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAGetQuadSensErrWeights", MSG_NO_QUADSENSI); 
    return(IDA_NO_QUADSENS);
  }
  Ns = IDA_mem->ida_Ns;

  if (IDA_mem->ida_errconQS)
    for (is=0; is<Ns; is++)
      N_VScale(ONE, IDA_mem->ida_ewtQS[is], eQSweight[is]);

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetQuadSensStats(void *ida_mem, long int *nrhsQSevals, long int *nQSetfails)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetQuadSensStats", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_quadr_sensi == SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_QUADSENS, "IDAS", "IDAGetQuadSensStats", MSG_NO_QUADSENSI); 
    return(IDA_NO_QUADSENS);
  }

  *nrhsQSevals = IDA_mem->ida_nrQSe;
  *nQSetfails = IDA_mem->ida_netfQS;

  return(IDA_SUCCESS);
}



/* 
 * =================================================================
 * FSA optional output functions
 * =================================================================
 */

/*-----------------------------------------------------------------*/

int IDAGetSensConsistentIC(void *ida_mem, N_Vector *yyS0, N_Vector *ypS0)
{
  IDAMem IDA_mem;
  int is;
 
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSensConsistentIC", MSG_NO_MEM);
    return (IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem; 

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetSensConsistentIC", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  if (IDA_mem->ida_kused != 0) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "IDAGetSensConsistentIC", MSG_TOO_LATE);
    return(IDA_ILL_INPUT);
  }

  if(yyS0 != NULL) {
    for (is=0; is<IDA_mem->ida_Ns; is++)
      N_VScale(ONE, IDA_mem->ida_phiS[0][is], yyS0[is]);
  }

  if(ypS0 != NULL) {
    for (is=0; is<IDA_mem->ida_Ns; is++)
      N_VScale(ONE, IDA_mem->ida_phiS[1][is], ypS0[is]);
  }

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetSensNumResEvals(void *ida_mem, long int *nrSevals)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGeSensNumResEvals", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetSensNumResEvals", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  *nrSevals = IDA_mem->ida_nrSe;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetNumResEvalsSens(void *ida_mem, long int *nrevalsS)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetNumResEvalsSens", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetNumResEvalsSens", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  *nrevalsS = IDA_mem->ida_nreS;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetSensNumErrTestFails(void *ida_mem, long int *nSetfails)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSensNumErrTestFails", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetSensNumErrTestFails", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  *nSetfails = IDA_mem->ida_netfS;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetSensNumLinSolvSetups(void *ida_mem, long int *nlinsetupsS)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSensNumLinSolvSetups", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetSensNumLinSolvSetups", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  *nlinsetupsS = IDA_mem->ida_nsetupsS;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetSensErrWeights(void *ida_mem, N_Vector_S eSweight)
{
  IDAMem IDA_mem;
  int is;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSensErrWeights", MSG_NO_MEM);    
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetSensErrWeights", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  for (is=0; is<IDA_mem->ida_Ns; is++)
    N_VScale(ONE, IDA_mem->ida_ewtS[is], eSweight[is]);

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetSensStats(void *ida_mem, long int *nrSevals, long int *nrevalsS, 
                      long int *nSetfails, long int *nlinsetupsS)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSensStats", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetSensStats", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  *nrSevals = IDA_mem->ida_nrSe;
  *nrevalsS = IDA_mem->ida_nreS;
  *nSetfails = IDA_mem->ida_netfS;
  *nlinsetupsS = IDA_mem->ida_nsetupsS;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetSensNumNonlinSolvIters(void *ida_mem, long int *nSniters)
{
  IDAMem IDA_mem;
  long int nls_iters;
  int retval;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDAGetSensNumNonlinSolvIters", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS",
                    "IDAGetSensNumNonlinSolvIters", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  *nSniters = IDA_mem->ida_nniS;

  /* check that the NLS is non-NULL */
  if (IDA_mem->NLSstg == NULL) {
    IDAProcessError(NULL, IDA_MEM_FAIL, "IDAS",
                    "IDAGetSensNumNonlinSolvIters", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  /* get number of iterations from the NLS */
  retval = SUNNonlinSolGetNumIters(IDA_mem->NLSstg, &nls_iters);
  if (retval != IDA_SUCCESS) return(retval);

  /* update the number of nonlinear iterations */
  *nSniters += nls_iters;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetSensNumNonlinSolvConvFails(void *ida_mem, long int *nSncfails)
{
  IDAMem IDA_mem;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "IDAGetSensNumNonlinSolvConvFails", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS", "IDAGetSensNumNonlinSolvConvFails", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  *nSncfails = IDA_mem->ida_ncfnS;

  return(IDA_SUCCESS);
}

/*-----------------------------------------------------------------*/

int IDAGetSensNonlinSolvStats(void *ida_mem, long int *nSniters, long int *nSncfails)
{
  IDAMem IDA_mem;
  long int nls_iters;
  int retval;

  if (ida_mem==NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDAGetSensNonlinSolvstats", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }

  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_sensi==SUNFALSE) {
    IDAProcessError(IDA_mem, IDA_NO_SENS, "IDAS",
                    "IDAGetSensNonlinSolvStats", MSG_NO_SENSI);
    return(IDA_NO_SENS);
  }

  *nSniters = IDA_mem->ida_nniS;
  *nSncfails = IDA_mem->ida_ncfnS;

  /* check that the NLS is non-NULL */
  if (IDA_mem->NLSstg == NULL) {
    IDAProcessError(NULL, IDA_MEM_FAIL, "IDAS",
                    "IDAGetSensNumNonlinSolvStats", MSG_MEM_FAIL);
    return(IDA_MEM_FAIL);
  }

  /* get number of iterations from the NLS */
  retval = SUNNonlinSolGetNumIters(IDA_mem->NLSstg, &nls_iters);
  if (retval != IDA_SUCCESS) return(retval);

  /* update the number of nonlinear iterations */
  *nSniters += nls_iters;

  return(IDA_SUCCESS);
}

/*
 * =================================================================
 * IDAGetReturnFlagName
 * =================================================================
 */


char *IDAGetReturnFlagName(long int flag)
{
  char *name;

  name = (char *)malloc(24*sizeof(char));

  switch(flag) {
  case IDA_SUCCESS:
    sprintf(name,"IDA_SUCCESS");
    break;
  case IDA_TSTOP_RETURN:
    sprintf(name,"IDA_TSTOP_RETURN");
    break;
  case IDA_ROOT_RETURN:
    sprintf(name,"IDA_ROOT_RETURN");
    break;
  case IDA_TOO_MUCH_WORK:
    sprintf(name,"IDA_TOO_MUCH_WORK");
    break;
  case IDA_TOO_MUCH_ACC:
    sprintf(name,"IDA_TOO_MUCH_ACC");
    break;
  case IDA_ERR_FAIL:
    sprintf(name,"IDA_ERR_FAIL");
    break;
  case IDA_CONV_FAIL:
    sprintf(name,"IDA_CONV_FAIL");
    break;
  case IDA_LINIT_FAIL:
    sprintf(name,"IDA_LINIT_FAIL");
    break;
  case IDA_LSETUP_FAIL:
    sprintf(name,"IDA_LSETUP_FAIL");
    break;
  case IDA_LSOLVE_FAIL:
    sprintf(name,"IDA_LSOLVE_FAIL");
    break;
  case IDA_CONSTR_FAIL:
    sprintf(name,"IDA_CONSTR_FAIL");
    break;
  case IDA_RES_FAIL:
    sprintf(name,"IDA_RES_FAIL");
    break;
  case IDA_FIRST_RES_FAIL:
    sprintf(name,"IDA_FIRST_RES_FAIL");
    break;
  case IDA_REP_RES_ERR:
    sprintf(name,"IDA_REP_RES_ERR");
    break;
  case IDA_RTFUNC_FAIL:
    sprintf(name,"IDA_RTFUNC_FAIL");
    break;
  case IDA_MEM_FAIL:
    sprintf(name,"IDA_MEM_FAIL");
    break;
  case IDA_MEM_NULL:
    sprintf(name,"IDA_MEM_NULL");
    break;
  case IDA_ILL_INPUT:
    sprintf(name,"IDA_ILL_INPUT");
    break;
  case IDA_NO_MALLOC:
    sprintf(name,"IDA_NO_MALLOC");
    break;
  case IDA_BAD_T:
    sprintf(name,"IDA_BAD_T");
    break;
  case IDA_BAD_K:
    sprintf(name,"IDA_BAD_K");
    break;
  case IDA_BAD_DKY:
    sprintf(name,"IDA_BAD_DKY");
    break;
  case IDA_BAD_EWT:
    sprintf(name,"IDA_BAD_EWT");
    break;
  case IDA_NO_RECOVERY:
    sprintf(name,"IDA_NO_RECOVERY");
    break;
  case IDA_LINESEARCH_FAIL:
    sprintf(name,"IDA_LINESEARCH_FAIL");
    break;
  case IDA_NO_SENS:
    sprintf(name,"IDA_NO_SENS");
    break;
  case IDA_SRES_FAIL:
    sprintf(name, "IDA_SRES_FAIL");
    break;
  case IDA_REP_SRES_ERR:
    sprintf(name, "IDA_REP_SRES_ERR");
    break;
  case IDA_BAD_IS:
    sprintf(name,"IDA_BAD_IS");
    break;
  case IDA_NO_QUAD:
    sprintf(name,"IDA_NO_QUAD");
    break;
  case IDA_NO_QUADSENS:
    sprintf(name, "IDA_NO_QUADSENS");
    break;
  case IDA_QSRHS_FAIL:
    sprintf(name, "IDA_QSRHS_FAIL");
    break;

    /* IDAA flags follow below. */
  case IDA_NO_ADJ:
    sprintf(name, "IDA_NO_ADJ");
    break;
  case IDA_BAD_TB0:
    sprintf(name, "IDA_BAD_TB0");
    break;
  case IDA_REIFWD_FAIL:
    sprintf(name, "IDA_REIFWD_FAIL");
    break;
  case IDA_FWD_FAIL:
    sprintf(name, "IDA_FWD_FAIL");
    break;
  case IDA_GETY_BADT:
    sprintf(name, "IDA_GETY_BADT");
    break;
  case IDA_NO_BCK:
    sprintf(name, "IDA_NO_BCK");
    break;
  case IDA_NO_FWD:
    sprintf(name,"IDA_NO_FWD");
    break;
  default:
    sprintf(name,"NONE");
  }

  return(name);
}

