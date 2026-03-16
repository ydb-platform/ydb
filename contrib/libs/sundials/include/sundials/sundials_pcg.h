/*---------------------------------------------------------------
 Programmer(s): Daniel R. Reynolds @ SMU
 ----------------------------------------------------------------
 LLNS/SMU Copyright Start
 Copyright (c) 2002-2018, Southern Methodist University and
 Lawrence Livermore National Security
 
 This work was performed under the auspices of the U.S. Department
 of Energy by Southern Methodist University and Lawrence Livermore
 National Laboratory under Contract DE-AC52-07NA27344.
 Produced at Southern Methodist University and the Lawrence
 Livermore National Laboratory.
 
 All rights reserved.
 For details, see the LICENSE file.
 LLNS/SMU Copyright End
 ----------------------------------------------------------------
 This is the header for the preconditioned conjugate gradient
 solver in SUNDIALS.
 ---------------------------------------------------------------*/

#ifndef _PCG_H
#define _PCG_H

#include <sundials/sundials_iterative.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/*---------------------------------------------------------------
 Types: struct PcgMemRec and struct *PcgMem
 ----------------------------------------------------------------
 A variable declaration of type struct *PcgMem denotes a pointer
 to a data structure of type struct PcgMemRec. The PcgMemRec
 structure contains numerous fields that must be accessed by the
 PCG linear solver module.
  * l_max  maximum Krylov subspace dimension that PcgSolve will
       be permitted to use
  * r  vector (type N_Vector) which holds the preconditioned
       linear system residual
  * p, z and Ap vectors (type N_Vector) used for workspace by
       the PCG algorithm
 --------------------------------------------------------------*/
typedef struct {
  int l_max;
  N_Vector r;
  N_Vector p;
  N_Vector z;
  N_Vector Ap;
} PcgMemRec, *PcgMem;

/*---------------------------------------------------------------
 Function : PcgMalloc
 ----------------------------------------------------------------
 PcgMalloc allocates additional memory needed by the PCG linear
 solver module.

   l_max  maximum Krylov subspace dimension that PcgSolve will
          be permitted to use

   vec_tmpl implementation-specific template vector (of type
          N_Vector)

 If successful, PcgMalloc returns a non-NULL memory pointer. If
 an error occurs, then a NULL pointer is returned.
 --------------------------------------------------------------*/

SUNDIALS_EXPORT PcgMem PcgMalloc(int l_max, N_Vector vec_tmpl);

/*---------------------------------------------------------------
 Function : PcgSolve
 ----------------------------------------------------------------
 PcgSolve solves the linear system Ax = b by means of a
 preconditioned Conjugate-Gradient (PCG) iterative method.

  mem  pointer to an internal memory block allocated during a
       prior call to PcgMalloc

  A_data  pointer to a data structure containing information
       about the coefficient matrix A (passed to user-supplied
       function referenced by atimes (function pointer))

  x  vector (type N_Vector) containing initial guess x_0 upon
       entry, but which upon return contains an approximate
       solution of the linear system Ax = b (solution only
       valid if return value is either PCG_SUCCESS or
       PCG_RES_REDUCED)

  b  vector (type N_Vector) set to the right-hand side vector b
       of the linear system (unchanged by function)

  pretype  variable (type int) indicating the type of
       preconditioning to be used (see sundials_iterative.h);
       Note: since CG is for symmetric problems, preconditioning
       is applied symmetrically by default, so any nonzero flag
       will indicate to use the preconditioner.

  delta  tolerance on the L2 norm of the residual (if the
       return value == PCG_SUCCESS, then ||b-Ax||_L2 <= delta)

  P_data  pointer to a data structure containing preconditioner
       information (passed to user-supplied function referenced
       by psolve (function pointer))

  w  vector (type N_Vector) used in computing the residual norm
       for stopping solver (unchanged by function).  This is
       needed since PCG cannot utilize the same scaling vectors
       as used in the other SUNDIALS solvers, due to
       symmetry-breaking nature of scaling operators.

  atimes  user-supplied routine responsible for computing the
       matrix-vector product Ax (see sundials_iterative.h)

  psolve  user-supplied routine responsible for solving the
       preconditioned linear system Pz = r (ignored if
       pretype == PREC_NONE) (see sundials_iterative.h)

  res_norm  pointer (type realtype*) to the L2 norm of the
       residual (if return value is either PCG_SUCCESS or
       PCG_RES_REDUCED, then
            *res_norm = ||b-Ax||_L2, where x is
       the computed approximate solution)

  nli  pointer (type int*) to the total number of linear
       iterations performed

  nps  pointer (type int*) to the total number of calls made
       to the psolve routine
 --------------------------------------------------------------*/

SUNDIALS_EXPORT int PcgSolve(PcgMem mem, void *A_data, N_Vector x, N_Vector b,
                             int pretype, realtype delta, void *P_data,
                             N_Vector w, ATimesFn atimes, PSolveFn psolve,
                             realtype *res_norm, int *nli, int *nps);

/* Return values for PcgSolve */
#define PCG_SUCCESS            0  /* PCG algorithm converged          */
#define PCG_RES_REDUCED        1  /* PCG did NOT converge, but the
                                     residual was reduced             */
#define PCG_CONV_FAIL          2  /* PCG algorithm failed to converge */
#define PCG_PSOLVE_FAIL_REC    3  /* psolve failed recoverably        */
#define PCG_ATIMES_FAIL_REC    4  /* atimes failed recoverably        */
#define PCG_PSET_FAIL_REC      5  /* pset failed recoverably          */

#define PCG_MEM_NULL          -1  /* mem argument is NULL             */
#define PCG_ATIMES_FAIL_UNREC -2  /* atimes returned failure flag     */
#define PCG_PSOLVE_FAIL_UNREC -3  /* psolve failed unrecoverably      */
#define PCG_PSET_FAIL_UNREC   -4  /* pset failed unrecoverably        */

/*---------------------------------------------------------------
 Function : PcgFree
 ----------------------------------------------------------------
 PcgFree frees the memory allocated by a call to PcgMalloc.
 It is illegal to use the pointer mem after a call to PcgFree.
 ---------------------------------------------------------------*/

SUNDIALS_EXPORT void PcgFree(PcgMem mem);

#ifdef __cplusplus
}
#endif

#endif
