/* -----------------------------------------------------------------
 Programmer(s): Daniel R. Reynolds and Hilari C. Tiedeman @ SMU
 -------------------------------------------------------------------
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
 -------------------------------------------------------------------
 This is the header file for the implementation of SPFGMR Krylov
 iterative linear solver.  The SPFGMR algorithm is based on the
 Scaled Preconditioned Flexible GMRES (Generalized Minimal Residual)
 method [Y. Saad, SIAM J. Sci. Comput., 1993].

 The SPFGMR algorithm solves a linear system A x = b.
 Preconditioning is only allowed on the right.
 Scaling is allowed on the right, and restarts are also allowed.
 We denote the preconditioner and scaling matrices as follows:
   P = right preconditioner
   S1 = diagonal matrix of scale factors for P-inverse b
   S2 = diagonal matrix of scale factors for x
 The matrices A and P are not required explicitly; only
 routines that provide A and P-inverse as operators are required.

 In this notation, SPFGMR applies the underlying FGMRES method to
 the equivalent transformed system
   Abar xbar = bbar , where
   Abar = S1 A (P-inverse) (S2-inverse),
   bbar = S1 b , and   xbar = S2 P x .

 The scaling matrix must be chosen so that the vectors S1 b and
 S2 P x have dimensionless components.  If preconditioning is not
 performed (P = I), then S2 must be a scaling for x, while S1 is a
 scaling for b.  Similarly, if preconditioning is performed, then S1
 must be a scaling for b, while S2 is a scaling for P x, and may
 also be taken as a scaling for b.

 The stopping test for the SPFGMR iterations is on the L2 norm of
 the scaled preconditioned residual:
      || bbar - Abar xbar ||_2  <  delta
 with an input test constant delta.

 The usage of this SPFGMR solver involves supplying two routines
 and making three calls.  The user-supplied routines are
    atimes (A_data, x, y) to compute y = A x, given x,
 and
    psolve (P_data, y, x, lr)
                to solve P x = y for x, given y.
 The three user calls are:
    mem  = SpfgmrMalloc(lmax, vec_tmpl);
           to initialize memory,
    flag = SpfgmrSolve(mem,A_data,x,b,...,
                       P_data,s1,s2,atimes,psolve,...);
           to solve the system, and
    SpfgmrFree(mem);
           to free the memory created by SpfgmrMalloc.
 Complete details for specifying atimes and psolve and for the
 usage calls are given in the paragraphs below and in iterative.h.
 -----------------------------------------------------------------*/

#ifndef _SPFGMR_H
#define _SPFGMR_H

#include <sundials/sundials_iterative.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/* -----------------------------------------------------------------
 Types: SpfgmrMemRec, SpfgmrMem
 -------------------------------------------------------------------
 SpfgmrMem is a pointer to an SpfgmrMemRec which contains
 the memory needed by SpfgmrSolve. The SpfgmrMalloc routine
 returns a pointer of type SpfgmrMem which should then be passed
 in subsequent calls to SpfgmrSolve. The SpfgmrFree routine frees
 the memory allocated by SpfgmrMalloc.

 l_max is the maximum Krylov dimension that SpfgmrSolve will be
 permitted to use.

 V is the array of Krylov basis vectors v_1, ..., v_(l_max+1),
 stored in V[0], ..., V[l_max], where l_max is the second
 parameter to SpfgmrMalloc. Each v_i is a vector of type
 N_Vector.

 Z is the array of preconditioned basis vectors z_1, ...,
 z_(l_max+1), stored in Z[0], ..., Z[l_max], where l_max is the
 second parameter to SpfgmrMalloc. Each z_i is a vector of type
 N_Vector.

 Hes is the (l_max+1) x l_max Hessenberg matrix. It is stored
 row-wise so that the (i,j)th element is given by Hes[i][j].

 givens is a length 2*l_max array which represents the
 Givens rotation matrices that arise in the algorithm. The
 Givens rotation matrices F_0, F_1, ..., F_j, where F_i is

             1
               1
                 c_i  -s_i      <--- row i
                 s_i   c_i
                           1
                             1

 are represented in the givens vector as
 givens[0]=c_0, givens[1]=s_0, givens[2]=c_1, givens[3]=s_1,
 ..., givens[2j]=c_j, givens[2j+1]=s_j.

 xcor is a vector (type N_Vector) which holds the scaled,
 preconditioned correction to the initial guess.

 yg is a length (l_max+1) array of realtype used to hold "short"
 vectors (e.g. y and g).

 vtemp is a vector (type N_Vector) used as temporary vector
 storage during calculations.
 -----------------------------------------------------------------*/
typedef struct _SpfgmrMemRec {
  int l_max;
  N_Vector *V;
  N_Vector *Z;
  realtype **Hes;
  realtype *givens;
  N_Vector xcor;
  realtype *yg;
  N_Vector vtemp;
} SpfgmrMemRec, *SpfgmrMem;

/*----------------------------------------------------------------
 Function : SpfgmrMalloc
 -----------------------------------------------------------------
 SpfgmrMalloc allocates the memory used by SpfgmrSolve. It
 returns a pointer of type SpfgmrMem which the user of the
 SPGMR package should pass to SpfgmrSolve. The parameter l_max
 is the maximum Krylov dimension that SpfgmrSolve will be
 permitted to use. The parameter vec_tmpl is a pointer to an
 N_Vector used as a template to create new vectors by duplication.
 This routine returns NULL if there is a memory request failure.
 ---------------------------------------------------------------*/

SUNDIALS_EXPORT SpfgmrMem SpfgmrMalloc(int l_max, N_Vector vec_tmpl);

/*----------------------------------------------------------------
 Function : SpfgmrSolve
 -----------------------------------------------------------------
 SpfgmrSolve solves the linear system Ax = b using the SPFGMR
 method. The return values are given by the symbolic constants
 below. The first SpfgmrSolve parameter is a pointer to memory
 allocated by a prior call to SpfgmrMalloc.

 mem is the pointer returned by SpfgmrMalloc to the structure
 containing the memory needed by SpfgmrSolve.

 A_data is a pointer to information about the coefficient
 matrix A. This pointer is passed to the user-supplied function
 atimes.

 x is the initial guess x_0 upon entry and the solution
 N_Vector upon exit with return value SPFGMR_SUCCESS or
 SPFGMR_RES_REDUCED. For all other return values, the output x
 is undefined.

 b is the right hand side N_Vector. It is undisturbed by this
 function.

 pretype is the type of preconditioning to be used. Its
 legal possible values are enumerated in iterative.h. These
 values are PREC_NONE, PREC_LEFT, PREC_RIGHT and PREC_BOTH;
 however since this solver can only precondition on the right,
 then right-preconditioning will be done if any of the values
 PREC_LEFT, PREC_RIGHT or PREC_BOTH are provided..

 gstype is the type of Gram-Schmidt orthogonalization to be
 used. Its legal values are enumerated in iterativ.h. These
 values are MODIFIED_GS=0 and CLASSICAL_GS=1.

 delta is the tolerance on the L2 norm of the scaled,
 preconditioned residual. On return with value SPFGMR_SUCCESS,
 this residual satisfies || s1 P1_inv (b - Ax) ||_2 <= delta.

 max_restarts is the maximum number of times the algorithm is
 allowed to restart.

 maxit is the maximum number of iterations allowed within the
 solve.  This value must be less than or equal to the "l_max"
 value previously supplied to SpfgmrMalloc.  If maxit is too
 large, l_max will be used instead.

 P_data is a pointer to preconditioner information. This
 pointer is passed to the user-supplied function psolve.

 s1 is an N_Vector of positive scale factors for b. (Not
 tested for positivity.)  Pass NULL if no scaling on b is
 required.

 s2 is an N_Vector of positive scale factors for P x, where
 P is the right preconditioner. (Not tested for positivity.)
 Pass NULL if no scaling on P x is required.

 atimes is the user-supplied function which performs the
 operation of multiplying A by a given vector. Its description
 is given in iterative.h.

 psolve is the user-supplied function which solves a
 preconditioner system Pz = r, where P is P1 or P2. Its full
 description is given in iterative.h. The psolve function will
 not be called if pretype is NONE; in that case, the user
 should pass NULL for psolve.

 res_norm is a pointer to the L2 norm of the scaled,
 preconditioned residual. On return with value SPFGMR_SUCCESS or
 SPFGMR_RES_REDUCED, (*res_norm) contains the value
 || s1 (b - Ax) ||_2 for the computed solution x.
 For all other return values, (*res_norm) is undefined. The
 caller is responsible for allocating the memory (*res_norm)
 to be filled in by SpfgmrSolve.

 nli is a pointer to the number of linear iterations done in
 the execution of SpfgmrSolve. The caller is responsible for
 allocating the memory (*nli) to be filled in by SpfgmrSolve.

 nps is a pointer to the number of calls made to psolve during
 the execution of SpfgmrSolve. The caller is responsible for
 allocating the memory (*nps) to be filled in by SpfgmrSolve.

 Note: Repeated calls can be made to SpfgmrSolve with varying
 input arguments. If, however, the problem size N or the
 maximum Krylov dimension l_max changes, then a call to
 SpfgmrMalloc must be made to obtain new memory for SpfgmrSolve
 to use.
 ---------------------------------------------------------------*/

SUNDIALS_EXPORT int SpfgmrSolve(SpfgmrMem mem, void *A_data, N_Vector x,
                                N_Vector b, int pretype, int gstype,
                                realtype delta, int max_restarts,
                                int maxit, void *P_data, N_Vector s1,
                                N_Vector s2, ATimesFn atimes, PSolveFn psolve,
                                realtype *res_norm, int *nli, int *nps);


/* Return values for SpfgmrSolve */
#define SPFGMR_SUCCESS            0  /* Converged                     */
#define SPFGMR_RES_REDUCED        1  /* Did not converge, but reduced
                                        norm of residual              */
#define SPFGMR_CONV_FAIL          2  /* Failed to converge            */
#define SPFGMR_QRFACT_FAIL        3  /* QRfact found singular matrix  */
#define SPFGMR_PSOLVE_FAIL_REC    4  /* psolve failed recoverably     */
#define SPFGMR_ATIMES_FAIL_REC    5  /* atimes failed recoverably     */
#define SPFGMR_PSET_FAIL_REC      6  /* pset failed recoverably       */

#define SPFGMR_MEM_NULL          -1  /* mem argument is NULL          */
#define SPFGMR_ATIMES_FAIL_UNREC -2  /* atimes returned failure flag  */
#define SPFGMR_PSOLVE_FAIL_UNREC -3  /* psolve failed unrecoverably   */
#define SPFGMR_GS_FAIL           -4  /* Gram-Schmidt routine faiuled  */
#define SPFGMR_QRSOL_FAIL        -5  /* QRsol found singular R        */
#define SPFGMR_PSET_FAIL_UNREC   -6  /* pset failed unrecoverably     */

/*----------------------------------------------------------------
 Function : SpfgmrFree
 -----------------------------------------------------------------
 SpfgmrMalloc frees the memory allocated by SpfgmrMalloc. It is
 illegal to use the pointer mem after a call to SpfgmrFree.
 ---------------------------------------------------------------*/

SUNDIALS_EXPORT void SpfgmrFree(SpfgmrMem mem);

/*----------------------------------------------------------------
 Macro: SPFGMR_VTEMP
 -----------------------------------------------------------------
 This macro provides access to the work vector vtemp in the
 memory block of the SPFGMR module.  The argument mem is the
 memory pointer returned by SpfgmrMalloc, of type SpfgmrMem,
 and the macro value is of type N_Vector.
 On a return from SpfgmrSolve with *nli = 0, this vector
 contains the scaled preconditioned initial residual,
 s1 * P1_inverse * (b - A x_0).
 ---------------------------------------------------------------*/

#define SPFGMR_VTEMP(mem) (mem->vtemp)

#ifdef __cplusplus
}
#endif

#endif
