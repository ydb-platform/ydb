/* zla_gbrcond_c.f -- translated by f2c (version 20061008).
   You must link the resulting object file with libf2c:
	on Microsoft Windows system, link with libf2c.lib;
	on Linux or Unix systems, link with .../path/to/libf2c.a -lm
	or, if you install libf2c.a in a standard place, with -lf2c -lm
	-- in that order, at the end of the command line, as in
		cc *.o -lf2c -lm
	Source for libf2c is in /netlib/f2c/libf2c.zip, e.g.,

		http://www.netlib.org/f2c/libf2c.zip
*/

#include "f2c.h"
#include "blaswrap.h"

/* Table of constant values */

static integer c__1 = 1;

doublereal zla_gbrcond_c__(char *trans, integer *n, integer *kl, integer *ku, 
	doublecomplex *ab, integer *ldab, doublecomplex *afb, integer *ldafb, 
	integer *ipiv, doublereal *c__, logical *capply, integer *info, 
	doublecomplex *work, doublereal *rwork, ftnlen trans_len)
{
    /* System generated locals */
    integer ab_dim1, ab_offset, afb_dim1, afb_offset, i__1, i__2, i__3, i__4;
    doublereal ret_val, d__1, d__2;
    doublecomplex z__1;

    /* Builtin functions */
    double d_imag(doublecomplex *);

    /* Local variables */
    integer i__, j, kd, ke;
    doublereal tmp;
    integer kase;
    extern logical lsame_(char *, char *);
    integer isave[3];
    doublereal anorm;
    extern /* Subroutine */ int zlacn2_(integer *, doublecomplex *, 
	    doublecomplex *, doublereal *, integer *, integer *), xerbla_(
	    char *, integer *);
    doublereal ainvnm;
    extern /* Subroutine */ int zgbtrs_(char *, integer *, integer *, integer 
	    *, integer *, doublecomplex *, integer *, integer *, 
	    doublecomplex *, integer *, integer *);
    logical notrans;


/*     -- LAPACK routine (version 3.2.1)                               -- */
/*     -- Contributed by James Demmel, Deaglan Halligan, Yozo Hida and -- */
/*     -- Jason Riedy of Univ. of California Berkeley.                 -- */
/*     -- April 2009                                                   -- */

/*     -- LAPACK is a software package provided by Univ. of Tennessee, -- */
/*     -- Univ. of California Berkeley and NAG Ltd.                    -- */

/*     .. */
/*     .. Scalar Arguments .. */
/*     .. */
/*     .. Array Arguments .. */


/*  Purpose */
/*  ======= */

/*     ZLA_GBRCOND_C Computes the infinity norm condition number of */
/*     op(A) * inv(diag(C)) where C is a DOUBLE PRECISION vector. */

/*  Arguments */
/*  ========= */

/*     TRANS   (input) CHARACTER*1 */
/*     Specifies the form of the system of equations: */
/*       = 'N':  A * X = B     (No transpose) */
/*       = 'T':  A**T * X = B  (Transpose) */
/*       = 'C':  A**H * X = B  (Conjugate Transpose = Transpose) */

/*     N       (input) INTEGER */
/*     The number of linear equations, i.e., the order of the */
/*     matrix A.  N >= 0. */

/*     KL      (input) INTEGER */
/*     The number of subdiagonals within the band of A.  KL >= 0. */

/*     KU      (input) INTEGER */
/*     The number of superdiagonals within the band of A.  KU >= 0. */

/*     AB      (input) COMPLEX*16 array, dimension (LDAB,N) */
/*     On entry, the matrix A in band storage, in rows 1 to KL+KU+1. */
/*     The j-th column of A is stored in the j-th column of the */
/*     array AB as follows: */
/*     AB(KU+1+i-j,j) = A(i,j) for max(1,j-KU)<=i<=min(N,j+kl) */

/*     LDAB    (input) INTEGER */
/*     The leading dimension of the array AB.  LDAB >= KL+KU+1. */

/*     AFB     (input) COMPLEX*16 array, dimension (LDAFB,N) */
/*     Details of the LU factorization of the band matrix A, as */
/*     computed by ZGBTRF.  U is stored as an upper triangular */
/*     band matrix with KL+KU superdiagonals in rows 1 to KL+KU+1, */
/*     and the multipliers used during the factorization are stored */
/*     in rows KL+KU+2 to 2*KL+KU+1. */

/*     LDAFB   (input) INTEGER */
/*     The leading dimension of the array AFB.  LDAFB >= 2*KL+KU+1. */

/*     IPIV    (input) INTEGER array, dimension (N) */
/*     The pivot indices from the factorization A = P*L*U */
/*     as computed by ZGBTRF; row i of the matrix was interchanged */
/*     with row IPIV(i). */

/*     C       (input) DOUBLE PRECISION array, dimension (N) */
/*     The vector C in the formula op(A) * inv(diag(C)). */

/*     CAPPLY  (input) LOGICAL */
/*     If .TRUE. then access the vector C in the formula above. */

/*     INFO    (output) INTEGER */
/*       = 0:  Successful exit. */
/*     i > 0:  The ith argument is invalid. */

/*     WORK    (input) COMPLEX*16 array, dimension (2*N). */
/*     Workspace. */

/*     RWORK   (input) DOUBLE PRECISION array, dimension (N). */
/*     Workspace. */

/*  ===================================================================== */

/*     .. Local Scalars .. */
/*     .. */
/*     .. Local Arrays .. */
/*     .. */
/*     .. External Functions .. */
/*     .. */
/*     .. External Subroutines .. */
/*     .. */
/*     .. Intrinsic Functions .. */
/*     .. */
/*     .. Statement Functions .. */
/*     .. */
/*     .. Statement Function Definitions .. */
/*     .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    ab_dim1 = *ldab;
    ab_offset = 1 + ab_dim1;
    ab -= ab_offset;
    afb_dim1 = *ldafb;
    afb_offset = 1 + afb_dim1;
    afb -= afb_offset;
    --ipiv;
    --c__;
    --work;
    --rwork;

    /* Function Body */
    ret_val = 0.;

    *info = 0;
    notrans = lsame_(trans, "N");
    if (! notrans && ! lsame_(trans, "T") && ! lsame_(
	    trans, "C")) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (*kl < 0 || *kl > *n - 1) {
	*info = -3;
    } else if (*ku < 0 || *ku > *n - 1) {
	*info = -4;
    } else if (*ldab < *kl + *ku + 1) {
	*info = -6;
    } else if (*ldafb < (*kl << 1) + *ku + 1) {
	*info = -8;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZLA_GBRCOND_C", &i__1);
	return ret_val;
    }

/*     Compute norm of op(A)*op2(C). */

    anorm = 0.;
    kd = *ku + 1;
    ke = *kl + 1;
    if (notrans) {
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    tmp = 0.;
	    if (*capply) {
/* Computing MAX */
		i__2 = i__ - *kl;
/* Computing MIN */
		i__4 = i__ + *ku;
		i__3 = min(i__4,*n);
		for (j = max(i__2,1); j <= i__3; ++j) {
		    i__2 = kd + i__ - j + j * ab_dim1;
		    tmp += ((d__1 = ab[i__2].r, abs(d__1)) + (d__2 = d_imag(&
			    ab[kd + i__ - j + j * ab_dim1]), abs(d__2))) / 
			    c__[j];
		}
	    } else {
/* Computing MAX */
		i__3 = i__ - *kl;
/* Computing MIN */
		i__4 = i__ + *ku;
		i__2 = min(i__4,*n);
		for (j = max(i__3,1); j <= i__2; ++j) {
		    i__3 = kd + i__ - j + j * ab_dim1;
		    tmp += (d__1 = ab[i__3].r, abs(d__1)) + (d__2 = d_imag(&
			    ab[kd + i__ - j + j * ab_dim1]), abs(d__2));
		}
	    }
	    rwork[i__] = tmp;
	    anorm = max(anorm,tmp);
	}
    } else {
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    tmp = 0.;
	    if (*capply) {
/* Computing MAX */
		i__2 = i__ - *kl;
/* Computing MIN */
		i__4 = i__ + *ku;
		i__3 = min(i__4,*n);
		for (j = max(i__2,1); j <= i__3; ++j) {
		    i__2 = ke - i__ + j + i__ * ab_dim1;
		    tmp += ((d__1 = ab[i__2].r, abs(d__1)) + (d__2 = d_imag(&
			    ab[ke - i__ + j + i__ * ab_dim1]), abs(d__2))) / 
			    c__[j];
		}
	    } else {
/* Computing MAX */
		i__3 = i__ - *kl;
/* Computing MIN */
		i__4 = i__ + *ku;
		i__2 = min(i__4,*n);
		for (j = max(i__3,1); j <= i__2; ++j) {
		    i__3 = ke - i__ + j + i__ * ab_dim1;
		    tmp += (d__1 = ab[i__3].r, abs(d__1)) + (d__2 = d_imag(&
			    ab[ke - i__ + j + i__ * ab_dim1]), abs(d__2));
		}
	    }
	    rwork[i__] = tmp;
	    anorm = max(anorm,tmp);
	}
    }

/*     Quick return if possible. */

    if (*n == 0) {
	ret_val = 1.;
	return ret_val;
    } else if (anorm == 0.) {
	return ret_val;
    }

/*     Estimate the norm of inv(op(A)). */

    ainvnm = 0.;

    kase = 0;
L10:
    zlacn2_(n, &work[*n + 1], &work[1], &ainvnm, &kase, isave);
    if (kase != 0) {
	if (kase == 2) {

/*           Multiply by R. */

	    i__1 = *n;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = i__;
		i__3 = i__;
		i__4 = i__;
		z__1.r = rwork[i__4] * work[i__3].r, z__1.i = rwork[i__4] * 
			work[i__3].i;
		work[i__2].r = z__1.r, work[i__2].i = z__1.i;
	    }

	    if (notrans) {
		zgbtrs_("No transpose", n, kl, ku, &c__1, &afb[afb_offset], 
			ldafb, &ipiv[1], &work[1], n, info);
	    } else {
		zgbtrs_("Conjugate transpose", n, kl, ku, &c__1, &afb[
			afb_offset], ldafb, &ipiv[1], &work[1], n, info);
	    }

/*           Multiply by inv(C). */

	    if (*capply) {
		i__1 = *n;
		for (i__ = 1; i__ <= i__1; ++i__) {
		    i__2 = i__;
		    i__3 = i__;
		    i__4 = i__;
		    z__1.r = c__[i__4] * work[i__3].r, z__1.i = c__[i__4] * 
			    work[i__3].i;
		    work[i__2].r = z__1.r, work[i__2].i = z__1.i;
		}
	    }
	} else {

/*           Multiply by inv(C'). */

	    if (*capply) {
		i__1 = *n;
		for (i__ = 1; i__ <= i__1; ++i__) {
		    i__2 = i__;
		    i__3 = i__;
		    i__4 = i__;
		    z__1.r = c__[i__4] * work[i__3].r, z__1.i = c__[i__4] * 
			    work[i__3].i;
		    work[i__2].r = z__1.r, work[i__2].i = z__1.i;
		}
	    }

	    if (notrans) {
		zgbtrs_("Conjugate transpose", n, kl, ku, &c__1, &afb[
			afb_offset], ldafb, &ipiv[1], &work[1], n, info);
	    } else {
		zgbtrs_("No transpose", n, kl, ku, &c__1, &afb[afb_offset], 
			ldafb, &ipiv[1], &work[1], n, info);
	    }

/*           Multiply by R. */

	    i__1 = *n;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = i__;
		i__3 = i__;
		i__4 = i__;
		z__1.r = rwork[i__4] * work[i__3].r, z__1.i = rwork[i__4] * 
			work[i__3].i;
		work[i__2].r = z__1.r, work[i__2].i = z__1.i;
	    }
	}
	goto L10;
    }

/*     Compute the estimate of the reciprocal condition number. */

    if (ainvnm != 0.) {
	ret_val = 1. / ainvnm;
    }

    return ret_val;

} /* zla_gbrcond_c__ */
