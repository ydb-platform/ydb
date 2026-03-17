/*
 *   Copyright 2018, University Corporation for Atmospheric Research
 *   See top level COPYRIGHT file for copying and redistribution conditions.
 */
/* $Id: fbits.h,v 1.2 1995/05/26 20:46:46 davis Exp $ */

#ifndef _FBITS_H_
#define _FBITS_H_

/*
 * Macros for dealing with flag bits.
 */
#define fSet(t, f)       ((t) |= (f))
#define fClr(t, f)       ((t) &= ~(f))
#define fIsSet(t, f)     ((t) & (f))
#define fMask(t, f)     ((t) & ~(f))

/*
 * Propositions
 */
/* a implies b */
#define pIf(a,b) (!(a) || (b))
/* a if and only if b, use == when it makes sense */
#define pIff(a,b) (((a) && (b)) || (!(a) && !(b)))

#endif /*!FBITS_H_*/
