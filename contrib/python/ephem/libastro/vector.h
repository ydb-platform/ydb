#ifndef __SATVECTOR_H
#define __SATVECTOR_H

/* $Id: vector.h,v 1.1 2000/09/25 17:21:25 ecdowney Exp $ */

#define dotp(A,B) ((A).x*(B).x+(A).y*(B).y+(A).z*(B).z)

#define crossp(A,B,C) {(C).x=(A).y*(B).z-(A).z*(B).y;(C).y=(A).z*(B).x-(A).x*(B).z;(C).z=(A).x*(B).y-(A).y*(B).x;}

#define vecabs(V) (sqrt((V).x*(V).x+(V).y*(V).y+(V).z*(V).z))
#define vecsq(V) ((V).x*(V).x+(V).y*(V).y+(V).z*(V).z)
#define vecsub(A,B,C) {(C).x=(A).x-(B).x;(C).y=(A).y-(B).y;(C).z=(A).z-(B).z;}
#define vecscale(A,k) {(A).x*=(k);(A).y*=(k);(A).z*=(k);}

#endif /* __SATVECTOR_H */

