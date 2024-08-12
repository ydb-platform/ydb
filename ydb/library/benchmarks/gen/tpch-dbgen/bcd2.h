/*
* $Id: bcd2.h,v 1.2 2005/01/03 20:08:58 jms Exp $
*
* Revision History
* ===================
* $Log: bcd2.h,v $
* Revision 1.2  2005/01/03 20:08:58  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:45  jms
* re-establish external server
*
* Revision 1.1.1.1  2003/04/03 18:54:21  jms
* recreation after CVS crash
*
* Revision 1.1.1.1  2003/04/03 18:54:21  jms
* initial checkin
*
*
*/
int bin_bcd2(long binary, long *low_res, long *high_res);
int bcd2_bin(long *dest, long bcd);
int bcd2_add(long *bcd_low, long *bcd_high, long addend);
int bcd2_sub(long *bcd_low, long *bcd_high, long subend);
int bcd2_mul(long *bcd_low, long *bcd_high, long multiplier);
int bcd2_div(long *bcd_low, long *bcd_high, long divisor);
long bcd2_mod(long *bcd_low, long *bcd_high, long modulo);
long bcd2_cmp(long *bcd_low, long *bcd_high, long compare);
