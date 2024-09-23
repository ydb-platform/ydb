/*
* $Id: rng64.h,v 1.2 2005/01/03 20:08:59 jms Exp $
*
* Revision History
* ===================
* $Log: rng64.h,v $
* Revision 1.2  2005/01/03 20:08:59  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:47  jms
* re-establish external server
*
* Revision 1.1.1.1  2003/08/08 21:57:34  jms
* recreation after CVS crash
*
* Revision 1.1  2003/08/08 21:57:34  jms
* first integration of rng64 for o_custkey and l_partkey
*
*
*/
DSS_HUGE AdvanceRand64( DSS_HUGE nSeed, DSS_HUGE nCount);
void dss_random64(DSS_HUGE *tgt, DSS_HUGE nLow, DSS_HUGE nHigh, long stream);
DSS_HUGE NextRand64(DSS_HUGE nSeed);
