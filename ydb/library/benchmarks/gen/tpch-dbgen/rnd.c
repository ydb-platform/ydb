/*
* $Id: rnd.c,v 1.7 2006/07/31 17:23:09 jms Exp $
*
* Revision History
* ===================
* $Log: rnd.c,v $
* Revision 1.7  2006/07/31 17:23:09  jms
* fix to parallelism problem
*
* Revision 1.6  2005/10/25 17:26:38  jms
* check in integration between microsoft changes and baseline code
*
* Revision 1.5  2005/10/14 23:16:54  jms
* fix for answer set compliance
*
* Revision 1.4  2005/09/23 22:29:35  jms
* fix to assume 64b support in the 32b RNG calls. Should speed generation, and corrects a problem with FK between Customer and Orders
*
* Revision 1.3  2005/03/04 21:43:23  jms
* correct segfult in random()
*
* Revision 1.2  2005/01/03 20:08:59  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:47  jms
* re-establish external server
*
* Revision 1.7  2004/04/08 17:34:15  jms
* cleanup SOLARIS/SUN ifdefs; now all use SUN
*
* Revision 1.6  2004/03/26 20:22:56  jms
* correct Solaris header
*
* Revision 1.5  2004/03/02 20:50:50  jms
* MP/RAS porting changes
*
* Revision 1.4  2004/02/18 16:37:33  jms
* add int32_t for solaris
*
* Revision 1.3  2004/02/18 16:26:49  jms
* 32/64 bit changes for overflow handling needed additional changes when ported back to windows
*
* Revision 1.2  2004/02/18 16:17:32  jms
* add 32bit specific changes to UnifInt
*
* Revision 1.1.1.1  2003/08/08 21:50:34  jms
* recreation after CVS crash
*
* Revision 1.3  2003/08/08 21:35:26  jms
* first integration of rng64 for o_custkey and l_partkey
*
* Revision 1.2  2003/08/07 17:58:34  jms
* Convery RNG to 64bit space as preparation for new large scale RNG
*
* Revision 1.1.1.1  2003/04/03 18:54:21  jms
* initial checkin
*
*
*/
/* 
 * RANDOM.C -- Implements Park & Miller's "Minimum Standard" RNG
 * 
 * (Reference:  CACM, Oct 1988, pp 1192-1201)
 * 
 * NextRand:  Computes next random integer
 * UnifInt:   Yields an long uniformly distributed between given bounds 
 * UnifReal: ields a real uniformly distributed between given bounds   
 * Exponential: Yields a real exponentially distributed with given mean
 * 
 */

#include "config.h"
#include <stdio.h>
#include <math.h>
#ifdef LINUX
#include <stdint.h>
#endif
#ifdef IBM
#include <inttypes.h>
#endif
#ifdef SUN
#include <inttypes.h>
#endif
#ifdef ATT
#include <sys/bitypes.h>
#endif
#ifdef WIN32
#define int32_t	__int32
#endif
#include "dss.h"
#include "rnd.h" 

char *env_config PROTO((char *tag, char *dflt));
void NthElement(DSS_HUGE, DSS_HUGE *);

void
dss_random(DSS_HUGE *tgt, DSS_HUGE lower, DSS_HUGE upper, long stream)
{
	*tgt = UnifInt(lower, upper, stream);
	Seed[stream].usage += 1;

	return;
}

void
row_start(int t)	\
{
	int i;
	for (i=0; i <= MAX_STREAM; i++) 
		Seed[i].usage = 0 ; 
	
	return;
}

void
row_stop(int t)	\
	{ 
	int i;
	
	/* need to allow for handling the master and detail together */
	if (t == ORDER_LINE)
		t = ORDER;
	if (t == PART_PSUPP)
		t = PART;
	
	for (i=0; i <= MAX_STREAM; i++)
		if ((Seed[i].table == t) || (Seed[i].table == tdefs[t].child))
			{ 
			if (set_seeds && (Seed[i].usage > Seed[i].boundary))
				{
				fprintf(stderr, "\nSEED CHANGE: seed[%d].usage = %d\n", 
					i, Seed[i].usage); 
				Seed[i].boundary = Seed[i].usage;
				} 
			else 
				{
				NthElement((Seed[i].boundary - Seed[i].usage), &Seed[i].value);
#ifdef RNG_TEST
				Seed[i].nCalls += Seed[i].boundary - Seed[i].usage;
#endif
				}
			} 
		return;
	}

void
dump_seeds(int tbl)
{
	int i;

	for (i=0; i <= MAX_STREAM; i++)
		if (Seed[i].table == tbl)
#ifdef RNG_TEST
			printf("%d(%ld):\t%ld\n", i, Seed[i].nCalls, Seed[i].value);
#else
			printf("%d:\t%ld\n", i, Seed[i].value);
#endif
	return;
}

/******************************************************************

   NextRand:  Computes next random integer

*******************************************************************/

/*
 * long NextRand( long nSeed )
 */
DSS_HUGE
NextRand(DSS_HUGE nSeed)

/*
 * nSeed is the previous random number; the returned value is the 
 * next random number. The routine generates all numbers in the 
 * range 1 .. nM-1.
 */

{
	nSeed = (nSeed * 16807) % 2147483647;
    return (nSeed);
}

/******************************************************************

   UnifInt:  Yields an long uniformly distributed between given bounds

*******************************************************************/

/*
 * long UnifInt( long nLow, long nHigh, long nStream )
 */
DSS_HUGE
UnifInt(DSS_HUGE nLow, DSS_HUGE nHigh, long nStream)

/*
 * Returns an integer uniformly distributed between nLow and nHigh, 
 * including * the endpoints.  nStream is the random number stream.   
 * Stream 0 is used if nStream is not in the range 0..MAX_STREAM.
 */

{
    double          dRange;
    DSS_HUGE            nTemp,
		nRange;
    int32_t	nLow32 = (int32_t)nLow,
		nHigh32 = (int32_t)nHigh;
	
    if (nStream < 0 || nStream > MAX_STREAM)
        nStream = 0;
	
	if ((nHigh == MAX_LONG) && (nLow == 0))
	{
		dRange = DOUBLE_CAST (nHigh32 - nLow32 + 1);
		nRange = nHigh32 - nLow32 + 1;
	}
	else
	{
		dRange = DOUBLE_CAST (nHigh - nLow + 1);
		nRange = nHigh - nLow + 1;
	}

    Seed[nStream].value = NextRand(Seed[nStream].value);
#ifdef RNG_TEST
	Seed[nStream].nCalls += 1;
#endif
	nTemp = (DSS_HUGE) (((double) Seed[nStream].value / dM) * (dRange));
    return (nLow + nTemp);
}



