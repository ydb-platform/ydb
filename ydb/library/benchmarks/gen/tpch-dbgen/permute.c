/*
* $Id: permute.c,v 1.3 2007/01/04 21:29:21 jms Exp $
*
* Revision History
* ===================
* $Log: permute.c,v $
* Revision 1.3  2007/01/04 21:29:21  jms
* Porting changes uncovered as part of move to VS2005. No impact on data set
*
* Revision 1.2  2005/01/03 20:08:59  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:47  jms
* re-establish external server
*
* Revision 1.1.1.1  2003/08/07 17:58:34  jms
* recreation after CVS crash
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
* permute.c -- a permutation generator for the query 
*              sequences in TPC-H and TPC-R
*/

#ifdef TEST
#define DECLARER
#endif
#include "config.h"
#include "dss.h"
#ifdef TEST
#include <stdlib.h>
#if (defined(_POSIX_)||!defined(WIN32))		/* Change for Windows NT */
#include <unistd.h>
#include <sys/wait.h>
#endif /* WIN32 */
#include <stdio.h>				/* */
#include <limits.h>
#include <math.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#ifdef HP
#include <strings.h>
#endif
#if (defined(WIN32)&&!defined(_POSIX_))
#include <process.h>
#pragma warning(disable:4201)
#pragma warning(disable:4214)
#pragma warning(disable:4514)
#define WIN32_LEAN_AND_MEAN
#define NOATOM
#define NOGDICAPMASKS
#define NOMETAFILE
#define NOMINMAX
#define NOMSG
#define NOOPENFILE
#define NORASTEROPS
#define NOSCROLL
#define NOSOUND
#define NOSYSMETRICS
#define NOTEXTMETRIC
#define NOWH
#define NOCOMM
#define NOKANJI
#define NOMCX
#include <windows.h>
#pragma warning(default:4201)
#pragma warning(default:4214)
#endif
#endif

DSS_HUGE NextRand(DSS_HUGE seed);
void	permute(long *set, int cnt, long stream);
void	permute_dist(distribution *d, long stream);
long seed;
char *eol[2] = {" ", "},"};
extern seed_t Seed[];
#ifdef TEST
tdef tdefs = { NULL };
#endif


#define MAX_QUERY	22
#define ITERATIONS	1000
#define UNSET	0

void	permute(long *a, int c, long s)
{
    int i;
    static DSS_HUGE source;
    static long *set, temp;
    
	if (a != (long *)NULL)
	{
		for (i=0; i < c; i++)
		{
			RANDOM(source, (long)i, (long)(c - 1), s);
			temp = *(a + source);
			*(a + source) = *(a + i) ;
			*(a + i) = temp;
		}
	}
	
	return;
}

void	permute_dist(distribution *d, long stream)
{
	static distribution *dist = NULL;
	int i;
	
	if (d != NULL)
	{
		if (d->permute == (long *)NULL)
		{
			d->permute = (long *)malloc(sizeof(long) * DIST_SIZE(d));
			MALLOC_CHECK(d->permute);
		}
		for (i=0; i < DIST_SIZE(d); i++) 
			*(d->permute + i) = i;
		permute(d->permute, DIST_SIZE(d), stream);
	}
	else
		INTERNAL_ERROR("Bad call to permute_dist");	

	return;
}


#ifdef TEST

main(int ac, char *av[])
	{
	long *sequence, 
		i,
		j,
		streams = UNSET,
		*a;
	char sep;
	int index = 0;
	
	set_seeds = 0;
	sequence = (long *)malloc(MAX_QUERY * sizeof(long));
	a = sequence;
	for (i=0; i < MAX_QUERY; i++)
		*(sequence + i) = i;
	if (ac < 3) 
		goto usage;
	Seed[0].value = (long)atoi(av[1]);
	streams = atoi(av[2]);
	if (Seed[0].value == UNSET || streams == UNSET) 
		goto usage;
	
	index = 0;
	printf("long permutation[%d][%d] = {\n", streams, MAX_QUERY);
	for (j=0; j < streams; j++)
		{
		sep = '{';
		printf("%s\n", eol[index]);
		for (i=0; i < MAX_QUERY; i++)
			{
			printf("%c%2d", sep, *permute(a, MAX_QUERY, 0) + 1);
			a = (long *)NULL;
			sep = ',';
			}
		a = sequence;
		index=1;
		}
	printf("}\n};\n");
	return(0);
	
usage:
	printf("Usage: %s <seed> <streams>\n",av[0]);
	printf("  uses <seed> to start the generation of <streams> permutations of [1..%d]\n", MAX_QUERY);
	return(-1);
	
	}
#endif /* TEST */
