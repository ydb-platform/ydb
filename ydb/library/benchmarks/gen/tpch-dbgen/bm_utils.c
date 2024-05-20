/*
* $Id: bm_utils.c,v 1.4 2006/04/12 18:00:55 jms Exp $
*
* Revision History
* ===================
* $Log: bm_utils.c,v $
* Revision 1.4  2006/04/12 18:00:55  jms
* add missing parameter to call to gen_seed
*
* Revision 1.3  2005/10/14 23:16:54  jms
* fix for answer set compliance
*
* Revision 1.2  2005/01/03 20:08:58  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:46  jms
* re-establish external server
*
* Revision 1.3  2004/02/18 14:05:53  jms
* porting changes for LINUX and 64 bit RNG
*
* Revision 1.2  2004/01/22 05:49:29  jms
* AIX porting (AIX 5.1)
*
* Revision 1.1.1.1  2003/08/08 21:35:26  jms
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
 *
 * Various routines that handle distributions, value selections and
 * seed value management for the DSS benchmark. Current functions:
 * env_config -- set config vars with optional environment override
 * yes_no -- ask simple yes/no question and return boolean result
 * a_rnd(min, max) -- random alphanumeric within length range
 * pick_str(size, set) -- select a string from the set of size
 * read_dist(file, name, distribution *) -- read named dist from file
 * tbl_open(path, mode) -- std fopen with lifenoise
 * julian(date) -- julian date correction
 * rowcnt(tbl) -- proper scaling of given table
 * e_str(set, min, max) -- build an embedded str
 * agg_str() -- build a string from the named set
 * dsscasecmp() -- version of strcasecmp()
 * dssncasecmp() -- version of strncasecmp()
 * getopt()
 * set_state() -- initialize the RNG
 */

#include "config.h"
#include "dss.h"
#include <stdio.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#ifdef HP
#include <strings.h>
#endif            /* HP */
#include <ctype.h>
#include <math.h>
#ifndef _POSIX_SOURCE
#include <malloc.h>
#endif /* POSIX_SOURCE */
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
/* Lines added by Chuck McDevitt for WIN32 support */
#ifdef WIN32
#ifndef _POSIX_
#include <io.h>
#ifndef S_ISREG
#define S_ISREG(m) ( ((m) & _S_IFMT) == _S_IFREG )
#define S_ISFIFO(m) ( ((m) & _S_IFMT) == _S_IFIFO )
#endif 
#endif
#ifndef stat
#define stat _stat
#endif
#ifndef fdopen
#define fdopen _fdopen
#endif
#ifndef open
#define open _open
#endif
#ifndef O_RDONLY
#define O_RDONLY _O_RDONLY
#endif
#ifndef O_WRONLY
#define O_WRONLY _O_WRONLY
#endif
#ifndef O_CREAT
#define O_CREAT _O_CREAT
#endif
#endif
/* End of lines added by Chuck McDevitt for WIN32 support */
#include "dsstypes.h"


static char alpha_num[65] =
"0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";

#if defined(__STDC__) || defined(__cplusplus)
#define PROTO(s) s
#else
#define PROTO(s) ()
#endif

#ifndef WIN32
char     *getenv PROTO((const char *name));
#endif
void usage();
long *permute_dist(distribution *d, long stream);
extern seed_t Seed[];

/*
 * env_config: look for a environmental variable setting and return its
 * value; otherwise return the default supplied
 */
char     *
env_config(char *var, char *dflt)
{
   static char *evar;

   if ((evar = getenv(var)) != NULL)
      return (evar);
   else
      return (dflt);
}

/*
 * return the answer to a yes/no question as a boolean
 */
long
yes_no(char *prompt)
{
    char      reply[128];

#ifdef WIN32
/* Disable warning about conditional expression is constant */ 
#pragma warning(disable:4127)
#endif 

    while (1)
        {
#ifdef WIN32
#pragma warning(default:4127)
#endif 
        printf("%s [Y/N]: ", prompt);
        fgets(reply, 128, stdin);
        switch (*reply)
            {
            case 'y':
            case 'Y':
                return (1);
            case 'n':
            case 'N':
                return (0);
            default:
                printf("Please answer 'yes' or 'no'.\n");
            }
        }
}

/*
 * generate a random string with length randomly selected in [min, max]
 * and using the characters in alphanum (currently includes a space
 * and comma)
 */
void
a_rnd(int min, int max, int column, char *dest)
{
   DSS_HUGE      i,
             len,
             char_int;

   RANDOM(len, min, max, column);
   for (i = 0; i < len; i++)
      {
      if (i % 5 == 0)
        RANDOM(char_int, 0, MAX_LONG, column);
      *(dest + i) = alpha_num[char_int & 077];
      char_int >>= 6;
      }
   *(dest + len) = '\0';
   return;
}

/*
 * embed a randomly selected member of distribution d in alpha-numeric
 * noise of a length rendomly selected between min and max at a random
 * position
 */
void
e_str(distribution *d, int min, int max, int stream, char *dest)
{
    char strtmp[MAXAGG_LEN + 1];
    DSS_HUGE loc;
    int len;

    a_rnd(min, max, stream, dest);
    pick_str(d, stream, strtmp);
    len = (int)strlen(strtmp);
    RANDOM(loc, 0, ((int)strlen(dest) - 1 - len), stream);
    strncpy(dest + loc, strtmp, len);

    return;
}


/*
 * return the string associate with the LSB of a uniformly selected
 * long in [1, max] where max is determined by the distribution
 * being queried
 */
int
pick_str(distribution *s, int c, char *target)
{
    long      i = 0;
    DSS_HUGE      j;

    RANDOM(j, 1, s->list[s->count - 1].weight, c);
    while (s->list[i].weight < j)
        i++;
    strcpy(target, s->list[i].text);
    return(i);
}

/*
 * unjulian (long date) -- return(date - STARTDATE)
 */
long
unjulian(long date)
{
    int i;
    long res = 0;

    for (i = STARTDATE / 1000; i < date / 1000; i++)
        res += 365 + LEAP(i);
    res += date %  1000 - 1;

    return(res);
}

long
julian(long date)
{
    long       offset;
    long      result;
    long      yr;
    long      yend;

    offset = date - STARTDATE;
    result = STARTDATE;

#ifdef WIN32
/* Disable warning about conditional expression is constant */ 
#pragma warning(disable:4127)
#endif 

    while (1)
        {
#ifdef WIN32 
#pragma warning(default:4127)
#endif 
        yr = result / 1000;
        yend = yr * 1000 + 365 + LEAP(yr);
        if (result + offset > yend)   /* overflow into next year */
            {
            offset -= yend - result + 1;
            result += 1000;
            continue;
            }
        else
            break;
        }
    return (result + offset);
}

/*
* load a distribution from a flat file into the target structure;
* should be rewritten to allow multiple dists in a file
*/
void
read_dist(char *path, char *name, distribution *target)
{
FILE     *fp;
char      line[256],
         token[256],
        *c;
long      weight,
         count = 0,
         name_set = 0;

    if (d_path == NULL)
		{
		sprintf(line, "%s%c%s", 
			env_config(CONFIG_TAG, CONFIG_DFLT), PATH_SEP, path);
		fp = fopen(line, "r");
		OPEN_CHECK(fp, line);
		}
	else
		{
		fp = fopen(d_path, "r");
		OPEN_CHECK(fp, d_path);
		}
    while (fgets(line, sizeof(line), fp) != NULL)
        {
        if ((c = strchr(line, '\n')) != NULL)
            *c = '\0';
        if ((c = strchr(line, '#')) != NULL)
            *c = '\0';
        if (*line == '\0')
            continue;

        if (!name_set)
            {
            if (dsscasecmp(strtok(line, "\n\t "), "BEGIN"))
                continue;
            if (dsscasecmp(strtok(NULL, "\n\t "), name))
                continue;
            name_set = 1;
            continue;
            }
        else
            {
            if (!dssncasecmp(line, "END", 3))
                {
                fclose(fp);
                return;
                }
            }

        if (sscanf(line, "%[^|]|%ld", token, &weight) != 2)
            continue;

        if (!dsscasecmp(token, "count"))
            {
            target->count = weight;
            target->list =
                (set_member *)
                    malloc((size_t)(weight * sizeof(set_member)));
            MALLOC_CHECK(target->list);
            target->max = 0;
            continue;
            }
        target->list[count].text =
            (char *) malloc((size_t)((int)strlen(token) + 1));
        MALLOC_CHECK(target->list[count].text);
        strcpy(target->list[count].text, token);
        target->max += weight;
        target->list[count].weight = target->max;

        count += 1;
        } /* while fgets() */

    if (count != target->count)
        {
        fprintf(stderr, "Read error on dist '%s'\n", name);
        fclose(fp);
        exit(1);
        }
	target->permute = (long *)NULL;
    fclose(fp);
    return;
}

/*
 * standard file open with life noise
 */

FILE     *
tbl_open(int tbl, char *mode)
{
    char      prompt[256];
    char      fullpath[256];
    FILE     *f;
    struct stat fstats;
    int      retcode;

    if (*tdefs[tbl].name == PATH_SEP)
        strcpy(fullpath, tdefs[tbl].name);
    else
        sprintf(fullpath, "%s%c%s",
            env_config(PATH_TAG, PATH_DFLT), PATH_SEP, tdefs[tbl].name);

    retcode = stat(fullpath, &fstats);
    if (retcode) {
		if (errno != ENOENT) {
			fprintf(stderr, "stat(%s) failed.\n", fullpath);
			exit(-1);
		} else
			f = fopen(fullpath, mode);  // create and open the file
	} else {
		/* note this code asumes we are writing but tests if mode == r -jrg */
		if (S_ISREG(fstats.st_mode) && !force && *mode != 'r' ) {
			sprintf(prompt, "Do you want to overwrite %s ?", fullpath);
			if (!yes_no(prompt))
				exit(0);
			f = fopen(fullpath, mode);
		} else if (S_ISFIFO(fstats.st_mode))
			{
			retcode =
				open(fullpath, ((*mode == 'r')?O_RDONLY:O_WRONLY)|O_CREAT, 0664);
			f = fdopen(retcode, mode);
			}
		else
			f = fopen(fullpath, mode);
	}
    OPEN_CHECK(f, fullpath);

    return (f);
}


/*
 * agg_str(set, count) build an aggregated string from count unique
 * selections taken from set
 */
void
agg_str(distribution *set, long count, long col, char *dest)
{
	distribution *d;
	int i;

	d = set;
	*dest = '\0';

	permute_dist(d, col);
	for (i=0; i < count; i++)
		{
		strcat(dest, DIST_MEMBER(set,DIST_PERMUTE(d, i)));
		strcat(dest, " ");
		}
	*(dest + (int)strlen(dest) - 1) = '\0';

    return;
}


long
dssncasecmp(char *s1, char *s2, int n)
{
    for (; n > 0; ++s1, ++s2, --n)
        if (tolower(*s1) != tolower(*s2))
            return ((tolower(*s1) < tolower(*s2)) ? -1 : 1);
        else if (*s1 == '\0')
            return (0);
        return (0);
}

long
dsscasecmp(char *s1, char *s2)
{
    for (; tolower(*s1) == tolower(*s2); ++s1, ++s2)
        if (*s1 == '\0')
            return (0);
    return ((tolower(*s1) < tolower(*s2)) ? -1 : 1);
}

#ifndef STDLIB_HAS_GETOPT
int optind = 0;
int opterr = 0;
char *optarg = NULL;

int
getopt(int ac, char **av, char *opt)
{
    static char *nextchar = NULL;
    char *cp;
    char hold;

    if (optarg == NULL)
        {
        optarg = (char *)malloc(BUFSIZ);
        MALLOC_CHECK(optarg);
        }

    if (!nextchar || *nextchar == '\0')
        {
        optind++;
        if (optind == ac)
            return(-1);
        nextchar = av[optind];
        if (*nextchar != '-')
            return(-1);
        nextchar +=1;
        }

    if (nextchar && *nextchar == '-')   /* -- termination */
        {
        optind++;
        return(-1);
        }
    else        /* found an option */
        {
        cp = strchr(opt, *nextchar);
        nextchar += 1;
        if (cp == NULL) /* not defined for this run */
            return('?');
        if (*(cp + 1) == ':')   /* option takes an argument */
            {
            if (*nextchar)
                {
                hold = *cp;
                cp = optarg;
                while (*nextchar)
                    *cp++ = *nextchar++;
                *cp = '\0';
                *cp = hold;
                }
            else        /* white space separated, use next arg */
                {
                if (++optind == ac)
                    return('?');
                strcpy(optarg, av[optind]);
                }
            nextchar = NULL;
            }
        return(*cp);
        }
}
#endif /* STDLIB_HAS_GETOPT */

char **
mk_ascdate(void)
{
    char **m;
    dss_time_t t;
    DSS_HUGE i;

    m = (char**) malloc((size_t)(TOTDATE * sizeof (char *)));
    MALLOC_CHECK(m);
    for (i = 0; i < TOTDATE; i++)
        {
        mk_time(i + 1, &t);
        m[i] = strdup(t.alpha);
        }

    return(m);
}

/*
 * set_state() -- initialize the RNG so that
 * appropriate data sets can be generated.
 * For each table that is to be generated, calculate the number of rows/child, and send that to the
 * seed generation routine in speed_seed.c. Note: assumes that tables are completely independent.
 * Returns the number of rows to be generated by the named step.
 */
DSS_HUGE
set_state(int table, long sf, long procs, long step, DSS_HUGE *extra_rows)
{
    int i;
	DSS_HUGE rowcount, remainder, result;
	
    if (sf == 0 || step == 0)
        return(0);

	rowcount = tdefs[table].base;
	rowcount *= sf;
	*extra_rows = rowcount % procs;
	rowcount /= procs;
	result = rowcount;
	for (i=0; i < step - 1; i++)
		{
		if (table == LINE)	/* special case for shared seeds */
			tdefs[table].gen_seed(1, rowcount);
		else
			tdefs[table].gen_seed(0, rowcount);
		/* need to set seeds of child in case there's a dependency */
		/* NOTE: this assumes that the parent and child have the same base row count */
			if (tdefs[table].child != NONE) 
			tdefs[tdefs[table].child].gen_seed(0,rowcount);
		}
	if (step > procs)	/* moving to the end to generate updates */
		tdefs[table].gen_seed(0, *extra_rows);

	return(result);
}
