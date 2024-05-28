/*
* $Id: driver.c,v 1.7 2008/09/24 22:35:21 jms Exp $
*
* Revision History
* ===================
* $Log: driver.c,v $
* Revision 1.7  2008/09/24 22:35:21  jms
* remove build number header
*
* Revision 1.6  2008/09/24 22:30:29  jms
* remove build number from default header
*
* Revision 1.5  2008/03/21 17:38:39  jms
* changes for 2.6.3
*
* Revision 1.4  2006/04/26 23:01:10  jms
* address update generation problems
*
* Revision 1.3  2005/10/28 02:54:35  jms
* add release.h changes
*
* Revision 1.2  2005/01/03 20:08:58  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:46  jms
* re-establish external server
*
* Revision 1.5  2004/04/07 20:17:29  jms
* bug #58 (join fails between order/lineitem)
*
* Revision 1.4  2004/02/18 16:26:49  jms
* 32/64 bit changes for overflow handling needed additional changes when ported back to windows
*
* Revision 1.3  2004/01/22 05:49:29  jms
* AIX porting (AIX 5.1)
*
* Revision 1.2  2004/01/22 03:54:12  jms
* 64 bit support changes for customer address
*
* Revision 1.1.1.1  2003/08/08 21:50:33  jms
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
/* main driver for dss banchmark */

#define DECLARER				/* EXTERN references get defined here */
#define NO_FUNC (int (*) ()) NULL	/* to clean up tdefs */
#define NO_LFUNC (long (*) ()) NULL		/* to clean up tdefs */

#include "config.h"
#include "release.h"
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

#include "dss.h"
#include "dsstypes.h"

/*
* Function prototypes
*/
void	usage (void);
void	kill_load (void);
int		pload (int tbl);
void	gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num);
int		pr_drange (int tbl, DSS_HUGE min, DSS_HUGE cnt, long num);
int		set_files (int t, int pload);
int		partial (int, int);


extern int optind, opterr;
extern char *optarg;
DSS_HUGE rowcnt = 0, minrow = 0;
long upd_num = 0;
double flt_scale;
#if (defined(WIN32)&&!defined(_POSIX_))
char *spawn_args[25];
#endif
#ifdef RNG_TEST
extern seed_t Seed[];
#endif
static int bTableSet = 0;


/*
* general table descriptions. See dss.h for details on structure
* NOTE: tables with no scaling info are scaled according to
* another table
*
*
* the following is based on the tdef structure defined in dss.h as:
* typedef struct
* {
* char     *name;            -- name of the table; 
*                               flat file output in <name>.tbl
* long      base;            -- base scale rowcount of table; 
*                               0 if derived
* int       (*loader) ();    -- function to present output
* long      (*gen_seed) ();  -- functions to seed the RNG
* int       child;           -- non-zero if there is an associated detail table
* unsigned long vtotal;      -- "checksum" total 
* }         tdef;
*
*/

/*
* flat file print functions; used with -F(lat) option
*/
int pr_cust (customer_t * c, int mode);
int pr_line (order_t * o, int mode);
int pr_order (order_t * o, int mode);
int pr_part (part_t * p, int mode);
int pr_psupp (part_t * p, int mode);
int pr_supp (supplier_t * s, int mode);
int pr_order_line (order_t * o, int mode);
int pr_part_psupp (part_t * p, int mode);
int pr_nation (code_t * c, int mode);
int pr_region (code_t * c, int mode);

/*
* seed generation functions; used with '-O s' option
*/
long sd_cust (int child, DSS_HUGE skip_count);
long sd_line (int child, DSS_HUGE skip_count);
long sd_order (int child, DSS_HUGE skip_count);
long sd_part (int child, DSS_HUGE skip_count);
long sd_psupp (int child, DSS_HUGE skip_count);
long sd_supp (int child, DSS_HUGE skip_count);
long sd_order_line (int child, DSS_HUGE skip_count);
long sd_part_psupp (int child, DSS_HUGE skip_count);

tdef tdefs[] =
{
	{"part.tbl", "part table", 200000,
		pr_part, sd_part, PSUPP, 0},
	{"partsupp.tbl", "partsupplier table", 200000,
		pr_psupp, sd_psupp, NONE, 0},
	{"supplier.tbl", "suppliers table", 10000,
		pr_supp, sd_supp, NONE, 0},
	{"customer.tbl", "customers table", 150000,
		pr_cust, sd_cust, NONE, 0},
	{"orders.tbl", "order table", 150000,
		pr_order, sd_order, LINE, 0},
	{"lineitem.tbl", "lineitem table", 150000,
		pr_line, sd_line, NONE, 0},
	{"orders.tbl", "orders/lineitem tables", 150000,
		pr_order_line, sd_order, LINE, 0},
	{"part.tbl", "part/partsupplier tables", 200000,
		pr_part_psupp, sd_part, PSUPP, 0},
	{"nation.tbl", "nation table", NATIONS_MAX,
		pr_nation, NO_LFUNC, NONE, 0},
	{"region.tbl", "region table", NATIONS_MAX,
		pr_region, NO_LFUNC, NONE, 0},
};

/*
* re-set default output file names 
*/
int
set_files (int i, int pload)
{
	char line[80], *new_name;
	
	if (table & (1 << i))
child_table:
	{
		if (pload != -1)
			sprintf (line, "%s.%d", tdefs[i].name, pload);
		else
		{
			printf ("Enter new destination for %s data: ",
				tdefs[i].name);
			if (fgets (line, sizeof (line), stdin) == NULL)
				return (-1);;
			if ((new_name = strchr (line, '\n')) != NULL)
				*new_name = '\0';
			if ((int)strlen (line) == 0)
				return (0);
		}
		new_name = (char *) malloc ((int)strlen (line) + 1);
		MALLOC_CHECK (new_name);
		strcpy (new_name, line);
		tdefs[i].name = new_name;
		if (tdefs[i].child != NONE)
		{
			i = tdefs[i].child;
			tdefs[i].child = NONE;
			goto child_table;
		}
	}
	
	return (0);
}



/*
* read the distributions needed in the benchamrk
*/
void
load_dists (void)
{
	read_dist (env_config (DIST_TAG, DIST_DFLT), "p_cntr", &p_cntr_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "colors", &colors);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "p_types", &p_types_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "nations", &nations);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "regions", &regions);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "o_oprio",
		&o_priority_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "instruct",
		&l_instruct_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "smode", &l_smode_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "category",
		&l_category_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "rflag", &l_rflag_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "msegmnt", &c_mseg_set);

	/* load the distributions that contain text generation */
	read_dist (env_config (DIST_TAG, DIST_DFLT), "nouns", &nouns);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "verbs", &verbs);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "adjectives", &adjectives);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "adverbs", &adverbs);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "auxillaries", &auxillaries);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "terminators", &terminators);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "articles", &articles);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "prepositions", &prepositions);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "grammar", &grammar);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "np", &np);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "vp", &vp);
	
}

/*
* generate a particular table
*/
void
gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num)
{
	static order_t o;
	supplier_t supp;
	customer_t cust;
	part_t part;
	code_t code;
	static int completed = 0;
	DSS_HUGE i;

	DSS_HUGE rows_per_segment=0;
	DSS_HUGE rows_this_segment=-1;
	DSS_HUGE residual_rows=0;

	if (insert_segments)
		{
		rows_per_segment = count / insert_segments;
		residual_rows = count - (rows_per_segment * insert_segments);
		}

	for (i = start; count; count--, i++)
	{
		LIFENOISE (1000, i);
		row_start(tnum);

		switch (tnum)
		{
		case LINE:
		case ORDER:
  		case ORDER_LINE: 
			mk_order (i, &o, upd_num % 10000);

		  if (insert_segments  && (upd_num > 0))
			if((upd_num / 10000) < residual_rows)
				{
				if((++rows_this_segment) > rows_per_segment) 
					{						
					rows_this_segment=0;
					upd_num += 10000;					
					}
				}
			else
				{
				if((++rows_this_segment) >= rows_per_segment) 
					{
					rows_this_segment=0;
					upd_num += 10000;
					}
				}

			if (set_seeds == 0)
				tdefs[tnum].loader(&o, upd_num);
			break;
		case SUPP:
			mk_supp (i, &supp);
			if (set_seeds == 0)
				tdefs[tnum].loader(&supp, upd_num);
			break;
		case CUST:
			mk_cust (i, &cust);
			if (set_seeds == 0)
				tdefs[tnum].loader(&cust, upd_num);
			break;
		case PSUPP:
		case PART:
  		case PART_PSUPP: 
			mk_part (i, &part);
			if (set_seeds == 0)
				tdefs[tnum].loader(&part, upd_num);
			break;
		case NATION:
			mk_nation (i, &code);
			if (set_seeds == 0)
				tdefs[tnum].loader(&code, 0);
			break;
		case REGION:
			mk_region (i, &code);
			if (set_seeds == 0)
				tdefs[tnum].loader(&code, 0);
			break;
		}
		row_stop(tnum);
		if (set_seeds && (i % tdefs[tnum].base) < 2)
		{
			printf("\nSeeds for %s at rowcount %ld\n", tdefs[tnum].comment, i);
			dump_seeds(tnum);
		}
	}
	completed |= 1 << tnum;
}



void
usage (void)
{
	fprintf (stderr, "%s\n%s\n\t%s\n%s %s\n\n",
		"USAGE:",
		"dbgen [-{vf}][-T {pcsoPSOL}]",
		"[-s <scale>][-C <procs>][-S <step>]",
		"dbgen [-v] [-O m] [-s <scale>]",
		"[-U <updates>]");
	fprintf (stderr, "Basic Options\n===========================\n");
	fprintf (stderr, "-C <n> -- separate data set into <n> chunks (requires -S, default: 1)\n");
	fprintf (stderr, "-f     -- force. Overwrite existing files\n");
	fprintf (stderr, "-h     -- display this message\n");
	fprintf (stderr, "-q     -- enable QUIET mode\n");
	fprintf (stderr, "-s <n> -- set Scale Factor (SF) to  <n> (default: 1) \n");
	fprintf (stderr, "-S <n> -- build the <n>th step of the data/update set (used with -C or -U)\n");
	fprintf (stderr, "-U <n> -- generate <n> update sets\n");
	fprintf (stderr, "-v     -- enable VERBOSE mode\n");
	fprintf (stderr, "\nAdvanced Options\n===========================\n");
	fprintf (stderr, "-b <s> -- load distributions for <s> (default: dists.dss)\n");
    fprintf (stderr, "-d <n> -- split deletes between <n> files (requires -U)\n");
    fprintf (stderr, "-i <n> -- split inserts between <n> files (requires -U)\n");
	fprintf (stderr, "-T c   -- generate cutomers ONLY\n");
	fprintf (stderr, "-T l   -- generate nation/region ONLY\n");
	fprintf (stderr, "-T L   -- generate lineitem ONLY\n");
	fprintf (stderr, "-T n   -- generate nation ONLY\n");
	fprintf (stderr, "-T o   -- generate orders/lineitem ONLY\n");
	fprintf (stderr, "-T O   -- generate orders ONLY\n");
	fprintf (stderr, "-T p   -- generate parts/partsupp ONLY\n");
	fprintf (stderr, "-T P   -- generate parts ONLY\n");
	fprintf (stderr, "-T r   -- generate region ONLY\n");
	fprintf (stderr, "-T s   -- generate suppliers ONLY\n");
	fprintf (stderr, "-T S   -- generate partsupp ONLY\n");
	fprintf (stderr,
		"\nTo generate the SF=1 (1GB), validation database population, use:\n");
	fprintf (stderr, "\tdbgen -vf -s 1\n");
	fprintf (stderr, "\nTo generate updates for a SF=1 (1GB), use:\n");
	fprintf (stderr, "\tdbgen -v -U 1 -s 1\n");
}

/*
* int partial(int tbl, int s) -- generate the s-th part of the named tables data
*/
int
partial (int tbl, int s)
{
	DSS_HUGE rowcnt;
	DSS_HUGE extra;
	
	if (verbose > 0)
	{
		fprintf (stderr, "\tStarting to load stage %d of %d for %s...",
			s, children, tdefs[tbl].comment);
	}
	
	set_files (tbl, s);
	
	rowcnt = set_state(tbl, scale, children, s, &extra);

	if (s == children)
		gen_tbl (tbl, rowcnt * (s - 1) + 1, rowcnt + extra, upd_num);
	else
		gen_tbl (tbl, rowcnt * (s - 1) + 1, rowcnt, upd_num);
	
	if (verbose > 0)
		fprintf (stderr, "done.\n");
	
	return (0);
}

void
process_options (int count, char **vector)
{
	int option;
	FILE *pF;
	
	while ((option = getopt (count, vector,
		"b:C:d:fi:hO:P:qs:S:T:U:v")) != -1)
	switch (option)
	{
		case 'b':				/* load distributions from named file */
			d_path = (char *)malloc((int)strlen(optarg) + 1);
			MALLOC_CHECK(d_path);
			strcpy(d_path, optarg);
			if ((pF = fopen(d_path, "r")) == NULL)
			{
				fprintf(stderr, "ERROR: Invalid argument to -b");
				exit(-1);
			}
			else
				fclose(pF);

			break;
		case 'C':
			children = atoi (optarg);
			break;
		case 'd':
			delete_segments = atoi (optarg);
			break;
		case 'f':				/* blind overwrites; Force */
			force = 1;
			break;
		case 'i':
			insert_segments = atoi (optarg);
			break;
		case 'q':				/* all prompts disabled */
			verbose = -1;
			break;
		case 's':				/* scale by Percentage of base rowcount */
		case 'P':				/* for backward compatibility */
			flt_scale = atof (optarg);
			if (flt_scale < MIN_SCALE)
			{
				int i;
				int int_scale;

				scale = 1;
				int_scale = (int)(1000 * flt_scale);
				for (i = PART; i < REGION; i++)
				{
					tdefs[i].base = (DSS_HUGE)(int_scale * tdefs[i].base)/1000;
					if (tdefs[i].base < 1)
						tdefs[i].base = 1;
				}
			}
			else
				scale = (long) flt_scale;
			if (scale > MAX_SCALE)
			{
				fprintf (stderr, "%s %5.0f %s\n\t%s\n\n",
					"NOTE: Data generation for scale factors >",
					MAX_SCALE,
					"GB is still in development,",
					"and is not yet supported.\n");
				fprintf (stderr,
					"Your resulting data set MAY NOT BE COMPLIANT!\n");
			}
			break;
		case 'S':				/* generate a particular STEP */
			step = atoi (optarg);
			break;
		case 'U':				/* generate flat files for update stream */
			updates = atoi (optarg);
			break;
		case 'v':				/* life noises enabled */
			verbose = 1;
			break;
		case 'T':				/* generate a specifc table */
			switch (*optarg)
			{
			case 'c':			/* generate customer ONLY */
				table = 1 << CUST;
				bTableSet = 1;
				break;
			case 'L':			/* generate lineitems ONLY */
				table = 1 << LINE;
				bTableSet = 1;
				break;
			case 'l':			/* generate code table ONLY */
				table = 1 << NATION;
				table |= 1 << REGION;
				bTableSet = 1;
				break;
			case 'n':			/* generate nation table ONLY */
				table = 1 << NATION;
				bTableSet = 1;
				break;
			case 'O':			/* generate orders ONLY */
				table = 1 << ORDER;
				bTableSet = 1;
				break;
			case 'o':			/* generate orders/lineitems ONLY */
				table = 1 << ORDER_LINE;
				bTableSet = 1;
				break;
			case 'P':			/* generate part ONLY */
				table = 1 << PART;
				bTableSet = 1;
				break;
			case 'p':			/* generate part/partsupp ONLY */
				table = 1 << PART_PSUPP;
				bTableSet = 1;
				break;
			case 'r':			/* generate region table ONLY */
				table = 1 << REGION;
				bTableSet = 1;
				break;
			case 'S':			/* generate partsupp ONLY */
				table = 1 << PSUPP;
				bTableSet = 1;
				break;
			case 's':			/* generate suppliers ONLY */
				table = 1 << SUPP;
				bTableSet = 1;
				break;
			default:
				fprintf (stderr, "Unknown table name %s\n",
					optarg);
				usage ();
				exit (1);
			}
			break;
		case 'O':				/* optional actions */
			switch (tolower (*optarg))
			{
			case 's':			/* calibrate the RNG usage */
				set_seeds = 1;
				break;
			default:
				fprintf (stderr, "Unknown option name %s\n",
					optarg);
				usage ();
				exit (1);
			}
			break;
		default:
			printf ("ERROR: option '%c' unknown.\n",
				*(vector[optind] + 1));
		case 'h':				/* something unexpected */
			fprintf (stderr,
				"%s Population Generator (Version %d.%d.%d build %d)\n",
				NAME, VERSION, RELEASE, PATCH, BUILD);
			fprintf (stderr, "Copyright %s %s\n", TPC, C_DATES);
			usage ();
			exit (1);
	}

	return;
}

void validate_options(void)
{
	// DBGenOptions, 3.1
	if (children != 1)
	{
		if (updates != 0)
		{
			fprintf(stderr, "ERROR: -C is not valid when generating updates\n");
			exit(-1);
		}
		if (step == -1)
		{
			fprintf(stderr, "ERROR: -S must be specified when generating data in multiple chunks\n");
			exit(-1);
		}
	}

	// DBGenOptions, 3.3
	if (updates == 0)
	{
		if ((insert_segments != 0) || (delete_segments != 0))
		{
			fprintf(stderr, "ERROR: -d/-i are only valid when generating updates\n");
			exit(-1);
		}
	}

	// DBGenOptions, 3.9
	if (step != -1)
	{
		if ((children == 1) && (updates == 0))
		{
			fprintf(stderr, "ERROR: -S is only valid when generating data in multiple chunks or generating updates\n");
			exit(-1);
		}
	}

	// DBGenOptions, 3.10
	if (bTableSet && (updates != 0))
	{
		fprintf(stderr, "ERROR: -T not valid when generating updates\n");
		exit(-1);
	}

	return;
}


/*
* MAIN
*
* assumes the existance of getopt() to clean up the command 
* line handling
*/
int
main (int ac, char **av)
{
	DSS_HUGE i;
	
	table = (1 << CUST) |
		(1 << SUPP) |
		(1 << NATION) |
		(1 << REGION) |
		(1 << PART_PSUPP) |
		(1 << ORDER_LINE);
	force = 0;
    insert_segments=0;
    delete_segments=0;
    insert_orders_segment=0;
    insert_lineitem_segment=0;
    delete_segment=0;
	verbose = 0;
	set_seeds = 0;
	scale = 1;
	flt_scale = 1.0;
	updates = 0;
	step = -1;
	tdefs[ORDER].base *=
		ORDERS_PER_CUST;			/* have to do this after init */
	tdefs[LINE].base *=
		ORDERS_PER_CUST;			/* have to do this after init */
	tdefs[ORDER_LINE].base *=
		ORDERS_PER_CUST;			/* have to do this after init */
	children = 1;
	d_path = NULL;
	
#ifdef NO_SUPPORT
	signal (SIGINT, exit);
#endif /* NO_SUPPORT */
	process_options (ac, av);
	validate_options();
#if (defined(WIN32)&&!defined(_POSIX_))
	for (i = 0; i < ac; i++)
	{
		spawn_args[i] = malloc (((int)strlen (av[i]) + 1) * sizeof (char));
		MALLOC_CHECK (spawn_args[i]);
		strcpy (spawn_args[i], av[i]);
	}
	spawn_args[ac] = NULL;
#endif
	
	if (verbose >= 0)
		{
		fprintf (stderr,
			"%s Population Generator (Version %d.%d.%d)\n",
			NAME, VERSION, RELEASE, PATCH);
		fprintf (stderr, "Copyright %s %s\n", TPC, C_DATES);
		}
	
	load_dists ();
#ifdef RNG_TEST
	for (i=0; i <= MAX_STREAM; i++)
		Seed[i].nCalls = 0;
#endif
	/* have to do this after init */
	tdefs[NATION].base = nations.count;
	tdefs[REGION].base = regions.count;
	
	/* 
	* updates are never parallelized 
	*/
	if (updates)
		{
		/* 
		 * set RNG to start generating rows beyond SF=scale
		 */
		set_state (ORDER, scale, 100, 101, &i); 
		rowcnt = (int)(tdefs[ORDER_LINE].base / 10000 * scale * UPD_PCT);
		if (step > 0)
			{
			/* 
			 * adjust RNG for any prior update generation
			 */
	      for (i=1; i < step; i++)
         {
			sd_order(0, rowcnt);
			sd_line(0, rowcnt);
         }
			upd_num = step - 1;
			}
		else
			upd_num = 0;

		while (upd_num < updates)
			{
			if (verbose > 0)
				fprintf (stderr,
				"Generating update pair #%d for %s",
				upd_num + 1, tdefs[ORDER_LINE].comment);
			insert_orders_segment=0;
			insert_lineitem_segment=0;
			delete_segment=0;
			minrow = upd_num * rowcnt + 1;
			gen_tbl (ORDER_LINE, minrow, rowcnt, upd_num + 1);
			if (verbose > 0)
				fprintf (stderr, "done.\n");
			pr_drange (ORDER_LINE, minrow, rowcnt, upd_num + 1);
			upd_num++;
			}

		exit (0);
		}
	
	/**
	** actual data generation section starts here
	**/

	/*
	* traverse the tables, invoking the appropriate data generation routine for any to be built
	*/
	for (i = PART; i <= REGION; i++)
		if (table & (1 << i))
		{
			if (children > 1 && i < NATION)
			{
				partial ((int)i, step);
			}
			else
			{
				minrow = 1;
				if (i < NATION)
					rowcnt = tdefs[i].base * scale;
				else
					rowcnt = tdefs[i].base;
				if (verbose > 0)
					fprintf (stderr, "Generating data for %s", tdefs[i].comment);
				gen_tbl ((int)i, minrow, rowcnt, upd_num);
				if (verbose > 0)
					fprintf (stderr, "done.\n");
			}
		}
			
		return (0);
}
