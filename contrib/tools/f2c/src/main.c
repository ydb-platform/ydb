/****************************************************************
Copyright 1990-1996, 2000-2001 by AT&T, Lucent Technologies and Bellcore.

Permission to use, copy, modify, and distribute this software
and its documentation for any purpose and without fee is hereby
granted, provided that the above copyright notice appear in all
copies and that both that the copyright notice and this
permission notice and warranty disclaimer appear in supporting
documentation, and that the names of AT&T, Bell Laboratories,
Lucent or Bellcore or any of their entities not be used in
advertising or publicity pertaining to distribution of the
software without specific, written prior permission.

AT&T, Lucent and Bellcore disclaim all warranties with regard to
this software, including all implied warranties of
merchantability and fitness.  In no event shall AT&T, Lucent or
Bellcore be liable for any special, indirect or consequential
damages or any damages whatsoever resulting from loss of use,
data or profits, whether in an action of contract, negligence or
other tortious action, arising out of or in connection with the
use or performance of this software.
****************************************************************/

extern char F2C_version[];

#include "defs.h"
#include "parse.h"

int complex_seen, dcomplex_seen;

LOCAL int Max_ftn_files;

int badargs;
char **ftn_files;
int current_ftn_file = 0;

flag ftn66flag = NO;
flag nowarnflag = NO;
flag noextflag = NO;
flag  no66flag = NO;		/* Must also set noextflag to this
					   same value */
flag zflag = YES;		/* recognize double complex intrinsics */
flag debugflag = NO;
flag onetripflag = NO;
flag shiftcase = YES;
flag undeftype = NO;
flag checksubs = NO;
flag r8flag = NO;
flag use_bs = YES;
flag keepsubs = NO;
flag byterev = NO;
int intr_omit;
static int no_cd, no_i90;
#ifdef TYQUAD
flag use_tyquad = YES;
#ifndef NO_LONG_LONG
flag allow_i8c = YES;
#endif
#endif
int tyreal = TYREAL;
int tycomplex = TYCOMPLEX;

int maxregvar = MAXREGVAR;	/* if maxregvar > MAXREGVAR, error */
int maxequiv = MAXEQUIV;
int maxext = MAXEXT;
int maxstno = MAXSTNO;
int maxctl = MAXCTL;
int maxhash = MAXHASH;
int maxliterals = MAXLITERALS;
int maxcontin = MAXCONTIN;
int maxlablist = MAXLABLIST;
int extcomm, ext1comm, useauto;
int can_include = YES;	/* so we can disable includes for netlib */

static char *def_i2 = "";

static int useshortints = NO;	/* YES => tyint = TYSHORT */
static int uselongints = NO;	/* YES => tyint = TYLONG */
int addftnsrc = NO;		/* Include ftn source in output */
int usedefsforcommon = NO;	/* Use #defines for common reference */
int forcedouble = YES;		/* force real functions to double */
int dneg = NO;			/* f77 treatment of unary minus */
int Ansi = YES;
int def_equivs = YES;
int tyioint = TYLONG;
int szleng = SZLENG;
int inqmask = M(TYLONG)|M(TYLOGICAL);
int wordalign = NO;
int forcereal = NO;
int warn72 = NO;
static int help, showver, skipC, skipversion;
char *file_name, *filename0, *parens;
int Castargs = 1;
static int Castargs1;
static int typedefs = 0;
int chars_per_wd, gflag, protostatus;
int infertypes = 1;
char used_rets[TYSUBR+1];
extern char *tmpdir;
static int h0align = 0;
char *halign, *ohalign;
int krparens = NO;
int hsize;	/* for padding under -h */
int htype;	/* for wr_equiv_init under -h */
int trapuv;
chainp Iargs;

#define f2c_entry(swit,count,type,store,size) \
	p_entry ("-", swit, 0, count, type, store, size)

static arg_info table[] = {
    f2c_entry ("w66", P_NO_ARGS, P_INT, &ftn66flag, YES),
    f2c_entry ("w", P_NO_ARGS, P_INT, &nowarnflag, YES),
    f2c_entry ("66", P_NO_ARGS, P_INT, &no66flag, YES),
    f2c_entry ("1", P_NO_ARGS, P_INT, &onetripflag, YES),
    f2c_entry ("onetrip", P_NO_ARGS, P_INT, &onetripflag, YES),
    f2c_entry ("I2", P_NO_ARGS, P_INT, &useshortints, YES),
    f2c_entry ("I4", P_NO_ARGS, P_INT, &uselongints, YES),
    f2c_entry ("U", P_NO_ARGS, P_INT, &shiftcase, NO),
    f2c_entry ("u", P_NO_ARGS, P_INT, &undeftype, YES),
    f2c_entry ("O", P_ONE_ARG, P_INT, &maxregvar, 0),
    f2c_entry ("C", P_NO_ARGS, P_INT, &checksubs, YES),
    f2c_entry ("Nq", P_ONE_ARG, P_INT, &maxequiv, 0),
    f2c_entry ("Nx", P_ONE_ARG, P_INT, &maxext, 0),
    f2c_entry ("Ns", P_ONE_ARG, P_INT, &maxstno, 0),
    f2c_entry ("Nc", P_ONE_ARG, P_INT, &maxctl, 0),
    f2c_entry ("Nn", P_ONE_ARG, P_INT, &maxhash, 0),
    f2c_entry ("NL", P_ONE_ARG, P_INT, &maxliterals, 0),
    f2c_entry ("NC", P_ONE_ARG, P_INT, &maxcontin, 0),
    f2c_entry ("Nl", P_ONE_ARG, P_INT, &maxlablist, 0),
    f2c_entry ("c", P_NO_ARGS, P_INT, &addftnsrc, YES),
    f2c_entry ("p", P_NO_ARGS, P_INT, &usedefsforcommon, YES),
    f2c_entry ("R", P_NO_ARGS, P_INT, &forcedouble, NO),
    f2c_entry ("!R", P_NO_ARGS, P_INT, &forcedouble, YES),
    f2c_entry ("A", P_NO_ARGS, P_INT, &Ansi, YES),
    f2c_entry ("K", P_NO_ARGS, P_INT, &Ansi, NO),
    f2c_entry ("ext", P_NO_ARGS, P_INT, &noextflag, YES),
    f2c_entry ("z", P_NO_ARGS, P_INT, &zflag, NO),
    f2c_entry ("a", P_NO_ARGS, P_INT, &useauto, YES),
    f2c_entry ("r8", P_NO_ARGS, P_INT, &r8flag, YES),
    f2c_entry ("i2", P_NO_ARGS, P_INT, &tyioint, NO),
    f2c_entry ("w8", P_NO_ARGS, P_INT, &wordalign, YES),
    f2c_entry ("!I", P_NO_ARGS, P_INT, &can_include, NO),
    f2c_entry ("W", P_ONE_ARG, P_INT, &chars_per_wd, 0),
    f2c_entry ("g", P_NO_ARGS, P_INT, &gflag, YES),
    f2c_entry ("T", P_ONE_ARG, P_STRING, &tmpdir, 0),
    f2c_entry ("E", P_NO_ARGS, P_INT, &extcomm, 1),
    f2c_entry ("e1c", P_NO_ARGS, P_INT, &ext1comm, 1),
    f2c_entry ("ec", P_NO_ARGS, P_INT, &ext1comm, 2),
    f2c_entry ("C++", P_NO_ARGS, P_INT, &Ansi, 2),
    f2c_entry ("P", P_NO_ARGS, P_INT, &Castargs, 3),
    f2c_entry ("Ps", P_NO_ARGS, P_INT, &protostatus, 1),
    f2c_entry ("!P", P_NO_ARGS, P_INT, &Castargs, 0),
    f2c_entry ("!c", P_NO_ARGS, P_INT, &skipC, 1),
    f2c_entry ("!it", P_NO_ARGS, P_INT, &infertypes, 0),
    f2c_entry ("h", P_NO_ARGS, P_INT, &h0align, 1),
    f2c_entry ("hd", P_NO_ARGS, P_INT, &h0align, 2),
    f2c_entry ("kr", P_NO_ARGS, P_INT, &krparens, 1),
    f2c_entry ("krd", P_NO_ARGS, P_INT, &krparens, 2),
    f2c_entry ("!bs", P_NO_ARGS, P_INT, &use_bs, NO),
    f2c_entry ("r", P_NO_ARGS, P_INT, &forcereal, YES),
    f2c_entry ("72", P_NO_ARGS, P_INT, &warn72, 1),
    f2c_entry ("f", P_NO_ARGS, P_INT, &warn72, 2),
    f2c_entry ("s", P_NO_ARGS, P_INT, &keepsubs, 1),
    f2c_entry ("d", P_ONE_ARG, P_STRING, &outbuf, 0),
    f2c_entry ("cd", P_NO_ARGS, P_INT, &no_cd, 1),
    f2c_entry ("i90", P_NO_ARGS, P_INT, &no_i90, 2),
    f2c_entry ("trapuv", P_NO_ARGS, P_INT, &trapuv, 1),
#ifdef TYQUAD
#ifndef NO_LONG_LONG
    f2c_entry ("!i8const", P_NO_ARGS, P_INT, &allow_i8c, NO),
#endif
    f2c_entry ("!i8", P_NO_ARGS, P_INT, &use_tyquad, NO),
#endif

	/* options omitted from man pages */

	/* -b ==> for unformatted I/O, call do_unio (for noncharacter  */
	/* data of length > 1 byte) and do_ucio (for the rest) rather  */
	/* than do_uio.  This permits modifying libI77 to byte-reverse */
	/* numeric data. */

    f2c_entry ("b", P_NO_ARGS, P_INT, &byterev, YES),

	/* -ev ==> implement equivalence with initialized pointers */
    f2c_entry ("ev", P_NO_ARGS, P_INT, &def_equivs, NO),

	/* -!it used to be the default when -it was more agressive */

    f2c_entry ("it", P_NO_ARGS, P_INT, &infertypes, 1),

	/* -Pd is similar to -P, but omits :ref: lines */
    f2c_entry ("Pd", P_NO_ARGS, P_INT, &Castargs, 2),

	/* -t ==> emit typedefs (under -A or -C++) for procedure
		argument types used.  This is meant for netlib's
		f2c service, so -A and -C++ will work with older
		versions of f2c.h
		*/
    f2c_entry ("t", P_NO_ARGS, P_INT, &typedefs, 1),

	/* -!V ==> omit version msg (to facilitate using diff in
		regression testing)
		*/
    f2c_entry ("!V", P_NO_ARGS, P_INT, &skipversion, 1),

	/* -Dnnn = debug level nnn */

    f2c_entry ("D", P_ONE_ARG, P_INT, &debugflag, YES),

	/* -dneg ==> under (default) -!R, imitate f77's bizarre	*/
	/* treatment of unary minus of REAL expressions by	*/
	/* promoting them to DOUBLE PRECISION . */

    f2c_entry ("dneg", P_NO_ARGS, P_INT, &dneg, YES),

	/* -?, --help, -v, --version */

    f2c_entry ("?", P_NO_ARGS, P_INT, &help, YES),
    f2c_entry ("-help", P_NO_ARGS, P_INT, &help, YES),

    f2c_entry ("v", P_NO_ARGS, P_INT, &showver, YES),
    f2c_entry ("-version", P_NO_ARGS, P_INT, &showver, YES)

}; /* table */

extern char *c_functions;	/* "c_functions"	*/
extern char *coutput;		/* "c_output"		*/
extern char *initfname;		/* "raw_data"		*/
extern char *blkdfname;		/* "block_data"		*/
extern char *p1_file;		/* "p1_file"		*/
extern char *p1_bakfile;	/* "p1_file.BAK"	*/
extern char *sortfname;		/* "init_file"		*/
extern char *proto_fname;	/* "proto_file"		*/
FILE *protofile;

 void
set_externs(Void)
{
    static char *hset[3] = { 0, "integer", "doublereal" };

/* Adjust the global flags according to the command line parameters */

    if (chars_per_wd > 0) {
	typesize[TYADDR] = typesize[TYLONG] = typesize[TYREAL] =
		typesize[TYLOGICAL] = chars_per_wd;
	typesize[TYINT1] = typesize[TYLOGICAL1] = 1;
	typesize[TYDREAL] = typesize[TYCOMPLEX] = chars_per_wd << 1;
	typesize[TYDCOMPLEX] = chars_per_wd << 2;
	typesize[TYSHORT] = typesize[TYLOGICAL2] = chars_per_wd >> 1;
	typesize[TYCILIST] = 5*chars_per_wd;
	typesize[TYICILIST] = 6*chars_per_wd;
	typesize[TYOLIST] = 9*chars_per_wd;
	typesize[TYCLLIST] = 3*chars_per_wd;
	typesize[TYALIST] = 2*chars_per_wd;
	typesize[TYINLIST] = 26*chars_per_wd;
	}

    if (wordalign)
	typealign[TYDREAL] = typealign[TYDCOMPLEX] = typealign[TYREAL];
    if (!tyioint) {
	tyioint = TYSHORT;
	szleng = typesize[TYSHORT];
	def_i2 = "#define f2c_i2 1\n";
	inqmask = M(TYSHORT)|M(TYLOGICAL2);
	goto checklong;
	}
    else
	szleng = typesize[TYLONG];
    if (useshortints) {
	/* inqmask = M(TYLONG); */
	/* used to disallow LOGICAL in INQUIRE under -I2 */
 checklong:
	protorettypes[TYLOGICAL] = "shortlogical";
	casttypes[TYLOGICAL] = "K_fp";
	if (uselongints)
		err ("Can't use both long and short ints");
	else {
		tyint = tylogical = TYSHORT;
		tylog = TYLOGICAL2;
		}
	}
    else if (uselongints)
	tyint = TYLONG;

    if (h0align) {
	if (tyint == TYLONG && wordalign)
		h0align = 1;
    	ohalign = halign = hset[h0align];
	htype = h0align == 1 ? tyint : TYDREAL;
	hsize = typesize[htype];
	}

    if (no66flag)
	noextflag = no66flag;
    if (noextflag)
	zflag = 0;

    if (r8flag) {
	tyreal = TYDREAL;
	tycomplex = TYDCOMPLEX;
	r8fix();
	}
    if (forcedouble) {
	protorettypes[TYREAL] = "E_f";
	casttypes[TYREAL] = "E_fp";
	}
    else
	dneg = 0;

#ifndef NO_LONG_LONG
    if (!use_tyquad)
	allow_i8c = 0;
#endif

    if (maxregvar > MAXREGVAR) {
	warni("-O%d: too many register variables", maxregvar);
	maxregvar = MAXREGVAR;
    } /* if maxregvar > MAXREGVAR */

/* Check the list of input files */

    {
	int bad, i, cur_max = Max_ftn_files;

	for (i = bad = 0; i < cur_max && ftn_files[i]; i++)
	    if (ftn_files[i][0] == '-') {
		errstr ("Invalid flag '%s'", ftn_files[i]);
		bad++;
		}
	if (bad)
		exit(1);

    } /* block */
} /* set_externs */


 static int
comm2dcl(Void)
{
	Extsym *ext;
	if (ext1comm)
		for(ext = extsymtab; ext < nextext; ext++)
			if (ext->extstg == STGCOMMON && !ext->extinit)
				return ext1comm;
	return 0;
	}

 static void
#ifdef KR_headers
write_typedefs(outfile)
	FILE *outfile;
#else
write_typedefs(FILE *outfile)
#endif
{
	register int i;
	register char *s, *p = 0;
	static char st[4] = { TYREAL, TYCOMPLEX, TYDCOMPLEX, TYCHAR };
	static char stl[4] = { 'E', 'C', 'Z', 'H' };

	for(i = 0; i <= TYSUBR; i++)
		if (s = usedcasts[i]) {
			if (!p) {
				p = (char*)(Ansi == 1 ? "()" : "(...)");
				nice_printf(outfile,
				"/* Types for casting procedure arguments: */\
\n\n#ifndef F2C_proc_par_types\n");
				if (i == 0) {
					nice_printf(outfile,
			"typedef int /* Unknown procedure type */ (*%s)%s;\n",
						 s, p);
					continue;
					}
				}
			nice_printf(outfile, "typedef %s (*%s)%s;\n",
					c_type_decl(i,1), s, p);
			}
	for(i = !forcedouble; i < 4; i++)
		if (used_rets[st[i]])
			nice_printf(outfile,
				"typedef %s %c_f; /* %s function */\n",
				p = (char*)(i ? "VOID" : "doublereal"),
				stl[i], ftn_types[st[i]]);
	if (p)
		nice_printf(outfile, "#endif\n\n");
	}

 static void
#ifdef KR_headers
commonprotos(outfile)
	register FILE *outfile;
#else
commonprotos(register FILE *outfile)
#endif
{
	register Extsym *e, *ee;
	register Argtypes *at;
	Atype *a, *ae;
	int k;
	extern int proc_protochanges;

	if (!outfile)
		return;
	for (e = extsymtab, ee = nextext; e < ee; e++)
		if (e->extstg == STGCOMMON && e->allextp)
			nice_printf(outfile, "/* comlen %s %ld */\n",
				e->cextname, e->maxleng);
	if (Castargs1 < 3)
		return;

	/* -Pr: special comments conveying current knowledge
	    of external references */

	k = proc_protochanges;
	for (e = extsymtab, ee = nextext; e < ee; e++)
		if (e->extstg == STGEXT
		&& e->cextname != e->fextname)	/* not a library function */
		    if (at = e->arginfo) {
			if ((!e->extinit || at->changes & 1)
				/* not defined here or
					changed since definition */
			&& at->nargs >= 0) {
				nice_printf(outfile, "/*:ref: %s %d %d",
					e->cextname, e->extype, at->nargs);
				a = at->atypes;
				for(ae = a + at->nargs; a < ae; a++)
					nice_printf(outfile, " %d", a->type);
				nice_printf(outfile, " */\n");
				if (at->changes & 1)
					k++;
				}
			}
		    else if (e->extype)
			/* typed external, never invoked */
			nice_printf(outfile, "/*:ref: %s %d :*/\n",
				e->cextname, e->extype);
	if (k) {
		nice_printf(outfile,
	"/* Rerunning f2c -P may change prototypes or declarations. */\n");
		if (nerr)
			return;
		if (protostatus)
			done(4);
		if (protofile != stdout) {
			fprintf(diagfile,
	"Rerunning \"f2c -P ... %s %s\" may change prototypes or declarations.\n",
				filename0, proto_fname);
			fflush(diagfile);
			}
		}
	}

 static int
#ifdef KR_headers
I_args(argc, a)
	int argc;
	char **a;
#else
I_args(int argc, char **a)
#endif
{
	char **a0, **a1, **ae, *s;

	ae = a + argc;
	a0 = a;
	for(a1 = ++a; a < ae; a++) {
		if (!(s = *a))
			break;
		if (*s == '-' && s[1] == 'I' && s[2]
		  && (s[3] || s[2] != '2' && s[2] != '4'))
			Iargs = mkchain(s+2, Iargs);
		else
			*a1++ = s;
		}
	Iargs = revchain(Iargs);
	*a1 = 0;
	return a1 - a0;
	}

 static void
omit_non_f(Void)
{
	/* complain about ftn_files that do not end in .f or .F */

	char *s, *s1;
	int i, k;

	for(i = k = 0; s = ftn_files[k]; k++) {
		s1 = s + strlen(s);
		if (s1 - s >= 3) {
			s1 -= 2;
			if (*s1 == '.') switch(s1[1]) {
			  case 'f':
			  case 'F':
				ftn_files[i++] = s;
				continue;
			  }
			}
		fprintf(diagfile, "\"%s\" does not end in .f or .F\n", s);
		}
	if (i != k) {
		fflush(diagfile);
		if (!i)
			exit(1);
		ftn_files[i] = 0;
		}
	}

 static void
show_version(Void)
{
	printf("f2c (Fortran to C Translator) version %s.\n", F2C_version);
	}

 static void
#ifdef KR_headers
show_help(progname) char *progname;
#else
show_help(char *progname)
#endif
{
	show_version();
	if (!progname)
		progname = "f2c";
	printf("Usage: %s [ option ... ] [file ...]\n%s%s%s%s%s%s%s",
	progname,
	"For usage details, see the man page, f2c.1.\n",
	"For technical details, see the f2c report.\n",
	"Both are available from netlib, e.g.,\n",
	"\thttps://www.netlib.org/f2c/f2c.1\n",
	"\thttps://www.netlib.org/f2c/f2c.pdf\nor\n",
	"\thttps://ampl.com/netlib/f2c/f2c.1\n",
	"\thttps://ampl.com/netlib/f2c/f2c.pdf\n");
	}

 int retcode = 0;

 int
#ifdef KR_headers
main(argc, argv)
	int argc;
	char **argv;
#else
main(int argc, char **argv)
#endif
{
	int c2d, k;
	FILE *c_output;
	char *cdfilename;
	static char stderrbuf[BUFSIZ];
	extern char **dfltproc, *dflt1proc[];
	extern char link_msg[];

	diagfile = stderr;
	setbuf(stderr, stderrbuf);	/* arrange for fast error msgs */

	argkludge(&argc, &argv);		/* for _WIN32 */
	argc = I_args(argc, argv);	/* extract -I args */
	Max_ftn_files = argc - 1;
	ftn_files = (char **)ckalloc((argc+1)*sizeof(char *));

	parse_args (argc, argv, table, sizeof(table)/sizeof(arg_info),
		ftn_files, Max_ftn_files);
	if (badargs)
		return 1;
	if (help) {
		show_help(argv[0]);
		return 0;
		}
	if (showver && !ftn_files[0]) {
		show_version();
		return 0;
		}
	intr_omit = no_cd | no_i90;
	if (keepsubs && checksubs) {
		warn("-C suppresses -s\n");
		keepsubs = 0;
		}
	if (!can_include && ext1comm == 2)
		ext1comm = 1;
	if (ext1comm && !extcomm)
		extcomm = 2;
	if (protostatus)
		Castargs = 3;
	Castargs1 = Castargs;
	if (!Ansi) {
		Castargs = 0;
		parens = "()";
		}
	else if (!Castargs)
		parens = (char*)(Ansi == 1 ? "()" : "(...)");
	else
		dfltproc = dflt1proc;

	outbuf_adjust();
	set_externs();
	fileinit();
	read_Pfiles(ftn_files);
	omit_non_f();

	for(k = 0; ftn_files[k+1]; k++)
		if (dofork(ftn_files[k]))
			break;
	filename0 = file_name = ftn_files[current_ftn_file = k];

	set_tmp_names();
	sigcatch(0);

	c_file   = opf(c_functions, textwrite);
	pass1_file=opf(p1_file, binwrite);
	initkey();
	if (file_name && *file_name) {
		cdfilename = coutput;
		if (debugflag != 1) {
			coutput = c_name(file_name,'c');
			cdfilename = copys(outbtail);
			if (Castargs1 >= 2)
				proto_fname = c_name(file_name,'P');
			}
		if (skipC)
			coutput = 0;
		else if (!(c_output = fopen(coutput, textwrite))) {
			file_name = coutput;
			coutput = 0;	/* don't delete read-only .c file */
			fatalstr("can't open %.86s", file_name);
			}

		if (Castargs1 >= 2
		&& !(protofile = fopen(proto_fname, textwrite)))
			fatalstr("Can't open %.84s\n", proto_fname);
		}
	else {
		file_name = "";
		cdfilename = "f2c_out.c";
		c_output = stdout;
		coutput = 0;
		if (Castargs1 >= 2) {
			protofile = stdout;
			if (!skipC)
				printf("#ifdef P_R_O_T_O_T_Y_P_E_S\n");
			}
		}

	if(inilex( copys(file_name) ))
		done(1);
	if (filename0) {
		fprintf(diagfile, "%s:\n", file_name);
		fflush(diagfile);
		}

	procinit();
	if(k = yyparse())
	{
		fprintf(diagfile, "Bad parse, return code %d\n", k);
		done(1);
	}

	commonprotos(protofile);
	if (protofile == stdout && !skipC)
		printf("#endif\n\n");

	if (nerr || skipC)
		goto C_skipped;


/* Write out the declarations which are global to this file */

	if ((c2d = comm2dcl()) == 1)
		nice_printf(c_output, "/*>>>'/dev/null'<<<*/\n\n\
/* Split this into several files by piping it through\n\n\
sed \"s/^\\/\\*>>>'\\(.*\\)'<<<\\*\\/\\$/cat >'\\1' <<'\\/*<<<\\1>>>*\\/'/\" | /bin/sh\n\
 */\n\
/*<<</dev/null>>>*/\n\
/*>>>'%s'<<<*/\n", cdfilename);
	if (gflag)
		nice_printf (c_output, "#line 1 \"%s\"\n", file_name);
	if (!skipversion) {
		nice_printf (c_output, "/* %s -- translated by f2c ", file_name);
		nice_printf (c_output, "(version %s).\n", F2C_version);
		nice_printf (c_output,
	"   You must link the resulting object file with libf2c:\n\
	%s\n*/\n\n", link_msg);
		}
	if (Ansi == 2)
		nice_printf(c_output,
			"#ifdef __cplusplus\nextern \"C\" {\n#endif\n");
	nice_printf (c_output, "%s#include \"f2c.h\"\n\n", def_i2);
	if (trapuv)
		nice_printf(c_output, "extern void _uninit_f2c(%s);\n%s\n\n",
			Ansi ? "void*,int,long" : "", "extern double _0;");
	if (gflag)
		nice_printf (c_output, "#line 1 \"%s\"\n", file_name);
	if (Castargs && typedefs)
		write_typedefs(c_output);
	nice_printf (c_file, "\n");
	fclose (c_file);
	c_file = c_output;		/* HACK to get the next indenting
					   to work */
	wr_common_decls (c_output);
	if (blkdfile)
		list_init_data(&blkdfile, blkdfname, c_output);
	wr_globals (c_output);
	if ((c_file = fopen (c_functions, textread)) == (FILE *) NULL)
	    Fatal("main - couldn't reopen c_functions");
	ffilecopy (c_file, c_output);
	if (*main_alias) {
	    nice_printf (c_output, "/* Main program alias */ ");
	    nice_printf (c_output, "int %s () { MAIN__ ();%s }\n",
		    main_alias, Ansi ? " return 0;" : "");
	    }
	if (Ansi == 2)
		nice_printf(c_output,
			"#ifdef __cplusplus\n\t}\n#endif\n");
	if (c2d) {
		if (c2d == 1)
			fprintf(c_output, "/*<<<%s>>>*/\n", cdfilename);
		else
			fclose(c_output);
		def_commons(c_output);
		}
	if (c2d != 2)
		fclose (c_output);

 C_skipped:
	if(parstate != OUTSIDE)
		{
		warn("missing final end statement");
		endproc();
		nerr = 1;
		}
	done(nerr ? 1 : 0);
	/* NOT REACHED */ return 0;
}


 FILEP
#ifdef KR_headers
opf(fn, mode)
	char *fn;
	char *mode;
#else
opf(char *fn, char *mode)
#endif
{
	FILEP fp;
	if( fp = fopen(fn, mode) )
		return(fp);

	fatalstr("cannot open intermediate file %s", fn);
	/* NOT REACHED */ return 0;
}


 void
#ifdef KR_headers
clf(p, what, quit)
	FILEP *p;
	char *what;
	int quit;
#else
clf(FILEP *p, char *what, int quit)
#endif
{
	if(p!=NULL && *p!=NULL && *p!=stdout)
	{
		if(ferror(*p)) {
			fprintf(stderr, "I/O error on %s\n", what);
			if (quit)
				done(3);
			retcode = 3;
			}
		fclose(*p);
	}
	*p = NULL;
}


 void
#ifdef KR_headers
done(k)
	int k;
#else
done(int k)
#endif
{
	clf(&initfile, "initfile", 0);
	clf(&c_file, "c_file", 0);
	clf(&pass1_file, "pass1_file", 0);
	Un_link_all(k);
	exit(k|retcode);
}
