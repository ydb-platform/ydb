/* code to convert between .edb format and an Obj */

#include <stdio.h>
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "astro.h"
#include "preferences.h"


int get_fields (char *s, int delim, char *fields[]);

#define MAXDBLINE       512     /* longest allowed db line */

#define FLDSEP          ','     /* major field separator */
#define SUBFLD          '|'     /* subfield separator */
#define MAXFLDS 20              /* must be more than on any expected line */
#define	MAXESGOOD	100	/* max earth satellite good, days */

static char *enm (char *flds[MAXFLDS]);
static int crack_f (Obj *op, char *flds[MAXFLDS], int nf, char whynot[]);
static int crack_e (Obj *op, char *flds[MAXFLDS], int nf, char whynot[]);
static int crack_h (Obj *op, char *flds[MAXFLDS], int nf, char whynot[]);
static int crack_p (Obj *op, char *flds[MAXFLDS], int nf, char whynot[]);
static int crack_E (Obj *op, char *flds[MAXFLDS], int nf, char whynot[]);
static int crack_P (Obj *op, char *flds[MAXFLDS], int nf, char whynot[]);
static int crack_B (Obj *op, char *flds[MAXFLDS], int nf, char whynot[]);
static int crack_name (Obj *op, char *flds[MAXFLDS], int nf,
    char nm[][MAXNM], int nnm);
static void crack_year (char *bp, double *p);
static void crack_okdates (char *fld, float *startok, float *endok);
static int get_okdates (char *lp, float *sp, float *ep);
static int tle_sum (char *l);
static double tle_fld (char *l, int from, int thru);
static double tle_expfld (char *l, int start);
static void write_f (Obj *op, char lp[]);
static void write_e (Obj *op, char lp[]);
static void write_h (Obj *op, char lp[]);
static void write_p (Obj *op, char lp[]);
static void write_E (Obj *op, char lp[]);
static void write_P (Obj *op, char lp[]);
static void write_B (Obj *op, char lp[]);

/* crack the given .edb database line into op.
 * if ok
 *   return number of names in nm[], or 1 if nm == NULL
 * else
 *   if whynot
 *     if not even a candidate
 *       set whynot[0] = '\0'
 *     else
 *       fill whynot with reason message.
 *   return -1
 * only the first name is stored in op, all names (up to nnm) are in nm[], or
 *   ignored if nm == NULL.
 */
int
db_crack_line (char s[], Obj *op, char nm[][MAXNM], int nnm, char whynot[])
{
	char copy[MAXDBLINE];	/* work copy; leave s untouched */
	char *flds[MAXFLDS];	/* point to each field for easy reference */
	int nf;
	int i;

	/* init no response */
	if (whynot)
	    whynot[0] = '\0';

	/* basic initial check */
	if (dbline_candidate (s) < 0)
	    return (-1);

	/* do all the parsing on a copy */
	(void) strncpy (copy, s, MAXDBLINE-1);
	copy[MAXDBLINE-1] = '\0';
	i = strlen(copy);
	if (copy[i-1] == '\n')
	    copy[i-1] = '\0';

	/* parse into main fields */
	nf = get_fields (copy, FLDSEP, flds);

	/* need at least 2: name and type */
	if (nf < 2) {
	    if (whynot)
		sprintf (whynot, "Bogus: %s", s);
	    return (-1);
	}

	/* switch out on type of object - the second field */
	switch (flds[1][0]) {

	case 'f':
	    if (crack_f (op, flds, nf, whynot) < 0)
		return (-1);
	    break;

	case 'e':
	    if (crack_e (op, flds, nf, whynot) < 0)
		return (-1);
	    break;

	case 'h':
	    if (crack_h (op, flds, nf, whynot) < 0)
		return (-1);
	    break;

	case 'p':
	    if (crack_p (op, flds, nf, whynot) < 0)
		return (-1);
	    break;

	case 'B':
	    if (crack_B (op, flds, nf, whynot) < 0)
		return (-1);
	    break;

	case 'E':
	    if (crack_E (op, flds, nf, whynot) < 0)
		return (-1);
	    break;

	case 'P':
	    if (crack_P (op, flds, nf, whynot) < 0)
		return (-1);
	    break;

	default:
	    if (whynot)
		sprintf (whynot, "%s: Unknown type %c for %s", enm(flds),
							flds[1][0], flds[0]);
	    return (-1);
	}

	return (crack_name (op, flds, nf, nm, nnm));
}

/* write the given Obj in .edb format to lp[].
 * we do _not_ include a trailing '\n'.
 */
void
db_write_line (Obj *op, char lp[])
{
	switch (op->o_type) {
	case FIXED:
	    write_f (op, lp);
	    break;

	case BINARYSTAR:
	    write_B (op, lp);
	    break;

	case ELLIPTICAL:
	    write_e (op, lp);
	    break;

	case HYPERBOLIC:
	    write_h (op, lp);
	    break;

	case PARABOLIC:
	    write_p (op, lp);
	    break;

	case EARTHSAT:
	    write_E (op, lp);
	    break;

	case PLANET:
	    write_P (op, lp);
	    break;

	default:
	    printf ("Unknown type for %s: %d\n", op->o_name, op->o_type);
	    abort();
	}
}

/* given 3 lines, first of which is name and next 2 are TLE, fill op.
 * we skip leading whitespace on all lines.
 * we do /not/ assume the 2 TLE lines are 0 terminated, but we do reach out into
 *   each as far as 69 chars.
 * we detect nonconformance as efficiently as possible.
 * name ends at first '\0', '\r' or '\n'.
 * set startok/endok.
 * if ok return 0 else return -1
 * (PyEphem enchancement:) else returns -2 for a checksum error
 */
int
db_tle (char *name, char *l1, char *l2, Obj *op)
{
	double ep;
	int i;

	/* check for correct line numbers, macthing satellite numbers and
	 * correct checksums.
	 */
	while (isspace(*l1))
	    l1++;
	if (*l1 != '1')
	    return (-1);
	while (isspace(*l2))
	    l2++;
	if (*l2 != '2')
	    return (-1);
	if (strncmp (l1+2, l2+2, 5))
	    return (-1);
	if (tle_sum (l1) < 0)
	    return (-2);
	if (tle_sum (l2) < 0)
	    return (-2);

	/* assume it's ok from here out */

	/* fresh */
	zero_mem ((void *)op, sizeof(ObjES));
	op->o_type = EARTHSAT;

	/* name, sans leading and trailing whitespace */
	while (isspace(*name))
	    name++;
	i = strcspn (name, "\r\n");
	while (i > 0 && name[i-1] == ' ')
	    --i;
	if (i == 0)
	    return (-1);
	if (i > MAXNM-1)
	    i = MAXNM-1;
	sprintf (op->o_name, "%.*s", i, name);

	/* goodies from "line 1" */
	op->es_drag = (float) tle_expfld (l1, 54);
	op->es_decay = (float) tle_fld (l1, 34, 43);
	i = (int) tle_fld (l1, 19, 20);
	if (i < 57)
	    i += 100;
	cal_mjd (1, tle_fld(l1, 21, 32), i+1900, &ep);
	op->es_epoch = ep;

	/* goodies from "line 2" */
	op->es_n = tle_fld (l2, 53, 63);
	op->es_inc = (float)tle_fld (l2, 9, 16);
	op->es_raan = (float)tle_fld (l2, 18, 25);
	op->es_e = (float)(tle_fld (l2, 27, 33) * 1e-7);
	op->es_ap = (float)tle_fld (l2, 35, 42);
	op->es_M = (float)tle_fld (l2, 44, 51);
	op->es_orbit = (int)tle_fld (l2, 64, 68);

	/* limit date range to decay period that changes period by 1% but
	 * never more than MAXESGOOD.
	 * es_n is rev/day, es_decay is (rev/day)/day
	 */
	if (fabs(op->es_decay) > 0) {
	    double dt = 0.01*op->es_n/fabs(op->es_decay);
	    if (dt > MAXESGOOD)
		dt = MAXESGOOD;
	    op->es_startok = op->es_epoch - dt;
	    op->es_endok = op->es_epoch + dt;
	}

	/* yes! */
	return (0);
}

/* return 0 if op has no date range information or what it does have brackets
 * now, else -1
 */
int
dateRangeOK (Now *np, Obj *op)
{
	float *sp, *ep;

	switch (op->o_type) {
	case ELLIPTICAL:
	    sp = &op->e_startok;
	    ep = &op->e_endok;
	    break;
	case HYPERBOLIC:
	    sp = &op->h_startok;
	    ep = &op->h_endok;
	    break;
	case PARABOLIC:
	    sp = &op->p_startok;
	    ep = &op->p_endok;
	    break;
	case EARTHSAT:
	    sp = &op->es_startok;
	    ep = &op->es_endok;
	    break;
	default:
	    return (0);
	}

	if (*sp <= mjd && (!*ep || mjd <= *ep))
	    return (0);
	return (-1);
}

/* given a null-terminated string, fill in fields[] with the starting addresses
 * of each field delimited by delim or '\0'.
 * N.B. each character matching delim is REPLACED BY '\0' IN PLACE.
 * N.B. 0-length fields count, so even if *s=='\0' we return 1.
 * return the number of fields.
 */
int
get_fields (char *s, int delim, char *fields[])
{
	int n;
	char c;

	*fields = s;
	n = 0;
	do {
	    c = *s++;
	    if (c == delim || c == '\0') {
		s[-1] = '\0';
		*++fields = s;
		n++;
	    }
	} while (c);

	return (n);
}

/* return 0 if buf qualifies as a database line worthy of a cracking
 * attempt, else -1.
 */
int
dbline_candidate (char *buf)
{
	char c = buf[0];

	return (c == '#' || c == '!' || isspace(c) ? -1 : 0);
}

/* return 0 if TLE checksum is ok, else -1 */
static int
tle_sum (char *l)
{
	char *lastl = l + 68;
	int sum;

	for (sum = 0; l < lastl; ) {
	    char c = *l++;
	    if (c == '\0')
		return (-1);
	    if (isdigit(c))
		sum += c - '0';
	    else if (c == '-')
		sum++;
	}

	return (*l - '0' == (sum%10) ? 0 : -1);
}

/* extract the given columns and return value.
 * N.B. from and to are 1-based within l
 */
static double
tle_fld (char *l, int from, int thru)
{
	char buf[32];

	sprintf (buf, "%.*s", thru-from+1, l+from-1);
	return (atod (buf));
}

/* extract the exponential value starting at the given column.
 * N.B. start is 1-based within l
 */
static double
tle_expfld (char *l, int start)
{
	char buf[32];
	double v;

	sprintf (buf, ".%.*s", 5, l+start);
	v = atod (buf) * pow (10.0, tle_fld(l, start+6, start+7));
	if (l[start-1] == '-')
	    v = -v;
	return (v);
}

static int
crack_f (Obj *op, char *flds[MAXFLDS], int nf, char whynot[])
{
	char *sflds[MAXFLDS];
	double tmp;
	int nsf, status;

	if (nf < 5 || nf > 7) {
	    if (whynot)
		sprintf (whynot, "%s: type f needs 5-7 fields, not %d",
								enm(flds),nf);
	    return (-1);
	}

	zero_mem ((void *)op, sizeof(ObjF));
	op->o_type = FIXED;

	nsf = get_fields(flds[1], SUBFLD, sflds);
	if (nsf > 1) {
	    switch (sflds[1][0]) {
	    case 'A': case 'B': case 'C': case 'D': case 'F': case 'G':
	    case 'H': case 'K': case 'J': case 'L': case 'M': case 'N':
	    case 'O': case 'P': case 'Q': case 'R': case 'S': case 'T':
	    case 'U': case 'V': case 'Y':
		op->f_class = sflds[1][0];
		if (op->f_class == 'B')
		    op->f_class = 'D';	/* merge B and D since BINARYSTAR */
		break;
	    default:
		if (whynot)
		    sprintf (whynot, "%s: Bad f class: %c", enm(flds),
		    						sflds[1][0]);
		return (-1);
	    }
	} else
	    op->f_class = 'T';		/* default to star-like */
	if (nsf > 2) {
	    /* fill f_spect all the way */
	    char buf[sizeof(op->f_spect)+1];
	    memset (buf, 0, sizeof(buf));
	    sprintf (buf, "%.*s", (int)sizeof(op->f_spect), sflds[2]);
	    memcpy (op->f_spect, buf, (int)sizeof(op->f_spect));
	}

	nsf = get_fields(flds[2], SUBFLD, sflds);
	status = f_scansexa (sflds[0], &tmp);
	if (status < 0) {
		if (whynot)
		sprintf (whynot, "%s: Invalid angle string '%s'", enm(flds), sflds[0]);
		return (-1);
	}
	op->f_RA = hrrad(tmp);
	if (nsf > 1)
	    op->f_pmRA = (float) 1.327e-11*atod(sflds[1]);/*mas/yr->rad/dy*/

	nsf = get_fields(flds[3], SUBFLD, sflds);
	status = f_scansexa (sflds[0], &tmp);
	if (status < 0) {
		if (whynot)
		sprintf (whynot, "%s: Invalid angle string '%s'", enm(flds), sflds[0]);
		return (-1);
	}
	op->f_dec = degrad(tmp);
	if (nsf > 1)
	    op->f_pmdec = (float)1.327e-11*atod(sflds[1]);/*mas/yr->rad/dy*/
	if (fabs(op->f_dec) < PI/2)
	    op->f_pmRA /= cos (op->f_dec);

	set_fmag (op, atod(flds[4]));

	if (nf > 5 && flds[5][0]) {
	    tmp = op->f_epoch;
	    crack_year (flds[5], &tmp);
	    op->f_epoch = tmp;
	} else
	    op->f_epoch = J2000;	/* default */

	if (nf > 6) {
	    op->f_size = (float) atod(flds[6]);

	    /* optional minor axis and position angle subfields */
	    nsf = get_fields(flds[6], SUBFLD, sflds);
	    if (nsf == 3) {
		set_ratio(op, op->s_size, atod(sflds[1]));
		set_pa(op,degrad(atod(sflds[2])));
	    } else {
		set_ratio(op,1,1);	/* round */
		set_pa(op,0.0);
	    }
	}

	return (0);
}

static int
crack_e (Obj *op, char *flds[MAXFLDS], int nf, char whynot[])
{
	if (nf != 13 && nf != 14) {
	    if (whynot)
		sprintf (whynot, "%s: type e needs 13 or 14 fields, not %d",
								enm(flds), nf);
	    return (-1);
	}

	zero_mem ((void *)op, sizeof(ObjE));
	op->o_type = ELLIPTICAL;

	op->e_inc = (float) atod (flds[2]);
	op->e_Om = (float) atod (flds[3]);
	op->e_om = (float) atod (flds[4]);
	op->e_a = (float) atod (flds[5]);
	/* retired op->e_n = (float) atod (flds[6]); */
	op->e_e = atod (flds[7]);
	op->e_M = (float) atod (flds[8]);
	crack_year (flds[9], &op->e_cepoch);
	crack_okdates (flds[9], &op->e_startok, &op->e_endok);
	crack_year (flds[10], &op->e_epoch);

	/* magnitude model gk or HG(default). allow prefixes in either field */
	op->e_mag.whichm = flds[11][0] == 'g' ? MAG_gk : MAG_HG;
	if (isdigit(flds[11][0]))
	    op->e_mag.m1 = (float) atod(&flds[11][0]);
	else
	    op->e_mag.m1 = (float) atod(&flds[11][1]);
	if (isdigit(flds[12][0]))
	    op->e_mag.m2 = (float) atod(&flds[12][0]);
	else
	    op->e_mag.m2 = (float) atod(&flds[12][1]);

	if (nf == 14)
	    op->e_size = (float) atod (flds[13]);

	return (0);
}

static int
crack_h (Obj *op, char *flds[MAXFLDS], int nf, char whynot[])
{
	if (nf != 11 && nf != 12) {
	    if (whynot)
		sprintf (whynot, "%s: type h needs 11 or 12 fields, not %d",
								enm(flds), nf);
	    return (-1);
	}

	zero_mem ((void *)op, sizeof(ObjH));
	op->o_type = HYPERBOLIC;

	crack_year (flds[2], &op->h_ep);
	crack_okdates (flds[2], &op->h_startok, &op->h_endok);
	op->h_inc = (float) atod (flds[3]);
	op->h_Om = (float) atod (flds[4]);
	op->h_om = (float) atod (flds[5]);
	op->h_e = (float) atod (flds[6]);
	op->h_qp = (float) atod (flds[7]);
	crack_year (flds[8], &op->h_epoch);
	op->h_g = (float) atod (flds[9]);
	op->h_k = (float) atod (flds[10]);

	if (nf == 12)
	    op->h_size = (float) atod (flds[11]);

	return (0);
}

static int
crack_p (Obj *op, char *flds[MAXFLDS], int nf, char whynot[])
{
	if (nf != 10 && nf != 11) {
	    if (whynot)
		sprintf (whynot, "%s: type p needs 10 or 11 fields, not %d",	
								enm(flds), nf);
	    return (-1);
	}

	zero_mem ((void *)op, sizeof(ObjP));
	op->o_type = PARABOLIC;

	crack_year (flds[2], &op->p_ep);
	crack_okdates (flds[2], &op->p_startok, &op->p_endok);
	op->p_inc = (float) atod (flds[3]);
	op->p_om = (float) atod (flds[4]);
	op->p_qp = (float) atod (flds[5]);
	op->p_Om = (float) atod (flds[6]);
	crack_year (flds[7], &op->p_epoch);
	op->p_g = (float) atod (flds[8]);
	op->p_k = (float) atod (flds[9]);

	if (nf == 11)
	    op->p_size = (float) atod (flds[10]);

	return (0);
}

static int
crack_E (Obj *op, char *flds[MAXFLDS], int nf, char whynot[])
{
	if (nf != 11 && nf != 12) {
	    if (whynot)
		sprintf (whynot, "%s: type E needs 11 or 12 fields, not %d",
							    enm(flds), nf);
	    return (-1);
	}

	zero_mem ((void *)op, sizeof(ObjES));
	op->o_type = EARTHSAT;
	crack_year (flds[2], &op->es_epoch);
	crack_okdates (flds[2], &op->es_startok, &op->es_endok);
	op->es_inc = (float) atod (flds[3]);
	op->es_raan = (float) atod (flds[4]);
	op->es_e = (float) atod (flds[5]);
	op->es_ap = (float) atod (flds[6]);
	op->es_M = (float) atod (flds[7]);
	op->es_n = atod (flds[8]);
	op->es_decay = (float) atod (flds[9]);
	op->es_orbit = atoi (flds[10]);
	if (nf == 12)
	    op->es_drag = (float) atod (flds[11]);

	/* if not already specified, limit date range to decay period that
	 * changes period by 1% but never longer than MAXESGOOD.
	 * es_n is rev/day, es_decay is (rev/day)/day
	 */
	if (op->es_startok == 0 && op->es_endok == 0 && fabs(op->es_decay) > 0){
	    double dt = 0.01*op->es_n/fabs(op->es_decay);
	    if (dt > MAXESGOOD)
		dt = MAXESGOOD;
	    op->es_startok = op->es_epoch - dt;
	    op->es_endok = op->es_epoch + dt;
	}

	return (0);
}

static int
crack_P (Obj *op, char *flds[MAXFLDS], int nf, char whynot[])
{
	Obj *bi;
	int nbi;
	int i;

	nbi = getBuiltInObjs (&bi);

	for (i = 0; i < nbi; i++) {
	    Obj *bop = bi + i;
	    if (is_type(bop,PLANETM) && !strcmp (flds[0], bop->o_name)) {
		memcpy ((void *)op, bop, sizeof(ObjPl));
		return (0);
	    }
	}

	if (whynot)
	    sprintf (whynot, "%s: Unknown planet or moon", enm(flds));
	return (-1);
}

static int
crack_B (Obj *op, char *flds[MAXFLDS], int nf, char whynot[])
{
	char *sflds[MAXFLDS];
	double tmp;
	int nsf, status;

	if (nf != 7) {
	    if (whynot)
		sprintf (whynot, "%s: B need 7 fields, not %d", enm(flds), nf);
	    return (-1);
	}

	zero_mem ((void *)op, sizeof(ObjB));
	op->o_type = BINARYSTAR;

	nsf = get_fields(flds[1], SUBFLD, sflds);
	if (nsf > 1) {
	    switch (sflds[1][0]) {
	    case 'a': case 'c': case 'e': case 'x': case 'y': case 'o':
	    case 's': case 't': case 'u': case 'v': case 'b': case 'd':
	    case 'q': case 'r': case 'p': case 'U': case 'V': case 'Y':
		op->f_class = sflds[1][0];
		break;
	    default:
		if (whynot)
		    sprintf (whynot, "%s: Bad B class: %c", enm(flds),
		    						sflds[1][0]);
		return (-1);
	    }
	}
	if (nsf > 2) {
	    /* fill f_spect all the way */
	    char buf[sizeof(op->f_spect)+1];
	    memset (buf, 0, sizeof(buf));
	    sprintf (buf, "%.*s", (int)sizeof(op->f_spect), sflds[2]);
	    memcpy (op->f_spect, buf, (int)sizeof(op->f_spect));
	}
	if (nsf > 3) {
	    /* fill b_2spect all the way */
	    char buf[sizeof(op->b_2spect)+1];
	    memset (buf, 0, sizeof(buf));
	    sprintf (buf, "%.*s", (int)sizeof(op->b_2spect), sflds[3]);
	    memcpy (op->b_2spect, buf, (int)sizeof(op->b_2spect));
	}

	nsf = get_fields(flds[2], SUBFLD, sflds);
	status = f_scansexa (sflds[0], &tmp);
	if (status < 0) {
		if (whynot)
		sprintf (whynot, "%s: Invalid angle string '%s'", enm(flds), sflds[0]);
		return (-1);
	}
	op->f_RA = hrrad(tmp);
	if (nsf > 1)
	    op->f_pmRA = (float) 1.327e-11*atod(sflds[1]);/*mas/yr->rad/dy*/

	nsf = get_fields(flds[3], SUBFLD, sflds);
	status = f_scansexa (sflds[0], &tmp);
	if (status < 0) {
		if (whynot)
		sprintf (whynot, "%s: Invalid angle string '%s'", enm(flds), sflds[0]);
		return (-1);
	}
	op->f_dec = degrad(tmp);
	if (nsf > 1)
	    op->f_pmdec = (float)1.327e-11*atod(sflds[1]);/*mas/yr->rad/dy*/
	if (fabs(op->f_dec) < PI/2)
	    op->f_pmRA /= cos (op->f_dec);

	nsf = get_fields(flds[4], SUBFLD, sflds);
	if (nsf > 0)
	    set_fmag (op, atod(sflds[0]));
	if (nsf > 1)
	    op->b_2mag = (short)floor((atod(sflds[1]))*MAGSCALE + 0.5);

	if (flds[5][0]) {
	    tmp = op->f_epoch;
	    crack_year (flds[5], &tmp);
	    op->f_epoch = tmp;
	} else
	    op->f_epoch = J2000;	/* default */

	nsf = get_fields(flds[6], SUBFLD, sflds);
	if (nsf == 7) {
	    int l;
	    char c;

	    op->b_bo.bo_a = atod(sflds[0]);
	    op->b_bo.bo_i = atod(sflds[1]);
	    op->b_bo.bo_O = atod(sflds[2]);
	    op->b_bo.bo_e = atod(sflds[3]);
	    op->b_bo.bo_T = atod(sflds[4]);
	    op->b_bo.bo_o = atod(sflds[5]);
	    op->b_bo.bo_P = atod(sflds[6]);

	    /* reject some weird entries actually seen in real lists */
	    if (op->b_bo.bo_a <= 0) {
		if (whynot)
		    sprintf (whynot, "%s: Bogus B semi major axis: %g",
						    enm(flds), op->b_bo.bo_a);
		return (-1);
	    }
	    if (op->b_bo.bo_P <= 0) {
		if (whynot)
		    sprintf (whynot, "%s: Bogus B period: %g", enm(flds),
		    						op->b_bo.bo_P);
		return (-1);
	    }

	    /* scale period */
	    l = strlen (sflds[6]);
	    c = sflds[6][l-1];
	    switch (c) {
	    case 'y': case 'Y':
		break;
	    case 'h': case 'H':
		op->b_bo.bo_P /= (24.0*365.25);
		break;
	    case 'd': case 'D':
		op->b_bo.bo_P /= 365.25;
		break;
	    default:
		if (c != ' ' && !isdigit(c)) {
		    if (whynot)
			sprintf (whynot,"%s: B period suffix not Y, D or H: %c",
								enm(flds), c);
		    return (-1);
		}
	    }

	} else if (nsf==3 || nsf==6 || nsf==9) {
	    double yr;
	    int i;

	    op->b_nbp = nsf/3;
	    for (i = 0; i < nsf; i += 3) {
		tmp = 0;
		crack_year (sflds[i+0], &tmp);
		mjd_year (tmp, &yr);
		op->b_bp[i/3].bp_ep = (float)yr;
		op->b_bp[i/3].bp_sep = atod(sflds[i+1]);
		op->b_bp[i/3].bp_pa = degrad(atod(sflds[i+2]));
	    }
	} else {
	    if (whynot)
		sprintf (whynot,
		       "%s: type B needs 3,6 or 7 subfields in field 7, not %d",
								enm(flds), nsf);
	    return (-1);
	}

	return (0);
}

/* put all names in nm but load only the first into o_name */
static int
crack_name (Obj *op, char *flds[MAXFLDS], int nf, char nm[][MAXNM], int nnm)
{
	char *sflds[MAXFLDS];
	int nsf;
	int i;
	
	nsf = get_fields (flds[0], SUBFLD, sflds);
	for (i = 0; nm && i < nsf && i < nnm; i++) {
	    strncpy (nm[i], sflds[i], MAXNM);
	    nm[i][MAXNM-1] = '\0';
	}
	strncpy (op->o_name, sflds[0], MAXNM-1);
	return (nsf);
}

/* simple name cracker just for error messages */
static char *
enm (char *flds[MAXFLDS])
{
	char *sflds[MAXFLDS];
	int nsf = get_fields (flds[0], SUBFLD, sflds);
	return (nsf > 0 ? sflds[0] : "Unknown");
}

/* given either a decimal year (xxxx[.xxx]) or a calendar (x/x/x) date
 * convert it to an mjd and store it at *p.
 */
static void
crack_year (char *bp, double *p)
{
	int m, y;
	double d;

	mjd_cal (*p, &m, &d, &y);	/* init with current */
	f_sscandate (bp, PREF_MDY, &m, &d, &y);
	cal_mjd (m, d, y, p);
}

/* crack the startok and endok date fields found in several Obj types.
 * set to 0 if blank or any problems.
 */
static void
crack_okdates (char *fld, float *startok, float *endok)
{
	char *sflds[MAXFLDS];
	double tmp;
	int m, y;
	double d;
	int nsf;

	*startok = *endok = 0;
	nsf = get_fields(fld, SUBFLD, sflds);
	if (nsf > 1) {
	    d = m = y = 0;
	    f_sscandate (sflds[1], PREF_MDY, &m, &d, &y);
	    cal_mjd (m, d, y, &tmp);
	    *startok = (float)tmp;
	    if (nsf > 2) {
		d = m = y = 0;
		f_sscandate (sflds[2], PREF_MDY, &m, &d, &y);
		cal_mjd (m, d, y, &tmp);
		*endok = (float)tmp;
	    }
	}
}

/* add startok and endok to string at lp if non-zero.
 * return number of characters added.
 */
static int
get_okdates (char *lp, float *sp, float *ep)
{
	char *lp0 = lp;

	if (*sp || *ep) {
	    *lp++ = '|';
	    if (*sp)
		lp += fs_date (lp, PREF_MDY, *sp);
	    if (*ep) {
		*lp++ = '|';
		lp += fs_date (lp, PREF_MDY, *ep);
	    }
	}

	return (lp - lp0);
}

static void
write_f (Obj *op, char lp[])
{
	double tmp;

	lp += sprintf (lp, "%s,f", op->o_name);
	if (op->f_class)
	    lp += sprintf (lp, "|%c", op->f_class);
	if (op->f_spect[0])
	    lp += sprintf (lp, "|%.*s", (int)sizeof(op->f_spect), op->f_spect);
	*lp++ = ',';
	lp += fs_sexa (lp, radhr(op->f_RA), 2, 36000);
	if (op->f_pmRA)
	    lp += sprintf (lp, "|%.6g",cos(op->f_dec)*op->f_pmRA/1.327e-11);
	*lp++ = ',';
	lp += fs_sexa (lp, raddeg(op->f_dec), 3, 3600);
	if (op->f_pmdec)
	    lp += sprintf (lp, "|%.6g", op->f_pmdec/1.327e-11);
	lp += sprintf (lp, ",%.2f", get_mag(op));
	mjd_year (op->f_epoch, &tmp);
	lp += sprintf (lp, ",%.6g", tmp); /* %.7g gives 2000.001 */
	lp += sprintf (lp, ",%.7g", op->f_size);
	if (op->f_size && (op->f_ratio || op->f_pa))
	    lp += sprintf (lp,"|%g|%g", op->f_size*get_ratio(op),
							    raddeg(get_pa(op)));
}

static void
write_e (Obj *op, char lp[])
{
	lp += sprintf (lp, "%s,e", op->o_name);
	lp += sprintf (lp, ",%.7g", op->e_inc);
	lp += sprintf (lp, ",%.7g", op->e_Om);
	lp += sprintf (lp, ",%.7g", op->e_om);
	lp += sprintf (lp, ",%.7g", op->e_a);
	lp += sprintf (lp, ",%.7g", 0.0);		/* retired op->e_n */
	lp += sprintf (lp, ",%.7g", op->e_e);
	lp += sprintf (lp, ",%.7g", op->e_M);
	*lp++ = ',';
	lp += fs_date (lp, PREF_MDY, op->e_cepoch);
	lp += get_okdates (lp, &op->e_startok, &op->e_endok);
	*lp++ = ',';
	lp += fs_date (lp, PREF_MDY, op->e_epoch);
	if (op->e_mag.whichm == MAG_gk)
	    lp += sprintf (lp, ",g%.7g", op->e_mag.m1);
	else if (op->e_mag.whichm == MAG_HG)
	    lp += sprintf (lp, ",H%.7g", op->e_mag.m1);
	else
	    lp += sprintf (lp, ",%.7g", op->e_mag.m1);
	lp += sprintf (lp, ",%.7g", op->e_mag.m2);
	lp += sprintf (lp, ",%.7g", op->e_size);
}

static void
write_h (Obj *op, char lp[])
{
	lp += sprintf (lp, "%s,h", op->o_name);
	*lp++ = ',';
	lp += fs_date (lp, PREF_MDY, op->h_ep);
	lp += get_okdates (lp, &op->h_startok, &op->h_endok);
	lp += sprintf (lp, ",%.7g", op->h_inc);
	lp += sprintf (lp, ",%.7g", op->h_Om);
	lp += sprintf (lp, ",%.7g", op->h_om);
	lp += sprintf (lp, ",%.7g", op->h_e);
	lp += sprintf (lp, ",%.7g", op->h_qp);
	*lp++ = ',';
	lp += fs_date (lp, PREF_MDY, op->h_epoch);
	lp += sprintf (lp, ",%.7g", op->h_g);
	lp += sprintf (lp, ",%.7g", op->h_k);
	lp += sprintf (lp, ",%.7g", op->h_size);
}

static void
write_p (Obj *op, char lp[])
{
	lp += sprintf (lp, "%s,p", op->o_name);
	*lp++ = ',';
	lp += fs_date (lp, PREF_MDY, op->p_ep);
	lp += get_okdates (lp, &op->p_startok, &op->p_endok);
	lp += sprintf (lp, ",%.7g", op->p_inc);
	lp += sprintf (lp, ",%.7g", op->p_om);
	lp += sprintf (lp, ",%.7g", op->p_qp);
	lp += sprintf (lp, ",%.7g", op->p_Om);
	*lp++ = ',';
	lp += fs_date (lp, PREF_MDY, op->p_epoch);
	lp += sprintf (lp, ",%.7g", op->p_g);
	lp += sprintf (lp, ",%.7g", op->p_k);
	lp += sprintf (lp, ",%.7g", op->p_size);
}

static void
write_E (Obj *op, char lp[])
{
	double d;
	int m, y;

	lp += sprintf (lp, "%s,E", op->o_name);
	*lp++ = ',';
	mjd_cal (op->es_epoch, &m, &d, &y); /* need more day prec than fs_date*/
	lp += sprintf (lp, "%d/%.12g/%d", m, d, y);
	lp += get_okdates (lp, &op->es_startok, &op->es_endok);
	lp += sprintf (lp, ",%.8g", op->es_inc);
	lp += sprintf (lp, ",%.8g", op->es_raan);
	lp += sprintf (lp, ",%.8g", op->es_e);
	lp += sprintf (lp, ",%.8g", op->es_ap);
	lp += sprintf (lp, ",%.8g", op->es_M);
	lp += sprintf (lp, ",%.12g", op->es_n);		/* double */
	lp += sprintf (lp, ",%.8g", op->es_decay);
	lp += sprintf (lp, ",%d", op->es_orbit);
	lp += sprintf (lp, ",%.8g", op->es_drag);
}

static void
write_B (Obj *op, char lp[])
{
	double tmp;

	lp += sprintf (lp, "%s,B", op->o_name);
	if (op->f_class)
	    lp += sprintf (lp, "|%c", op->f_class);
	if (op->f_spect[0])
	    lp += sprintf (lp, "|%.*s", (int)sizeof(op->f_spect), op->f_spect);
	if (op->b_2spect[0])
	    lp += sprintf (lp, "|%.*s", (int)sizeof(op->b_2spect),op->b_2spect);
	*lp++ = ',';
	lp += fs_sexa (lp, radhr(op->f_RA), 2, 36000);
	if (op->f_pmRA)
	    lp += sprintf (lp, "|%.6g",cos(op->f_dec)*op->f_pmRA/1.327e-11);
	*lp++ = ',';
	lp += fs_sexa (lp, raddeg(op->f_dec), 3, 3600);
	if (op->f_pmdec)
	    lp += sprintf (lp, "|%.6g", op->f_pmdec/1.327e-11);
	lp += sprintf (lp, ",%.2f", get_mag(op));
	lp += sprintf (lp, "|%.2f", op->b_2mag/MAGSCALE);
	mjd_year (op->f_epoch, &tmp);
	lp += sprintf (lp, ",%.6g", tmp); /* %.7g gives 2000.001 */
	if (op->b_nbp == 0) {
	    lp += sprintf (lp, ",%.6g",  op->b_bo.bo_a);
	    lp += sprintf (lp, "|%.6g",  op->b_bo.bo_i);
	    lp += sprintf (lp, "|%.6g",  op->b_bo.bo_O);
	    lp += sprintf (lp, "|%.6g",  op->b_bo.bo_e);
	    lp += sprintf (lp, "|%.6g",  op->b_bo.bo_T);
	    lp += sprintf (lp, "|%.6g",  op->b_bo.bo_o);
	    lp += sprintf (lp, "|%.6gy", op->b_bo.bo_P);
	} else {
	    int i;

	    for (i = 0; i < op->b_nbp; i++) {
		BinPos *bp = &op->b_bp[i];
		lp += sprintf (lp, "%c%.6g", i==0?',':'|', bp->bp_ep);
		lp += sprintf (lp, "|%.6g", bp->bp_sep);
		lp += sprintf (lp, "|%.6g", raddeg(bp->bp_pa));
	    }
	}
}

static void
write_P (Obj *op, char lp[])
{

	lp += sprintf (lp, "%s,P", op->o_name);
}

