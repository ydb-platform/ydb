/*
 * Copyright (c) 1997-8,2007-8 Andrew G Morgan <morgan@kernel.org>
 * Copyright (c) 1997 Andrew Main <zefram@dcs.warwick.ac.uk>
 *
 * This file deals with exchanging internal and textual
 * representations of capability sets.
 */

#define _GNU_SOURCE
#include <stdio.h>

#define LIBCAP_PLEASE_INCLUDE_ARRAY
#include "libcap.h"

#include <ctype.h>
#include <limits.h>

/* Maximum output text length (16 per cap) */
#define CAP_TEXT_SIZE    (16*__CAP_MAXBITS)

/*
 * Parse a textual representation of capabilities, returning an internal
 * representation.
 */

#define raise_cap_mask(flat, c)  (flat)[CAP_TO_INDEX(c)] |= CAP_TO_MASK(c)

static void setbits(cap_t a, const __u32 *b, cap_flag_t set, unsigned blks)
{
    int n;
    for (n = blks; n--; ) {
	a->u[n].flat[set] |= b[n];
    }
}

static void clrbits(cap_t a, const __u32 *b, cap_flag_t set, unsigned blks)
{
    int n;
    for (n = blks; n--; )
	a->u[n].flat[set] &= ~b[n];
}

static char const *namcmp(char const *str, char const *nam)
{
    while (*nam && tolower((unsigned char)*str) == *nam) {
	str++;
	nam++;
    }
    if (*nam || isalnum((unsigned char)*str) || *str == '_')
	return NULL;
    return str;
}

static void forceall(__u32 *flat, __u32 value, unsigned blks)
{
    unsigned n;

    for (n = blks; n--; flat[n] = value);

    return;
}

static int lookupname(char const **strp)
{
    union {
	char const *constp;
	char *p;
    } str;

    str.constp = *strp;
    if (isdigit(*str.constp)) {
	unsigned long n = strtoul(str.constp, &str.p, 0);
	if (n >= __CAP_MAXBITS)
	    return -1;
	*strp = str.constp;
	return n;
    } else {
	int c;
	unsigned len;

	for (len=0; (c = str.constp[len]); ++len) {
	    if (!(isalpha(c) || (c == '_'))) {
		break;
	    }
	}

#ifdef GPERF_DOWNCASE
	const struct __cap_token_s *token_info;

	token_info = __cap_lookup_name(str.constp, len);
	if (token_info != NULL) {
	    *strp = str.constp + len;
	    return token_info->index;
	}
#else /* ie., ndef GPERF_DOWNCASE */
	char const *s;
	unsigned n;

	for (n = __CAP_BITS; n--; )
	    if (_cap_names[n] && (s = namcmp(str.constp, _cap_names[n]))) {
		*strp = s;
		return n;
	    }
#endif /* def GPERF_DOWNCASE */

	return -1;   	/* No definition available */
    }
}

cap_t cap_from_text(const char *str)
{
    cap_t res;
    int n;
    unsigned cap_blks;

    if (str == NULL) {
	_cap_debug("bad argument");
	errno = EINVAL;
	return NULL;
    }

    if (!(res = cap_init()))
	return NULL;

    switch (res->head.version) {
    case _LINUX_CAPABILITY_VERSION_1:
	cap_blks = _LINUX_CAPABILITY_U32S_1;
	break;
    case _LINUX_CAPABILITY_VERSION_2:
	cap_blks = _LINUX_CAPABILITY_U32S_2;
	break;
    case _LINUX_CAPABILITY_VERSION_3:
	cap_blks = _LINUX_CAPABILITY_U32S_3;
	break;
    default:
	errno = EINVAL;
	return NULL;
    }
    
    _cap_debug("%s", str);

    for (;;) {
	__u32 list[__CAP_BLKS];
	char op;
	int flags = 0, listed=0;

	forceall(list, 0, __CAP_BLKS);

	/* skip leading spaces */
	while (isspace((unsigned char)*str))
	    str++;
	if (!*str) {
	    _cap_debugcap("e = ", *res, CAP_EFFECTIVE);
	    _cap_debugcap("i = ", *res, CAP_INHERITABLE);
	    _cap_debugcap("p = ", *res, CAP_PERMITTED);

	    return res;
	}

	/* identify caps specified by this clause */
	if (isalnum((unsigned char)*str) || *str == '_') {
	    for (;;) {
		if (namcmp(str, "all")) {
		    str += 3;
		    forceall(list, ~0, cap_blks);
		} else {
		    n = lookupname(&str);
		    if (n == -1)
			goto bad;
		    raise_cap_mask(list, n);
		}
		if (*str != ',')
		    break;
		if (!isalnum((unsigned char)*++str) && *str != '_')
		    goto bad;
	    }
	    listed = 1;
	} else if (*str == '+' || *str == '-') {
	    goto bad;                    /* require a list of capabilities */
	} else {
	    forceall(list, ~0, cap_blks);
	}

	/* identify first operation on list of capabilities */
	op = *str++;
	if (op == '=' && (*str == '+' || *str == '-')) {
	    if (!listed)
		goto bad;
	    op = (*str++ == '+' ? 'P':'M'); /* skip '=' and take next op */
	} else if (op != '+' && op != '-' && op != '=')
	    goto bad;

	/* cycle through list of actions */
	do {
	    _cap_debug("next char = `%c'", *str);
	    if (*str && !isspace(*str)) {
		switch (*str++) {    /* Effective, Inheritable, Permitted */
		case 'e':
		    flags |= LIBCAP_EFF;
		    break;
		case 'i':
		    flags |= LIBCAP_INH;
		    break;
		case 'p':
		    flags |= LIBCAP_PER;
		    break;
		default:
		    goto bad;
		}
	    } else if (op != '=') {
		_cap_debug("only '=' can be followed by space");
		goto bad;
	    }

	    _cap_debug("how to read?");
	    switch (op) {               /* how do we interpret the caps? */
	    case '=':
	    case 'P':                                              /* =+ */
	    case 'M':                                              /* =- */
		clrbits(res, list, CAP_EFFECTIVE, cap_blks);
		clrbits(res, list, CAP_PERMITTED, cap_blks);
		clrbits(res, list, CAP_INHERITABLE, cap_blks);
		if (op == 'M')
		    goto minus;
		/* fall through */
	    case '+':
		if (flags & LIBCAP_EFF)
		    setbits(res, list, CAP_EFFECTIVE, cap_blks);
		if (flags & LIBCAP_PER)
		    setbits(res, list, CAP_PERMITTED, cap_blks);
		if (flags & LIBCAP_INH)
		    setbits(res, list, CAP_INHERITABLE, cap_blks);
		break;
	    case '-':
	    minus:
		if (flags & LIBCAP_EFF)
		    clrbits(res, list, CAP_EFFECTIVE, cap_blks);
		if (flags & LIBCAP_PER)
		    clrbits(res, list, CAP_PERMITTED, cap_blks);
		if (flags & LIBCAP_INH)
		    clrbits(res, list, CAP_INHERITABLE, cap_blks);
		break;
	    }

	    /* new directive? */
	    if (*str == '+' || *str == '-') {
		if (!listed) {
		    _cap_debug("for + & - must list capabilities");
		    goto bad;
		}
		flags = 0;                       /* reset the flags */
		op = *str++;
		if (!isalpha(*str))
		    goto bad;
	    }
	} while (*str && !isspace(*str));
	_cap_debug("next clause");
    }

bad:
    cap_free(res);
    res = NULL;
    errno = EINVAL;
    return res;
}

/*
 * lookup a capability name and return its numerical value
 */
int cap_from_name(const char *name, cap_value_t *value_p)
{
    int n;

    if (((n = lookupname(&name)) >= 0) && (value_p != NULL)) {
	*value_p = (unsigned) n;
    }
    return -(n < 0);
}

/*
 * Convert a single capability index number into a string representation
 */
char *cap_to_name(cap_value_t cap)
{
    if ((cap < 0) || (cap >= __CAP_BITS)) {
#if UINT_MAX != 4294967295U
# error Recompile with correctly sized numeric array
#endif
	char *tmp, *result;

	asprintf(&tmp, "%u", cap);
	result = _libcap_strdup(tmp);
	free(tmp);

	return result;
    } else {
	return _libcap_strdup(_cap_names[cap]);
    }
}

/*
 * Convert an internal representation to a textual one. The textual
 * representation is stored in static memory. It will be overwritten
 * on the next occasion that this function is called.
 */

static int getstateflags(cap_t caps, int capno)
{
    int f = 0;

    if (isset_cap(caps, capno, CAP_EFFECTIVE)) {
	f |= LIBCAP_EFF;
    }
    if (isset_cap(caps, capno, CAP_PERMITTED)) {
	f |= LIBCAP_PER;
    }
    if (isset_cap(caps, capno, CAP_INHERITABLE)) {
	f |= LIBCAP_INH;
    }

    return f;
}

#define CAP_TEXT_BUFFER_ZONE 100

char *cap_to_text(cap_t caps, ssize_t *length_p)
{
    char buf[CAP_TEXT_SIZE+CAP_TEXT_BUFFER_ZONE];
    char *p;
    int histo[8];
    int m, t;
    unsigned n;
    unsigned cap_maxbits, cap_blks;

    /* Check arguments */
    if (!good_cap_t(caps)) {
	errno = EINVAL;
	return NULL;
    }

    switch (caps->head.version) {
    case _LINUX_CAPABILITY_VERSION_1:
	cap_blks = _LINUX_CAPABILITY_U32S_1;
	break;
    case _LINUX_CAPABILITY_VERSION_2:
	cap_blks = _LINUX_CAPABILITY_U32S_2;
	break;
    case _LINUX_CAPABILITY_VERSION_3:
	cap_blks = _LINUX_CAPABILITY_U32S_3;
	break;
    default:
	errno = EINVAL;
	return NULL;
    }

    cap_maxbits = 32 * cap_blks;

    _cap_debugcap("e = ", *caps, CAP_EFFECTIVE);
    _cap_debugcap("i = ", *caps, CAP_INHERITABLE);
    _cap_debugcap("p = ", *caps, CAP_PERMITTED);

    memset(histo, 0, sizeof(histo));

    /* default prevailing state to the upper - unnamed bits */
    for (n = cap_maxbits-1; n > __CAP_BITS; n--)
	histo[getstateflags(caps, n)]++;

    /* find which combination of capability sets shares the most bits
       we bias to preferring non-set (m=0) with the >= 0 test. Failing
       to do this causes strange things to happen with older systems
       that don't know about bits 32+. */
    for (m=t=7; t--; )
	if (histo[t] >= histo[m])
	    m = t;

    /* capture remaining bits - selecting m from only the unnamed bits,
       we maximize the likelihood that we won't see numeric capability
       values in the text output. */
    while (n--)
	histo[getstateflags(caps, n)]++;

    /* blank is not a valid capability set */
    p = sprintf(buf, "=%s%s%s",
		(m & LIBCAP_EFF) ? "e" : "",
		(m & LIBCAP_INH) ? "i" : "",
		(m & LIBCAP_PER) ? "p" : "" ) + buf;

    for (t = 8; t--; )
	if (t != m && histo[t]) {
	    *p++ = ' ';
	    for (n = 0; n < cap_maxbits; n++)
		if (getstateflags(caps, n) == t) {
		    char *this_cap_name;

		    this_cap_name = cap_to_name(n);
		    if ((strlen(this_cap_name) + (p - buf)) > CAP_TEXT_SIZE) {
			cap_free(this_cap_name);
			errno = ERANGE;
			return NULL;
		    }
		    p += sprintf(p, "%s,", this_cap_name);
		    cap_free(this_cap_name);
		}
	    p--;
	    n = t & ~m;
	    if (n)
		p += sprintf(p, "+%s%s%s",
			     (n & LIBCAP_EFF) ? "e" : "",
			     (n & LIBCAP_INH) ? "i" : "",
			     (n & LIBCAP_PER) ? "p" : "");
	    n = ~t & m;
	    if (n)
		p += sprintf(p, "-%s%s%s",
			     (n & LIBCAP_EFF) ? "e" : "",
			     (n & LIBCAP_INH) ? "i" : "",
			     (n & LIBCAP_PER) ? "p" : "");
	    if (p - buf > CAP_TEXT_SIZE) {
		errno = ERANGE;
		return NULL;
	    }
	}

    _cap_debug("%s", buf);
    if (length_p) {
	*length_p = p - buf;
    }

    return (_libcap_strdup(buf));
}
