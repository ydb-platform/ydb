/*
 * fy-utf8.c - utf8 handling methods
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

#include <libfyaml.h>

#include "fy-utf8.h"

const int8_t fy_utf8_width_table[32] = {
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	0, 0, 0, 0, 0, 0, 0, 0,
	2, 2, 2, 2, 3, 3, 4, 0,
};

int fy_utf8_get_generic(const void *ptr, int left, int *widthp)
{
	const uint8_t *p = ptr;
	int i, width, value;

	if (left < 1)
		return FYUG_EOF;

	/* this is the slow path */
	width = fy_utf8_width_by_first_octet(p[0]);
	if (!width)
		return FYUG_INV;
	if (width > left)
		return FYUG_PARTIAL;

	/* initial value */
	value = *p++ & (0xff >> width);
	for (i = 1; i < width; i++) {
		if ((*p & 0xc0) != 0x80)
			return FYUG_INV;
		value = (value << 6) | (*p++ & 0x3f);
	}

	/* check for validity */
	if ((width == 4 && value < 0x10000) ||
	    (width == 3 && value <   0x800) ||
	    (width == 2 && value <    0x80) ||
	    (value >= 0xd800 && value <= 0xdfff) || value >= 0x110000)
		return FYUG_INV;

	*widthp = width;

	return value;
}

int fy_utf8_get_right_generic(const void *ptr, int left, int *widthp)
{
	const uint8_t *s, *e;
	uint8_t v;

	s = ptr;
	e =  s + left;

	if (left < 1)
		return FYUG_EOF;

	/* single byte sequence */
	v = e[-1];
	if ((v & 0x80) == 0) {
		if (widthp)
			*widthp = 1;
		return (int)v & 0x7f;
	}

	/* the last byte must be & 0xc0 == 0x80 */
	if ((v & 0xc0) != 0x80)
		return FYUG_INV;

	/* at least two byte sequence */
	if (left < 2)
		return FYUG_EOF;

	v = e[-2];
	/* the first byte of the sequence (must be a two byte sequence) */
	if ((v & 0xc0) != 0x80) {
		/* two byte start is 110x_xxxx */
		if ((v & 0xe0) != 0xc0)
			return FYUG_INV;
		return fy_utf8_get(e - 2, 2, widthp);
	}

	/* at least three byte sequence */
	if (left < 3)
		return FYUG_EOF;

	v = e[-3];
	/* the first byte of the sequence (must be a three byte sequence) */
	if ((v & 0xc0) != 0x80) {
		/* three byte start is 1110_xxxx */
		if ((v & 0xf0) != 0xe0)
			return FYUG_INV;
		return fy_utf8_get(e - 3, 3, widthp);
	}

	/* at least four byte sequence */
	if (left < 4)
		return FYUG_EOF;

	v = e[-4];

	/* the first byte of the sequence (must be a four byte sequence) */
	/* four byte start is 1111_0xxx */
	if ((v & 0xf8) != 0xf0) {
		return FYUG_INV;
	}
	return fy_utf8_get(e - 4, 4, widthp);
}

struct fy_utf8_fmt_esc_map {
	const int *ch;
	const int *map;
};

static const struct fy_utf8_fmt_esc_map esc_all = {
	.ch  = (const int []){ '\\', '\0', '\b', '\r', '\t', '\f', '\n', '\v', '\a', '\e', 0x85, 0xa0, 0x2028, 0x2029, -1 },
	.map = (const int []){ '\\',  '0',  'b',  'r',  't',  'f',  'n',  'v',  'a',  'e',  'N',  '_',    'L',    'P',  0 }
};

static inline int esc_map(const struct fy_utf8_fmt_esc_map *esc_map, int c)
{
	const int *ch;
	int cc;

	ch = esc_map->ch;
	while ((cc = *ch++) >= 0) {
		if (cc == c)
			return esc_map->map[(ch - esc_map->ch) - 1];
	}
	return -1;
}

static inline int fy_utf8_esc_map(int c, enum fy_utf8_escape esc)
{
	if (esc == fyue_none)
		return -1;
	if (esc == fyue_singlequote && c == '\'')
		return '\'';
	if (fy_utf8_escape_is_any_doublequote(esc) && c == '"')
		return '"';
	return esc_map(&esc_all, c);
}

int fy_utf8_format_text_length(const char *buf, size_t len,
			       enum fy_utf8_escape esc)
{
	int c, w, l;
	const char *s, *e;

	s = buf;
	e = buf + len;
	l = 0;
	while (s < e) {
		c = fy_utf8_get(s, e - s, &w);
		if (!w || c < 0)
			break;
		s += w;
		if (fy_utf8_esc_map(c, esc))
			w = 2;
		l += w;
	}
	return l + 1;
}

char *fy_utf8_format_text(const char *buf, size_t len,
			  char *out, size_t maxsz,
			  enum fy_utf8_escape esc)
{
	int c, w, cc;
	const char *s, *e;
	char *os, *oe;

	s = buf;
	e = buf + len;
	os = out;
	oe = out + maxsz - 1;
	while (s < e) {
		c = fy_utf8_get(s, e - s, &w);
		if (!w || c < 0)
			break;
		s += w;

		if ((cc = fy_utf8_esc_map(c, esc)) > 0) {
			if (os + 2 > oe)
				break;
			*os++ = '\\';
			*os++ = cc;
			continue;
		}

		if (os + w > oe)
			break;

		os = fy_utf8_put_unchecked(os, c);
	}
	*os++ = '\0';
	return out;
}

char *fy_utf8_format(int c, char *buf, enum fy_utf8_escape esc)
{
	int cc;
	char *s;

	if (!fy_utf8_is_valid(c)) {
		*buf = '\0';
		return buf;
	}

	s = buf;
	if ((cc = fy_utf8_esc_map(c, esc)) > 0) {
		*s++ = '\\';
		*s++ = cc;
	} else
		s = fy_utf8_put_unchecked(s, c);
	*s = '\0';
	return buf;
}

char *fy_utf8_format_text_alloc(const char *buf, size_t len, enum fy_utf8_escape esc)
{
	int outsz;
	char *out;

	outsz = fy_utf8_format_text_length(buf, len, esc);
	if (outsz < 0)
		return NULL;
	out = malloc(outsz);
	if (!out)
		return NULL;
	fy_utf8_format_text(buf, len, out, outsz, esc);

	return out;
}

const void *fy_utf8_memchr_generic(const void *s, int c, size_t n)
{
	int cc, w;
	const void *e;

	e = (char*)s + n;
	while (s < e && (cc = fy_utf8_get(s, (char*)e - (char*)s, &w)) >= 0) {
		if (c == cc)
			return s;
		s = (char*)s + w;
	}

	return NULL;
}

/* parse an escape and return utf8 value */
int fy_utf8_parse_escape(const char **strp, size_t len, enum fy_utf8_escape esc)
{
	const char *s, *e;
	char c;
	int i, value, code_length, cc, w;
	unsigned int hi_surrogate, lo_surrogate;

	/* why do you bother us? */
	if (esc == fyue_none)
		return -1;

	if (!strp || !*strp || len < 2)
		return -1;

	value = -1;

	s = *strp;
	e = s + len;

	c = *s++;

	if (esc == fyue_singlequote) {
		if (c != '\'')
			goto out;
		c = *s++;
		if (c != '\'')
			goto out;

		value = '\'';
		goto out;
	}

	/* get '\\' */
	if (c != '\\')
		goto out;

	c = *s++;

	/* common YAML & JSON escapes */
	switch (c) {
	case 'b':
		value = '\b';
		break;
	case 'f':
		value = '\f';
		break;
	case 'n':
		value = '\n';
		break;
	case 'r':
		value = '\r';
		break;
	case 't':
		value = '\t';
		break;
	case '"':
		value = '"';
		break;
	case '/':
		value = '/';
		break;
	case '\\':
		value = '\\';
		break;
	default:
		break;
	}

	if (value >= 0)
		goto out;

	if (esc == fyue_doublequote || esc == fyue_doublequote_yaml_1_1) {
		switch (c) {
		case '0':
			value = '\0';
			break;
		case 'a':
			value = '\a';
			break;
		case '\t':
			value = '\t';
			break;
		case 'v':
			value = '\v';
			break;
		case 'e':
			value = '\e';
			break;
		case ' ':
			value = ' ';
			break;
		case 'N':
			value = 0x85;	/* NEL */
			break;
		case '_':
			value = 0xa0;
			break;
		case 'L':
			value = 0x2028;	/* LS */
			break;
		case 'P':	/* PS 0x2029 */
			value = 0x2029; /* PS */
			break;
		default:
			/* weird unicode escapes */
			if ((uint8_t)c >= 0x80) {
				/* in non yaml-1.1 mode we don't allow this craziness */
				if (esc == fyue_doublequote)
					goto out;

				cc = fy_utf8_get(s - 1, e - (s - 1), &w);
				switch (cc) {
				case 0x2028:
				case 0x2029:
				case 0x85:
				case 0xa0:
					value = cc;
					break;
				default:
					break;
				}
			}
			break;
		}
		if (value >= 0)
			goto out;
	}

	/* finally try the unicode escapes */
	code_length = 0;

	if (esc == fyue_doublequote || esc == fyue_doublequote_yaml_1_1) {
		switch (c) {
		case 'x':
			code_length = 2;
			break;
		case 'u':
			code_length = 4;
			break;
		case 'U':
			code_length = 8;
			break;
		default:
			return -1;
		}
	} else if (esc == fyue_doublequote_json && c == 'u')
		code_length = 4;

	if (!code_length || code_length > (e - s))
		goto out;

	value = 0;
	for (i = 0; i < code_length; i++) {
		c = *s++;
		value <<= 4;
		if (c >= '0' && c <= '9')
			value |= c - '0';
		else if (c >= 'a' && c <= 'f')
			value |= 10 + c - 'a';
		else if (c >= 'A' && c <= 'F')
			value |= 10 + c - 'A';
		else
			goto out;
	}

	/* hi/lo surrogate pair */
	if (code_length == 4 && value >= 0xd800 && value <= 0xdbff &&
		(e - s) >= 6 && s[0] == '\\' && s[1] == 'u') {
		hi_surrogate = value;

		s += 2;

		value = 0;
		for (i = 0; i < code_length; i++) {
			c = *s++;
			value <<= 4;
			if (c >= '0' && c <= '9')
				value |= c - '0';
			else if (c >= 'a' && c <= 'f')
				value |= 10 + c - 'a';
			else if (c >= 'A' && c <= 'F')
				value |= 10 + c - 'A';
			else
				return -1;
		}
		lo_surrogate = value;
		value = 0x10000 + (hi_surrogate - 0xd800) * 0x400 + (lo_surrogate - 0xdc00);
	}

out:
	*strp = s;

	return value;
}

uint8_t fy_utf8_low_ascii_flags[0x80] = {
	[0x00] = F_NON_PRINT,				// NUL '\0' (null character)
	[0x01] = F_NON_PRINT, 				// SOH (start of heading)
	[0x02] = F_NON_PRINT,				// STX (start of text)
	[0x03] = F_NON_PRINT,				// ETX (end of text)
	[0x04] = F_NON_PRINT,				// EOT (end of transmission)
	[0x05] = F_NON_PRINT,				// ENQ (enquiry)
	[0x06] = F_NON_PRINT,				// ACK (acknowledge)
	[0x07] = F_NON_PRINT | F_QUOTE_ESC,		// BEL '\a' (bell)
	[0x08] = F_NON_PRINT | F_QUOTE_ESC,		// BS  '\b' (backspace)
	[0x09] = F_NON_PRINT | F_QUOTE_ESC | F_WS,	// HT  '\t' (horizontal tab)
	[0x0A] = F_NON_PRINT | F_QUOTE_ESC | F_LB,	// LF  '\n' (new line)
	[0x0B] = F_NON_PRINT | F_QUOTE_ESC,		// VT  '\v' (vertical tab)
	[0x0C] = F_NON_PRINT | F_QUOTE_ESC,		// FF  '\f' (form feed)
	[0x0D] = F_NON_PRINT | F_QUOTE_ESC | F_LB,	// CR  '\r' (carriage ret)
	[0x0E] = F_NON_PRINT,				// SO  (shift out)
	[0x0F] = F_NON_PRINT,				// SI  (shift in)
	[0x10] = F_NON_PRINT,				// DLE (data link escape)
	[0x11] = F_NON_PRINT,				// DC1 (device control 1)
	[0x12] = F_NON_PRINT,				// DC2 (device control 2)
	[0x13] = F_NON_PRINT,				// DC3 (device control 3)
	[0x14] = F_NON_PRINT,				// DC4 (device control 4)
	[0x15] = F_NON_PRINT,				// NAK (negative ack.)
	[0x16] = F_NON_PRINT,				// SYN (synchronous idle)
	[0x17] = F_NON_PRINT,				// ETB (end of trans. blk)
	[0x18] = F_NON_PRINT,				// CAN (cancel)
	[0x19] = F_NON_PRINT,				// EM  (end of medium)
	[0x1A] = F_NON_PRINT,				// SUB (substitute)
	[0x1B] = F_NON_PRINT,				// ESC (escape)
	[0x1C] = F_NON_PRINT,				// FS  (file separator)
	[0x1D] = F_NON_PRINT,				// GS  (group separator)
	[0x1E] = F_NON_PRINT,				// RS  (record separator)
	[0x1F] = F_NON_PRINT,				// US  (unit separator)
	[' ']  = F_WS,
	['!']  = F_PUNCT,
	['"']  = F_PUNCT,
	['#']  = F_PUNCT,
	['$']  = F_PUNCT,
	['%']  = F_PUNCT,
	['&']  = F_PUNCT,
	['\''] = F_PUNCT,
	['(']  = F_PUNCT,
	[')']  = F_PUNCT,
	['*']  = F_PUNCT,
	['+']  = F_PUNCT,
	[',']  = F_PUNCT,
	['-']  = F_PUNCT,
	['.']  = F_PUNCT,
	['/']  = F_PUNCT,
	['0']  = F_DIGIT | F_SIMPLE_SCALAR,
	['1']  = F_DIGIT | F_SIMPLE_SCALAR,
	['2']  = F_DIGIT | F_SIMPLE_SCALAR,
	['3']  = F_DIGIT | F_SIMPLE_SCALAR,
	['4']  = F_DIGIT | F_SIMPLE_SCALAR,
	['5']  = F_DIGIT | F_SIMPLE_SCALAR,
	['6']  = F_DIGIT | F_SIMPLE_SCALAR,
	['7']  = F_DIGIT | F_SIMPLE_SCALAR,
	['8']  = F_DIGIT | F_SIMPLE_SCALAR,
	['9']  = F_DIGIT | F_SIMPLE_SCALAR,
	[':']  = F_PUNCT,
	[';']  = F_PUNCT,
	['<']  = F_PUNCT,
	['=']  = F_PUNCT,
	['>']  = F_PUNCT,
	['?']  = F_PUNCT,
	['@']  = F_PUNCT,
	['A']  = F_LETTER | F_SIMPLE_SCALAR,
	['B']  = F_LETTER | F_SIMPLE_SCALAR,
	['C']  = F_LETTER | F_SIMPLE_SCALAR,
	['D']  = F_LETTER | F_SIMPLE_SCALAR,
	['E']  = F_LETTER | F_SIMPLE_SCALAR,
	['F']  = F_LETTER | F_SIMPLE_SCALAR,
	['G']  = F_LETTER | F_SIMPLE_SCALAR,
	['H']  = F_LETTER | F_SIMPLE_SCALAR,
	['I']  = F_LETTER | F_SIMPLE_SCALAR,
	['J']  = F_LETTER | F_SIMPLE_SCALAR,
	['K']  = F_LETTER | F_SIMPLE_SCALAR,
	['L']  = F_LETTER | F_SIMPLE_SCALAR,
	['M']  = F_LETTER | F_SIMPLE_SCALAR,
	['N']  = F_LETTER | F_SIMPLE_SCALAR,
	['O']  = F_LETTER | F_SIMPLE_SCALAR,
	['P']  = F_LETTER | F_SIMPLE_SCALAR,
	['Q']  = F_LETTER | F_SIMPLE_SCALAR,
	['R']  = F_LETTER | F_SIMPLE_SCALAR,
	['S']  = F_LETTER | F_SIMPLE_SCALAR,
	['T']  = F_LETTER | F_SIMPLE_SCALAR,
	['U']  = F_LETTER | F_SIMPLE_SCALAR,
	['V']  = F_LETTER | F_SIMPLE_SCALAR,
	['W']  = F_LETTER | F_SIMPLE_SCALAR,
	['X']  = F_LETTER | F_SIMPLE_SCALAR,
	['Y']  = F_LETTER | F_SIMPLE_SCALAR,
	['Z']  = F_LETTER | F_SIMPLE_SCALAR,
	['[']  = F_PUNCT,
	['\\'] = F_PUNCT,  				// '\\'
	[']']  = F_PUNCT,
	['^']  = F_PUNCT,
	['_']  = F_PUNCT | F_SIMPLE_SCALAR,
	['`']  = F_PUNCT,
	['a']  = F_LETTER | F_SIMPLE_SCALAR,
	['b']  = F_LETTER | F_SIMPLE_SCALAR,
	['c']  = F_LETTER | F_SIMPLE_SCALAR,
	['d']  = F_LETTER | F_SIMPLE_SCALAR,
	['e']  = F_LETTER | F_SIMPLE_SCALAR,
	['f']  = F_LETTER | F_SIMPLE_SCALAR,
	['g']  = F_LETTER | F_SIMPLE_SCALAR,
	['h']  = F_LETTER | F_SIMPLE_SCALAR,
	['i']  = F_LETTER | F_SIMPLE_SCALAR,
	['j']  = F_LETTER | F_SIMPLE_SCALAR,
	['k']  = F_LETTER | F_SIMPLE_SCALAR,
	['l']  = F_LETTER | F_SIMPLE_SCALAR,
	['m']  = F_LETTER | F_SIMPLE_SCALAR,
	['n']  = F_LETTER | F_SIMPLE_SCALAR,
	['o']  = F_LETTER | F_SIMPLE_SCALAR,
	['p']  = F_LETTER | F_SIMPLE_SCALAR,
	['q']  = F_LETTER | F_SIMPLE_SCALAR,
	['r']  = F_LETTER | F_SIMPLE_SCALAR,
	['s']  = F_LETTER | F_SIMPLE_SCALAR,
	['t']  = F_LETTER | F_SIMPLE_SCALAR,
	['u']  = F_LETTER | F_SIMPLE_SCALAR,
	['v']  = F_LETTER | F_SIMPLE_SCALAR,
	['w']  = F_LETTER | F_SIMPLE_SCALAR,
	['x']  = F_LETTER | F_SIMPLE_SCALAR,
	['y']  = F_LETTER | F_SIMPLE_SCALAR,
	['z']  = F_LETTER | F_SIMPLE_SCALAR,
	['{']  = F_PUNCT,
	['|']  = F_PUNCT,
	['}']  = F_PUNCT,
	['~']  = F_PUNCT,
	[0x7F] = F_NON_PRINT,				// DEL
};
