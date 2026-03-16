#line 1 "src/spss/readstat_sav_parse.rl"
#include <limits.h>
#include <stdlib.h>
#include "../readstat.h"
#include "../readstat_malloc.h"
#include "../readstat_strings.h"

#include "readstat_sav.h"
#include "readstat_sav_parse.h"


#line 21 "src/spss/readstat_sav_parse.rl"


typedef struct varlookup {
	char      name[8*4+1];
	int       index;
} varlookup_t;

static int compare_key_varlookup(const void *elem1, const void *elem2) {
	const char *key = (const char *)elem1;
	const varlookup_t *v = (const varlookup_t *)elem2;
	return strcasecmp(key, v->name);
}

static int compare_varlookups(const void *elem1, const void *elem2) {
	const varlookup_t *v1 = (const varlookup_t *)elem1;
	const varlookup_t *v2 = (const varlookup_t *)elem2;
	return strcasecmp(v1->name, v2->name);
}

static int count_vars(sav_ctx_t *ctx) {
	int i;
	spss_varinfo_t *last_info = NULL;
	int var_count = 0;
	for (i=0; i<ctx->var_index; i++) {
		spss_varinfo_t *info = ctx->varinfo[i];
		if (last_info == NULL || strcmp(info->name, last_info->name) != 0) {
			var_count++;
		}
		last_info = info;
	}
	return var_count;
}

static varlookup_t *build_lookup_table(int var_count, sav_ctx_t *ctx) {
	varlookup_t *table = readstat_malloc(var_count * sizeof(varlookup_t));
	int offset = 0;
	int i;
	spss_varinfo_t *last_info = NULL;
	for (i=0; i<ctx->var_index; i++) {
		spss_varinfo_t *info = ctx->varinfo[i];
		
		if (last_info == NULL || strcmp(info->name, last_info->name) != 0) {
			varlookup_t *entry = &table[offset++];
			
			memcpy(entry->name, info->name, sizeof(info->name));
			entry->index = info->index;
		}
		last_info = info;
	}
	qsort(table, var_count, sizeof(varlookup_t), &compare_varlookups);
	return table;
}


#line 68 "src/spss/readstat_sav_parse.c"
static const signed char _sav_long_variable_parse_actions[] = {
	0, 1, 1, 1, 5, 2, 2, 0,
	3, 6, 4, 3, 0
};

static const short _sav_long_variable_parse_key_offsets[] = {
	0, 0, 5, 19, 33, 47, 61, 75,
	89, 103, 104, 108, 113, 118, 123, 128,
	133, 138, 143, 148, 153, 158, 163, 168,
	173, 178, 183, 188, 193, 198, 203, 208,
	213, 218, 223, 228, 233, 238, 243, 248,
	253, 258, 263, 268, 273, 278, 283, 288,
	293, 298, 303, 308, 313, 318, 323, 328,
	333, 338, 343, 348, 353, 358, 363, 368,
	373, 378, 383, 388, 393, 398, 403, 408,
	413, 418, 423, 428, 0
};

static const unsigned char _sav_long_variable_parse_trans_keys[] = {
	255u, 0u, 63u, 91u, 127u, 47u, 61u, 96u,
	255u, 0u, 34u, 37u, 45u, 58u, 63u, 91u,
	94u, 123u, 127u, 47u, 61u, 96u, 255u, 0u,
	34u, 37u, 45u, 58u, 63u, 91u, 94u, 123u,
	127u, 47u, 61u, 96u, 255u, 0u, 34u, 37u,
	45u, 58u, 63u, 91u, 94u, 123u, 127u, 47u,
	61u, 96u, 255u, 0u, 34u, 37u, 45u, 58u,
	63u, 91u, 94u, 123u, 127u, 47u, 61u, 96u,
	255u, 0u, 34u, 37u, 45u, 58u, 63u, 91u,
	94u, 123u, 127u, 47u, 61u, 96u, 255u, 0u,
	34u, 37u, 45u, 58u, 63u, 91u, 94u, 123u,
	127u, 47u, 61u, 96u, 255u, 0u, 34u, 37u,
	45u, 58u, 63u, 91u, 94u, 123u, 127u, 61u,
	127u, 255u, 0u, 31u, 9u, 127u, 255u, 0u,
	31u, 255u, 0u, 63u, 91u, 127u, 9u, 127u,
	255u, 0u, 31u, 9u, 127u, 255u, 0u, 31u,
	9u, 127u, 255u, 0u, 31u, 9u, 127u, 255u,
	0u, 31u, 9u, 127u, 255u, 0u, 31u, 9u,
	127u, 255u, 0u, 31u, 9u, 127u, 255u, 0u,
	31u, 9u, 127u, 255u, 0u, 31u, 9u, 127u,
	255u, 0u, 31u, 9u, 127u, 255u, 0u, 31u,
	9u, 127u, 255u, 0u, 31u, 9u, 127u, 255u,
	0u, 31u, 9u, 127u, 255u, 0u, 31u, 9u,
	127u, 255u, 0u, 31u, 9u, 127u, 255u, 0u,
	31u, 9u, 127u, 255u, 0u, 31u, 9u, 127u,
	255u, 0u, 31u, 9u, 127u, 255u, 0u, 31u,
	9u, 127u, 255u, 0u, 31u, 9u, 127u, 255u,
	0u, 31u, 9u, 127u, 255u, 0u, 31u, 9u,
	127u, 255u, 0u, 31u, 9u, 127u, 255u, 0u,
	31u, 9u, 127u, 255u, 0u, 31u, 9u, 127u,
	255u, 0u, 31u, 9u, 127u, 255u, 0u, 31u,
	9u, 127u, 255u, 0u, 31u, 9u, 127u, 255u,
	0u, 31u, 9u, 127u, 255u, 0u, 31u, 9u,
	127u, 255u, 0u, 31u, 9u, 127u, 255u, 0u,
	31u, 9u, 127u, 255u, 0u, 31u, 9u, 127u,
	255u, 0u, 31u, 9u, 127u, 255u, 0u, 31u,
	9u, 127u, 255u, 0u, 31u, 9u, 127u, 255u,
	0u, 31u, 9u, 127u, 255u, 0u, 31u, 9u,
	127u, 255u, 0u, 31u, 9u, 127u, 255u, 0u,
	31u, 9u, 127u, 255u, 0u, 31u, 9u, 127u,
	255u, 0u, 31u, 9u, 127u, 255u, 0u, 31u,
	9u, 127u, 255u, 0u, 31u, 9u, 127u, 255u,
	0u, 31u, 9u, 127u, 255u, 0u, 31u, 9u,
	127u, 255u, 0u, 31u, 9u, 127u, 255u, 0u,
	31u, 9u, 127u, 255u, 0u, 31u, 9u, 127u,
	255u, 0u, 31u, 9u, 127u, 255u, 0u, 31u,
	9u, 127u, 255u, 0u, 31u, 9u, 127u, 255u,
	0u, 31u, 9u, 127u, 255u, 0u, 31u, 9u,
	127u, 255u, 0u, 31u, 9u, 127u, 255u, 0u,
	31u, 9u, 127u, 255u, 0u, 31u, 9u, 127u,
	255u, 0u, 31u, 9u, 127u, 255u, 0u, 31u,
	9u, 127u, 255u, 0u, 31u, 9u, 127u, 255u,
	0u, 31u, 9u, 127u, 255u, 0u, 31u, 9u,
	127u, 255u, 0u, 31u, 9u, 0u
};

static const signed char _sav_long_variable_parse_single_lengths[] = {
	0, 1, 4, 4, 4, 4, 4, 4,
	4, 1, 2, 3, 1, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 1, 0
};

static const signed char _sav_long_variable_parse_range_lengths[] = {
	0, 2, 5, 5, 5, 5, 5, 5,
	5, 0, 1, 1, 2, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 0, 0
};

static const short _sav_long_variable_parse_index_offsets[] = {
	0, 0, 4, 14, 24, 34, 44, 54,
	64, 74, 76, 80, 85, 89, 94, 99,
	104, 109, 114, 119, 124, 129, 134, 139,
	144, 149, 154, 159, 164, 169, 174, 179,
	184, 189, 194, 199, 204, 209, 214, 219,
	224, 229, 234, 239, 244, 249, 254, 259,
	264, 269, 274, 279, 284, 289, 294, 299,
	304, 309, 314, 319, 324, 329, 334, 339,
	344, 349, 354, 359, 364, 369, 374, 379,
	384, 389, 394, 399, 0
};

static const signed char _sav_long_variable_parse_cond_targs[] = {
	0, 0, 0, 2, 0, 10, 0, 0,
	0, 0, 0, 0, 0, 3, 0, 10,
	0, 0, 0, 0, 0, 0, 0, 4,
	0, 10, 0, 0, 0, 0, 0, 0,
	0, 5, 0, 10, 0, 0, 0, 0,
	0, 0, 0, 6, 0, 10, 0, 0,
	0, 0, 0, 0, 0, 7, 0, 10,
	0, 0, 0, 0, 0, 0, 0, 8,
	0, 10, 0, 0, 0, 0, 0, 0,
	0, 9, 10, 0, 0, 0, 0, 11,
	12, 0, 0, 0, 13, 0, 0, 0,
	2, 12, 0, 0, 0, 14, 12, 0,
	0, 0, 15, 12, 0, 0, 0, 16,
	12, 0, 0, 0, 17, 12, 0, 0,
	0, 18, 12, 0, 0, 0, 19, 12,
	0, 0, 0, 20, 12, 0, 0, 0,
	21, 12, 0, 0, 0, 22, 12, 0,
	0, 0, 23, 12, 0, 0, 0, 24,
	12, 0, 0, 0, 25, 12, 0, 0,
	0, 26, 12, 0, 0, 0, 27, 12,
	0, 0, 0, 28, 12, 0, 0, 0,
	29, 12, 0, 0, 0, 30, 12, 0,
	0, 0, 31, 12, 0, 0, 0, 32,
	12, 0, 0, 0, 33, 12, 0, 0,
	0, 34, 12, 0, 0, 0, 35, 12,
	0, 0, 0, 36, 12, 0, 0, 0,
	37, 12, 0, 0, 0, 38, 12, 0,
	0, 0, 39, 12, 0, 0, 0, 40,
	12, 0, 0, 0, 41, 12, 0, 0,
	0, 42, 12, 0, 0, 0, 43, 12,
	0, 0, 0, 44, 12, 0, 0, 0,
	45, 12, 0, 0, 0, 46, 12, 0,
	0, 0, 47, 12, 0, 0, 0, 48,
	12, 0, 0, 0, 49, 12, 0, 0,
	0, 50, 12, 0, 0, 0, 51, 12,
	0, 0, 0, 52, 12, 0, 0, 0,
	53, 12, 0, 0, 0, 54, 12, 0,
	0, 0, 55, 12, 0, 0, 0, 56,
	12, 0, 0, 0, 57, 12, 0, 0,
	0, 58, 12, 0, 0, 0, 59, 12,
	0, 0, 0, 60, 12, 0, 0, 0,
	61, 12, 0, 0, 0, 62, 12, 0,
	0, 0, 63, 12, 0, 0, 0, 64,
	12, 0, 0, 0, 65, 12, 0, 0,
	0, 66, 12, 0, 0, 0, 67, 12,
	0, 0, 0, 68, 12, 0, 0, 0,
	69, 12, 0, 0, 0, 70, 12, 0,
	0, 0, 71, 12, 0, 0, 0, 72,
	12, 0, 0, 0, 73, 12, 0, 0,
	0, 74, 12, 0, 0, 0, 75, 12,
	0, 0, 1, 2, 3, 4, 5, 6,
	7, 8, 9, 10, 11, 12, 13, 14,
	15, 16, 17, 18, 19, 20, 21, 22,
	23, 24, 25, 26, 27, 28, 29, 30,
	31, 32, 33, 34, 35, 36, 37, 38,
	39, 40, 41, 42, 43, 44, 45, 46,
	47, 48, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62,
	63, 64, 65, 66, 67, 68, 69, 70,
	71, 72, 73, 74, 75, 0
};

static const signed char _sav_long_variable_parse_cond_actions[] = {
	0, 0, 0, 1, 0, 5, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 5,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 5, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 5, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 5, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 5,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 5, 0, 0, 0, 0, 0, 0,
	0, 0, 5, 0, 0, 0, 0, 3,
	8, 0, 0, 0, 0, 0, 0, 0,
	1, 8, 0, 0, 0, 0, 8, 0,
	0, 0, 0, 8, 0, 0, 0, 0,
	8, 0, 0, 0, 0, 8, 0, 0,
	0, 0, 8, 0, 0, 0, 0, 8,
	0, 0, 0, 0, 8, 0, 0, 0,
	0, 8, 0, 0, 0, 0, 8, 0,
	0, 0, 0, 8, 0, 0, 0, 0,
	8, 0, 0, 0, 0, 8, 0, 0,
	0, 0, 8, 0, 0, 0, 0, 8,
	0, 0, 0, 0, 8, 0, 0, 0,
	0, 8, 0, 0, 0, 0, 8, 0,
	0, 0, 0, 8, 0, 0, 0, 0,
	8, 0, 0, 0, 0, 8, 0, 0,
	0, 0, 8, 0, 0, 0, 0, 8,
	0, 0, 0, 0, 8, 0, 0, 0,
	0, 8, 0, 0, 0, 0, 8, 0,
	0, 0, 0, 8, 0, 0, 0, 0,
	8, 0, 0, 0, 0, 8, 0, 0,
	0, 0, 8, 0, 0, 0, 0, 8,
	0, 0, 0, 0, 8, 0, 0, 0,
	0, 8, 0, 0, 0, 0, 8, 0,
	0, 0, 0, 8, 0, 0, 0, 0,
	8, 0, 0, 0, 0, 8, 0, 0,
	0, 0, 8, 0, 0, 0, 0, 8,
	0, 0, 0, 0, 8, 0, 0, 0,
	0, 8, 0, 0, 0, 0, 8, 0,
	0, 0, 0, 8, 0, 0, 0, 0,
	8, 0, 0, 0, 0, 8, 0, 0,
	0, 0, 8, 0, 0, 0, 0, 8,
	0, 0, 0, 0, 8, 0, 0, 0,
	0, 8, 0, 0, 0, 0, 8, 0,
	0, 0, 0, 8, 0, 0, 0, 0,
	8, 0, 0, 0, 0, 8, 0, 0,
	0, 0, 8, 0, 0, 0, 0, 8,
	0, 0, 0, 0, 8, 0, 0, 0,
	0, 8, 0, 0, 0, 0, 8, 0,
	0, 0, 0, 8, 0, 0, 0, 0,
	8, 0, 0, 0, 0, 8, 0, 0,
	0, 0, 8, 0, 0, 0, 0, 8,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 8, 0, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 0
};

static const short _sav_long_variable_parse_eof_trans[] = {
	402, 403, 404, 405, 406, 407, 408, 409,
	410, 411, 412, 413, 414, 415, 416, 417,
	418, 419, 420, 421, 422, 423, 424, 425,
	426, 427, 428, 429, 430, 431, 432, 433,
	434, 435, 436, 437, 438, 439, 440, 441,
	442, 443, 444, 445, 446, 447, 448, 449,
	450, 451, 452, 453, 454, 455, 456, 457,
	458, 459, 460, 461, 462, 463, 464, 465,
	466, 467, 468, 469, 470, 471, 472, 473,
	474, 475, 476, 477, 0
};

static const int sav_long_variable_parse_start = 1;

static const int sav_long_variable_parse_en_main = 1;


#line 79 "src/spss/readstat_sav_parse.rl"


readstat_error_t sav_parse_long_variable_names_record(void *data, int count, sav_ctx_t *ctx) {
	unsigned char *c_data = (unsigned char *)data;
	int var_count = count_vars(ctx);
	readstat_error_t retval = READSTAT_OK;
	
	char temp_key[8+1];
	char temp_val[64+1];
	unsigned char *str_start = NULL;
	size_t str_len = 0;
	
	char error_buf[8192];
	unsigned char *p = c_data;
	unsigned char *pe = c_data + count;
	
	varlookup_t *table = build_lookup_table(var_count, ctx);
	
	unsigned char *eof = pe;
	
	int cs;
	
	
#line 351 "src/spss/readstat_sav_parse.c"
	{
		cs = (int)sav_long_variable_parse_start;
	}
	
#line 356 "src/spss/readstat_sav_parse.c"
	{
		int _klen;
		unsigned int _trans = 0;
		const unsigned char * _keys;
		const signed char * _acts;
		unsigned int _nacts;
		_resume: {}
		if ( p == pe && p != eof )
			goto _out;
		if ( p == eof ) {
			if ( _sav_long_variable_parse_eof_trans[cs] > 0 ) {
				_trans = (unsigned int)_sav_long_variable_parse_eof_trans[cs] - 1;
			}
		}
		else {
			_keys = ( _sav_long_variable_parse_trans_keys + (_sav_long_variable_parse_key_offsets[cs]));
			_trans = (unsigned int)_sav_long_variable_parse_index_offsets[cs];
			
			_klen = (int)_sav_long_variable_parse_single_lengths[cs];
			if ( _klen > 0 ) {
				const unsigned char *_lower = _keys;
				const unsigned char *_upper = _keys + _klen - 1;
				const unsigned char *_mid;
				while ( 1 ) {
					if ( _upper < _lower ) {
						_keys += _klen;
						_trans += (unsigned int)_klen;
						break;
					}
					
					_mid = _lower + ((_upper-_lower) >> 1);
					if ( ( (*( p))) < (*( _mid)) )
						_upper = _mid - 1;
					else if ( ( (*( p))) > (*( _mid)) )
						_lower = _mid + 1;
					else {
						_trans += (unsigned int)(_mid - _keys);
						goto _match;
					}
				}
			}
			
			_klen = (int)_sav_long_variable_parse_range_lengths[cs];
			if ( _klen > 0 ) {
				const unsigned char *_lower = _keys;
				const unsigned char *_upper = _keys + (_klen<<1) - 2;
				const unsigned char *_mid;
				while ( 1 ) {
					if ( _upper < _lower ) {
						_trans += (unsigned int)_klen;
						break;
					}
					
					_mid = _lower + (((_upper-_lower) >> 1) & ~1);
					if ( ( (*( p))) < (*( _mid)) )
						_upper = _mid - 2;
					else if ( ( (*( p))) > (*( _mid + 1)) )
						_lower = _mid + 2;
					else {
						_trans += (unsigned int)((_mid - _keys)>>1);
						break;
					}
				}
			}
			
			_match: {}
		}
		cs = (int)_sav_long_variable_parse_cond_targs[_trans];
		
		if ( _sav_long_variable_parse_cond_actions[_trans] != 0 ) {
			
			_acts = ( _sav_long_variable_parse_actions + (_sav_long_variable_parse_cond_actions[_trans]));
			_nacts = (unsigned int)(*( _acts));
			_acts += 1;
			while ( _nacts > 0 ) {
				switch ( (*( _acts)) )
				{
					case 0:  {
						{
#line 13 "src/spss/readstat_sav_parse.rl"
							
							memcpy(temp_key, str_start, str_len);
							temp_key[str_len] = '\0';
						}
						
#line 442 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 1:  {
						{
#line 20 "src/spss/readstat_sav_parse.rl"
							str_start = p; }
						
#line 451 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 2:  {
						{
#line 20 "src/spss/readstat_sav_parse.rl"
							str_len = p - str_start; }
						
#line 460 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 3:  {
						{
#line 102 "src/spss/readstat_sav_parse.rl"
							
							varlookup_t *found = bsearch(temp_key, table, var_count, sizeof(varlookup_t), &compare_key_varlookup);
							if (found) {
								spss_varinfo_t *info = ctx->varinfo[found->index];
								memcpy(info->longname, temp_val, str_len);
								info->longname[str_len] = '\0';
							} else if (ctx->handle.error) {
								snprintf(error_buf, sizeof(error_buf), "Failed to find %s", temp_key);
								ctx->handle.error(error_buf, ctx->user_ctx);
							}
						}
						
#line 479 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 4:  {
						{
#line 114 "src/spss/readstat_sav_parse.rl"
							
							memcpy(temp_val, str_start, str_len);
							temp_val[str_len] = '\0';
						}
						
#line 491 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 5:  {
						{
#line 119 "src/spss/readstat_sav_parse.rl"
							str_start = p; }
						
#line 500 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 6:  {
						{
#line 119 "src/spss/readstat_sav_parse.rl"
							str_len = p - str_start; }
						
#line 509 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
				}
				_nacts -= 1;
				_acts += 1;
			}
			
		}
		
		if ( p == eof ) {
			if ( cs >= 11 )
				goto _out;
		}
		else {
			if ( cs != 0 ) {
				p += 1;
				goto _resume;
			}
		}
		_out: {}
	}
	
#line 127 "src/spss/readstat_sav_parse.rl"
	
	
	if (cs < 
#line 537 "src/spss/readstat_sav_parse.c"
	11
#line 129 "src/spss/readstat_sav_parse.rl"
	|| p != pe) {
		if (ctx->handle.error) {
			snprintf(error_buf, sizeof(error_buf), "Error parsing string \"%.*s\" around byte #%ld/%d, character %c", 
			count, (char *)data, (long)(p - c_data), count, *p);
			ctx->handle.error(error_buf, ctx->user_ctx);
		}
		retval = READSTAT_ERROR_PARSE;
	}
	
	
	if (table)
		free(table);
	
	/* suppress warning */
	(void)sav_long_variable_parse_en_main;
	
	return retval;
}


#line 560 "src/spss/readstat_sav_parse.c"
static const signed char _sav_very_long_string_parse_actions[] = {
	0, 1, 1, 1, 3, 1, 4, 2,
	2, 0, 2, 5, 4, 0
};

static const signed char _sav_very_long_string_parse_key_offsets[] = {
	0, 0, 5, 19, 33, 47, 61, 75,
	89, 103, 104, 106, 110, 112, 0
};

static const unsigned char _sav_very_long_string_parse_trans_keys[] = {
	255u, 0u, 63u, 91u, 127u, 47u, 61u, 96u,
	255u, 0u, 34u, 37u, 45u, 58u, 63u, 91u,
	94u, 123u, 127u, 47u, 61u, 96u, 255u, 0u,
	34u, 37u, 45u, 58u, 63u, 91u, 94u, 123u,
	127u, 47u, 61u, 96u, 255u, 0u, 34u, 37u,
	45u, 58u, 63u, 91u, 94u, 123u, 127u, 47u,
	61u, 96u, 255u, 0u, 34u, 37u, 45u, 58u,
	63u, 91u, 94u, 123u, 127u, 47u, 61u, 96u,
	255u, 0u, 34u, 37u, 45u, 58u, 63u, 91u,
	94u, 123u, 127u, 47u, 61u, 96u, 255u, 0u,
	34u, 37u, 45u, 58u, 63u, 91u, 94u, 123u,
	127u, 47u, 61u, 96u, 255u, 0u, 34u, 37u,
	45u, 58u, 63u, 91u, 94u, 123u, 127u, 61u,
	48u, 57u, 0u, 9u, 48u, 57u, 0u, 9u,
	255u, 0u, 63u, 91u, 127u, 0u
};

static const signed char _sav_very_long_string_parse_single_lengths[] = {
	0, 1, 4, 4, 4, 4, 4, 4,
	4, 1, 0, 2, 2, 1, 0
};

static const signed char _sav_very_long_string_parse_range_lengths[] = {
	0, 2, 5, 5, 5, 5, 5, 5,
	5, 0, 1, 1, 0, 2, 0
};

static const signed char _sav_very_long_string_parse_index_offsets[] = {
	0, 0, 4, 14, 24, 34, 44, 54,
	64, 74, 76, 78, 82, 85, 0
};

static const signed char _sav_very_long_string_parse_cond_targs[] = {
	0, 0, 0, 2, 0, 10, 0, 0,
	0, 0, 0, 0, 0, 3, 0, 10,
	0, 0, 0, 0, 0, 0, 0, 4,
	0, 10, 0, 0, 0, 0, 0, 0,
	0, 5, 0, 10, 0, 0, 0, 0,
	0, 0, 0, 6, 0, 10, 0, 0,
	0, 0, 0, 0, 0, 7, 0, 10,
	0, 0, 0, 0, 0, 0, 0, 8,
	0, 10, 0, 0, 0, 0, 0, 0,
	0, 9, 10, 0, 11, 0, 12, 13,
	11, 0, 12, 13, 0, 0, 0, 0,
	2, 0, 1, 2, 3, 4, 5, 6,
	7, 8, 9, 10, 11, 12, 13, 0
};

static const signed char _sav_very_long_string_parse_cond_actions[] = {
	0, 0, 0, 1, 0, 7, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 7,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 7, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 7, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 7, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 7,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 7, 0, 0, 0, 0, 0, 0,
	0, 0, 7, 0, 10, 0, 3, 3,
	5, 0, 0, 0, 0, 0, 0, 0,
	1, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 3, 0, 0, 0
};

static const signed char _sav_very_long_string_parse_eof_trans[] = {
	90, 91, 92, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 0
};

static const int sav_very_long_string_parse_start = 1;

static const int sav_very_long_string_parse_en_main = 1;


#line 153 "src/spss/readstat_sav_parse.rl"


readstat_error_t sav_parse_very_long_string_record(void *data, int count, sav_ctx_t *ctx) {
	unsigned char *c_data = (unsigned char *)data;
	int var_count = count_vars(ctx);
	readstat_error_t retval = READSTAT_OK;
	
	char temp_key[8*4+1];
	unsigned int temp_val = 0;
	unsigned char *str_start = NULL;
	size_t str_len = 0;
	
	size_t error_buf_len = 1024 + count;
	char *error_buf = NULL;
	unsigned char *p = c_data;
	unsigned char *pe = c_data + count;
	unsigned char *eof = pe;
	
	varlookup_t *table = NULL;
	int cs;
	
	error_buf = readstat_malloc(error_buf_len);
	table = build_lookup_table(var_count, ctx);
	
	
#line 672 "src/spss/readstat_sav_parse.c"
	{
		cs = (int)sav_very_long_string_parse_start;
	}
	
#line 677 "src/spss/readstat_sav_parse.c"
	{
		int _klen;
		unsigned int _trans = 0;
		const unsigned char * _keys;
		const signed char * _acts;
		unsigned int _nacts;
		_resume: {}
		if ( p == pe && p != eof )
			goto _out;
		if ( p == eof ) {
			if ( _sav_very_long_string_parse_eof_trans[cs] > 0 ) {
				_trans = (unsigned int)_sav_very_long_string_parse_eof_trans[cs] - 1;
			}
		}
		else {
			_keys = ( _sav_very_long_string_parse_trans_keys + (_sav_very_long_string_parse_key_offsets[cs]));
			_trans = (unsigned int)_sav_very_long_string_parse_index_offsets[cs];
			
			_klen = (int)_sav_very_long_string_parse_single_lengths[cs];
			if ( _klen > 0 ) {
				const unsigned char *_lower = _keys;
				const unsigned char *_upper = _keys + _klen - 1;
				const unsigned char *_mid;
				while ( 1 ) {
					if ( _upper < _lower ) {
						_keys += _klen;
						_trans += (unsigned int)_klen;
						break;
					}
					
					_mid = _lower + ((_upper-_lower) >> 1);
					if ( ( (*( p))) < (*( _mid)) )
						_upper = _mid - 1;
					else if ( ( (*( p))) > (*( _mid)) )
						_lower = _mid + 1;
					else {
						_trans += (unsigned int)(_mid - _keys);
						goto _match;
					}
				}
			}
			
			_klen = (int)_sav_very_long_string_parse_range_lengths[cs];
			if ( _klen > 0 ) {
				const unsigned char *_lower = _keys;
				const unsigned char *_upper = _keys + (_klen<<1) - 2;
				const unsigned char *_mid;
				while ( 1 ) {
					if ( _upper < _lower ) {
						_trans += (unsigned int)_klen;
						break;
					}
					
					_mid = _lower + (((_upper-_lower) >> 1) & ~1);
					if ( ( (*( p))) < (*( _mid)) )
						_upper = _mid - 2;
					else if ( ( (*( p))) > (*( _mid + 1)) )
						_lower = _mid + 2;
					else {
						_trans += (unsigned int)((_mid - _keys)>>1);
						break;
					}
				}
			}
			
			_match: {}
		}
		cs = (int)_sav_very_long_string_parse_cond_targs[_trans];
		
		if ( _sav_very_long_string_parse_cond_actions[_trans] != 0 ) {
			
			_acts = ( _sav_very_long_string_parse_actions + (_sav_very_long_string_parse_cond_actions[_trans]));
			_nacts = (unsigned int)(*( _acts));
			_acts += 1;
			while ( _nacts > 0 ) {
				switch ( (*( _acts)) )
				{
					case 0:  {
						{
#line 13 "src/spss/readstat_sav_parse.rl"
							
							memcpy(temp_key, str_start, str_len);
							temp_key[str_len] = '\0';
						}
						
#line 763 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 1:  {
						{
#line 20 "src/spss/readstat_sav_parse.rl"
							str_start = p; }
						
#line 772 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 2:  {
						{
#line 20 "src/spss/readstat_sav_parse.rl"
							str_len = p - str_start; }
						
#line 781 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 3:  {
						{
#line 178 "src/spss/readstat_sav_parse.rl"
							
							varlookup_t *found = bsearch(temp_key, table, var_count, sizeof(varlookup_t), &compare_key_varlookup);
							if (found) {
								ctx->varinfo[found->index]->string_length = temp_val;
								ctx->varinfo[found->index]->write_format.width = temp_val;
								ctx->varinfo[found->index]->print_format.width = temp_val;
							}
						}
						
#line 797 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 4:  {
						{
#line 187 "src/spss/readstat_sav_parse.rl"
							
							if ((( (*( p)))) != '\0') {
								unsigned char digit = (( (*( p)))) - '0';
								if (temp_val <= (UINT_MAX - digit) / 10) {
									temp_val = 10 * temp_val + digit;
								} else {
									{p += 1; goto _out; }
								}
							}
						}
						
#line 815 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
					case 5:  {
						{
#line 198 "src/spss/readstat_sav_parse.rl"
							temp_val = 0; }
						
#line 824 "src/spss/readstat_sav_parse.c"
						
						break; 
					}
				}
				_nacts -= 1;
				_acts += 1;
			}
			
		}
		
		if ( p == eof ) {
			if ( cs >= 11 )
				goto _out;
		}
		else {
			if ( cs != 0 ) {
				p += 1;
				goto _resume;
			}
		}
		_out: {}
	}
	
#line 206 "src/spss/readstat_sav_parse.rl"
	
	
	if (cs < 
#line 852 "src/spss/readstat_sav_parse.c"
	11
#line 208 "src/spss/readstat_sav_parse.rl"
	|| p != pe) {
		if (ctx->handle.error) {
			snprintf(error_buf, error_buf_len, "Parsed %ld of %ld bytes. Remaining bytes: %.*s",
			(long)(p - c_data), (long)(pe - c_data), (int)(pe - p), p);
			ctx->handle.error(error_buf, ctx->user_ctx);
		}
		retval = READSTAT_ERROR_PARSE;
	}
	
	if (table)
		free(table);
	if (error_buf)
		free(error_buf);
	
	/* suppress warning */
	(void)sav_very_long_string_parse_en_main;
	
	return retval;
}
