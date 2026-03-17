#line 1 "src/spss/readstat_spss_parse.rl"

#include <stdlib.h>
#include "../readstat.h"

#include "readstat_spss.h"
#include "readstat_spss_parse.h"


#line 11 "src/spss/readstat_spss_parse.c"
static const signed char _spss_format_parser_actions[] = {
	0, 1, 1, 1, 2, 1, 3, 1,
	4, 1, 5, 1, 6, 1, 7, 1,
	8, 1, 9, 1, 10, 1, 11, 1,
	12, 1, 13, 1, 14, 1, 15, 1,
	16, 1, 17, 1, 18, 1, 19, 1,
	20, 1, 21, 1, 22, 1, 23, 1,
	24, 1, 25, 1, 26, 1, 27, 1,
	28, 1, 29, 1, 30, 1, 31, 1,
	32, 1, 33, 1, 34, 1, 35, 1,
	36, 1, 37, 1, 38, 1, 39, 1,
	40, 2, 0, 1, 3, 4, 0, 1,
	3, 5, 0, 1, 3, 6, 0, 1,
	3, 7, 0, 1, 3, 8, 0, 1,
	3, 9, 0, 1, 3, 10, 0, 1,
	3, 11, 0, 1, 3, 12, 0, 1,
	3, 13, 0, 1, 3, 14, 0, 1,
	3, 15, 0, 1, 3, 16, 0, 1,
	3, 17, 0, 1, 3, 18, 0, 1,
	3, 19, 0, 1, 3, 20, 0, 1,
	3, 21, 0, 1, 3, 22, 0, 1,
	3, 23, 0, 1, 3, 24, 0, 1,
	3, 25, 0, 1, 3, 26, 0, 1,
	3, 27, 0, 1, 3, 28, 0, 1,
	3, 29, 0, 1, 3, 30, 0, 1,
	3, 31, 0, 1, 3, 32, 0, 1,
	3, 33, 0, 1, 3, 34, 0, 1,
	3, 35, 0, 1, 3, 36, 0, 1,
	3, 37, 0, 1, 3, 38, 0, 1,
	3, 39, 0, 1, 3, 40, 0, 1,
	0
};

static const short _spss_format_parser_key_offsets[] = {
	0, 0, 34, 36, 38, 40, 42, 44,
	46, 50, 60, 62, 64, 66, 72, 74,
	76, 78, 80, 82, 86, 88, 90, 92,
	94, 96, 98, 100, 102, 104, 106, 108,
	110, 112, 114, 118, 122, 124, 126, 128,
	130, 132, 134, 136, 138, 140, 142, 144,
	146, 148, 150, 152, 154, 156, 158, 160,
	162, 164, 166, 168, 172, 174, 176, 178,
	180, 182, 184, 186, 188, 194, 197, 199,
	201, 203, 205, 207, 209, 211, 213, 215,
	219, 221, 223, 225, 227, 231, 233, 235,
	237, 239, 241, 243, 245, 247, 255, 257,
	261, 263, 265, 267, 271, 273, 275, 277,
	279, 281, 283, 0
};

static const char _spss_format_parser_trans_keys[] = {
	65, 67, 68, 69, 70, 73, 74, 77,
	78, 80, 81, 82, 83, 84, 87, 89,
	90, 97, 99, 100, 101, 102, 105, 106,
	109, 110, 112, 113, 114, 115, 116, 119,
	121, 122, 48, 57, 65, 97, 84, 116,
	69, 101, 69, 101, 88, 120, 67, 79,
	99, 111, 65, 66, 67, 68, 69, 97,
	98, 99, 100, 101, 77, 109, 77, 109,
	65, 97, 65, 79, 84, 97, 111, 116,
	84, 116, 69, 101, 73, 105, 77, 109,
	69, 101, 76, 84, 108, 116, 76, 108,
	65, 97, 82, 114, 73, 105, 77, 109,
	69, 101, 65, 97, 84, 116, 69, 101,
	66, 98, 68, 100, 65, 97, 84, 116,
	69, 101, 79, 84, 111, 116, 78, 89,
	110, 121, 84, 116, 72, 104, 82, 114,
	73, 105, 77, 109, 69, 101, 84, 116,
	66, 98, 69, 101, 88, 120, 89, 121,
	82, 114, 66, 98, 69, 101, 88, 120,
	68, 100, 65, 97, 84, 116, 69, 101,
	73, 105, 77, 109, 69, 101, 75, 107,
	68, 89, 100, 121, 65, 97, 89, 121,
	82, 114, 77, 109, 68, 100, 72, 104,
	77, 109, 83, 115, 68, 72, 100, 104,
	48, 57, 46, 48, 57, 48, 57, 48,
	57, 48, 57, 48, 57, 48, 57, 48,
	57, 48, 57, 48, 57, 48, 57, 84,
	116, 48, 57, 48, 57, 48, 57, 48,
	57, 48, 57, 68, 100, 48, 57, 48,
	57, 48, 57, 48, 57, 48, 57, 48,
	57, 48, 57, 48, 57, 48, 57, 67,
	73, 75, 99, 105, 107, 48, 57, 48,
	57, 72, 104, 48, 57, 48, 57, 48,
	57, 48, 57, 72, 104, 48, 57, 48,
	57, 48, 57, 48, 57, 48, 57, 48,
	57, 48, 57, 48, 57, 0
};

static const signed char _spss_format_parser_single_lengths[] = {
	0, 34, 0, 2, 2, 2, 2, 2,
	4, 10, 2, 2, 2, 6, 2, 2,
	2, 2, 2, 4, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 4, 4, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 4, 2, 2, 2, 2,
	2, 2, 2, 2, 4, 1, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 2,
	0, 0, 0, 0, 2, 0, 0, 0,
	0, 0, 0, 0, 0, 6, 0, 2,
	0, 0, 0, 2, 0, 0, 0, 0,
	0, 0, 0, 0
};

static const signed char _spss_format_parser_range_lengths[] = {
	0, 0, 1, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 0
};

static const short _spss_format_parser_index_offsets[] = {
	0, 0, 35, 37, 40, 43, 46, 49,
	52, 57, 68, 71, 74, 77, 84, 87,
	90, 93, 96, 99, 104, 107, 110, 113,
	116, 119, 122, 125, 128, 131, 134, 137,
	140, 143, 146, 151, 156, 159, 162, 165,
	168, 171, 174, 177, 180, 183, 186, 189,
	192, 195, 198, 201, 204, 207, 210, 213,
	216, 219, 222, 225, 230, 233, 236, 239,
	242, 245, 248, 251, 254, 260, 263, 265,
	267, 269, 271, 273, 275, 277, 279, 281,
	285, 287, 289, 291, 293, 297, 299, 301,
	303, 305, 307, 309, 311, 313, 321, 323,
	327, 329, 331, 333, 337, 339, 341, 343,
	345, 347, 349, 0
};

static const signed char _spss_format_parser_cond_targs[] = {
	68, 8, 13, 84, 86, 29, 30, 34,
	92, 93, 46, 48, 51, 55, 58, 63,
	106, 68, 8, 13, 84, 86, 29, 30,
	34, 92, 93, 46, 48, 51, 55, 58,
	63, 106, 0, 70, 0, 4, 4, 0,
	5, 5, 0, 71, 71, 0, 7, 7,
	0, 72, 72, 0, 9, 10, 9, 10,
	0, 73, 74, 75, 76, 77, 73, 74,
	75, 76, 77, 0, 11, 11, 0, 12,
	12, 0, 78, 78, 0, 14, 19, 23,
	14, 19, 23, 0, 15, 15, 0, 79,
	79, 0, 17, 17, 0, 18, 18, 0,
	80, 80, 0, 20, 82, 20, 82, 0,
	21, 21, 0, 22, 22, 0, 81, 81,
	0, 24, 24, 0, 25, 25, 0, 83,
	83, 0, 27, 27, 0, 28, 28, 0,
	85, 85, 0, 87, 87, 0, 31, 31,
	0, 32, 32, 0, 33, 33, 0, 88,
	88, 0, 35, 39, 35, 39, 0, 36,
	38, 36, 38, 0, 37, 37, 0, 89,
	89, 0, 90, 90, 0, 40, 40, 0,
	41, 41, 0, 91, 91, 0, 94, 94,
	0, 95, 95, 0, 45, 45, 0, 96,
	96, 0, 47, 47, 0, 98, 98, 0,
	99, 99, 0, 50, 50, 0, 100, 100,
	0, 52, 52, 0, 53, 53, 0, 54,
	54, 0, 101, 101, 0, 56, 56, 0,
	57, 57, 0, 102, 102, 0, 59, 59,
	0, 60, 62, 60, 62, 0, 61, 61,
	0, 103, 103, 0, 104, 104, 0, 64,
	64, 0, 65, 65, 0, 66, 66, 0,
	67, 67, 0, 105, 105, 0, 3, 6,
	3, 6, 69, 0, 2, 69, 0, 70,
	0, 69, 0, 69, 0, 69, 0, 69,
	0, 69, 0, 69, 0, 69, 0, 69,
	0, 16, 16, 69, 0, 69, 0, 69,
	0, 69, 0, 69, 0, 26, 26, 69,
	0, 69, 0, 69, 0, 69, 0, 69,
	0, 69, 0, 69, 0, 69, 0, 69,
	0, 42, 43, 97, 42, 43, 97, 69,
	0, 69, 0, 44, 44, 69, 0, 69,
	0, 69, 0, 69, 0, 49, 49, 69,
	0, 69, 0, 69, 0, 69, 0, 69,
	0, 69, 0, 69, 0, 69, 0, 0,
	1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 13, 14, 15, 16,
	17, 18, 19, 20, 21, 22, 23, 24,
	25, 26, 27, 28, 29, 30, 31, 32,
	33, 34, 35, 36, 37, 38, 39, 40,
	41, 42, 43, 44, 45, 46, 47, 48,
	49, 50, 51, 52, 53, 54, 55, 56,
	57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 67, 68, 69, 70, 71, 72,
	73, 74, 75, 76, 77, 78, 79, 80,
	81, 82, 83, 84, 85, 86, 87, 88,
	89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100, 101, 102, 103, 104,
	105, 106, 0
};

static const short _spss_format_parser_cond_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 81, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 84, 0, 3, 1, 0, 1,
	0, 160, 0, 88, 0, 204, 0, 208,
	0, 212, 0, 216, 0, 220, 0, 92,
	0, 0, 0, 144, 0, 152, 0, 96,
	0, 200, 0, 168, 0, 0, 0, 140,
	0, 224, 0, 100, 0, 104, 0, 164,
	0, 180, 0, 184, 0, 172, 0, 136,
	0, 0, 0, 0, 0, 0, 0, 112,
	0, 196, 0, 0, 0, 116, 0, 108,
	0, 120, 0, 188, 0, 0, 0, 124,
	0, 128, 0, 228, 0, 148, 0, 176,
	0, 192, 0, 156, 0, 132, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 7, 3, 5, 45, 9,
	67, 69, 71, 73, 75, 11, 37, 41,
	13, 65, 49, 35, 77, 15, 17, 47,
	55, 57, 51, 33, 21, 63, 23, 19,
	25, 59, 27, 29, 79, 39, 53, 61,
	43, 31, 0
};

static const short _spss_format_parser_eof_trans[] = {
	352, 353, 354, 355, 356, 357, 358, 359,
	360, 361, 362, 363, 364, 365, 366, 367,
	368, 369, 370, 371, 372, 373, 374, 375,
	376, 377, 378, 379, 380, 381, 382, 383,
	384, 385, 386, 387, 388, 389, 390, 391,
	392, 393, 394, 395, 396, 397, 398, 399,
	400, 401, 402, 403, 404, 405, 406, 407,
	408, 409, 410, 411, 412, 413, 414, 415,
	416, 417, 418, 419, 420, 421, 422, 423,
	424, 425, 426, 427, 428, 429, 430, 431,
	432, 433, 434, 435, 436, 437, 438, 439,
	440, 441, 442, 443, 444, 445, 446, 447,
	448, 449, 450, 451, 452, 453, 454, 455,
	456, 457, 458, 0
};

static const int spss_format_parser_start = 1;

static const int spss_format_parser_en_main = 1;


#line 11 "src/spss/readstat_spss_parse.rl"


// For minimum width information see
// https://www.ibm.com/support/knowledgecenter/SSLVMB_sub/statistics_reference_project_ddita/spss/base/syn_date_and_time_date_time_formats.html
readstat_error_t spss_parse_format(const char *data, int count, spss_format_t *fmt) {
	unsigned char *p = (unsigned char *)data;
	unsigned char *pe = (unsigned char *)data + count;
	unsigned char *eof = pe;
	
	int cs;
	unsigned int integer = 0;
	
	
#line 310 "src/spss/readstat_spss_parse.c"
	{
		cs = (int)spss_format_parser_start;
	}
	
#line 315 "src/spss/readstat_spss_parse.c"
	{
		int _klen;
		unsigned int _trans = 0;
		const char * _keys;
		const signed char * _acts;
		unsigned int _nacts;
		_resume: {}
		if ( p == pe && p != eof )
			goto _out;
		if ( p == eof ) {
			if ( _spss_format_parser_eof_trans[cs] > 0 ) {
				_trans = (unsigned int)_spss_format_parser_eof_trans[cs] - 1;
			}
		}
		else {
			_keys = ( _spss_format_parser_trans_keys + (_spss_format_parser_key_offsets[cs]));
			_trans = (unsigned int)_spss_format_parser_index_offsets[cs];
			
			_klen = (int)_spss_format_parser_single_lengths[cs];
			if ( _klen > 0 ) {
				const char *_lower = _keys;
				const char *_upper = _keys + _klen - 1;
				const char *_mid;
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
			
			_klen = (int)_spss_format_parser_range_lengths[cs];
			if ( _klen > 0 ) {
				const char *_lower = _keys;
				const char *_upper = _keys + (_klen<<1) - 2;
				const char *_mid;
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
		cs = (int)_spss_format_parser_cond_targs[_trans];
		
		if ( _spss_format_parser_cond_actions[_trans] != 0 ) {
			
			_acts = ( _spss_format_parser_actions + (_spss_format_parser_cond_actions[_trans]));
			_nacts = (unsigned int)(*( _acts));
			_acts += 1;
			while ( _nacts > 0 ) {
				switch ( (*( _acts)) )
				{
					case 0:  {
						{
#line 24 "src/spss/readstat_spss_parse.rl"
							
							integer = 0;
						}
						
#line 400 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 1:  {
						{
#line 28 "src/spss/readstat_spss_parse.rl"
							
							integer = 10 * integer + ((( (*( p)))) - '0');
						}
						
#line 411 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 2:  {
						{
#line 32 "src/spss/readstat_spss_parse.rl"
							
							fmt->width = integer;
						}
						
#line 422 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 3:  {
						{
#line 36 "src/spss/readstat_spss_parse.rl"
							
							fmt->decimal_places = integer;
						}
						
#line 433 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 4:  {
						{
#line 40 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_A; }
						
#line 442 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 5:  {
						{
#line 41 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_AHEX; }
						
#line 451 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 6:  {
						{
#line 42 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_COMMA; }
						
#line 460 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 7:  {
						{
#line 43 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_DOLLAR; }
						
#line 469 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 8:  {
						{
#line 44 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_F; }
						
#line 478 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 9:  {
						{
#line 45 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_IB; }
						
#line 487 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 10:  {
						{
#line 46 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_PIBHEX; }
						
#line 496 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 11:  {
						{
#line 47 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_P; }
						
#line 505 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 12:  {
						{
#line 48 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_PIB; }
						
#line 514 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 13:  {
						{
#line 49 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_PK; }
						
#line 523 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 14:  {
						{
#line 50 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_RB; }
						
#line 532 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 15:  {
						{
#line 51 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_RBHEX; }
						
#line 541 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 16:  {
						{
#line 52 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_Z; }
						
#line 550 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 17:  {
						{
#line 53 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_N; }
						
#line 559 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 18:  {
						{
#line 54 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_E; }
						
#line 568 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 19:  {
						{
#line 55 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_DATE; fmt->width = 11; }
						
#line 577 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 20:  {
						{
#line 56 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_TIME; }
						
#line 586 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 21:  {
						{
#line 57 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_DATETIME; fmt->width = 20; }
						
#line 595 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 22:  {
						{
#line 58 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_YMDHMS; fmt->width = 19; }
						
#line 604 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 23:  {
						{
#line 59 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_ADATE; fmt->width = 10; }
						
#line 613 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 24:  {
						{
#line 60 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_JDATE; }
						
#line 622 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 25:  {
						{
#line 61 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_DTIME; fmt->width = 23; }
						
#line 631 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 26:  {
						{
#line 62 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_MTIME; }
						
#line 640 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 27:  {
						{
#line 63 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_WKDAY; }
						
#line 649 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 28:  {
						{
#line 64 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_MONTH; }
						
#line 658 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 29:  {
						{
#line 65 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_MOYR; }
						
#line 667 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 30:  {
						{
#line 66 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_QYR; }
						
#line 676 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 31:  {
						{
#line 67 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_WKYR; fmt->width = 10; }
						
#line 685 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 32:  {
						{
#line 68 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_PCT; }
						
#line 694 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 33:  {
						{
#line 69 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_DOT; }
						
#line 703 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 34:  {
						{
#line 70 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_CCA; }
						
#line 712 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 35:  {
						{
#line 71 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_CCB; }
						
#line 721 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 36:  {
						{
#line 72 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_CCC; }
						
#line 730 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 37:  {
						{
#line 73 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_CCD; }
						
#line 739 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 38:  {
						{
#line 74 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_CCE; }
						
#line 748 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 39:  {
						{
#line 75 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_EDATE; fmt->width = 10; }
						
#line 757 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
					case 40:  {
						{
#line 76 "src/spss/readstat_spss_parse.rl"
							fmt->type = SPSS_FORMAT_TYPE_SDATE; fmt->width = 10; }
						
#line 766 "src/spss/readstat_spss_parse.c"
						
						break; 
					}
				}
				_nacts -= 1;
				_acts += 1;
			}
			
		}
		
		if ( p == eof ) {
			if ( cs >= 68 )
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
	
#line 89 "src/spss/readstat_spss_parse.rl"
	
	
	/* suppress warning */
	(void)spss_format_parser_en_main;
	
	if (cs < 
#line 797 "src/spss/readstat_spss_parse.c"
	68
#line 94 "src/spss/readstat_spss_parse.rl"
	|| p != eof) {
		return READSTAT_ERROR_PARSE;
	}
	
	return READSTAT_OK;
}
