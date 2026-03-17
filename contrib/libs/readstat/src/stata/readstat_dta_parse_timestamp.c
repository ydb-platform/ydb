#line 1 "src/stata/readstat_dta_parse_timestamp.rl"

#include <time.h>
#include "../readstat.h"
#include "readstat_dta_parse_timestamp.h"


#line 9 "src/stata/readstat_dta_parse_timestamp.c"
static const signed char _dta_timestamp_parse_actions[] = {
	0, 1, 0, 1, 2, 1, 3, 1,
	4, 1, 5, 1, 6, 1, 7, 1,
	8, 1, 9, 1, 10, 1, 11, 1,
	12, 1, 13, 1, 14, 1, 15, 1,
	16, 1, 17, 2, 1, 0, 0
};

static const signed char _dta_timestamp_parse_key_offsets[] = {
	0, 0, 3, 5, 8, 26, 34, 36,
	37, 39, 42, 45, 48, 50, 52, 53,
	55, 59, 63, 64, 66, 68, 70, 71,
	73, 75, 76, 80, 82, 86, 87, 88,
	90, 96, 97, 98, 100, 102, 103, 107,
	109, 110, 112, 114, 115, 0
};

static const char _dta_timestamp_parse_trans_keys[] = {
	32, 48, 57, 48, 57, 32, 48, 57,
	65, 68, 69, 70, 74, 77, 78, 79,
	83, 97, 100, 101, 102, 106, 109, 110,
	111, 115, 66, 71, 80, 85, 98, 103,
	112, 117, 82, 114, 32, 48, 57, 32,
	48, 57, 32, 48, 57, 58, 48, 57,
	48, 57, 79, 111, 32, 71, 103, 69,
	73, 101, 105, 67, 90, 99, 122, 32,
	67, 99, 78, 110, 69, 101, 32, 69,
	101, 66, 98, 32, 65, 85, 97, 117,
	78, 110, 76, 78, 108, 110, 32, 32,
	65, 97, 73, 82, 89, 105, 114, 121,
	32, 32, 79, 111, 86, 118, 32, 67,
	75, 99, 107, 84, 116, 32, 69, 101,
	80, 112, 32, 48, 57, 0
};

static const signed char _dta_timestamp_parse_single_lengths[] = {
	0, 1, 0, 1, 18, 8, 2, 1,
	0, 1, 1, 1, 0, 2, 1, 2,
	4, 4, 1, 2, 2, 2, 1, 2,
	2, 1, 4, 2, 4, 1, 1, 2,
	6, 1, 1, 2, 2, 1, 4, 2,
	1, 2, 2, 1, 0, 0
};

static const signed char _dta_timestamp_parse_range_lengths[] = {
	0, 1, 1, 1, 0, 0, 0, 0,
	1, 1, 1, 1, 1, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 1, 0
};

static const short _dta_timestamp_parse_index_offsets[] = {
	0, 0, 3, 5, 8, 27, 36, 39,
	41, 43, 46, 49, 52, 54, 57, 59,
	62, 67, 72, 74, 77, 80, 83, 85,
	88, 91, 93, 98, 101, 106, 108, 110,
	113, 120, 122, 124, 127, 130, 132, 137,
	140, 142, 145, 148, 150, 0
};

static const signed char _dta_timestamp_parse_cond_targs[] = {
	2, 3, 0, 3, 0, 4, 3, 0,
	5, 16, 20, 23, 26, 31, 35, 38,
	41, 5, 16, 20, 23, 26, 31, 35,
	38, 41, 0, 6, 13, 6, 15, 6,
	13, 6, 15, 0, 7, 7, 0, 8,
	0, 9, 0, 10, 9, 0, 10, 11,
	0, 12, 11, 0, 44, 0, 14, 14,
	0, 8, 0, 14, 14, 0, 17, 19,
	17, 19, 0, 18, 18, 18, 18, 0,
	8, 0, 18, 18, 0, 21, 21, 0,
	22, 22, 0, 8, 0, 24, 24, 0,
	25, 25, 0, 8, 0, 27, 28, 27,
	28, 0, 22, 22, 0, 29, 30, 29,
	30, 0, 8, 0, 8, 0, 32, 32,
	0, 33, 34, 33, 33, 34, 33, 0,
	8, 0, 8, 0, 36, 36, 0, 37,
	37, 0, 8, 0, 39, 39, 39, 39,
	0, 40, 40, 0, 8, 0, 42, 42,
	0, 43, 43, 0, 8, 0, 44, 0,
	0, 1, 2, 3, 4, 5, 6, 7,
	8, 9, 10, 11, 12, 13, 14, 15,
	16, 17, 18, 19, 20, 21, 22, 23,
	24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39,
	40, 41, 42, 43, 44, 0
};

static const signed char _dta_timestamp_parse_cond_actions[] = {
	0, 35, 0, 35, 0, 3, 1, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 11,
	0, 35, 0, 29, 1, 0, 0, 35,
	0, 31, 1, 0, 35, 0, 0, 0,
	0, 19, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	27, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 5, 0, 0, 0, 0,
	0, 0, 0, 7, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 17, 0, 15, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	13, 0, 9, 0, 0, 0, 0, 0,
	0, 0, 25, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 23, 0, 0, 0,
	0, 0, 0, 0, 21, 0, 1, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 33, 0
};

static const short _dta_timestamp_parse_eof_trans[] = {
	153, 154, 155, 156, 157, 158, 159, 160,
	161, 162, 163, 164, 165, 166, 167, 168,
	169, 170, 171, 172, 173, 174, 175, 176,
	177, 178, 179, 180, 181, 182, 183, 184,
	185, 186, 187, 188, 189, 190, 191, 192,
	193, 194, 195, 196, 197, 0
};

static const int dta_timestamp_parse_start = 1;

static const int dta_timestamp_parse_en_main = 1;


#line 9 "src/stata/readstat_dta_parse_timestamp.rl"


readstat_error_t dta_parse_timestamp(const char *data, size_t len, struct tm *timestamp,
readstat_error_handler error_handler, void *user_ctx) {
	readstat_error_t retval = READSTAT_OK;
	const char *p = data;
	const char *pe = p + len;
	const char *eof = pe;
	int cs;
	unsigned int temp_val = 0;
	
#line 154 "src/stata/readstat_dta_parse_timestamp.c"
	{
		cs = (int)dta_timestamp_parse_start;
	}
	
#line 159 "src/stata/readstat_dta_parse_timestamp.c"
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
			if ( _dta_timestamp_parse_eof_trans[cs] > 0 ) {
				_trans = (unsigned int)_dta_timestamp_parse_eof_trans[cs] - 1;
			}
		}
		else {
			_keys = ( _dta_timestamp_parse_trans_keys + (_dta_timestamp_parse_key_offsets[cs]));
			_trans = (unsigned int)_dta_timestamp_parse_index_offsets[cs];
			
			_klen = (int)_dta_timestamp_parse_single_lengths[cs];
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
			
			_klen = (int)_dta_timestamp_parse_range_lengths[cs];
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
		cs = (int)_dta_timestamp_parse_cond_targs[_trans];
		
		if ( _dta_timestamp_parse_cond_actions[_trans] != 0 ) {
			
			_acts = ( _dta_timestamp_parse_actions + (_dta_timestamp_parse_cond_actions[_trans]));
			_nacts = (unsigned int)(*( _acts));
			_acts += 1;
			while ( _nacts > 0 ) {
				switch ( (*( _acts)) )
				{
					case 0:  {
						{
#line 20 "src/stata/readstat_dta_parse_timestamp.rl"
							
							temp_val = 10 * temp_val + ((( (*( p)))) - '0');
						}
						
#line 244 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 1:  {
						{
#line 24 "src/stata/readstat_dta_parse_timestamp.rl"
							temp_val = 0; }
						
#line 253 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 2:  {
						{
#line 26 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mday = temp_val; }
						
#line 262 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 3:  {
						{
#line 29 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 0; }
						
#line 271 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 4:  {
						{
#line 30 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 1; }
						
#line 280 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 5:  {
						{
#line 31 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 2; }
						
#line 289 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 6:  {
						{
#line 32 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 3; }
						
#line 298 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 7:  {
						{
#line 33 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 4; }
						
#line 307 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 8:  {
						{
#line 34 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 5; }
						
#line 316 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 9:  {
						{
#line 35 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 6; }
						
#line 325 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 10:  {
						{
#line 36 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 7; }
						
#line 334 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 11:  {
						{
#line 37 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 8; }
						
#line 343 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 12:  {
						{
#line 38 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 9; }
						
#line 352 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 13:  {
						{
#line 39 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 10; }
						
#line 361 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 14:  {
						{
#line 40 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_mon = 11; }
						
#line 370 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 15:  {
						{
#line 42 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_year = temp_val - 1900; }
						
#line 379 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 16:  {
						{
#line 44 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_hour = temp_val; }
						
#line 388 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
					case 17:  {
						{
#line 46 "src/stata/readstat_dta_parse_timestamp.rl"
							timestamp->tm_min = temp_val; }
						
#line 397 "src/stata/readstat_dta_parse_timestamp.c"
						
						break; 
					}
				}
				_nacts -= 1;
				_acts += 1;
			}
			
		}
		
		if ( p == eof ) {
			if ( cs >= 44 )
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
	
#line 52 "src/stata/readstat_dta_parse_timestamp.rl"
	
	
	if (cs < 
#line 425 "src/stata/readstat_dta_parse_timestamp.c"
	44
#line 54 "src/stata/readstat_dta_parse_timestamp.rl"
	|| p != pe) {
		char error_buf[1024];
		if (error_handler) {
			snprintf(error_buf, sizeof(error_buf), "Invalid timestamp string (length=%d): %.*s", (int)len, (int)len, data);
			error_handler(error_buf, user_ctx);
		}
		retval = READSTAT_ERROR_BAD_TIMESTAMP_STRING;
	}
	
	(void)dta_timestamp_parse_en_main;
	
	return retval;
}
