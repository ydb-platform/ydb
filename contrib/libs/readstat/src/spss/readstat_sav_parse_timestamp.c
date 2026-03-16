#line 1 "src/spss/readstat_sav_parse_timestamp.rl"

#include <time.h>
#include "../readstat.h"
#include "../readstat_iconv.h"

#include "readstat_sav.h"
#include "readstat_sav_parse_timestamp.h"


#line 12 "src/spss/readstat_sav_parse_timestamp.c"
static const signed char _sav_time_parse_actions[] = {
	0, 1, 0, 1, 2, 1, 3, 1,
	4, 1, 5, 2, 1, 0, 0
};

static const signed char _sav_time_parse_key_offsets[] = {
	0, 0, 3, 5, 6, 9, 11, 12,
	15, 17, 19, 21, 23, 0
};

static const char _sav_time_parse_trans_keys[] = {
	32, 48, 57, 48, 57, 58, 32, 48,
	57, 48, 57, 58, 32, 48, 57, 48,
	57, 48, 57, 48, 57, 48, 57, 0
};

static const signed char _sav_time_parse_single_lengths[] = {
	0, 1, 0, 1, 1, 0, 1, 1,
	0, 0, 0, 0, 0, 0
};

static const signed char _sav_time_parse_range_lengths[] = {
	0, 1, 1, 0, 1, 1, 0, 1,
	1, 1, 1, 1, 0, 0
};

static const signed char _sav_time_parse_index_offsets[] = {
	0, 0, 3, 5, 7, 10, 12, 14,
	17, 19, 21, 23, 25, 0
};

static const signed char _sav_time_parse_cond_targs[] = {
	2, 11, 0, 3, 0, 4, 0, 5,
	10, 0, 6, 0, 7, 0, 8, 9,
	0, 12, 0, 12, 0, 6, 0, 3,
	0, 0, 0, 1, 2, 3, 4, 5,
	6, 7, 8, 9, 10, 11, 12, 0
};

static const signed char _sav_time_parse_cond_actions[] = {
	0, 3, 0, 11, 0, 5, 0, 0,
	3, 0, 11, 0, 7, 0, 0, 3,
	0, 11, 0, 1, 0, 1, 0, 1,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 9, 0
};

static const signed char _sav_time_parse_eof_trans[] = {
	27, 28, 29, 30, 31, 32, 33, 34,
	35, 36, 37, 38, 39, 0
};

static const int sav_time_parse_start = 1;

static const int sav_time_parse_en_main = 1;


#line 12 "src/spss/readstat_sav_parse_timestamp.rl"


readstat_error_t sav_parse_time(const char *data, size_t len, struct tm *timestamp,
readstat_error_handler error_cb, void *user_ctx) {
	readstat_error_t retval = READSTAT_OK;
	char error_buf[8192];
	const char *p = data;
	const char *pe = p + len;
	const char *eof = pe;
	int cs;
	int temp_val = 0;
	
#line 83 "src/spss/readstat_sav_parse_timestamp.c"
	{
		cs = (int)sav_time_parse_start;
	}
	
#line 88 "src/spss/readstat_sav_parse_timestamp.c"
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
			if ( _sav_time_parse_eof_trans[cs] > 0 ) {
				_trans = (unsigned int)_sav_time_parse_eof_trans[cs] - 1;
			}
		}
		else {
			_keys = ( _sav_time_parse_trans_keys + (_sav_time_parse_key_offsets[cs]));
			_trans = (unsigned int)_sav_time_parse_index_offsets[cs];
			
			_klen = (int)_sav_time_parse_single_lengths[cs];
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
			
			_klen = (int)_sav_time_parse_range_lengths[cs];
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
		cs = (int)_sav_time_parse_cond_targs[_trans];
		
		if ( _sav_time_parse_cond_actions[_trans] != 0 ) {
			
			_acts = ( _sav_time_parse_actions + (_sav_time_parse_cond_actions[_trans]));
			_nacts = (unsigned int)(*( _acts));
			_acts += 1;
			while ( _nacts > 0 ) {
				switch ( (*( _acts)) )
				{
					case 0:  {
						{
#line 24 "src/spss/readstat_sav_parse_timestamp.rl"
							
							temp_val = 10 * temp_val + ((( (*( p)))) - '0');
						}
						
#line 173 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 1:  {
						{
#line 28 "src/spss/readstat_sav_parse_timestamp.rl"
							temp_val = 0; }
						
#line 182 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 2:  {
						{
#line 28 "src/spss/readstat_sav_parse_timestamp.rl"
							temp_val = (( (*( p)))) - '0'; }
						
#line 191 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 3:  {
						{
#line 30 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_hour = temp_val; }
						
#line 200 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 4:  {
						{
#line 32 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_min = temp_val; }
						
#line 209 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 5:  {
						{
#line 34 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_sec = temp_val; }
						
#line 218 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
				}
				_nacts -= 1;
				_acts += 1;
			}
			
		}
		
		if ( p == eof ) {
			if ( cs >= 12 )
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
	
#line 40 "src/spss/readstat_sav_parse_timestamp.rl"
	
	
	if (cs < 
#line 246 "src/spss/readstat_sav_parse_timestamp.c"
	12
#line 42 "src/spss/readstat_sav_parse_timestamp.rl"
	|| p != pe) {
		if (error_cb) {
			snprintf(error_buf, sizeof(error_buf),
			"Invalid time string (length=%d): %.*s", (int)len, (int)len, data);
			error_cb(error_buf, user_ctx);
		}
		retval = READSTAT_ERROR_BAD_TIMESTAMP_STRING;
	}
	
	(void)sav_time_parse_en_main;
	
	return retval;
}


#line 264 "src/spss/readstat_sav_parse_timestamp.c"
static const signed char _sav_date_parse_actions[] = {
	0, 1, 0, 1, 1, 1, 3, 1,
	4, 1, 5, 1, 6, 1, 7, 1,
	8, 1, 9, 1, 10, 1, 11, 1,
	12, 1, 13, 1, 14, 1, 15, 2,
	2, 0, 0
};

static const signed char _sav_date_parse_key_offsets[] = {
	0, 0, 3, 6, 8, 16, 20, 21,
	23, 26, 29, 30, 32, 33, 34, 36,
	37, 39, 40, 42, 43, 45, 46, 50,
	51, 53, 55, 57, 59, 60, 62, 64,
	66, 68, 70, 72, 74, 75, 77, 78,
	80, 81, 83, 84, 86, 87, 89, 90,
	0
};

static const char _sav_date_parse_trans_keys[] = {
	32, 48, 57, 32, 48, 57, 32, 45,
	65, 68, 70, 74, 77, 78, 79, 83,
	80, 85, 112, 117, 82, 32, 45, 32,
	48, 57, 32, 48, 57, 71, 32, 45,
	114, 103, 69, 101, 67, 32, 45, 99,
	69, 101, 66, 32, 45, 98, 65, 85,
	97, 117, 78, 32, 45, 76, 78, 32,
	45, 32, 45, 110, 108, 110, 65, 97,
	82, 89, 32, 45, 32, 45, 114, 121,
	79, 111, 86, 32, 45, 118, 67, 99,
	84, 32, 45, 116, 69, 101, 80, 32,
	45, 112, 0
};

static const signed char _sav_date_parse_single_lengths[] = {
	0, 1, 1, 2, 8, 4, 1, 2,
	1, 1, 1, 2, 1, 1, 2, 1,
	2, 1, 2, 1, 2, 1, 4, 1,
	2, 2, 2, 2, 1, 2, 2, 2,
	2, 2, 2, 2, 1, 2, 1, 2,
	1, 2, 1, 2, 1, 2, 1, 0,
	0
};

static const signed char _sav_date_parse_range_lengths[] = {
	0, 1, 1, 0, 0, 0, 0, 0,
	1, 1, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0
};

static const short _sav_date_parse_index_offsets[] = {
	0, 0, 3, 6, 9, 18, 23, 25,
	28, 31, 34, 36, 39, 41, 43, 46,
	48, 51, 53, 56, 58, 61, 63, 68,
	70, 73, 76, 79, 82, 84, 87, 90,
	93, 96, 99, 102, 105, 107, 110, 112,
	115, 117, 120, 122, 125, 127, 130, 132,
	0
};

static const signed char _sav_date_parse_cond_targs[] = {
	2, 2, 0, 3, 3, 0, 4, 4,
	0, 5, 14, 18, 22, 30, 35, 39,
	43, 0, 6, 10, 12, 13, 0, 7,
	0, 8, 8, 0, 9, 9, 0, 47,
	47, 0, 11, 0, 8, 8, 0, 7,
	0, 11, 0, 15, 17, 0, 16, 0,
	8, 8, 0, 16, 0, 19, 21, 0,
	20, 0, 8, 8, 0, 20, 0, 23,
	25, 28, 29, 0, 24, 0, 8, 8,
	0, 26, 27, 0, 8, 8, 0, 8,
	8, 0, 24, 0, 26, 27, 0, 31,
	34, 0, 32, 33, 0, 8, 8, 0,
	8, 8, 0, 32, 33, 0, 36, 38,
	0, 37, 0, 8, 8, 0, 37, 0,
	40, 42, 0, 41, 0, 8, 8, 0,
	41, 0, 44, 46, 0, 45, 0, 8,
	8, 0, 45, 0, 0, 0, 1, 2,
	3, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18,
	19, 20, 21, 22, 23, 24, 25, 26,
	27, 28, 29, 30, 31, 32, 33, 34,
	35, 36, 37, 38, 39, 40, 41, 42,
	43, 44, 45, 46, 47, 0
};

static const signed char _sav_date_parse_cond_actions[] = {
	31, 31, 0, 1, 1, 0, 5, 5,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 13, 13, 0, 31, 31, 0, 1,
	1, 0, 0, 0, 21, 21, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	29, 29, 0, 0, 0, 0, 0, 0,
	0, 0, 9, 9, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 7, 7,
	0, 0, 0, 0, 19, 19, 0, 17,
	17, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 11, 11, 0,
	15, 15, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 27, 27, 0, 0, 0,
	0, 0, 0, 0, 0, 25, 25, 0,
	0, 0, 0, 0, 0, 0, 0, 23,
	23, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 3, 0
};

static const short _sav_date_parse_eof_trans[] = {
	134, 135, 136, 137, 138, 139, 140, 141,
	142, 143, 144, 145, 146, 147, 148, 149,
	150, 151, 152, 153, 154, 155, 156, 157,
	158, 159, 160, 161, 162, 163, 164, 165,
	166, 167, 168, 169, 170, 171, 172, 173,
	174, 175, 176, 177, 178, 179, 180, 181,
	0
};

static const int sav_date_parse_start = 1;

static const int sav_date_parse_en_main = 1;


#line 59 "src/spss/readstat_sav_parse_timestamp.rl"


readstat_error_t sav_parse_date(const char *data, size_t len, struct tm *timestamp,
readstat_error_handler error_cb, void *user_ctx) {
	readstat_error_t retval = READSTAT_OK;
	char error_buf[8192];
	const char *p = data;
	const char *pe = p + len;
	const char *eof = pe;
	int cs;
	int temp_val = 0;
	
#line 408 "src/spss/readstat_sav_parse_timestamp.c"
	{
		cs = (int)sav_date_parse_start;
	}
	
#line 413 "src/spss/readstat_sav_parse_timestamp.c"
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
			if ( _sav_date_parse_eof_trans[cs] > 0 ) {
				_trans = (unsigned int)_sav_date_parse_eof_trans[cs] - 1;
			}
		}
		else {
			_keys = ( _sav_date_parse_trans_keys + (_sav_date_parse_key_offsets[cs]));
			_trans = (unsigned int)_sav_date_parse_index_offsets[cs];
			
			_klen = (int)_sav_date_parse_single_lengths[cs];
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
			
			_klen = (int)_sav_date_parse_range_lengths[cs];
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
		cs = (int)_sav_date_parse_cond_targs[_trans];
		
		if ( _sav_date_parse_cond_actions[_trans] != 0 ) {
			
			_acts = ( _sav_date_parse_actions + (_sav_date_parse_cond_actions[_trans]));
			_nacts = (unsigned int)(*( _acts));
			_acts += 1;
			while ( _nacts > 0 ) {
				switch ( (*( _acts)) )
				{
					case 0:  {
						{
#line 71 "src/spss/readstat_sav_parse_timestamp.rl"
							
							char digit = ((( (*( p)))) - '0');
							if (digit >= 0 && digit <= 9) {
								temp_val = 10 * temp_val + digit;
							}
						}
						
#line 501 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 1:  {
						{
#line 78 "src/spss/readstat_sav_parse_timestamp.rl"
							
							if (temp_val < 70) {
								timestamp->tm_year = 100 + temp_val;
							} else {
								timestamp->tm_year = temp_val;
							}
						}
						
#line 516 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 2:  {
						{
#line 87 "src/spss/readstat_sav_parse_timestamp.rl"
							temp_val = 0; }
						
#line 525 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 3:  {
						{
#line 89 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mday = temp_val; }
						
#line 534 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 4:  {
						{
#line 94 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 0; }
						
#line 543 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 5:  {
						{
#line 95 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 1; }
						
#line 552 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 6:  {
						{
#line 96 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 2; }
						
#line 561 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 7:  {
						{
#line 97 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 3; }
						
#line 570 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 8:  {
						{
#line 98 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 4; }
						
#line 579 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 9:  {
						{
#line 99 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 5; }
						
#line 588 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 10:  {
						{
#line 100 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 6; }
						
#line 597 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 11:  {
						{
#line 101 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 7; }
						
#line 606 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 12:  {
						{
#line 102 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 8; }
						
#line 615 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 13:  {
						{
#line 103 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 9; }
						
#line 624 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 14:  {
						{
#line 104 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 10; }
						
#line 633 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
					case 15:  {
						{
#line 105 "src/spss/readstat_sav_parse_timestamp.rl"
							timestamp->tm_mon = 11; }
						
#line 642 "src/spss/readstat_sav_parse_timestamp.c"
						
						break; 
					}
				}
				_nacts -= 1;
				_acts += 1;
			}
			
		}
		
		if ( p == eof ) {
			if ( cs >= 47 )
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
	
#line 112 "src/spss/readstat_sav_parse_timestamp.rl"
	
	
	if (cs < 
#line 670 "src/spss/readstat_sav_parse_timestamp.c"
	47
#line 114 "src/spss/readstat_sav_parse_timestamp.rl"
	|| p != pe) {
		if (error_cb) {
			snprintf(error_buf, sizeof(error_buf),
			"Invalid date string (length=%d): %.*s", (int)len, (int)len, data);
			error_cb(error_buf, user_ctx);
		}
		retval = READSTAT_ERROR_BAD_TIMESTAMP_STRING;
	}
	
	(void)sav_date_parse_en_main;
	
	return retval;
}
