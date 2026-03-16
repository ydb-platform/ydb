#line 1 "src/spss/readstat_por_parse.rl"
#include <sys/types.h>

#include "../readstat.h"
#include "readstat_por_parse.h"


#line 9 "src/spss/readstat_por_parse.c"
static const signed char _por_field_parse_actions[] = {
	0, 1, 0, 1, 1, 1, 5, 1,
	8, 1, 9, 1, 10, 2, 2, 0,
	2, 3, 1, 2, 5, 10, 2, 7,
	10, 3, 4, 2, 0, 3, 6, 2,
	0, 0
};

static const signed char _por_field_parse_key_offsets[] = {
	0, 0, 8, 9, 14, 18, 23, 31,
	35, 40, 44, 48, 55, 0
};

static const char _por_field_parse_trans_keys[] = {
	32, 42, 45, 46, 48, 57, 65, 84,
	46, 46, 48, 57, 65, 84, 48, 57,
	65, 84, 47, 48, 57, 65, 84, 43,
	45, 46, 47, 48, 57, 65, 84, 48,
	57, 65, 84, 47, 48, 57, 65, 84,
	48, 57, 65, 84, 48, 57, 65, 84,
	43, 45, 47, 48, 57, 65, 84, 0
};

static const signed char _por_field_parse_single_lengths[] = {
	0, 4, 1, 1, 0, 1, 4, 0,
	1, 0, 0, 3, 0, 0
};

static const signed char _por_field_parse_range_lengths[] = {
	0, 2, 0, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 0, 0
};

static const signed char _por_field_parse_index_offsets[] = {
	0, 0, 7, 9, 13, 16, 20, 27,
	30, 34, 37, 40, 46, 0
};

static const signed char _por_field_parse_cond_targs[] = {
	1, 2, 3, 4, 6, 6, 0, 12,
	0, 4, 6, 6, 0, 5, 5, 0,
	12, 5, 5, 0, 7, 9, 10, 12,
	6, 6, 0, 8, 8, 0, 12, 8,
	8, 0, 8, 8, 0, 11, 11, 0,
	7, 9, 12, 11, 11, 0, 0, 0,
	1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 0
};

static const signed char _por_field_parse_cond_actions[] = {
	0, 9, 0, 0, 13, 13, 0, 11,
	0, 7, 25, 25, 0, 16, 16, 0,
	11, 3, 3, 0, 5, 5, 5, 19,
	1, 1, 0, 13, 13, 0, 22, 1,
	1, 0, 29, 29, 0, 16, 16, 0,
	0, 0, 11, 3, 3, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0
};

static const int por_field_parse_start = 1;

static const int por_field_parse_en_main = 1;


#line 9 "src/spss/readstat_por_parse.rl"


ssize_t readstat_por_parse_double(const char *data, size_t len, double *result, 
readstat_error_handler error_cb, void *user_ctx) {
	ssize_t retval = 0;
	double val = 0.0;
	double denom = 30.0;
	double temp_frac = 0.0;
	double num = 0.0;
	double exp = 0.0;
	
	double temp_val = 0.0;
	
	const unsigned char *p = (const unsigned char *)data;
	const unsigned char *pe = p + len;
	
	int cs;
	int is_negative = 0, exp_is_negative = 0;
	int success = 0;
	
	
#line 97 "src/spss/readstat_por_parse.c"
	{
		cs = (int)por_field_parse_start;
	}
	
#line 102 "src/spss/readstat_por_parse.c"
	{
		int _klen;
		unsigned int _trans = 0;
		const char * _keys;
		const signed char * _acts;
		unsigned int _nacts;
		_resume: {}
		if ( p == pe )
			goto _out;
		_keys = ( _por_field_parse_trans_keys + (_por_field_parse_key_offsets[cs]));
		_trans = (unsigned int)_por_field_parse_index_offsets[cs];
		
		_klen = (int)_por_field_parse_single_lengths[cs];
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
		
		_klen = (int)_por_field_parse_range_lengths[cs];
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
		cs = (int)_por_field_parse_cond_targs[_trans];
		
		if ( _por_field_parse_cond_actions[_trans] != 0 ) {
			
			_acts = ( _por_field_parse_actions + (_por_field_parse_cond_actions[_trans]));
			_nacts = (unsigned int)(*( _acts));
			_acts += 1;
			while ( _nacts > 0 ) {
				switch ( (*( _acts)) )
				{
					case 0:  {
						{
#line 30 "src/spss/readstat_por_parse.rl"
							
							if ((( (*( p)))) >= '0' && (( (*( p)))) <= '9') {
								temp_val = 30 * temp_val + ((( (*( p)))) - '0');
							} else if ((( (*( p)))) >= 'A' && (( (*( p)))) <= 'T') {
								temp_val = 30 * temp_val + (10 + (( (*( p)))) - 'A');
							}
						}
						
#line 184 "src/spss/readstat_por_parse.c"
						
						break; 
					}
					case 1:  {
						{
#line 38 "src/spss/readstat_por_parse.rl"
							
							if ((( (*( p)))) >= '0' && (( (*( p)))) <= '9') {
								temp_frac += ((( (*( p)))) - '0') / denom;
							} else if ((( (*( p)))) >= 'A' && (( (*( p)))) <= 'T') {
								temp_frac += (10 + (( (*( p)))) - 'A') / denom;
							}
							denom *= 30.0;
						}
						
#line 200 "src/spss/readstat_por_parse.c"
						
						break; 
					}
					case 2:  {
						{
#line 47 "src/spss/readstat_por_parse.rl"
							temp_val = 0; }
						
#line 209 "src/spss/readstat_por_parse.c"
						
						break; 
					}
					case 3:  {
						{
#line 49 "src/spss/readstat_por_parse.rl"
							temp_frac = 0.0; }
						
#line 218 "src/spss/readstat_por_parse.c"
						
						break; 
					}
					case 4:  {
						{
#line 53 "src/spss/readstat_por_parse.rl"
							is_negative = 1; }
						
#line 227 "src/spss/readstat_por_parse.c"
						
						break; 
					}
					case 5:  {
						{
#line 53 "src/spss/readstat_por_parse.rl"
							num = temp_val; }
						
#line 236 "src/spss/readstat_por_parse.c"
						
						break; 
					}
					case 6:  {
						{
#line 54 "src/spss/readstat_por_parse.rl"
							exp_is_negative = 1; }
						
#line 245 "src/spss/readstat_por_parse.c"
						
						break; 
					}
					case 7:  {
						{
#line 54 "src/spss/readstat_por_parse.rl"
							exp = temp_val; }
						
#line 254 "src/spss/readstat_por_parse.c"
						
						break; 
					}
					case 8:  {
						{
#line 56 "src/spss/readstat_por_parse.rl"
							is_negative = 1; }
						
#line 263 "src/spss/readstat_por_parse.c"
						
						break; 
					}
					case 9:  {
						{
#line 58 "src/spss/readstat_por_parse.rl"
							val = NAN; }
						
#line 272 "src/spss/readstat_por_parse.c"
						
						break; 
					}
					case 10:  {
						{
#line 60 "src/spss/readstat_por_parse.rl"
							success = 1; {p += 1; goto _out; } }
						
#line 281 "src/spss/readstat_por_parse.c"
						
						break; 
					}
				}
				_nacts -= 1;
				_acts += 1;
			}
			
		}
		
		if ( cs != 0 ) {
			p += 1;
			goto _resume;
		}
		_out: {}
	}
	
#line 64 "src/spss/readstat_por_parse.rl"
	
	
	if (!isnan(val)) {
		val = 1.0 * num + temp_frac;
		if (exp_is_negative)
			exp *= -1;
		if (exp) {
			val *= pow(30.0, exp);
		}
		if (is_negative)
			val *= -1;
	}
	
	if (!success) {
		retval = -1;
		if (error_cb) {
			char error_buf[1024];
			snprintf(error_buf, sizeof(error_buf), "Read bytes: %ld   String: %.*s  Ending state: %d",
			(long)(p - (const unsigned char *)data), (int)len, data, cs);
			error_cb(error_buf, user_ctx);
		}
	}
	
	if (retval == 0) {
		if (result)
			*result = val;
		
		retval = (p - (const unsigned char *)data);
	}
	
	/* suppress warning */
	(void)por_field_parse_en_main;
	
	return retval;
}
