
#line 1 "src/sas/readstat_xport_parse_format.rl"

#include "../readstat.h"
#include "readstat_xport.h"
#include "readstat_xport_parse_format.h"


#line 10 "src/sas/readstat_xport_parse_format.c"
static const char _xport_format_parse_actions[] = {
	0, 1, 0, 1, 1, 1, 3, 1, 
	4, 2, 2, 0, 3, 1, 2, 0
	
};

static const char _xport_format_parse_key_offsets[] = {
	0, 0, 7, 14, 23, 31, 31, 34, 
	42, 50, 58, 60, 62, 65, 73, 81
};

static const char _xport_format_parse_trans_keys[] = {
	95, 48, 57, 65, 90, 97, 122, 95, 
	48, 57, 65, 90, 97, 122, 36, 46, 
	95, 48, 57, 65, 90, 97, 122, 46, 
	95, 48, 57, 65, 90, 97, 122, 46, 
	48, 57, 46, 95, 48, 57, 65, 90, 
	97, 122, 46, 95, 48, 57, 65, 90, 
	97, 122, 46, 95, 48, 57, 65, 90, 
	97, 122, 48, 57, 48, 57, 46, 48, 
	57, 46, 95, 48, 57, 65, 90, 97, 
	122, 46, 95, 48, 57, 65, 90, 97, 
	122, 46, 95, 48, 57, 65, 90, 97, 
	122, 0
};

static const char _xport_format_parse_single_lengths[] = {
	0, 1, 1, 3, 2, 0, 1, 2, 
	2, 2, 0, 0, 1, 2, 2, 2
};

static const char _xport_format_parse_range_lengths[] = {
	0, 3, 3, 3, 3, 0, 1, 3, 
	3, 3, 1, 1, 1, 3, 3, 3
};

static const char _xport_format_parse_index_offsets[] = {
	0, 0, 5, 10, 17, 23, 24, 27, 
	33, 39, 45, 47, 49, 52, 58, 64
};

static const char _xport_format_parse_indicies[] = {
	2, 0, 2, 2, 1, 4, 3, 4, 
	4, 1, 5, 6, 8, 7, 8, 8, 
	1, 9, 11, 10, 11, 11, 1, 1, 
	12, 13, 1, 9, 0, 14, 0, 0, 
	1, 12, 2, 15, 2, 2, 1, 9, 
	2, 14, 2, 2, 1, 16, 1, 17, 
	1, 18, 19, 1, 6, 3, 20, 3, 
	3, 1, 18, 4, 21, 4, 4, 1, 
	6, 4, 20, 4, 4, 1, 0
};

static const char _xport_format_parse_trans_targs[] = {
	1, 0, 9, 2, 15, 4, 10, 12, 
	13, 5, 6, 7, 5, 6, 8, 8, 
	11, 11, 10, 12, 14, 14
};

static const char _xport_format_parse_trans_actions[] = {
	0, 0, 0, 0, 0, 0, 3, 12, 
	0, 3, 12, 0, 5, 1, 12, 1, 
	9, 1, 5, 1, 12, 1
};

static const char _xport_format_parse_eof_actions[] = {
	0, 0, 0, 3, 3, 0, 5, 3, 
	5, 3, 0, 7, 5, 3, 5, 3
};

static const int xport_format_parse_start = 3;

static const int xport_format_parse_en_main = 3;


#line 9 "src/sas/readstat_xport_parse_format.rl"


readstat_error_t xport_parse_format(const char *data, size_t len, xport_format_t *fmt,
        readstat_error_handler error_handler, void *user_ctx) {

    fmt->name[0] = '\0';
    fmt->width = 0;
    fmt->decimals = 0;

    readstat_error_t retval = READSTAT_OK;
    const char *p = data;
    const char *pe = p + len;
    const char *eof = pe;

    int cs;
    unsigned int temp_val = 0;
    size_t parsed_len = 0;

    
#line 106 "src/sas/readstat_xport_parse_format.c"
	{
	cs = xport_format_parse_start;
	}

#line 111 "src/sas/readstat_xport_parse_format.c"
	{
	int _klen;
	unsigned int _trans;
	const char *_acts;
	unsigned int _nacts;
	const char *_keys;

	if ( p == pe )
		goto _test_eof;
	if ( cs == 0 )
		goto _out;
_resume:
	_keys = _xport_format_parse_trans_keys + _xport_format_parse_key_offsets[cs];
	_trans = _xport_format_parse_index_offsets[cs];

	_klen = _xport_format_parse_single_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + _klen - 1;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + ((_upper-_lower) >> 1);
			if ( (*p) < *_mid )
				_upper = _mid - 1;
			else if ( (*p) > *_mid )
				_lower = _mid + 1;
			else {
				_trans += (unsigned int)(_mid - _keys);
				goto _match;
			}
		}
		_keys += _klen;
		_trans += _klen;
	}

	_klen = _xport_format_parse_range_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + (_klen<<1) - 2;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + (((_upper-_lower) >> 1) & ~1);
			if ( (*p) < _mid[0] )
				_upper = _mid - 2;
			else if ( (*p) > _mid[1] )
				_lower = _mid + 2;
			else {
				_trans += (unsigned int)((_mid - _keys)>>1);
				goto _match;
			}
		}
		_trans += _klen;
	}

_match:
	_trans = _xport_format_parse_indicies[_trans];
	cs = _xport_format_parse_trans_targs[_trans];

	if ( _xport_format_parse_trans_actions[_trans] == 0 )
		goto _again;

	_acts = _xport_format_parse_actions + _xport_format_parse_trans_actions[_trans];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 )
	{
		switch ( *_acts++ )
		{
	case 0:
#line 28 "src/sas/readstat_xport_parse_format.rl"
	{
            temp_val = 10 * temp_val + ((*p) - '0');
        }
	break;
	case 1:
#line 32 "src/sas/readstat_xport_parse_format.rl"
	{
            parsed_len = p - data;
            if (parsed_len < sizeof(fmt->name)) {
                memcpy(fmt->name, data, parsed_len);
                fmt->name[parsed_len] = '\0';
            }
        }
	break;
	case 2:
#line 40 "src/sas/readstat_xport_parse_format.rl"
	{ temp_val = 0; }
	break;
	case 3:
#line 47 "src/sas/readstat_xport_parse_format.rl"
	{ fmt->width = temp_val; }
	break;
#line 209 "src/sas/readstat_xport_parse_format.c"
		}
	}

_again:
	if ( cs == 0 )
		goto _out;
	if ( ++p != pe )
		goto _resume;
	_test_eof: {}
	if ( p == eof )
	{
	const char *__acts = _xport_format_parse_actions + _xport_format_parse_eof_actions[cs];
	unsigned int __nacts = (unsigned int) *__acts++;
	while ( __nacts-- > 0 ) {
		switch ( *__acts++ ) {
	case 1:
#line 32 "src/sas/readstat_xport_parse_format.rl"
	{
            parsed_len = p - data;
            if (parsed_len < sizeof(fmt->name)) {
                memcpy(fmt->name, data, parsed_len);
                fmt->name[parsed_len] = '\0';
            }
        }
	break;
	case 3:
#line 47 "src/sas/readstat_xport_parse_format.rl"
	{ fmt->width = temp_val; }
	break;
	case 4:
#line 48 "src/sas/readstat_xport_parse_format.rl"
	{ fmt->decimals = temp_val; }
	break;
#line 243 "src/sas/readstat_xport_parse_format.c"
		}
	}
	}

	_out: {}
	}

#line 54 "src/sas/readstat_xport_parse_format.rl"


    if (cs < 3|| p != pe || parsed_len + 1 > sizeof(fmt->name)) {
        char error_buf[1024];
        if (error_handler) {
            snprintf(error_buf, sizeof(error_buf), "Invalid format string (length=%d): %.*s", (int)len, (int)len, data);
            error_handler(error_buf, user_ctx);
        }
        retval = READSTAT_ERROR_BAD_FORMAT_STRING;
    }

    (void)xport_format_parse_en_main;

    return retval;
}
