#include <string.h>

int utf8str_codepoint_len( char const* s, int utf8len ) {
	int codepointLen = 0;
	unsigned char m4 = 128 + 64 + 32 + 16;
	unsigned char m3 = 128 + 64 + 32;
	unsigned char m2 = 128 + 64;
	for ( int i = 0; i < utf8len; ++ i, ++ codepointLen ) {
		char c = s[i];
		if ( ( c & m4 ) == m4 ) {
			i += 3;
		} else if ( ( c & m3 ) == m3 ) {
			i += 2;
		} else if ( ( c & m2 ) == m2 ) {
			i += 1;
		}
	}
	return ( codepointLen );
}

int context_len( char const* prefix ) {
	char const wb[] = " \t\n\r\v\f-=+*&^%$#@!,./?<>;:`~'\"[]{}()\\|";
	int i = (int)strlen( prefix ) - 1;
	int cl = 0;
	while ( i >= 0 ) {
		if ( strchr( wb, prefix[i] ) != NULL ) {
			break;
		}
		++ cl;
		-- i;
	}
	return ( cl );
}

