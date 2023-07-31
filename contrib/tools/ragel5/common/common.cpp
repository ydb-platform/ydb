/*
 *  Copyright 2006-2007 Adrian Thurston <thurston@cs.queensu.ca>
 */

/*  This file is part of Ragel.
 *
 *  Ragel is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 * 
 *  Ragel is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with Ragel; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA 
 */

#include "pcheck.h"
#include "common.h"
#include <string.h>
#include <assert.h>

#ifdef _WIN32
#include <malloc.h>
#else
#include <alloca.h>
#endif

HostType hostTypesC[] =
{
	{ "char",     0,       true,   CHAR_MIN,  CHAR_MAX,   sizeof(char) },
	{ "unsigned", "char",  false,  0,         UCHAR_MAX,  sizeof(unsigned char) },
	{ "short",    0,       true,   SHRT_MIN,  SHRT_MAX,   sizeof(short) },
	{ "unsigned", "short", false,  0,         USHRT_MAX,  sizeof(unsigned short) },
	{ "int",      0,       true,   INT_MIN,   INT_MAX,    sizeof(int) },
	{ "unsigned", "int",   false,  0,         UINT_MAX,   sizeof(unsigned int) },
	{ "long",     0,       true,   LONG_MIN,  LONG_MAX,   sizeof(long) },
	{ "unsigned", "long",  false,  0,         (long long)ULONG_MAX,  sizeof(unsigned long) }
};

HostType hostTypesD[] =
{
	{ "byte",     0,  true,   CHAR_MIN,  CHAR_MAX,    1 },
	{ "ubyte",    0,  false,  0,         UCHAR_MAX,   1 },
	{ "char",     0,  false,  0,         UCHAR_MAX,   1 },
	{ "short",    0,  true,   SHRT_MIN,  SHRT_MAX,    2 },
	{ "ushort",   0,  false,  0,         USHRT_MAX,   2 },
	{ "wchar",    0,  false,  0,         USHRT_MAX,   2 },
	{ "int",      0,  true,   INT_MIN,   INT_MAX,     4 },
	{ "uint",     0,  false,  0,         UINT_MAX,    4 },
	{ "dchar",    0,  false,  0,         UINT_MAX,    4 }
};

HostType hostTypesJava[] = 
{
	{ "byte",     0,  true,   CHAR_MIN,  CHAR_MAX,    1 },
	{ "short",    0,  true,   SHRT_MIN,  SHRT_MAX,    2 },
	{ "char",     0,  false,  0,         USHRT_MAX,   2 },
	{ "int",      0,  true,   INT_MIN,   INT_MAX,     4 },
};

HostType hostTypesRuby[] = 
{
	{ "byte",     0,  true,   CHAR_MIN,  CHAR_MAX,    1 },
	{ "short",    0,  true,   SHRT_MIN,  SHRT_MAX,    2 },
	{ "char",     0,  false,  0,         USHRT_MAX,   2 },
	{ "int",      0,  true,   INT_MIN,   INT_MAX,     4 },
};

HostLang hostLangC =    { hostTypesC,    8, hostTypesC+0,    true };
HostLang hostLangD =    { hostTypesD,    9, hostTypesD+2,    true };
HostLang hostLangJava = { hostTypesJava, 4, hostTypesJava+2, false };
HostLang hostLangRuby = { hostTypesRuby, 4, hostTypesRuby+2, false };

HostLang *hostLang = &hostLangC;
HostLangType hostLangType = CCode;

/* Construct a new parameter checker with for paramSpec. */
ParamCheck::ParamCheck(const char *paramSpec, int argc, char **argv)
:
	state(noparam),
	argOffset(0),
	curArg(0),
	iCurArg(1),
	paramSpec(paramSpec), 
	argc(argc), 
	argv(argv)
{
}

/* Check a single option. Returns the index of the next parameter.  Sets p to
 * the arg character if valid, 0 otherwise.  Sets parg to the parameter arg if
 * there is one, NULL otherwise. */
bool ParamCheck::check()
{
	bool requiresParam;

	if ( iCurArg >= argc ) {            /* Off the end of the arg list. */
		state = noparam;
		return false;
	}

	if ( argOffset != 0 && *argOffset == 0 ) {
		/* We are at the end of an arg string. */
		iCurArg += 1;
		if ( iCurArg >= argc ) {
			state = noparam;
			return false;
		}
		argOffset = 0;
	}

	if ( argOffset == 0 ) {
		/* Set the current arg. */
		curArg = argv[iCurArg];

		/* We are at the beginning of an arg string. */
		if ( argv[iCurArg] == 0 ||        /* Argv[iCurArg] is null. */
			 argv[iCurArg][0] != '-' ||   /* Not a param. */
			 argv[iCurArg][1] == 0 ) {    /* Only a dash. */
			parameter = 0;
			parameterArg = 0;

			iCurArg += 1;
			state = noparam;
			return true;
		}
		argOffset = argv[iCurArg] + 1;
	}

	/* Get the arg char. */
	char argChar = *argOffset;
	
	/* Loop over all the parms and look for a match. */
	const char *pSpec = paramSpec;
	while ( *pSpec != 0 ) {
		char pSpecChar = *pSpec;

		/* If there is a ':' following the char then
		 * it requires a parm.  If a parm is required
		 * then move ahead two in the parmspec. Otherwise
		 * move ahead one in the parm spec. */
		if ( pSpec[1] == ':' ) {
			requiresParam = true;
			pSpec += 2;
		}
		else {
			requiresParam = false;
			pSpec += 1;
		}

		/* Do we have a match. */
		if ( argChar == pSpecChar ) {
			if ( requiresParam ) {
				if ( argOffset[1] == 0 ) {
					/* The param must follow. */
					if ( iCurArg + 1 == argc ) {
						/* We are the last arg so there
						 * cannot be a parameter to it. */
						parameter = argChar;
						parameterArg = 0;
						iCurArg += 1;
						argOffset = 0;
						state = invalid;
						return true;
					}
					else {
						/* the parameter to the arg is the next arg. */
						parameter = pSpecChar;
						parameterArg = argv[iCurArg + 1];
						iCurArg += 2;
						argOffset = 0;
						state = match;
						return true;
					}
				}
				else {
					/* The param for the arg is built in. */
					parameter = pSpecChar;
					parameterArg = argOffset + 1;
					iCurArg += 1;
					argOffset = 0;
					state = match;
					return true;
				}
			}
			else {
				/* Good, we matched the parm and no
				 * arg is required. */
				parameter = pSpecChar;
				parameterArg = 0;
				argOffset += 1;
				state = match;
				return true;
			}
		}
	}

	/* We did not find a match. Bad Argument. */
	parameter = argChar;
	parameterArg = 0;
	argOffset += 1;
	state = invalid;
	return true;
}

void NormalizeWinPath(char* input) {
    const size_t len = strlen(input);
    char* res = static_cast<char*>(alloca(len + 1));
    for (size_t i = 0, j = 0; i <= len; ++i, ++j) {
        if (input[i] == '\\') {
            res[j] = '/';
            if (i < len - 2 && input[i + 1] == '\\')
                ++i;
            } else {
                res[j] = input[i];
            }
    }
    strcpy(input, res);
}

/* Counts newlines before sending sync. */
int output_filter::sync( )
{
	line += 1;
	return std::filebuf::sync();
}

/* Counts newlines before sending data out to file. */
std::streamsize output_filter::xsputn( const char *s, std::streamsize n )
{
	for ( int i = 0; i < n; i++ ) {
		if ( s[i] == '\n' )
			line += 1;
	}
	return std::filebuf::xsputn( s, n );
}

/* Scans a string looking for the file extension. If there is a file
 * extension then pointer returned points to inside the string
 * passed in. Otherwise returns null. */
char *findFileExtension( char *stemFile )
{
	char *ppos = stemFile + strlen(stemFile) - 1;

	/* Scan backwards from the end looking for the first dot.
	 * If we encounter a '/' before the first dot, then stop the scan. */
	while ( 1 ) {
		/* If we found a dot or got to the beginning of the string then
		 * we are done. */
		if ( ppos == stemFile || *ppos == '.' )
			break;

		/* If we hit a / then there is no extension. Done. */
		if ( *ppos == '/' ) {
			ppos = stemFile;
			break;
		}
		ppos--;
	} 

	/* If we got to the front of the string then bail we 
	 * did not find an extension  */
	if ( ppos == stemFile )
		ppos = 0;

	return ppos;
}

/* Make a file name from a stem. Removes the old filename suffix and
 * replaces it with a new one. Returns a newed up string. */
char *fileNameFromStem( char *stemFile, const char *suffix )
{
	int len = strlen( stemFile );
	assert( len > 0 );

	/* Get the extension. */
	char *ppos = findFileExtension( stemFile );

	/* If an extension was found, then shorten what we think the len is. */
	if ( ppos != 0 )
		len = ppos - stemFile;

	/* Make the return string from the stem and the suffix. */
	char *retVal = new char[ len + strlen( suffix ) + 1 ];
	strncpy( retVal, stemFile, len );
	strcpy( retVal + len, suffix );

	return retVal;
}


