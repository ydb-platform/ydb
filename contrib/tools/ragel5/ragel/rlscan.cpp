#line 1 "rlscan.rl"
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

#include <iostream>
#include <fstream>
#include <string.h>

#include "ragel.h"
#include "rlscan.h"

//#define LOG_TOKENS

using std::ifstream;
using std::istream;
using std::ostream;
using std::cout;
using std::cerr;
using std::endl;

enum InlineBlockType
{
	CurlyDelimited,
	SemiTerminated
};


/*
 * The Scanner for Importing
 */

#define IMP_Word 128
#define IMP_Literal 129
#define IMP_UInt 130
#define IMP_Define 131

#line 124 "rlscan.rl"



#line 60 "rlscan.cpp"
static const int inline_token_scan_start = 2;

static const int inline_token_scan_first_final = 2;

static const int inline_token_scan_error = -1;

#line 127 "rlscan.rl"

void Scanner::flushImport()
{
	int *p = token_data;
	int *pe = token_data + cur_token;

	
#line 75 "rlscan.cpp"
	{
	 tok_cs = inline_token_scan_start;
	 tok_tokstart = 0;
	 tok_tokend = 0;
	 tok_act = 0;
	}
#line 134 "rlscan.rl"
	
#line 84 "rlscan.cpp"
	{
	if ( p == pe )
		goto _out;
	switch (  tok_cs )
	{
tr0:
#line 122 "rlscan.rl"
	{{p = (( tok_tokend))-1;}}
	goto st2;
tr1:
#line 108 "rlscan.rl"
	{ tok_tokend = p+1;{ 
			int base = tok_tokstart - token_data;
			int nameOff = 0;
			int litOff = 2;

			directToParser( inclToParser, fileName, line, column, TK_Word, 
					token_strings[base+nameOff], token_lens[base+nameOff] );
			directToParser( inclToParser, fileName, line, column, '=', 0, 0 );
			directToParser( inclToParser, fileName, line, column, TK_Literal,
					token_strings[base+litOff], token_lens[base+litOff] );
			directToParser( inclToParser, fileName, line, column, ';', 0, 0 );
		}{p = (( tok_tokend))-1;}}
	goto st2;
tr2:
#line 80 "rlscan.rl"
	{ tok_tokend = p+1;{ 
			int base = tok_tokstart - token_data;
			int nameOff = 0;
			int numOff = 2;

			directToParser( inclToParser, fileName, line, column, TK_Word, 
					token_strings[base+nameOff], token_lens[base+nameOff] );
			directToParser( inclToParser, fileName, line, column, '=', 0, 0 );
			directToParser( inclToParser, fileName, line, column, TK_UInt,
					token_strings[base+numOff], token_lens[base+numOff] );
			directToParser( inclToParser, fileName, line, column, ';', 0, 0 );
		}{p = (( tok_tokend))-1;}}
	goto st2;
tr3:
#line 94 "rlscan.rl"
	{ tok_tokend = p+1;{ 
			int base = tok_tokstart - token_data;
			int nameOff = 1;
			int litOff = 2;

			directToParser( inclToParser, fileName, line, column, TK_Word, 
					token_strings[base+nameOff], token_lens[base+nameOff] );
			directToParser( inclToParser, fileName, line, column, '=', 0, 0 );
			directToParser( inclToParser, fileName, line, column, TK_Literal,
					token_strings[base+litOff], token_lens[base+litOff] );
			directToParser( inclToParser, fileName, line, column, ';', 0, 0 );
		}{p = (( tok_tokend))-1;}}
	goto st2;
tr4:
#line 66 "rlscan.rl"
	{ tok_tokend = p+1;{ 
			int base = tok_tokstart - token_data;
			int nameOff = 1;
			int numOff = 2;

			directToParser( inclToParser, fileName, line, column, TK_Word, 
					token_strings[base+nameOff], token_lens[base+nameOff] );
			directToParser( inclToParser, fileName, line, column, '=', 0, 0 );
			directToParser( inclToParser, fileName, line, column, TK_UInt,
					token_strings[base+numOff], token_lens[base+numOff] );
			directToParser( inclToParser, fileName, line, column, ';', 0, 0 );
		}{p = (( tok_tokend))-1;}}
	goto st2;
tr5:
#line 122 "rlscan.rl"
	{ tok_tokend = p+1;{p = (( tok_tokend))-1;}}
	goto st2;
tr8:
#line 122 "rlscan.rl"
	{ tok_tokend = p;{p = (( tok_tokend))-1;}}
	goto st2;
st2:
#line 1 "rlscan.rl"
	{ tok_tokstart = 0;}
	if ( ++p == pe )
		goto _out2;
case 2:
#line 1 "rlscan.rl"
	{ tok_tokstart = p;}
#line 170 "rlscan.cpp"
	switch( (*p) ) {
		case 128: goto tr6;
		case 131: goto tr7;
	}
	goto tr5;
tr6:
#line 1 "rlscan.rl"
	{ tok_tokend = p+1;}
	goto st3;
st3:
	if ( ++p == pe )
		goto _out3;
case 3:
#line 184 "rlscan.cpp"
	if ( (*p) == 61 )
		goto st0;
	goto tr8;
st0:
	if ( ++p == pe )
		goto _out0;
case 0:
	switch( (*p) ) {
		case 129: goto tr1;
		case 130: goto tr2;
	}
	goto tr0;
tr7:
#line 1 "rlscan.rl"
	{ tok_tokend = p+1;}
	goto st4;
st4:
	if ( ++p == pe )
		goto _out4;
case 4:
#line 205 "rlscan.cpp"
	if ( (*p) == 128 )
		goto st1;
	goto tr8;
st1:
	if ( ++p == pe )
		goto _out1;
case 1:
	switch( (*p) ) {
		case 129: goto tr3;
		case 130: goto tr4;
	}
	goto tr0;
	}
	_out2:  tok_cs = 2; goto _out; 
	_out3:  tok_cs = 3; goto _out; 
	_out0:  tok_cs = 0; goto _out; 
	_out4:  tok_cs = 4; goto _out; 
	_out1:  tok_cs = 1; goto _out; 

	_out: {}
	}
#line 135 "rlscan.rl"

	if ( tok_tokstart == 0 )
		cur_token = 0;
	else {
		cur_token = pe - tok_tokstart;
		int ts_offset = tok_tokstart - token_data;
		memmove( token_data, token_data+ts_offset, cur_token*sizeof(token_data[0]) );
		memmove( token_strings, token_strings+ts_offset, cur_token*sizeof(token_strings[0]) );
		memmove( token_lens, token_lens+ts_offset, cur_token*sizeof(token_lens[0]) );
	}
}

void Scanner::directToParser( Parser *toParser, const char *tokFileName, int tokLine, 
		int tokColumn, int type, char *tokdata, int toklen )
{
	InputLoc loc;

	#ifdef LOG_TOKENS
	cerr << "scanner:" << tokLine << ":" << tokColumn << 
			": sending token to the parser " << Parser_lelNames[type];
	cerr << " " << toklen;
	if ( tokdata != 0 )
		cerr << " " << tokdata;
	cerr << endl;
	#endif

	loc.fileName = tokFileName;
	loc.line = tokLine;
	loc.col = tokColumn;

	toParser->token( loc, type, tokdata, toklen );
}

void Scanner::importToken( int token, char *start, char *end )
{
	if ( cur_token == max_tokens )
		flushImport();

	token_data[cur_token] = token;
	if ( start == 0 ) {
		token_strings[cur_token] = 0;
		token_lens[cur_token] = 0;
	}
	else {
		int toklen = end-start;
		token_lens[cur_token] = toklen;
		token_strings[cur_token] = new char[toklen+1];
		memcpy( token_strings[cur_token], start, toklen );
		token_strings[cur_token][toklen] = 0;
	}
	cur_token++;
}

void Scanner::pass( int token, char *start, char *end )
{
	if ( importMachines )
		importToken( token, start, end );
	pass();
}

void Scanner::pass()
{
	updateCol();

	/* If no errors and we are at the bottom of the include stack (the
	 * source file listed on the command line) then write out the data. */
	if ( includeDepth == 0 && machineSpec == 0 && machineName == 0 )
		xmlEscapeHost( output, tokstart, tokend-tokstart );
}

/*
 * The scanner for processing sections, includes, imports, etc.
 */


#line 303 "rlscan.cpp"
static const int section_parse_start = 10;

static const int section_parse_first_final = 10;

static const int section_parse_error = 0;

#line 213 "rlscan.rl"



void Scanner::init( )
{
	
#line 317 "rlscan.cpp"
	{
	cs = section_parse_start;
	}
#line 219 "rlscan.rl"
}

bool Scanner::active()
{
	if ( ignoreSection )
		return false;

	if ( parser == 0 && ! parserExistsError ) {
		scan_error() << "there is no previous specification name" << endl;
		parserExistsError = true;
	}

	if ( parser == 0 )
		return false;

	return true;
}

ostream &Scanner::scan_error()
{
	/* Maintain the error count. */
	gblErrorCount += 1;
	cerr << fileName << ":" << line << ":" << column << ": ";
	return cerr;
}

bool Scanner::recursiveInclude(const char *inclFileName, char *inclSectionName )
{
	for ( IncludeStack::Iter si = includeStack; si.lte(); si++ ) {
		if ( strcmp( si->fileName, inclFileName ) == 0 &&
				strcmp( si->sectionName, inclSectionName ) == 0 )
		{
			return true;
		}
	}
	return false;	
}

void Scanner::updateCol()
{
	char *from = lastnl;
	if ( from == 0 )
		from = tokstart;
	//cerr << "adding " << tokend - from << " to column" << endl;
	column += tokend - from;
	lastnl = 0;
}

#line 442 "rlscan.rl"


void Scanner::token( int type, char c )
{
	token( type, &c, &c + 1 );
}

void Scanner::token( int type )
{
	token( type, 0, 0 );
}

void Scanner::token( int type, char *start, char *end )
{
	char *tokdata = 0;
	int toklen = 0;
	if ( start != 0 ) {
		toklen = end-start;
		tokdata = new char[toklen+1];
		memcpy( tokdata, start, toklen );
		tokdata[toklen] = 0;
	}

	processToken( type, tokdata, toklen );
}

void Scanner::processToken( int type, char *tokdata, int toklen )
{
	int *p = &type;
	int *pe = &type + 1;

	
#line 403 "rlscan.cpp"
	{
	if ( p == pe )
		goto _out;
	switch ( cs )
	{
tr2:
#line 289 "rlscan.rl"
	{
		/* Assign a name to the machine. */
		char *machine = word;

		if ( !importMachines && inclSectionTarg == 0 ) {
			ignoreSection = false;

			ParserDictEl *pdEl = parserDict.find( machine );
			if ( pdEl == 0 ) {
				pdEl = new ParserDictEl( machine );
				pdEl->value = new Parser( fileName, machine, sectionLoc );
				pdEl->value->init();
				parserDict.insert( pdEl );
			}

			parser = pdEl->value;
		}
		else if ( !importMachines && strcmp( inclSectionTarg, machine ) == 0 ) {
			/* found include target */
			ignoreSection = false;
			parser = inclToParser;
		}
		else {
			/* ignoring section */
			ignoreSection = true;
			parser = 0;
		}
	}
	goto st10;
tr6:
#line 323 "rlscan.rl"
	{
		if ( active() ) {
			char *inclSectionName = word;
			const char *inclFileName = 0;

			/* Implement defaults for the input file and section name. */
			if ( inclSectionName == 0 )
				inclSectionName = parser->sectionName;

			if ( lit != 0 ) 
				inclFileName = prepareFileName( lit, lit_len );
			else
				inclFileName = fileName;

			/* Check for a recursive include structure. Add the current file/section
			 * name then check if what we are including is already in the stack. */
			includeStack.append( IncludeStackItem( fileName, parser->sectionName ) );

			if ( recursiveInclude( inclFileName, inclSectionName ) )
				scan_error() << "include: this is a recursive include operation" << endl;
			else {
				/* Open the input file for reading. */
				ifstream *inFile = new ifstream( inclFileName );
				if ( ! inFile->is_open() ) {
					scan_error() << "include: could not open " << 
							inclFileName << " for reading" << endl;
				}

				Scanner scanner( inclFileName, *inFile, output, parser,
						inclSectionName, includeDepth+1, false );
				scanner.do_scan( );
				delete inFile;
			}

			/* Remove the last element (len-1) */
			includeStack.remove( -1 );
		}
	}
	goto st10;
tr10:
#line 372 "rlscan.rl"
	{
		if ( active() ) {
			char *importFileName = prepareFileName( lit, lit_len );

			/* Open the input file for reading. */
			ifstream *inFile = new ifstream( importFileName );
			if ( ! inFile->is_open() ) {
				scan_error() << "import: could not open " << 
						importFileName << " for reading" << endl;
			}

			Scanner scanner( importFileName, *inFile, output, parser,
					0, includeDepth+1, true );
			scanner.do_scan( );
			scanner.importToken( 0, 0, 0 );
			scanner.flushImport();
			delete inFile;
		}
	}
	goto st10;
tr13:
#line 414 "rlscan.rl"
	{
		if ( active() && machineSpec == 0 && machineName == 0 )
			output << "</write>\n";
	}
	goto st10;
tr14:
#line 425 "rlscan.rl"
	{
		/* Send the token off to the parser. */
		if ( active() )
			directToParser( parser, fileName, line, column, type, tokdata, toklen );
	}
	goto st10;
st10:
	if ( ++p == pe )
		goto _out10;
case 10:
#line 522 "rlscan.cpp"
	switch( (*p) ) {
		case 128: goto st1;
		case 129: goto st3;
		case 130: goto st6;
		case 131: goto tr18;
	}
	goto tr14;
st1:
	if ( ++p == pe )
		goto _out1;
case 1:
	if ( (*p) == 132 )
		goto tr1;
	goto tr0;
tr0:
#line 283 "rlscan.rl"
	{ scan_error() << "bad machine statement" << endl; }
	goto st0;
tr3:
#line 284 "rlscan.rl"
	{ scan_error() << "bad include statement" << endl; }
	goto st0;
tr8:
#line 285 "rlscan.rl"
	{ scan_error() << "bad import statement" << endl; }
	goto st0;
tr11:
#line 286 "rlscan.rl"
	{ scan_error() << "bad write statement" << endl; }
	goto st0;
#line 553 "rlscan.cpp"
st0:
	goto _out0;
tr1:
#line 280 "rlscan.rl"
	{ word = tokdata; word_len = toklen; }
	goto st2;
st2:
	if ( ++p == pe )
		goto _out2;
case 2:
#line 564 "rlscan.cpp"
	if ( (*p) == 59 )
		goto tr2;
	goto tr0;
st3:
	if ( ++p == pe )
		goto _out3;
case 3:
	switch( (*p) ) {
		case 132: goto tr4;
		case 133: goto tr5;
	}
	goto tr3;
tr4:
#line 279 "rlscan.rl"
	{ word = lit = 0; word_len = lit_len = 0; }
#line 280 "rlscan.rl"
	{ word = tokdata; word_len = toklen; }
	goto st4;
st4:
	if ( ++p == pe )
		goto _out4;
case 4:
#line 587 "rlscan.cpp"
	switch( (*p) ) {
		case 59: goto tr6;
		case 133: goto tr7;
	}
	goto tr3;
tr5:
#line 279 "rlscan.rl"
	{ word = lit = 0; word_len = lit_len = 0; }
#line 281 "rlscan.rl"
	{ lit = tokdata; lit_len = toklen; }
	goto st5;
tr7:
#line 281 "rlscan.rl"
	{ lit = tokdata; lit_len = toklen; }
	goto st5;
st5:
	if ( ++p == pe )
		goto _out5;
case 5:
#line 607 "rlscan.cpp"
	if ( (*p) == 59 )
		goto tr6;
	goto tr3;
st6:
	if ( ++p == pe )
		goto _out6;
case 6:
	if ( (*p) == 133 )
		goto tr9;
	goto tr8;
tr9:
#line 281 "rlscan.rl"
	{ lit = tokdata; lit_len = toklen; }
	goto st7;
st7:
	if ( ++p == pe )
		goto _out7;
case 7:
#line 626 "rlscan.cpp"
	if ( (*p) == 59 )
		goto tr10;
	goto tr8;
tr18:
#line 397 "rlscan.rl"
	{
		if ( active() && machineSpec == 0 && machineName == 0 ) {
			output << "<write"
					" def_name=\"" << parser->sectionName << "\""
					" line=\"" << line << "\""
					" col=\"" << column << "\""
					">";
		}
	}
	goto st8;
st8:
	if ( ++p == pe )
		goto _out8;
case 8:
#line 646 "rlscan.cpp"
	if ( (*p) == 132 )
		goto tr12;
	goto tr11;
tr12:
#line 408 "rlscan.rl"
	{
		if ( active() && machineSpec == 0 && machineName == 0 )
			output << "<arg>" << tokdata << "</arg>";
	}
	goto st9;
st9:
	if ( ++p == pe )
		goto _out9;
case 9:
#line 661 "rlscan.cpp"
	switch( (*p) ) {
		case 59: goto tr13;
		case 132: goto tr12;
	}
	goto tr11;
	}
	_out10: cs = 10; goto _out; 
	_out1: cs = 1; goto _out; 
	_out0: cs = 0; goto _out; 
	_out2: cs = 2; goto _out; 
	_out3: cs = 3; goto _out; 
	_out4: cs = 4; goto _out; 
	_out5: cs = 5; goto _out; 
	_out6: cs = 6; goto _out; 
	_out7: cs = 7; goto _out; 
	_out8: cs = 8; goto _out; 
	_out9: cs = 9; goto _out; 

	_out: {}
	}
#line 476 "rlscan.rl"


	updateCol();

	/* Record the last token for use in controlling the scan of subsequent
	 * tokens. */
	lastToken = type;
}

void Scanner::startSection( )
{
	parserExistsError = false;

	if ( includeDepth == 0 ) {
		if ( machineSpec == 0 && machineName == 0 )
			output << "</host>\n";
	}

	sectionLoc.fileName = fileName;
	sectionLoc.line = line;
	sectionLoc.col = 0;
}

void Scanner::endSection( )
{
	/* Execute the eof actions for the section parser. */
	
#line 710 "rlscan.cpp"
	{
	switch ( cs ) {
	case 1: 
	case 2: 
#line 283 "rlscan.rl"
	{ scan_error() << "bad machine statement" << endl; }
	break;
	case 3: 
	case 4: 
	case 5: 
#line 284 "rlscan.rl"
	{ scan_error() << "bad include statement" << endl; }
	break;
	case 6: 
	case 7: 
#line 285 "rlscan.rl"
	{ scan_error() << "bad import statement" << endl; }
	break;
	case 8: 
	case 9: 
#line 286 "rlscan.rl"
	{ scan_error() << "bad write statement" << endl; }
	break;
#line 734 "rlscan.cpp"
	}
	}

#line 505 "rlscan.rl"


	/* Close off the section with the parser. */
	if ( active() ) {
		InputLoc loc;
		loc.fileName = fileName;
		loc.line = line;
		loc.col = 0;

		parser->token( loc, TK_EndSection, 0, 0 );
	}

	if ( includeDepth == 0 ) {
		if ( machineSpec == 0 && machineName == 0 ) {
			/* The end section may include a newline on the end, so
			 * we use the last line, which will count the newline. */
			output << "<host line=\"" << line << "\">";
		}
	}
}

#line 917 "rlscan.rl"



#line 764 "rlscan.cpp"
static const int rlscan_start = 23;

static const int rlscan_first_final = 23;

static const int rlscan_error = 0;

#line 920 "rlscan.rl"

void Scanner::do_scan()
{
	int bufsize = 8;
	char *buf = new char[bufsize];
	const char last_char = 0;
	int cs, act, have = 0;
	int top, stack[1];
	int curly_count = 0;
	bool execute = true;
	bool singleLineSpec = false;
	InlineBlockType inlineBlockType = CurlyDelimited;

	/* Init the section parser and the character scanner. */
	init();
	
#line 788 "rlscan.cpp"
	{
	cs = rlscan_start;
	top = 0;
	tokstart = 0;
	tokend = 0;
	act = 0;
	}
#line 936 "rlscan.rl"

	while ( execute ) {
		char *p = buf + have;
		int space = bufsize - have;

		if ( space == 0 ) {
			/* We filled up the buffer trying to scan a token. Grow it. */
			bufsize = bufsize * 2;
			char *newbuf = new char[bufsize];

			/* Recompute p and space. */
			p = newbuf + have;
			space = bufsize - have;

			/* Patch up pointers possibly in use. */
			if ( tokstart != 0 )
				tokstart = newbuf + ( tokstart - buf );
			tokend = newbuf + ( tokend - buf );

			/* Copy the new buffer in. */
			memcpy( newbuf, buf, have );
			delete[] buf;
			buf = newbuf;
		}

		input.read( p, space );
		int len = input.gcount();

		/* If we see eof then append the EOF char. */
	 	if ( len == 0 ) {
			p[0] = last_char, len = 1;
			execute = false;
		}

		char *pe = p + len;
		
#line 833 "rlscan.cpp"
	{
	if ( p == pe )
		goto _out;
	goto _resume;

_again:
	switch ( cs ) {
		case 23: goto st23;
		case 24: goto st24;
		case 25: goto st25;
		case 1: goto st1;
		case 2: goto st2;
		case 26: goto st26;
		case 27: goto st27;
		case 28: goto st28;
		case 3: goto st3;
		case 4: goto st4;
		case 29: goto st29;
		case 5: goto st5;
		case 6: goto st6;
		case 7: goto st7;
		case 30: goto st30;
		case 31: goto st31;
		case 32: goto st32;
		case 33: goto st33;
		case 34: goto st34;
		case 35: goto st35;
		case 36: goto st36;
		case 37: goto st37;
		case 38: goto st38;
		case 39: goto st39;
		case 8: goto st8;
		case 9: goto st9;
		case 40: goto st40;
		case 10: goto st10;
		case 11: goto st11;
		case 41: goto st41;
		case 12: goto st12;
		case 13: goto st13;
		case 14: goto st14;
		case 42: goto st42;
		case 43: goto st43;
		case 15: goto st15;
		case 44: goto st44;
		case 45: goto st45;
		case 46: goto st46;
		case 47: goto st47;
		case 48: goto st48;
		case 49: goto st49;
		case 50: goto st50;
		case 51: goto st51;
		case 52: goto st52;
		case 53: goto st53;
		case 54: goto st54;
		case 55: goto st55;
		case 56: goto st56;
		case 57: goto st57;
		case 58: goto st58;
		case 59: goto st59;
		case 60: goto st60;
		case 61: goto st61;
		case 62: goto st62;
		case 63: goto st63;
		case 64: goto st64;
		case 65: goto st65;
		case 66: goto st66;
		case 67: goto st67;
		case 68: goto st68;
		case 69: goto st69;
		case 70: goto st70;
		case 71: goto st71;
		case 72: goto st72;
		case 73: goto st73;
		case 74: goto st74;
		case 75: goto st75;
		case 76: goto st76;
		case 77: goto st77;
		case 78: goto st78;
		case 79: goto st79;
		case 80: goto st80;
		case 81: goto st81;
		case 82: goto st82;
		case 83: goto st83;
		case 84: goto st84;
		case 85: goto st85;
		case 0: goto st0;
		case 86: goto st86;
		case 87: goto st87;
		case 88: goto st88;
		case 89: goto st89;
		case 90: goto st90;
		case 16: goto st16;
		case 91: goto st91;
		case 17: goto st17;
		case 92: goto st92;
		case 18: goto st18;
		case 93: goto st93;
		case 94: goto st94;
		case 95: goto st95;
		case 19: goto st19;
		case 20: goto st20;
		case 96: goto st96;
		case 97: goto st97;
		case 98: goto st98;
		case 99: goto st99;
		case 100: goto st100;
		case 21: goto st21;
		case 101: goto st101;
		case 102: goto st102;
		case 103: goto st103;
		case 104: goto st104;
		case 105: goto st105;
		case 106: goto st106;
		case 107: goto st107;
		case 108: goto st108;
		case 109: goto st109;
		case 110: goto st110;
		case 111: goto st111;
		case 112: goto st112;
		case 113: goto st113;
		case 114: goto st114;
		case 115: goto st115;
		case 116: goto st116;
		case 117: goto st117;
		case 118: goto st118;
		case 119: goto st119;
		case 120: goto st120;
		case 121: goto st121;
		case 122: goto st122;
		case 123: goto st123;
		case 124: goto st124;
		case 125: goto st125;
		case 126: goto st126;
		case 127: goto st127;
		case 128: goto st128;
		case 129: goto st129;
		case 130: goto st130;
		case 131: goto st131;
		case 132: goto st132;
		case 133: goto st133;
		case 134: goto st134;
		case 135: goto st135;
		case 136: goto st136;
		case 137: goto st137;
		case 138: goto st138;
		case 139: goto st139;
		case 140: goto st140;
		case 141: goto st141;
		case 142: goto st142;
		case 143: goto st143;
		case 144: goto st144;
		case 145: goto st145;
		case 146: goto st146;
		case 147: goto st147;
		case 148: goto st148;
		case 149: goto st149;
		case 150: goto st150;
		case 151: goto st151;
		case 152: goto st152;
		case 153: goto st153;
		case 154: goto st154;
		case 155: goto st155;
		case 156: goto st156;
		case 157: goto st157;
		case 158: goto st158;
		case 159: goto st159;
		case 160: goto st160;
		case 161: goto st161;
		case 162: goto st162;
		case 163: goto st163;
		case 164: goto st164;
		case 165: goto st165;
		case 166: goto st166;
		case 167: goto st167;
		case 168: goto st168;
		case 169: goto st169;
		case 170: goto st170;
		case 171: goto st171;
		case 172: goto st172;
		case 173: goto st173;
		case 174: goto st174;
		case 22: goto st22;
	default: break;
	}

	if ( ++p == pe )
		goto _out;
_resume:
	switch ( cs )
	{
tr2:
#line 899 "rlscan.rl"
	{tokend = p+1;{ pass( IMP_Literal, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st23;
tr10:
#line 898 "rlscan.rl"
	{tokend = p+1;{ pass(); }{p = ((tokend))-1;}}
	goto st23;
tr12:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
#line 898 "rlscan.rl"
	{tokend = p+1;{ pass(); }{p = ((tokend))-1;}}
	goto st23;
tr41:
#line 915 "rlscan.rl"
	{tokend = p+1;{ pass( *tokstart, 0, 0 ); }{p = ((tokend))-1;}}
	goto st23;
tr42:
#line 914 "rlscan.rl"
	{tokend = p+1;{p = ((tokend))-1;}}
	goto st23;
tr52:
#line 913 "rlscan.rl"
	{tokend = p;{ pass(); }{p = ((tokend))-1;}}
	goto st23;
tr53:
#line 915 "rlscan.rl"
	{tokend = p;{ pass( *tokstart, 0, 0 ); }{p = ((tokend))-1;}}
	goto st23;
tr55:
#line 907 "rlscan.rl"
	{tokend = p;{ 
			updateCol();
			singleLineSpec = true;
			startSection();
			{{p = ((tokend))-1;}{goto st88;}}
		}{p = ((tokend))-1;}}
	goto st23;
tr56:
#line 901 "rlscan.rl"
	{tokend = p+1;{ 
			updateCol();
			singleLineSpec = false;
			startSection();
			{{p = ((tokend))-1;}{goto st88;}}
		}{p = ((tokend))-1;}}
	goto st23;
tr57:
#line 897 "rlscan.rl"
	{tokend = p;{ pass( IMP_UInt, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st23;
tr58:
#line 1 "rlscan.rl"
	{	switch( act ) {
	case 137:
	{ pass( IMP_Define, 0, 0 ); }
	break;
	case 138:
	{ pass( IMP_Word, tokstart, tokend ); }
	break;
	default: break;
	}
	{p = ((tokend))-1;}}
	goto st23;
tr59:
#line 896 "rlscan.rl"
	{tokend = p;{ pass( IMP_Word, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st23;
st23:
#line 1 "rlscan.rl"
	{tokstart = 0;}
	if ( ++p == pe )
		goto _out23;
case 23:
#line 1 "rlscan.rl"
	{tokstart = p;}
#line 1105 "rlscan.cpp"
	switch( (*p) ) {
		case 0: goto tr42;
		case 9: goto st24;
		case 10: goto tr44;
		case 32: goto st24;
		case 34: goto tr45;
		case 37: goto st26;
		case 39: goto tr47;
		case 47: goto tr48;
		case 95: goto tr50;
		case 100: goto st32;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto st30;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr50;
	} else
		goto tr50;
	goto tr41;
tr44:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
	goto st24;
st24:
	if ( ++p == pe )
		goto _out24;
case 24:
#line 1139 "rlscan.cpp"
	switch( (*p) ) {
		case 9: goto st24;
		case 10: goto tr44;
		case 32: goto st24;
	}
	goto tr52;
tr45:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st25;
st25:
	if ( ++p == pe )
		goto _out25;
case 25:
#line 1154 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr1;
		case 34: goto tr2;
		case 92: goto st2;
	}
	goto st1;
tr1:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
	goto st1;
st1:
	if ( ++p == pe )
		goto _out1;
case 1:
#line 1173 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr1;
		case 34: goto tr2;
		case 92: goto st2;
	}
	goto st1;
st2:
	if ( ++p == pe )
		goto _out2;
case 2:
	if ( (*p) == 10 )
		goto tr1;
	goto st1;
st26:
	if ( ++p == pe )
		goto _out26;
case 26:
	if ( (*p) == 37 )
		goto st27;
	goto tr53;
st27:
	if ( ++p == pe )
		goto _out27;
case 27:
	if ( (*p) == 123 )
		goto tr56;
	goto tr55;
tr47:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st28;
st28:
	if ( ++p == pe )
		goto _out28;
case 28:
#line 1209 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr5;
		case 39: goto tr2;
		case 92: goto st4;
	}
	goto st3;
tr5:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
	goto st3;
st3:
	if ( ++p == pe )
		goto _out3;
case 3:
#line 1228 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr5;
		case 39: goto tr2;
		case 92: goto st4;
	}
	goto st3;
st4:
	if ( ++p == pe )
		goto _out4;
case 4:
	if ( (*p) == 10 )
		goto tr5;
	goto st3;
tr48:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st29;
st29:
	if ( ++p == pe )
		goto _out29;
case 29:
#line 1250 "rlscan.cpp"
	switch( (*p) ) {
		case 42: goto st5;
		case 47: goto st7;
	}
	goto tr53;
tr8:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
	goto st5;
st5:
	if ( ++p == pe )
		goto _out5;
case 5:
#line 1268 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr8;
		case 42: goto st6;
	}
	goto st5;
st6:
	if ( ++p == pe )
		goto _out6;
case 6:
	switch( (*p) ) {
		case 10: goto tr8;
		case 42: goto st6;
		case 47: goto tr10;
	}
	goto st5;
st7:
	if ( ++p == pe )
		goto _out7;
case 7:
	if ( (*p) == 10 )
		goto tr12;
	goto st7;
st30:
	if ( ++p == pe )
		goto _out30;
case 30:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st30;
	goto tr57;
tr50:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 896 "rlscan.rl"
	{act = 138;}
	goto st31;
tr64:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 895 "rlscan.rl"
	{act = 137;}
	goto st31;
st31:
	if ( ++p == pe )
		goto _out31;
case 31:
#line 1314 "rlscan.cpp"
	if ( (*p) == 95 )
		goto tr50;
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr50;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr50;
	} else
		goto tr50;
	goto tr58;
st32:
	if ( ++p == pe )
		goto _out32;
case 32:
	switch( (*p) ) {
		case 95: goto tr50;
		case 101: goto st33;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr50;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr50;
	} else
		goto tr50;
	goto tr59;
st33:
	if ( ++p == pe )
		goto _out33;
case 33:
	switch( (*p) ) {
		case 95: goto tr50;
		case 102: goto st34;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr50;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr50;
	} else
		goto tr50;
	goto tr59;
st34:
	if ( ++p == pe )
		goto _out34;
case 34:
	switch( (*p) ) {
		case 95: goto tr50;
		case 105: goto st35;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr50;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr50;
	} else
		goto tr50;
	goto tr59;
st35:
	if ( ++p == pe )
		goto _out35;
case 35:
	switch( (*p) ) {
		case 95: goto tr50;
		case 110: goto st36;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr50;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr50;
	} else
		goto tr50;
	goto tr59;
st36:
	if ( ++p == pe )
		goto _out36;
case 36:
	switch( (*p) ) {
		case 95: goto tr50;
		case 101: goto tr64;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr50;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr50;
	} else
		goto tr50;
	goto tr59;
tr15:
#line 606 "rlscan.rl"
	{tokend = p+1;{ token( IL_Literal, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr23:
#line 612 "rlscan.rl"
	{tokend = p+1;{ token( IL_Comment, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr25:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
#line 612 "rlscan.rl"
	{tokend = p+1;{ token( IL_Comment, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr26:
#line 602 "rlscan.rl"
	{{ token( TK_UInt, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr65:
#line 659 "rlscan.rl"
	{tokend = p+1;{ token( IL_Symbol, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr66:
#line 654 "rlscan.rl"
	{tokend = p+1;{
			scan_error() << "unterminated code block" << endl;
		}{p = ((tokend))-1;}}
	goto st37;
tr71:
#line 634 "rlscan.rl"
	{tokend = p+1;{ token( *tokstart, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr72:
#line 629 "rlscan.rl"
	{tokend = p+1;{ 
			whitespaceOn = true;
			token( *tokstart, tokstart, tokend );
		}{p = ((tokend))-1;}}
	goto st37;
tr77:
#line 622 "rlscan.rl"
	{tokend = p+1;{
			whitespaceOn = true;
			token( *tokstart, tokstart, tokend );
			if ( inlineBlockType == SemiTerminated )
				{{p = ((tokend))-1;}{goto st88;}}
		}{p = ((tokend))-1;}}
	goto st37;
tr80:
#line 636 "rlscan.rl"
	{tokend = p+1;{ 
			token( IL_Symbol, tokstart, tokend );
			curly_count += 1; 
		}{p = ((tokend))-1;}}
	goto st37;
tr81:
#line 641 "rlscan.rl"
	{tokend = p+1;{ 
			if ( --curly_count == 0 && inlineBlockType == CurlyDelimited ) {
				/* Inline code block ends. */
				token( '}' );
				{{p = ((tokend))-1;}{goto st88;}}
			}
			else {
				/* Either a semi terminated inline block or only the closing
				 * brace of some inner scope, not the block's closing brace. */
				token( IL_Symbol, tokstart, tokend );
			}
		}{p = ((tokend))-1;}}
	goto st37;
tr82:
#line 608 "rlscan.rl"
	{tokend = p;{ 
			if ( whitespaceOn ) 
				token( IL_WhiteSpace, tokstart, tokend );
		}{p = ((tokend))-1;}}
	goto st37;
tr83:
#line 659 "rlscan.rl"
	{tokend = p;{ token( IL_Symbol, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr84:
#line 602 "rlscan.rl"
	{tokend = p;{ token( TK_UInt, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr86:
#line 603 "rlscan.rl"
	{tokend = p;{ token( TK_Hex, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr87:
#line 614 "rlscan.rl"
	{tokend = p+1;{ token( TK_NameSep, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr88:
#line 1 "rlscan.rl"
	{	switch( act ) {
	case 1:
	{ token( KW_PChar ); }
	break;
	case 3:
	{ token( KW_CurState ); }
	break;
	case 4:
	{ token( KW_TargState ); }
	break;
	case 5:
	{ 
			whitespaceOn = false; 
			token( KW_Entry );
		}
	break;
	case 6:
	{ 
			whitespaceOn = false; 
			token( KW_Hold );
		}
	break;
	case 7:
	{ token( KW_Exec, 0, 0 ); }
	break;
	case 8:
	{ 
			whitespaceOn = false; 
			token( KW_Goto );
		}
	break;
	case 9:
	{ 
			whitespaceOn = false; 
			token( KW_Next );
		}
	break;
	case 10:
	{ 
			whitespaceOn = false; 
			token( KW_Call );
		}
	break;
	case 11:
	{ 
			whitespaceOn = false; 
			token( KW_Ret );
		}
	break;
	case 12:
	{ 
			whitespaceOn = false; 
			token( KW_Break );
		}
	break;
	case 13:
	{ token( TK_Word, tokstart, tokend ); }
	break;
	default: break;
	}
	{p = ((tokend))-1;}}
	goto st37;
tr89:
#line 600 "rlscan.rl"
	{tokend = p;{ token( TK_Word, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st37;
tr103:
#line 565 "rlscan.rl"
	{tokend = p;{ token( KW_Char ); }{p = ((tokend))-1;}}
	goto st37;
st37:
#line 1 "rlscan.rl"
	{tokstart = 0;}
	if ( ++p == pe )
		goto _out37;
case 37:
#line 1 "rlscan.rl"
	{tokstart = p;}
#line 1588 "rlscan.cpp"
	switch( (*p) ) {
		case 0: goto tr66;
		case 9: goto st38;
		case 10: goto tr68;
		case 32: goto st38;
		case 34: goto tr69;
		case 39: goto tr70;
		case 40: goto tr71;
		case 44: goto tr71;
		case 47: goto tr73;
		case 48: goto tr74;
		case 58: goto st45;
		case 59: goto tr77;
		case 95: goto tr78;
		case 102: goto st47;
		case 123: goto tr80;
		case 125: goto tr81;
	}
	if ( (*p) < 49 ) {
		if ( 41 <= (*p) && (*p) <= 42 )
			goto tr72;
	} else if ( (*p) > 57 ) {
		if ( (*p) > 90 ) {
			if ( 97 <= (*p) && (*p) <= 122 )
				goto tr78;
		} else if ( (*p) >= 65 )
			goto tr78;
	} else
		goto st43;
	goto tr65;
tr68:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
	goto st38;
st38:
	if ( ++p == pe )
		goto _out38;
case 38:
#line 1631 "rlscan.cpp"
	switch( (*p) ) {
		case 9: goto st38;
		case 10: goto tr68;
		case 32: goto st38;
	}
	goto tr82;
tr69:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st39;
st39:
	if ( ++p == pe )
		goto _out39;
case 39:
#line 1646 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr14;
		case 34: goto tr15;
		case 92: goto st9;
	}
	goto st8;
tr14:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
	goto st8;
st8:
	if ( ++p == pe )
		goto _out8;
case 8:
#line 1665 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr14;
		case 34: goto tr15;
		case 92: goto st9;
	}
	goto st8;
st9:
	if ( ++p == pe )
		goto _out9;
case 9:
	if ( (*p) == 10 )
		goto tr14;
	goto st8;
tr70:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st40;
st40:
	if ( ++p == pe )
		goto _out40;
case 40:
#line 1687 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr18;
		case 39: goto tr15;
		case 92: goto st11;
	}
	goto st10;
tr18:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
	goto st10;
st10:
	if ( ++p == pe )
		goto _out10;
case 10:
#line 1706 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr18;
		case 39: goto tr15;
		case 92: goto st11;
	}
	goto st10;
st11:
	if ( ++p == pe )
		goto _out11;
case 11:
	if ( (*p) == 10 )
		goto tr18;
	goto st10;
tr73:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st41;
st41:
	if ( ++p == pe )
		goto _out41;
case 41:
#line 1728 "rlscan.cpp"
	switch( (*p) ) {
		case 42: goto st12;
		case 47: goto st14;
	}
	goto tr83;
tr21:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
	goto st12;
st12:
	if ( ++p == pe )
		goto _out12;
case 12:
#line 1746 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr21;
		case 42: goto st13;
	}
	goto st12;
st13:
	if ( ++p == pe )
		goto _out13;
case 13:
	switch( (*p) ) {
		case 10: goto tr21;
		case 42: goto st13;
		case 47: goto tr23;
	}
	goto st12;
st14:
	if ( ++p == pe )
		goto _out14;
case 14:
	if ( (*p) == 10 )
		goto tr25;
	goto st14;
tr74:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st42;
st42:
	if ( ++p == pe )
		goto _out42;
case 42:
#line 1777 "rlscan.cpp"
	if ( (*p) == 120 )
		goto st15;
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st43;
	goto tr84;
st43:
	if ( ++p == pe )
		goto _out43;
case 43:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st43;
	goto tr84;
st15:
	if ( ++p == pe )
		goto _out15;
case 15:
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto st44;
	} else if ( (*p) > 70 ) {
		if ( 97 <= (*p) && (*p) <= 102 )
			goto st44;
	} else
		goto st44;
	goto tr26;
st44:
	if ( ++p == pe )
		goto _out44;
case 44:
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto st44;
	} else if ( (*p) > 70 ) {
		if ( 97 <= (*p) && (*p) <= 102 )
			goto st44;
	} else
		goto st44;
	goto tr86;
st45:
	if ( ++p == pe )
		goto _out45;
case 45:
	if ( (*p) == 58 )
		goto tr87;
	goto tr83;
tr78:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 600 "rlscan.rl"
	{act = 13;}
	goto st46;
tr102:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 595 "rlscan.rl"
	{act = 12;}
	goto st46;
tr107:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 587 "rlscan.rl"
	{act = 10;}
	goto st46;
tr109:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 566 "rlscan.rl"
	{act = 3;}
	goto st46;
tr114:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 568 "rlscan.rl"
	{act = 5;}
	goto st46;
tr116:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 578 "rlscan.rl"
	{act = 7;}
	goto st46;
tr119:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 579 "rlscan.rl"
	{act = 8;}
	goto st46;
tr122:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 574 "rlscan.rl"
	{act = 6;}
	goto st46;
tr125:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 583 "rlscan.rl"
	{act = 9;}
	goto st46;
tr126:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 564 "rlscan.rl"
	{act = 1;}
	goto st46;
tr128:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 591 "rlscan.rl"
	{act = 11;}
	goto st46;
tr132:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 567 "rlscan.rl"
	{act = 4;}
	goto st46;
st46:
	if ( ++p == pe )
		goto _out46;
case 46:
#line 1899 "rlscan.cpp"
	if ( (*p) == 95 )
		goto tr78;
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr88;
st47:
	if ( ++p == pe )
		goto _out47;
case 47:
	switch( (*p) ) {
		case 95: goto tr78;
		case 98: goto st48;
		case 99: goto st52;
		case 101: goto st57;
		case 103: goto st63;
		case 104: goto st66;
		case 110: goto st69;
		case 112: goto st72;
		case 114: goto st73;
		case 116: goto st75;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st48:
	if ( ++p == pe )
		goto _out48;
case 48:
	switch( (*p) ) {
		case 95: goto tr78;
		case 114: goto st49;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st49:
	if ( ++p == pe )
		goto _out49;
case 49:
	switch( (*p) ) {
		case 95: goto tr78;
		case 101: goto st50;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st50:
	if ( ++p == pe )
		goto _out50;
case 50:
	switch( (*p) ) {
		case 95: goto tr78;
		case 97: goto st51;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 98 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st51:
	if ( ++p == pe )
		goto _out51;
case 51:
	switch( (*p) ) {
		case 95: goto tr78;
		case 107: goto tr102;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st52:
	if ( ++p == pe )
		goto _out52;
case 52:
	switch( (*p) ) {
		case 95: goto tr78;
		case 97: goto st53;
		case 117: goto st55;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 98 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr103;
st53:
	if ( ++p == pe )
		goto _out53;
case 53:
	switch( (*p) ) {
		case 95: goto tr78;
		case 108: goto st54;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st54:
	if ( ++p == pe )
		goto _out54;
case 54:
	switch( (*p) ) {
		case 95: goto tr78;
		case 108: goto tr107;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st55:
	if ( ++p == pe )
		goto _out55;
case 55:
	switch( (*p) ) {
		case 95: goto tr78;
		case 114: goto st56;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st56:
	if ( ++p == pe )
		goto _out56;
case 56:
	switch( (*p) ) {
		case 95: goto tr78;
		case 115: goto tr109;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st57:
	if ( ++p == pe )
		goto _out57;
case 57:
	switch( (*p) ) {
		case 95: goto tr78;
		case 110: goto st58;
		case 120: goto st61;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st58:
	if ( ++p == pe )
		goto _out58;
case 58:
	switch( (*p) ) {
		case 95: goto tr78;
		case 116: goto st59;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st59:
	if ( ++p == pe )
		goto _out59;
case 59:
	switch( (*p) ) {
		case 95: goto tr78;
		case 114: goto st60;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st60:
	if ( ++p == pe )
		goto _out60;
case 60:
	switch( (*p) ) {
		case 95: goto tr78;
		case 121: goto tr114;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st61:
	if ( ++p == pe )
		goto _out61;
case 61:
	switch( (*p) ) {
		case 95: goto tr78;
		case 101: goto st62;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st62:
	if ( ++p == pe )
		goto _out62;
case 62:
	switch( (*p) ) {
		case 95: goto tr78;
		case 99: goto tr116;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st63:
	if ( ++p == pe )
		goto _out63;
case 63:
	switch( (*p) ) {
		case 95: goto tr78;
		case 111: goto st64;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st64:
	if ( ++p == pe )
		goto _out64;
case 64:
	switch( (*p) ) {
		case 95: goto tr78;
		case 116: goto st65;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st65:
	if ( ++p == pe )
		goto _out65;
case 65:
	switch( (*p) ) {
		case 95: goto tr78;
		case 111: goto tr119;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st66:
	if ( ++p == pe )
		goto _out66;
case 66:
	switch( (*p) ) {
		case 95: goto tr78;
		case 111: goto st67;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st67:
	if ( ++p == pe )
		goto _out67;
case 67:
	switch( (*p) ) {
		case 95: goto tr78;
		case 108: goto st68;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st68:
	if ( ++p == pe )
		goto _out68;
case 68:
	switch( (*p) ) {
		case 95: goto tr78;
		case 100: goto tr122;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st69:
	if ( ++p == pe )
		goto _out69;
case 69:
	switch( (*p) ) {
		case 95: goto tr78;
		case 101: goto st70;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st70:
	if ( ++p == pe )
		goto _out70;
case 70:
	switch( (*p) ) {
		case 95: goto tr78;
		case 120: goto st71;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st71:
	if ( ++p == pe )
		goto _out71;
case 71:
	switch( (*p) ) {
		case 95: goto tr78;
		case 116: goto tr125;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st72:
	if ( ++p == pe )
		goto _out72;
case 72:
	switch( (*p) ) {
		case 95: goto tr78;
		case 99: goto tr126;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st73:
	if ( ++p == pe )
		goto _out73;
case 73:
	switch( (*p) ) {
		case 95: goto tr78;
		case 101: goto st74;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st74:
	if ( ++p == pe )
		goto _out74;
case 74:
	switch( (*p) ) {
		case 95: goto tr78;
		case 116: goto tr128;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st75:
	if ( ++p == pe )
		goto _out75;
case 75:
	switch( (*p) ) {
		case 95: goto tr78;
		case 97: goto st76;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 98 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st76:
	if ( ++p == pe )
		goto _out76;
case 76:
	switch( (*p) ) {
		case 95: goto tr78;
		case 114: goto st77;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st77:
	if ( ++p == pe )
		goto _out77;
case 77:
	switch( (*p) ) {
		case 95: goto tr78;
		case 103: goto st78;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
st78:
	if ( ++p == pe )
		goto _out78;
case 78:
	switch( (*p) ) {
		case 95: goto tr78;
		case 115: goto tr132;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr78;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr78;
	} else
		goto tr78;
	goto tr89;
tr133:
#line 686 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st79;
tr134:
#line 681 "rlscan.rl"
	{tokend = p+1;{
			scan_error() << "unterminated OR literal" << endl;
		}{p = ((tokend))-1;}}
	goto st79;
tr135:
#line 676 "rlscan.rl"
	{tokend = p+1;{ token( RE_Dash, 0, 0 ); }{p = ((tokend))-1;}}
	goto st79;
tr137:
#line 679 "rlscan.rl"
	{tokend = p+1;{ token( RE_SqClose ); {{p = ((tokend))-1;}{cs = stack[--top]; goto _again;}} }{p = ((tokend))-1;}}
	goto st79;
tr138:
#line 673 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, tokstart+1, tokend ); }{p = ((tokend))-1;}}
	goto st79;
tr139:
#line 672 "rlscan.rl"
	{tokend = p+1;{ updateCol(); }{p = ((tokend))-1;}}
	goto st79;
tr140:
#line 664 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\0' ); }{p = ((tokend))-1;}}
	goto st79;
tr141:
#line 665 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\a' ); }{p = ((tokend))-1;}}
	goto st79;
tr142:
#line 666 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\b' ); }{p = ((tokend))-1;}}
	goto st79;
tr143:
#line 670 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\f' ); }{p = ((tokend))-1;}}
	goto st79;
tr144:
#line 668 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\n' ); }{p = ((tokend))-1;}}
	goto st79;
tr145:
#line 671 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\r' ); }{p = ((tokend))-1;}}
	goto st79;
tr146:
#line 667 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\t' ); }{p = ((tokend))-1;}}
	goto st79;
tr147:
#line 669 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\v' ); }{p = ((tokend))-1;}}
	goto st79;
st79:
#line 1 "rlscan.rl"
	{tokstart = 0;}
	if ( ++p == pe )
		goto _out79;
case 79:
#line 1 "rlscan.rl"
	{tokstart = p;}
#line 2531 "rlscan.cpp"
	switch( (*p) ) {
		case 0: goto tr134;
		case 45: goto tr135;
		case 92: goto st80;
		case 93: goto tr137;
	}
	goto tr133;
st80:
	if ( ++p == pe )
		goto _out80;
case 80:
	switch( (*p) ) {
		case 10: goto tr139;
		case 48: goto tr140;
		case 97: goto tr141;
		case 98: goto tr142;
		case 102: goto tr143;
		case 110: goto tr144;
		case 114: goto tr145;
		case 116: goto tr146;
		case 118: goto tr147;
	}
	goto tr138;
tr148:
#line 721 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st81;
tr149:
#line 716 "rlscan.rl"
	{tokend = p+1;{
			scan_error() << "unterminated regular expression" << endl;
		}{p = ((tokend))-1;}}
	goto st81;
tr150:
#line 711 "rlscan.rl"
	{tokend = p+1;{ token( RE_Star ); }{p = ((tokend))-1;}}
	goto st81;
tr151:
#line 710 "rlscan.rl"
	{tokend = p+1;{ token( RE_Dot ); }{p = ((tokend))-1;}}
	goto st81;
tr155:
#line 704 "rlscan.rl"
	{tokend = p;{ 
			token( RE_Slash, tokstart, tokend ); 
			{{p = ((tokend))-1;}{goto st88;}}
		}{p = ((tokend))-1;}}
	goto st81;
tr156:
#line 704 "rlscan.rl"
	{tokend = p+1;{ 
			token( RE_Slash, tokstart, tokend ); 
			{{p = ((tokend))-1;}{goto st88;}}
		}{p = ((tokend))-1;}}
	goto st81;
tr157:
#line 713 "rlscan.rl"
	{tokend = p;{ token( RE_SqOpen ); {{p = ((tokend))-1;}{stack[top++] = 81; goto st79;}} }{p = ((tokend))-1;}}
	goto st81;
tr158:
#line 714 "rlscan.rl"
	{tokend = p+1;{ token( RE_SqOpenNeg ); {{p = ((tokend))-1;}{stack[top++] = 81; goto st79;}} }{p = ((tokend))-1;}}
	goto st81;
tr159:
#line 701 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, tokstart+1, tokend ); }{p = ((tokend))-1;}}
	goto st81;
tr160:
#line 700 "rlscan.rl"
	{tokend = p+1;{ updateCol(); }{p = ((tokend))-1;}}
	goto st81;
tr161:
#line 692 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\0' ); }{p = ((tokend))-1;}}
	goto st81;
tr162:
#line 693 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\a' ); }{p = ((tokend))-1;}}
	goto st81;
tr163:
#line 694 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\b' ); }{p = ((tokend))-1;}}
	goto st81;
tr164:
#line 698 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\f' ); }{p = ((tokend))-1;}}
	goto st81;
tr165:
#line 696 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\n' ); }{p = ((tokend))-1;}}
	goto st81;
tr166:
#line 699 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\r' ); }{p = ((tokend))-1;}}
	goto st81;
tr167:
#line 695 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\t' ); }{p = ((tokend))-1;}}
	goto st81;
tr168:
#line 697 "rlscan.rl"
	{tokend = p+1;{ token( RE_Char, '\v' ); }{p = ((tokend))-1;}}
	goto st81;
st81:
#line 1 "rlscan.rl"
	{tokstart = 0;}
	if ( ++p == pe )
		goto _out81;
case 81:
#line 1 "rlscan.rl"
	{tokstart = p;}
#line 2643 "rlscan.cpp"
	switch( (*p) ) {
		case 0: goto tr149;
		case 42: goto tr150;
		case 46: goto tr151;
		case 47: goto st82;
		case 91: goto st83;
		case 92: goto st84;
	}
	goto tr148;
st82:
	if ( ++p == pe )
		goto _out82;
case 82:
	if ( (*p) == 105 )
		goto tr156;
	goto tr155;
st83:
	if ( ++p == pe )
		goto _out83;
case 83:
	if ( (*p) == 94 )
		goto tr158;
	goto tr157;
st84:
	if ( ++p == pe )
		goto _out84;
case 84:
	switch( (*p) ) {
		case 10: goto tr160;
		case 48: goto tr161;
		case 97: goto tr162;
		case 98: goto tr163;
		case 102: goto tr164;
		case 110: goto tr165;
		case 114: goto tr166;
		case 116: goto tr167;
		case 118: goto tr168;
	}
	goto tr159;
tr169:
#line 730 "rlscan.rl"
	{tokend = p+1;{
			scan_error() << "unterminated write statement" << endl;
		}{p = ((tokend))-1;}}
	goto st85;
tr172:
#line 728 "rlscan.rl"
	{tokend = p+1;{ token( ';' ); {{p = ((tokend))-1;}{goto st88;}} }{p = ((tokend))-1;}}
	goto st85;
tr174:
#line 727 "rlscan.rl"
	{tokend = p;{ updateCol(); }{p = ((tokend))-1;}}
	goto st85;
tr175:
#line 726 "rlscan.rl"
	{tokend = p;{ token( TK_Word, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st85;
st85:
#line 1 "rlscan.rl"
	{tokstart = 0;}
	if ( ++p == pe )
		goto _out85;
case 85:
#line 1 "rlscan.rl"
	{tokstart = p;}
#line 2709 "rlscan.cpp"
	switch( (*p) ) {
		case 0: goto tr169;
		case 32: goto st86;
		case 59: goto tr172;
		case 95: goto st87;
	}
	if ( (*p) < 65 ) {
		if ( 9 <= (*p) && (*p) <= 10 )
			goto st86;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto st87;
	} else
		goto st87;
	goto st0;
st0:
	goto _out0;
st86:
	if ( ++p == pe )
		goto _out86;
case 86:
	if ( (*p) == 32 )
		goto st86;
	if ( 9 <= (*p) && (*p) <= 10 )
		goto st86;
	goto tr174;
st87:
	if ( ++p == pe )
		goto _out87;
case 87:
	if ( (*p) == 95 )
		goto st87;
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto st87;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto st87;
	} else
		goto st87;
	goto tr175;
tr33:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
#line 790 "rlscan.rl"
	{tokend = p+1;{ updateCol(); }{p = ((tokend))-1;}}
	goto st88;
tr37:
#line 777 "rlscan.rl"
	{{ token( TK_UInt, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st88;
tr39:
#line 890 "rlscan.rl"
	{{ token( *tokstart ); }{p = ((tokend))-1;}}
	goto st88;
tr40:
#line 858 "rlscan.rl"
	{tokend = p+1;{ 
			updateCol();
			endSection();
			{{p = ((tokend))-1;}{goto st23;}}
		}{p = ((tokend))-1;}}
	goto st88;
tr176:
#line 890 "rlscan.rl"
	{tokend = p+1;{ token( *tokstart ); }{p = ((tokend))-1;}}
	goto st88;
tr177:
#line 886 "rlscan.rl"
	{tokend = p+1;{
			scan_error() << "unterminated ragel section" << endl;
		}{p = ((tokend))-1;}}
	goto st88;
tr179:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
#line 867 "rlscan.rl"
	{tokend = p+1;{
			updateCol();
			if ( singleLineSpec ) {
				endSection();
				{{p = ((tokend))-1;}{goto st23;}}
			}
		}{p = ((tokend))-1;}}
	goto st88;
tr188:
#line 787 "rlscan.rl"
	{tokend = p+1;{ token( RE_Slash ); {{p = ((tokend))-1;}{goto st81;}} }{p = ((tokend))-1;}}
	goto st88;
tr208:
#line 875 "rlscan.rl"
	{tokend = p+1;{ 
			if ( lastToken == KW_Export || lastToken == KW_Entry )
				token( '{' );
			else {
				token( '{' );
				curly_count = 1; 
				inlineBlockType = CurlyDelimited;
				{{p = ((tokend))-1;}{goto st37;}}
			}
		}{p = ((tokend))-1;}}
	goto st88;
tr211:
#line 864 "rlscan.rl"
	{tokend = p;{ updateCol(); }{p = ((tokend))-1;}}
	goto st88;
tr212:
#line 782 "rlscan.rl"
	{tokend = p;{ token( TK_Literal, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st88;
tr213:
#line 782 "rlscan.rl"
	{tokend = p+1;{ token( TK_Literal, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st88;
tr214:
#line 890 "rlscan.rl"
	{tokend = p;{ token( *tokstart ); }{p = ((tokend))-1;}}
	goto st88;
tr215:
#line 820 "rlscan.rl"
	{tokend = p+1;{ token( TK_AllGblError ); }{p = ((tokend))-1;}}
	goto st88;
tr216:
#line 804 "rlscan.rl"
	{tokend = p+1;{ token( TK_AllFromState ); }{p = ((tokend))-1;}}
	goto st88;
tr217:
#line 812 "rlscan.rl"
	{tokend = p+1;{ token( TK_AllEOF ); }{p = ((tokend))-1;}}
	goto st88;
tr218:
#line 839 "rlscan.rl"
	{tokend = p+1;{ token( TK_AllCond ); }{p = ((tokend))-1;}}
	goto st88;
tr219:
#line 828 "rlscan.rl"
	{tokend = p+1;{ token( TK_AllLocalError ); }{p = ((tokend))-1;}}
	goto st88;
tr220:
#line 796 "rlscan.rl"
	{tokend = p+1;{ token( TK_AllToState ); }{p = ((tokend))-1;}}
	goto st88;
tr221:
#line 821 "rlscan.rl"
	{tokend = p+1;{ token( TK_FinalGblError ); }{p = ((tokend))-1;}}
	goto st88;
tr222:
#line 805 "rlscan.rl"
	{tokend = p+1;{ token( TK_FinalFromState ); }{p = ((tokend))-1;}}
	goto st88;
tr223:
#line 813 "rlscan.rl"
	{tokend = p+1;{ token( TK_FinalEOF ); }{p = ((tokend))-1;}}
	goto st88;
tr224:
#line 840 "rlscan.rl"
	{tokend = p+1;{ token( TK_LeavingCond ); }{p = ((tokend))-1;}}
	goto st88;
tr225:
#line 829 "rlscan.rl"
	{tokend = p+1;{ token( TK_FinalLocalError ); }{p = ((tokend))-1;}}
	goto st88;
tr226:
#line 797 "rlscan.rl"
	{tokend = p+1;{ token( TK_FinalToState ); }{p = ((tokend))-1;}}
	goto st88;
tr227:
#line 843 "rlscan.rl"
	{tokend = p+1;{ token( TK_StarStar ); }{p = ((tokend))-1;}}
	goto st88;
tr228:
#line 844 "rlscan.rl"
	{tokend = p+1;{ token( TK_DashDash ); }{p = ((tokend))-1;}}
	goto st88;
tr229:
#line 845 "rlscan.rl"
	{tokend = p+1;{ token( TK_Arrow ); }{p = ((tokend))-1;}}
	goto st88;
tr230:
#line 842 "rlscan.rl"
	{tokend = p+1;{ token( TK_DotDot ); }{p = ((tokend))-1;}}
	goto st88;
tr231:
#line 777 "rlscan.rl"
	{tokend = p;{ token( TK_UInt, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st88;
tr233:
#line 778 "rlscan.rl"
	{tokend = p;{ token( TK_Hex, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st88;
tr234:
#line 856 "rlscan.rl"
	{tokend = p+1;{ token( TK_NameSep, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st88;
tr235:
#line 792 "rlscan.rl"
	{tokend = p+1;{ token( TK_ColonEquals ); }{p = ((tokend))-1;}}
	goto st88;
tr237:
#line 848 "rlscan.rl"
	{tokend = p;{ token( TK_ColonGt ); }{p = ((tokend))-1;}}
	goto st88;
tr238:
#line 849 "rlscan.rl"
	{tokend = p+1;{ token( TK_ColonGtGt ); }{p = ((tokend))-1;}}
	goto st88;
tr239:
#line 822 "rlscan.rl"
	{tokend = p+1;{ token( TK_NotStartGblError ); }{p = ((tokend))-1;}}
	goto st88;
tr240:
#line 806 "rlscan.rl"
	{tokend = p+1;{ token( TK_NotStartFromState ); }{p = ((tokend))-1;}}
	goto st88;
tr241:
#line 814 "rlscan.rl"
	{tokend = p+1;{ token( TK_NotStartEOF ); }{p = ((tokend))-1;}}
	goto st88;
tr242:
#line 850 "rlscan.rl"
	{tokend = p+1;{ token( TK_LtColon ); }{p = ((tokend))-1;}}
	goto st88;
tr244:
#line 830 "rlscan.rl"
	{tokend = p+1;{ token( TK_NotStartLocalError ); }{p = ((tokend))-1;}}
	goto st88;
tr245:
#line 798 "rlscan.rl"
	{tokend = p+1;{ token( TK_NotStartToState ); }{p = ((tokend))-1;}}
	goto st88;
tr246:
#line 835 "rlscan.rl"
	{tokend = p;{ token( TK_Middle ); }{p = ((tokend))-1;}}
	goto st88;
tr247:
#line 824 "rlscan.rl"
	{tokend = p+1;{ token( TK_MiddleGblError ); }{p = ((tokend))-1;}}
	goto st88;
tr248:
#line 808 "rlscan.rl"
	{tokend = p+1;{ token( TK_MiddleFromState ); }{p = ((tokend))-1;}}
	goto st88;
tr249:
#line 816 "rlscan.rl"
	{tokend = p+1;{ token( TK_MiddleEOF ); }{p = ((tokend))-1;}}
	goto st88;
tr250:
#line 832 "rlscan.rl"
	{tokend = p+1;{ token( TK_MiddleLocalError ); }{p = ((tokend))-1;}}
	goto st88;
tr251:
#line 800 "rlscan.rl"
	{tokend = p+1;{ token( TK_MiddleToState ); }{p = ((tokend))-1;}}
	goto st88;
tr252:
#line 846 "rlscan.rl"
	{tokend = p+1;{ token( TK_DoubleArrow ); }{p = ((tokend))-1;}}
	goto st88;
tr253:
#line 819 "rlscan.rl"
	{tokend = p+1;{ token( TK_StartGblError ); }{p = ((tokend))-1;}}
	goto st88;
tr254:
#line 803 "rlscan.rl"
	{tokend = p+1;{ token( TK_StartFromState ); }{p = ((tokend))-1;}}
	goto st88;
tr255:
#line 811 "rlscan.rl"
	{tokend = p+1;{ token( TK_StartEOF ); }{p = ((tokend))-1;}}
	goto st88;
tr256:
#line 838 "rlscan.rl"
	{tokend = p+1;{ token( TK_StartCond ); }{p = ((tokend))-1;}}
	goto st88;
tr257:
#line 827 "rlscan.rl"
	{tokend = p+1;{ token( TK_StartLocalError ); }{p = ((tokend))-1;}}
	goto st88;
tr258:
#line 795 "rlscan.rl"
	{tokend = p+1;{ token( TK_StartToState ); }{p = ((tokend))-1;}}
	goto st88;
tr259:
#line 823 "rlscan.rl"
	{tokend = p+1;{ token( TK_NotFinalGblError ); }{p = ((tokend))-1;}}
	goto st88;
tr260:
#line 807 "rlscan.rl"
	{tokend = p+1;{ token( TK_NotFinalFromState ); }{p = ((tokend))-1;}}
	goto st88;
tr261:
#line 815 "rlscan.rl"
	{tokend = p+1;{ token( TK_NotFinalEOF ); }{p = ((tokend))-1;}}
	goto st88;
tr262:
#line 831 "rlscan.rl"
	{tokend = p+1;{ token( TK_NotFinalLocalError ); }{p = ((tokend))-1;}}
	goto st88;
tr263:
#line 799 "rlscan.rl"
	{tokend = p+1;{ token( TK_NotFinalToState ); }{p = ((tokend))-1;}}
	goto st88;
tr264:
#line 1 "rlscan.rl"
	{	switch( act ) {
	case 62:
	{ token( KW_Machine ); }
	break;
	case 63:
	{ token( KW_Include ); }
	break;
	case 64:
	{ token( KW_Import ); }
	break;
	case 65:
	{ 
			token( KW_Write );
			{{p = ((tokend))-1;}{goto st85;}}
		}
	break;
	case 66:
	{ token( KW_Action ); }
	break;
	case 67:
	{ token( KW_AlphType ); }
	break;
	case 68:
	{ 
			token( KW_GetKey );
			inlineBlockType = SemiTerminated;
			{{p = ((tokend))-1;}{goto st37;}}
		}
	break;
	case 69:
	{ 
			token( KW_Access );
			inlineBlockType = SemiTerminated;
			{{p = ((tokend))-1;}{goto st37;}}
		}
	break;
	case 70:
	{ 
			token( KW_Variable );
			inlineBlockType = SemiTerminated;
			{{p = ((tokend))-1;}{goto st37;}}
		}
	break;
	case 71:
	{ token( KW_When ); }
	break;
	case 72:
	{ token( KW_Eof ); }
	break;
	case 73:
	{ token( KW_Err ); }
	break;
	case 74:
	{ token( KW_Lerr ); }
	break;
	case 75:
	{ token( KW_To ); }
	break;
	case 76:
	{ token( KW_From ); }
	break;
	case 77:
	{ token( KW_Export ); }
	break;
	case 78:
	{ token( TK_Word, tokstart, tokend ); }
	break;
	default: break;
	}
	{p = ((tokend))-1;}}
	goto st88;
tr265:
#line 784 "rlscan.rl"
	{tokend = p;{ token( RE_SqOpen ); {{p = ((tokend))-1;}{stack[top++] = 88; goto st79;}} }{p = ((tokend))-1;}}
	goto st88;
tr266:
#line 785 "rlscan.rl"
	{tokend = p+1;{ token( RE_SqOpenNeg ); {{p = ((tokend))-1;}{stack[top++] = 88; goto st79;}} }{p = ((tokend))-1;}}
	goto st88;
tr267:
#line 774 "rlscan.rl"
	{tokend = p;{ token( TK_Word, tokstart, tokend ); }{p = ((tokend))-1;}}
	goto st88;
tr336:
#line 853 "rlscan.rl"
	{tokend = p+1;{ token( TK_BarStar ); }{p = ((tokend))-1;}}
	goto st88;
st88:
#line 1 "rlscan.rl"
	{tokstart = 0;}
	if ( ++p == pe )
		goto _out88;
case 88:
#line 1 "rlscan.rl"
	{tokstart = p;}
#line 3117 "rlscan.cpp"
	switch( (*p) ) {
		case 0: goto tr177;
		case 9: goto st89;
		case 10: goto tr179;
		case 13: goto st89;
		case 32: goto st89;
		case 34: goto tr180;
		case 35: goto tr181;
		case 36: goto st93;
		case 37: goto st94;
		case 39: goto tr184;
		case 42: goto st96;
		case 45: goto st97;
		case 46: goto st98;
		case 47: goto tr188;
		case 48: goto tr189;
		case 58: goto st102;
		case 60: goto st104;
		case 61: goto st106;
		case 62: goto st107;
		case 64: goto st108;
		case 91: goto st110;
		case 95: goto tr196;
		case 97: goto st111;
		case 101: goto st125;
		case 102: goto st132;
		case 103: goto st135;
		case 105: goto st140;
		case 108: goto st150;
		case 109: goto st153;
		case 116: goto st159;
		case 118: goto st160;
		case 119: goto st167;
		case 123: goto tr208;
		case 124: goto st173;
		case 125: goto tr210;
	}
	if ( (*p) < 65 ) {
		if ( 49 <= (*p) && (*p) <= 57 )
			goto st100;
	} else if ( (*p) > 90 ) {
		if ( 98 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr176;
st89:
	if ( ++p == pe )
		goto _out89;
case 89:
	switch( (*p) ) {
		case 9: goto st89;
		case 13: goto st89;
		case 32: goto st89;
	}
	goto tr211;
tr180:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st90;
st90:
	if ( ++p == pe )
		goto _out90;
case 90:
#line 3182 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr29;
		case 34: goto st91;
		case 92: goto st17;
	}
	goto st16;
tr29:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
	goto st16;
st16:
	if ( ++p == pe )
		goto _out16;
case 16:
#line 3201 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr29;
		case 34: goto st91;
		case 92: goto st17;
	}
	goto st16;
st91:
	if ( ++p == pe )
		goto _out91;
case 91:
	if ( (*p) == 105 )
		goto tr213;
	goto tr212;
st17:
	if ( ++p == pe )
		goto _out17;
case 17:
	if ( (*p) == 10 )
		goto tr29;
	goto st16;
tr181:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st92;
st92:
	if ( ++p == pe )
		goto _out92;
case 92:
#line 3230 "rlscan.cpp"
	if ( (*p) == 10 )
		goto tr33;
	goto st18;
st18:
	if ( ++p == pe )
		goto _out18;
case 18:
	if ( (*p) == 10 )
		goto tr33;
	goto st18;
st93:
	if ( ++p == pe )
		goto _out93;
case 93:
	switch( (*p) ) {
		case 33: goto tr215;
		case 42: goto tr216;
		case 47: goto tr217;
		case 63: goto tr218;
		case 94: goto tr219;
		case 126: goto tr220;
	}
	goto tr214;
st94:
	if ( ++p == pe )
		goto _out94;
case 94:
	switch( (*p) ) {
		case 33: goto tr221;
		case 42: goto tr222;
		case 47: goto tr223;
		case 63: goto tr224;
		case 94: goto tr225;
		case 126: goto tr226;
	}
	goto tr214;
tr184:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st95;
st95:
	if ( ++p == pe )
		goto _out95;
case 95:
#line 3275 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr35;
		case 39: goto st91;
		case 92: goto st20;
	}
	goto st19;
tr35:
#line 532 "rlscan.rl"
	{ 
		lastnl = p; 
		column = 0;
		line++;
	}
	goto st19;
st19:
	if ( ++p == pe )
		goto _out19;
case 19:
#line 3294 "rlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr35;
		case 39: goto st91;
		case 92: goto st20;
	}
	goto st19;
st20:
	if ( ++p == pe )
		goto _out20;
case 20:
	if ( (*p) == 10 )
		goto tr35;
	goto st19;
st96:
	if ( ++p == pe )
		goto _out96;
case 96:
	if ( (*p) == 42 )
		goto tr227;
	goto tr214;
st97:
	if ( ++p == pe )
		goto _out97;
case 97:
	switch( (*p) ) {
		case 45: goto tr228;
		case 62: goto tr229;
	}
	goto tr214;
st98:
	if ( ++p == pe )
		goto _out98;
case 98:
	if ( (*p) == 46 )
		goto tr230;
	goto tr214;
tr189:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st99;
st99:
	if ( ++p == pe )
		goto _out99;
case 99:
#line 3339 "rlscan.cpp"
	if ( (*p) == 120 )
		goto st21;
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st100;
	goto tr231;
st100:
	if ( ++p == pe )
		goto _out100;
case 100:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st100;
	goto tr231;
st21:
	if ( ++p == pe )
		goto _out21;
case 21:
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto st101;
	} else if ( (*p) > 70 ) {
		if ( 97 <= (*p) && (*p) <= 102 )
			goto st101;
	} else
		goto st101;
	goto tr37;
st101:
	if ( ++p == pe )
		goto _out101;
case 101:
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto st101;
	} else if ( (*p) > 70 ) {
		if ( 97 <= (*p) && (*p) <= 102 )
			goto st101;
	} else
		goto st101;
	goto tr233;
st102:
	if ( ++p == pe )
		goto _out102;
case 102:
	switch( (*p) ) {
		case 58: goto tr234;
		case 61: goto tr235;
		case 62: goto st103;
	}
	goto tr214;
st103:
	if ( ++p == pe )
		goto _out103;
case 103:
	if ( (*p) == 62 )
		goto tr238;
	goto tr237;
st104:
	if ( ++p == pe )
		goto _out104;
case 104:
	switch( (*p) ) {
		case 33: goto tr239;
		case 42: goto tr240;
		case 47: goto tr241;
		case 58: goto tr242;
		case 62: goto st105;
		case 94: goto tr244;
		case 126: goto tr245;
	}
	goto tr214;
st105:
	if ( ++p == pe )
		goto _out105;
case 105:
	switch( (*p) ) {
		case 33: goto tr247;
		case 42: goto tr248;
		case 47: goto tr249;
		case 94: goto tr250;
		case 126: goto tr251;
	}
	goto tr246;
st106:
	if ( ++p == pe )
		goto _out106;
case 106:
	if ( (*p) == 62 )
		goto tr252;
	goto tr214;
st107:
	if ( ++p == pe )
		goto _out107;
case 107:
	switch( (*p) ) {
		case 33: goto tr253;
		case 42: goto tr254;
		case 47: goto tr255;
		case 63: goto tr256;
		case 94: goto tr257;
		case 126: goto tr258;
	}
	goto tr214;
st108:
	if ( ++p == pe )
		goto _out108;
case 108:
	switch( (*p) ) {
		case 33: goto tr259;
		case 42: goto tr260;
		case 47: goto tr261;
		case 94: goto tr262;
		case 126: goto tr263;
	}
	goto tr214;
tr196:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 774 "rlscan.rl"
	{act = 78;}
	goto st109;
tr274:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 755 "rlscan.rl"
	{act = 69;}
	goto st109;
tr277:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 744 "rlscan.rl"
	{act = 66;}
	goto st109;
tr283:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 745 "rlscan.rl"
	{act = 67;}
	goto st109;
tr287:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 766 "rlscan.rl"
	{act = 72;}
	goto st109;
tr288:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 767 "rlscan.rl"
	{act = 73;}
	goto st109;
tr292:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 771 "rlscan.rl"
	{act = 77;}
	goto st109;
tr295:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 770 "rlscan.rl"
	{act = 76;}
	goto st109;
tr300:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 750 "rlscan.rl"
	{act = 68;}
	goto st109;
tr306:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 739 "rlscan.rl"
	{act = 64;}
	goto st109;
tr311:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 738 "rlscan.rl"
	{act = 63;}
	goto st109;
tr314:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 768 "rlscan.rl"
	{act = 74;}
	goto st109;
tr320:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 737 "rlscan.rl"
	{act = 62;}
	goto st109;
tr321:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 769 "rlscan.rl"
	{act = 75;}
	goto st109;
tr328:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 760 "rlscan.rl"
	{act = 70;}
	goto st109;
tr332:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 765 "rlscan.rl"
	{act = 71;}
	goto st109;
tr335:
#line 1 "rlscan.rl"
	{tokend = p+1;}
#line 740 "rlscan.rl"
	{act = 65;}
	goto st109;
st109:
	if ( ++p == pe )
		goto _out109;
case 109:
#line 3559 "rlscan.cpp"
	if ( (*p) == 95 )
		goto tr196;
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr264;
st110:
	if ( ++p == pe )
		goto _out110;
case 110:
	if ( (*p) == 94 )
		goto tr266;
	goto tr265;
st111:
	if ( ++p == pe )
		goto _out111;
case 111:
	switch( (*p) ) {
		case 95: goto tr196;
		case 99: goto st112;
		case 108: goto st119;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st112:
	if ( ++p == pe )
		goto _out112;
case 112:
	switch( (*p) ) {
		case 95: goto tr196;
		case 99: goto st113;
		case 116: goto st116;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st113:
	if ( ++p == pe )
		goto _out113;
case 113:
	switch( (*p) ) {
		case 95: goto tr196;
		case 101: goto st114;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st114:
	if ( ++p == pe )
		goto _out114;
case 114:
	switch( (*p) ) {
		case 95: goto tr196;
		case 115: goto st115;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st115:
	if ( ++p == pe )
		goto _out115;
case 115:
	switch( (*p) ) {
		case 95: goto tr196;
		case 115: goto tr274;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st116:
	if ( ++p == pe )
		goto _out116;
case 116:
	switch( (*p) ) {
		case 95: goto tr196;
		case 105: goto st117;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st117:
	if ( ++p == pe )
		goto _out117;
case 117:
	switch( (*p) ) {
		case 95: goto tr196;
		case 111: goto st118;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st118:
	if ( ++p == pe )
		goto _out118;
case 118:
	switch( (*p) ) {
		case 95: goto tr196;
		case 110: goto tr277;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st119:
	if ( ++p == pe )
		goto _out119;
case 119:
	switch( (*p) ) {
		case 95: goto tr196;
		case 112: goto st120;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st120:
	if ( ++p == pe )
		goto _out120;
case 120:
	switch( (*p) ) {
		case 95: goto tr196;
		case 104: goto st121;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st121:
	if ( ++p == pe )
		goto _out121;
case 121:
	switch( (*p) ) {
		case 95: goto tr196;
		case 116: goto st122;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st122:
	if ( ++p == pe )
		goto _out122;
case 122:
	switch( (*p) ) {
		case 95: goto tr196;
		case 121: goto st123;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st123:
	if ( ++p == pe )
		goto _out123;
case 123:
	switch( (*p) ) {
		case 95: goto tr196;
		case 112: goto st124;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st124:
	if ( ++p == pe )
		goto _out124;
case 124:
	switch( (*p) ) {
		case 95: goto tr196;
		case 101: goto tr283;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st125:
	if ( ++p == pe )
		goto _out125;
case 125:
	switch( (*p) ) {
		case 95: goto tr196;
		case 111: goto st126;
		case 114: goto st127;
		case 120: goto st128;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st126:
	if ( ++p == pe )
		goto _out126;
case 126:
	switch( (*p) ) {
		case 95: goto tr196;
		case 102: goto tr287;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st127:
	if ( ++p == pe )
		goto _out127;
case 127:
	switch( (*p) ) {
		case 95: goto tr196;
		case 114: goto tr288;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st128:
	if ( ++p == pe )
		goto _out128;
case 128:
	switch( (*p) ) {
		case 95: goto tr196;
		case 112: goto st129;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st129:
	if ( ++p == pe )
		goto _out129;
case 129:
	switch( (*p) ) {
		case 95: goto tr196;
		case 111: goto st130;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st130:
	if ( ++p == pe )
		goto _out130;
case 130:
	switch( (*p) ) {
		case 95: goto tr196;
		case 114: goto st131;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st131:
	if ( ++p == pe )
		goto _out131;
case 131:
	switch( (*p) ) {
		case 95: goto tr196;
		case 116: goto tr292;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st132:
	if ( ++p == pe )
		goto _out132;
case 132:
	switch( (*p) ) {
		case 95: goto tr196;
		case 114: goto st133;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st133:
	if ( ++p == pe )
		goto _out133;
case 133:
	switch( (*p) ) {
		case 95: goto tr196;
		case 111: goto st134;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st134:
	if ( ++p == pe )
		goto _out134;
case 134:
	switch( (*p) ) {
		case 95: goto tr196;
		case 109: goto tr295;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st135:
	if ( ++p == pe )
		goto _out135;
case 135:
	switch( (*p) ) {
		case 95: goto tr196;
		case 101: goto st136;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st136:
	if ( ++p == pe )
		goto _out136;
case 136:
	switch( (*p) ) {
		case 95: goto tr196;
		case 116: goto st137;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st137:
	if ( ++p == pe )
		goto _out137;
case 137:
	switch( (*p) ) {
		case 95: goto tr196;
		case 107: goto st138;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st138:
	if ( ++p == pe )
		goto _out138;
case 138:
	switch( (*p) ) {
		case 95: goto tr196;
		case 101: goto st139;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st139:
	if ( ++p == pe )
		goto _out139;
case 139:
	switch( (*p) ) {
		case 95: goto tr196;
		case 121: goto tr300;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st140:
	if ( ++p == pe )
		goto _out140;
case 140:
	switch( (*p) ) {
		case 95: goto tr196;
		case 109: goto st141;
		case 110: goto st145;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st141:
	if ( ++p == pe )
		goto _out141;
case 141:
	switch( (*p) ) {
		case 95: goto tr196;
		case 112: goto st142;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st142:
	if ( ++p == pe )
		goto _out142;
case 142:
	switch( (*p) ) {
		case 95: goto tr196;
		case 111: goto st143;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st143:
	if ( ++p == pe )
		goto _out143;
case 143:
	switch( (*p) ) {
		case 95: goto tr196;
		case 114: goto st144;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st144:
	if ( ++p == pe )
		goto _out144;
case 144:
	switch( (*p) ) {
		case 95: goto tr196;
		case 116: goto tr306;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st145:
	if ( ++p == pe )
		goto _out145;
case 145:
	switch( (*p) ) {
		case 95: goto tr196;
		case 99: goto st146;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st146:
	if ( ++p == pe )
		goto _out146;
case 146:
	switch( (*p) ) {
		case 95: goto tr196;
		case 108: goto st147;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st147:
	if ( ++p == pe )
		goto _out147;
case 147:
	switch( (*p) ) {
		case 95: goto tr196;
		case 117: goto st148;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st148:
	if ( ++p == pe )
		goto _out148;
case 148:
	switch( (*p) ) {
		case 95: goto tr196;
		case 100: goto st149;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st149:
	if ( ++p == pe )
		goto _out149;
case 149:
	switch( (*p) ) {
		case 95: goto tr196;
		case 101: goto tr311;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st150:
	if ( ++p == pe )
		goto _out150;
case 150:
	switch( (*p) ) {
		case 95: goto tr196;
		case 101: goto st151;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st151:
	if ( ++p == pe )
		goto _out151;
case 151:
	switch( (*p) ) {
		case 95: goto tr196;
		case 114: goto st152;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st152:
	if ( ++p == pe )
		goto _out152;
case 152:
	switch( (*p) ) {
		case 95: goto tr196;
		case 114: goto tr314;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st153:
	if ( ++p == pe )
		goto _out153;
case 153:
	switch( (*p) ) {
		case 95: goto tr196;
		case 97: goto st154;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 98 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st154:
	if ( ++p == pe )
		goto _out154;
case 154:
	switch( (*p) ) {
		case 95: goto tr196;
		case 99: goto st155;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st155:
	if ( ++p == pe )
		goto _out155;
case 155:
	switch( (*p) ) {
		case 95: goto tr196;
		case 104: goto st156;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st156:
	if ( ++p == pe )
		goto _out156;
case 156:
	switch( (*p) ) {
		case 95: goto tr196;
		case 105: goto st157;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st157:
	if ( ++p == pe )
		goto _out157;
case 157:
	switch( (*p) ) {
		case 95: goto tr196;
		case 110: goto st158;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st158:
	if ( ++p == pe )
		goto _out158;
case 158:
	switch( (*p) ) {
		case 95: goto tr196;
		case 101: goto tr320;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st159:
	if ( ++p == pe )
		goto _out159;
case 159:
	switch( (*p) ) {
		case 95: goto tr196;
		case 111: goto tr321;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st160:
	if ( ++p == pe )
		goto _out160;
case 160:
	switch( (*p) ) {
		case 95: goto tr196;
		case 97: goto st161;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 98 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st161:
	if ( ++p == pe )
		goto _out161;
case 161:
	switch( (*p) ) {
		case 95: goto tr196;
		case 114: goto st162;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st162:
	if ( ++p == pe )
		goto _out162;
case 162:
	switch( (*p) ) {
		case 95: goto tr196;
		case 105: goto st163;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st163:
	if ( ++p == pe )
		goto _out163;
case 163:
	switch( (*p) ) {
		case 95: goto tr196;
		case 97: goto st164;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 98 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st164:
	if ( ++p == pe )
		goto _out164;
case 164:
	switch( (*p) ) {
		case 95: goto tr196;
		case 98: goto st165;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st165:
	if ( ++p == pe )
		goto _out165;
case 165:
	switch( (*p) ) {
		case 95: goto tr196;
		case 108: goto st166;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st166:
	if ( ++p == pe )
		goto _out166;
case 166:
	switch( (*p) ) {
		case 95: goto tr196;
		case 101: goto tr328;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st167:
	if ( ++p == pe )
		goto _out167;
case 167:
	switch( (*p) ) {
		case 95: goto tr196;
		case 104: goto st168;
		case 114: goto st170;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st168:
	if ( ++p == pe )
		goto _out168;
case 168:
	switch( (*p) ) {
		case 95: goto tr196;
		case 101: goto st169;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st169:
	if ( ++p == pe )
		goto _out169;
case 169:
	switch( (*p) ) {
		case 95: goto tr196;
		case 110: goto tr332;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st170:
	if ( ++p == pe )
		goto _out170;
case 170:
	switch( (*p) ) {
		case 95: goto tr196;
		case 105: goto st171;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st171:
	if ( ++p == pe )
		goto _out171;
case 171:
	switch( (*p) ) {
		case 95: goto tr196;
		case 116: goto st172;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st172:
	if ( ++p == pe )
		goto _out172;
case 172:
	switch( (*p) ) {
		case 95: goto tr196;
		case 101: goto tr335;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr196;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr196;
	} else
		goto tr196;
	goto tr267;
st173:
	if ( ++p == pe )
		goto _out173;
case 173:
	if ( (*p) == 42 )
		goto tr336;
	goto tr214;
tr210:
#line 1 "rlscan.rl"
	{tokend = p+1;}
	goto st174;
st174:
	if ( ++p == pe )
		goto _out174;
case 174:
#line 4653 "rlscan.cpp"
	if ( (*p) == 37 )
		goto st22;
	goto tr214;
st22:
	if ( ++p == pe )
		goto _out22;
case 22:
	if ( (*p) == 37 )
		goto tr40;
	goto tr39;
	}
	_out23: cs = 23; goto _out; 
	_out24: cs = 24; goto _out; 
	_out25: cs = 25; goto _out; 
	_out1: cs = 1; goto _out; 
	_out2: cs = 2; goto _out; 
	_out26: cs = 26; goto _out; 
	_out27: cs = 27; goto _out; 
	_out28: cs = 28; goto _out; 
	_out3: cs = 3; goto _out; 
	_out4: cs = 4; goto _out; 
	_out29: cs = 29; goto _out; 
	_out5: cs = 5; goto _out; 
	_out6: cs = 6; goto _out; 
	_out7: cs = 7; goto _out; 
	_out30: cs = 30; goto _out; 
	_out31: cs = 31; goto _out; 
	_out32: cs = 32; goto _out; 
	_out33: cs = 33; goto _out; 
	_out34: cs = 34; goto _out; 
	_out35: cs = 35; goto _out; 
	_out36: cs = 36; goto _out; 
	_out37: cs = 37; goto _out; 
	_out38: cs = 38; goto _out; 
	_out39: cs = 39; goto _out; 
	_out8: cs = 8; goto _out; 
	_out9: cs = 9; goto _out; 
	_out40: cs = 40; goto _out; 
	_out10: cs = 10; goto _out; 
	_out11: cs = 11; goto _out; 
	_out41: cs = 41; goto _out; 
	_out12: cs = 12; goto _out; 
	_out13: cs = 13; goto _out; 
	_out14: cs = 14; goto _out; 
	_out42: cs = 42; goto _out; 
	_out43: cs = 43; goto _out; 
	_out15: cs = 15; goto _out; 
	_out44: cs = 44; goto _out; 
	_out45: cs = 45; goto _out; 
	_out46: cs = 46; goto _out; 
	_out47: cs = 47; goto _out; 
	_out48: cs = 48; goto _out; 
	_out49: cs = 49; goto _out; 
	_out50: cs = 50; goto _out; 
	_out51: cs = 51; goto _out; 
	_out52: cs = 52; goto _out; 
	_out53: cs = 53; goto _out; 
	_out54: cs = 54; goto _out; 
	_out55: cs = 55; goto _out; 
	_out56: cs = 56; goto _out; 
	_out57: cs = 57; goto _out; 
	_out58: cs = 58; goto _out; 
	_out59: cs = 59; goto _out; 
	_out60: cs = 60; goto _out; 
	_out61: cs = 61; goto _out; 
	_out62: cs = 62; goto _out; 
	_out63: cs = 63; goto _out; 
	_out64: cs = 64; goto _out; 
	_out65: cs = 65; goto _out; 
	_out66: cs = 66; goto _out; 
	_out67: cs = 67; goto _out; 
	_out68: cs = 68; goto _out; 
	_out69: cs = 69; goto _out; 
	_out70: cs = 70; goto _out; 
	_out71: cs = 71; goto _out; 
	_out72: cs = 72; goto _out; 
	_out73: cs = 73; goto _out; 
	_out74: cs = 74; goto _out; 
	_out75: cs = 75; goto _out; 
	_out76: cs = 76; goto _out; 
	_out77: cs = 77; goto _out; 
	_out78: cs = 78; goto _out; 
	_out79: cs = 79; goto _out; 
	_out80: cs = 80; goto _out; 
	_out81: cs = 81; goto _out; 
	_out82: cs = 82; goto _out; 
	_out83: cs = 83; goto _out; 
	_out84: cs = 84; goto _out; 
	_out85: cs = 85; goto _out; 
	_out0: cs = 0; goto _out; 
	_out86: cs = 86; goto _out; 
	_out87: cs = 87; goto _out; 
	_out88: cs = 88; goto _out; 
	_out89: cs = 89; goto _out; 
	_out90: cs = 90; goto _out; 
	_out16: cs = 16; goto _out; 
	_out91: cs = 91; goto _out; 
	_out17: cs = 17; goto _out; 
	_out92: cs = 92; goto _out; 
	_out18: cs = 18; goto _out; 
	_out93: cs = 93; goto _out; 
	_out94: cs = 94; goto _out; 
	_out95: cs = 95; goto _out; 
	_out19: cs = 19; goto _out; 
	_out20: cs = 20; goto _out; 
	_out96: cs = 96; goto _out; 
	_out97: cs = 97; goto _out; 
	_out98: cs = 98; goto _out; 
	_out99: cs = 99; goto _out; 
	_out100: cs = 100; goto _out; 
	_out21: cs = 21; goto _out; 
	_out101: cs = 101; goto _out; 
	_out102: cs = 102; goto _out; 
	_out103: cs = 103; goto _out; 
	_out104: cs = 104; goto _out; 
	_out105: cs = 105; goto _out; 
	_out106: cs = 106; goto _out; 
	_out107: cs = 107; goto _out; 
	_out108: cs = 108; goto _out; 
	_out109: cs = 109; goto _out; 
	_out110: cs = 110; goto _out; 
	_out111: cs = 111; goto _out; 
	_out112: cs = 112; goto _out; 
	_out113: cs = 113; goto _out; 
	_out114: cs = 114; goto _out; 
	_out115: cs = 115; goto _out; 
	_out116: cs = 116; goto _out; 
	_out117: cs = 117; goto _out; 
	_out118: cs = 118; goto _out; 
	_out119: cs = 119; goto _out; 
	_out120: cs = 120; goto _out; 
	_out121: cs = 121; goto _out; 
	_out122: cs = 122; goto _out; 
	_out123: cs = 123; goto _out; 
	_out124: cs = 124; goto _out; 
	_out125: cs = 125; goto _out; 
	_out126: cs = 126; goto _out; 
	_out127: cs = 127; goto _out; 
	_out128: cs = 128; goto _out; 
	_out129: cs = 129; goto _out; 
	_out130: cs = 130; goto _out; 
	_out131: cs = 131; goto _out; 
	_out132: cs = 132; goto _out; 
	_out133: cs = 133; goto _out; 
	_out134: cs = 134; goto _out; 
	_out135: cs = 135; goto _out; 
	_out136: cs = 136; goto _out; 
	_out137: cs = 137; goto _out; 
	_out138: cs = 138; goto _out; 
	_out139: cs = 139; goto _out; 
	_out140: cs = 140; goto _out; 
	_out141: cs = 141; goto _out; 
	_out142: cs = 142; goto _out; 
	_out143: cs = 143; goto _out; 
	_out144: cs = 144; goto _out; 
	_out145: cs = 145; goto _out; 
	_out146: cs = 146; goto _out; 
	_out147: cs = 147; goto _out; 
	_out148: cs = 148; goto _out; 
	_out149: cs = 149; goto _out; 
	_out150: cs = 150; goto _out; 
	_out151: cs = 151; goto _out; 
	_out152: cs = 152; goto _out; 
	_out153: cs = 153; goto _out; 
	_out154: cs = 154; goto _out; 
	_out155: cs = 155; goto _out; 
	_out156: cs = 156; goto _out; 
	_out157: cs = 157; goto _out; 
	_out158: cs = 158; goto _out; 
	_out159: cs = 159; goto _out; 
	_out160: cs = 160; goto _out; 
	_out161: cs = 161; goto _out; 
	_out162: cs = 162; goto _out; 
	_out163: cs = 163; goto _out; 
	_out164: cs = 164; goto _out; 
	_out165: cs = 165; goto _out; 
	_out166: cs = 166; goto _out; 
	_out167: cs = 167; goto _out; 
	_out168: cs = 168; goto _out; 
	_out169: cs = 169; goto _out; 
	_out170: cs = 170; goto _out; 
	_out171: cs = 171; goto _out; 
	_out172: cs = 172; goto _out; 
	_out173: cs = 173; goto _out; 
	_out174: cs = 174; goto _out; 
	_out22: cs = 22; goto _out; 

	_out: {}
	}
#line 972 "rlscan.rl"

		/* Check if we failed. */
		if ( cs == rlscan_error ) {
			/* Machine failed before finding a token. I'm not yet sure if this
			 * is reachable. */
			scan_error() << "scanner error" << endl;
			exit(1);
		}

		/* Decide if we need to preserve anything. */
		char *preserve = tokstart;

		/* Now set up the prefix. */
		if ( preserve == 0 )
			have = 0;
		else {
			/* There is data that needs to be shifted over. */
			have = pe - preserve;
			memmove( buf, preserve, have );
			unsigned int shiftback = preserve - buf;
			if ( tokstart != 0 )
				tokstart -= shiftback;
			tokend -= shiftback;

			preserve = buf;
		}
	}

	delete[] buf;
}

void scan( char *fileName, istream &input, ostream &output )
{
}
