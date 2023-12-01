#line 1 "xmlscan.rl"
/*
 *  Copyright 2001-2007 Adrian Thurston <thurston@cs.queensu.ca>
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
#include <string.h>
#include "vector.h"
#include "xmlparse.h"
#include "buffer.h"

using std::istream;
using std::cout;
using std::cerr;
using std::endl;

#define BUFSIZE 4096


#line 37 "xmlscan.cpp"
static const int Scanner_start = 20;

static const int Scanner_first_final = 20;

static const int Scanner_error = 0;

#line 37 "xmlscan.rl"

#include "phash.h"

struct Scanner
{
	Scanner(const char *fileName, istream &input ) : 
		fileName(fileName),
		input(input), 
		curline(1), 
		curcol(1),
		p(0), pe(0), 
		done(false),
		data(0), data_len(0),
		value(0)
	{
		
#line 69 "xmlscan.cpp"
	{
	cs = Scanner_start;
	tokstart = 0;
	tokend = 0;
	act = 0;
	}
#line 63 "xmlscan.rl"

	}
	
	int scan();
	void adjustAttrPointers( int distance );
	std::ostream &error();

	const char *fileName;
	istream &input;

	/* Scanner State. */
	int cs, act, have, curline, curcol;
	char *tokstart, *tokend;
	char *p, *pe;
	int done;

	/* Token data */
	char *data;
	int data_len;
	int value;
	AttrMkList attrMkList;
	Buffer buffer;
	char *tag_id_start;
	int tag_id_len;
	int token_col, token_line;

	char buf[BUFSIZE];
};


#define TK_NO_TOKEN (-1)
#define TK_ERR 1
#define TK_SPACE 2
#define TK_EOF 3
#define TK_OpenTag 4
#define TK_CloseTag 5

#define ret_tok( _tok ) token = (_tok); data = tokstart

void Scanner::adjustAttrPointers( int distance )
{
	for ( AttrMkList::Iter attr = attrMkList; attr.lte(); attr++ ) {
		attr->id -= distance;
		attr->value -= distance;
	}
}

/* There is no claim that this is a proper XML parser, but it is good
 * enough for our purposes. */
#line 178 "xmlscan.rl"


int Scanner::scan( )
{
	int token = TK_NO_TOKEN;
	int space = 0, readlen = 0;
	char *attr_id_start = 0;
	char *attr_value_start = 0;
	int attr_id_len = 0;
	int attr_value_len = 0;

	attrMkList.empty();
	buffer.clear();

	while ( 1 ) {
		if ( p == pe ) {
			//printf("scanner: need more data\n");

			if ( tokstart == 0 )
				have = 0;
			else {
				/* There is data that needs to be shifted over. */
				//printf("scanner: buffer broken mid token\n");
				have = pe - tokstart;
				memmove( buf, tokstart, have );

				int distance = tokstart - buf;
				tokend -= distance;
				tag_id_start -= distance;
				attr_id_start -= distance;
				attr_value_start -= distance;
				adjustAttrPointers( distance );
				tokstart = buf;
			}

			p = buf + have;
			space = BUFSIZE - have;

			if ( space == 0 ) {
				/* We filled up the buffer trying to scan a token. */
				return TK_SPACE;
			}

			if ( done ) {
				//printf("scanner: end of file\n");
				p[0] = 0;
				readlen = 1;
			}
			else {
				input.read( p, space );
				readlen = input.gcount();
				if ( input.eof() ) {
					//printf("scanner: setting done flag\n");
					done = 1;
				}
			}

			pe = p + readlen;
		}

		
#line 188 "xmlscan.cpp"
	{
	if ( p == pe )
		goto _out;
	switch ( cs )
	{
tr6:
#line 115 "xmlscan.rl"
	{ curcol++; }
#line 168 "xmlscan.rl"
	{tokend = p+1;{ buffer.append( '&' ); }{p = ((tokend))-1;}}
	goto st20;
tr8:
#line 115 "xmlscan.rl"
	{ curcol++; }
#line 172 "xmlscan.rl"
	{tokend = p+1;{ buffer.append( '>' ); }{p = ((tokend))-1;}}
	goto st20;
tr10:
#line 115 "xmlscan.rl"
	{ curcol++; }
#line 170 "xmlscan.rl"
	{tokend = p+1;{ buffer.append( '<' ); }{p = ((tokend))-1;}}
	goto st20;
tr20:
#line 150 "xmlscan.rl"
	{ tag_id_len = p - tag_id_start; }
#line 115 "xmlscan.rl"
	{ curcol++; }
#line 160 "xmlscan.rl"
	{tokend = p+1;{ ret_tok( TK_CloseTag ); {{p = ((tokend))-1;}goto _out20;} }{p = ((tokend))-1;}}
	goto st20;
tr23:
#line 115 "xmlscan.rl"
	{ curcol++; }
#line 160 "xmlscan.rl"
	{tokend = p+1;{ ret_tok( TK_CloseTag ); {{p = ((tokend))-1;}goto _out20;} }{p = ((tokend))-1;}}
	goto st20;
tr27:
#line 150 "xmlscan.rl"
	{ tag_id_len = p - tag_id_start; }
#line 115 "xmlscan.rl"
	{ curcol++; }
#line 157 "xmlscan.rl"
	{tokend = p+1;{ ret_tok( TK_OpenTag ); {{p = ((tokend))-1;}goto _out20;} }{p = ((tokend))-1;}}
	goto st20;
tr30:
#line 115 "xmlscan.rl"
	{ curcol++; }
#line 157 "xmlscan.rl"
	{tokend = p+1;{ ret_tok( TK_OpenTag ); {{p = ((tokend))-1;}goto _out20;} }{p = ((tokend))-1;}}
	goto st20;
tr46:
#line 132 "xmlscan.rl"
	{
		attr_value_len = p - attr_value_start;

		AttrMarker newAttr;
		newAttr.id = attr_id_start;
		newAttr.idLen = attr_id_len;
		newAttr.value = attr_value_start;
		newAttr.valueLen = attr_value_len;
		attrMkList.append( newAttr );
	}
#line 115 "xmlscan.rl"
	{ curcol++; }
#line 157 "xmlscan.rl"
	{tokend = p+1;{ ret_tok( TK_OpenTag ); {{p = ((tokend))-1;}goto _out20;} }{p = ((tokend))-1;}}
	goto st20;
tr48:
#line 115 "xmlscan.rl"
	{ curcol++; }
#line 164 "xmlscan.rl"
	{tokend = p+1;{ buffer.append( *p ); }{p = ((tokend))-1;}}
	goto st20;
tr49:
#line 116 "xmlscan.rl"
	{ token_col = curcol; token_line = curline; }
#line 175 "xmlscan.rl"
	{tokend = p+1;{ ret_tok( TK_EOF ); {{p = ((tokend))-1;}goto _out20;} }{p = ((tokend))-1;}}
	goto st20;
tr50:
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
#line 164 "xmlscan.rl"
	{tokend = p+1;{ buffer.append( *p ); }{p = ((tokend))-1;}}
	goto st20;
st20:
#line 1 "xmlscan.rl"
	{tokstart = 0;}
	if ( ++p == pe )
		goto _out20;
case 20:
#line 1 "xmlscan.rl"
	{tokstart = p;}
#line 285 "xmlscan.cpp"
	switch( (*p) ) {
		case 0: goto tr49;
		case 10: goto tr50;
		case 38: goto tr51;
		case 60: goto tr52;
	}
	goto tr48;
tr51:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st1;
st1:
	if ( ++p == pe )
		goto _out1;
case 1:
#line 301 "xmlscan.cpp"
	switch( (*p) ) {
		case 97: goto tr0;
		case 103: goto tr2;
		case 108: goto tr3;
	}
	goto st0;
st0:
	goto _out0;
tr0:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st2;
st2:
	if ( ++p == pe )
		goto _out2;
case 2:
#line 318 "xmlscan.cpp"
	if ( (*p) == 109 )
		goto tr4;
	goto st0;
tr4:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st3;
st3:
	if ( ++p == pe )
		goto _out3;
case 3:
#line 330 "xmlscan.cpp"
	if ( (*p) == 112 )
		goto tr5;
	goto st0;
tr5:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st4;
st4:
	if ( ++p == pe )
		goto _out4;
case 4:
#line 342 "xmlscan.cpp"
	if ( (*p) == 59 )
		goto tr6;
	goto st0;
tr2:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st5;
st5:
	if ( ++p == pe )
		goto _out5;
case 5:
#line 354 "xmlscan.cpp"
	if ( (*p) == 116 )
		goto tr7;
	goto st0;
tr7:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st6;
st6:
	if ( ++p == pe )
		goto _out6;
case 6:
#line 366 "xmlscan.cpp"
	if ( (*p) == 59 )
		goto tr8;
	goto st0;
tr3:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st7;
st7:
	if ( ++p == pe )
		goto _out7;
case 7:
#line 378 "xmlscan.cpp"
	if ( (*p) == 116 )
		goto tr9;
	goto st0;
tr9:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st8;
st8:
	if ( ++p == pe )
		goto _out8;
case 8:
#line 390 "xmlscan.cpp"
	if ( (*p) == 59 )
		goto tr10;
	goto st0;
tr11:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st9;
tr12:
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st9;
tr52:
#line 116 "xmlscan.rl"
	{ token_col = curcol; token_line = curline; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st9;
st9:
	if ( ++p == pe )
		goto _out9;
case 9:
#line 414 "xmlscan.cpp"
	switch( (*p) ) {
		case 9: goto tr11;
		case 10: goto tr12;
		case 13: goto tr11;
		case 32: goto tr11;
		case 47: goto tr13;
		case 95: goto tr14;
	}
	if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr14;
	} else if ( (*p) >= 65 )
		goto tr14;
	goto st0;
tr13:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st10;
tr15:
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st10;
st10:
	if ( ++p == pe )
		goto _out10;
case 10:
#line 443 "xmlscan.cpp"
	switch( (*p) ) {
		case 9: goto tr13;
		case 10: goto tr15;
		case 13: goto tr13;
		case 32: goto tr13;
		case 95: goto tr16;
	}
	if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr16;
	} else if ( (*p) >= 65 )
		goto tr16;
	goto st0;
tr19:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st11;
tr16:
#line 149 "xmlscan.rl"
	{ tag_id_start = p; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st11;
st11:
	if ( ++p == pe )
		goto _out11;
case 11:
#line 471 "xmlscan.cpp"
	switch( (*p) ) {
		case 9: goto tr17;
		case 10: goto tr18;
		case 13: goto tr17;
		case 32: goto tr17;
		case 62: goto tr20;
		case 95: goto tr19;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto st0;
tr21:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st12;
tr22:
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st12;
tr17:
#line 150 "xmlscan.rl"
	{ tag_id_len = p - tag_id_start; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st12;
tr18:
#line 150 "xmlscan.rl"
	{ tag_id_len = p - tag_id_start; }
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st12;
st12:
	if ( ++p == pe )
		goto _out12;
case 12:
#line 517 "xmlscan.cpp"
	switch( (*p) ) {
		case 9: goto tr21;
		case 10: goto tr22;
		case 13: goto tr21;
		case 32: goto tr21;
		case 62: goto tr23;
	}
	goto st0;
tr26:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st13;
tr14:
#line 149 "xmlscan.rl"
	{ tag_id_start = p; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st13;
st13:
	if ( ++p == pe )
		goto _out13;
case 13:
#line 540 "xmlscan.cpp"
	switch( (*p) ) {
		case 9: goto tr24;
		case 10: goto tr25;
		case 13: goto tr24;
		case 32: goto tr24;
		case 62: goto tr27;
		case 95: goto tr26;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr26;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr26;
	} else
		goto tr26;
	goto st0;
tr28:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st14;
tr29:
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st14;
tr24:
#line 150 "xmlscan.rl"
	{ tag_id_len = p - tag_id_start; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st14;
tr25:
#line 150 "xmlscan.rl"
	{ tag_id_len = p - tag_id_start; }
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st14;
tr44:
#line 132 "xmlscan.rl"
	{
		attr_value_len = p - attr_value_start;

		AttrMarker newAttr;
		newAttr.id = attr_id_start;
		newAttr.idLen = attr_id_len;
		newAttr.value = attr_value_start;
		newAttr.valueLen = attr_value_len;
		attrMkList.append( newAttr );
	}
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st14;
tr45:
#line 132 "xmlscan.rl"
	{
		attr_value_len = p - attr_value_start;

		AttrMarker newAttr;
		newAttr.id = attr_id_start;
		newAttr.idLen = attr_id_len;
		newAttr.value = attr_value_start;
		newAttr.valueLen = attr_value_len;
		attrMkList.append( newAttr );
	}
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st14;
st14:
	if ( ++p == pe )
		goto _out14;
case 14:
#line 618 "xmlscan.cpp"
	switch( (*p) ) {
		case 9: goto tr28;
		case 10: goto tr29;
		case 13: goto tr28;
		case 32: goto tr28;
		case 62: goto tr30;
		case 95: goto tr31;
	}
	if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr31;
	} else if ( (*p) >= 65 )
		goto tr31;
	goto st0;
tr34:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st15;
tr31:
#line 124 "xmlscan.rl"
	{ attr_id_start = p; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st15;
tr47:
#line 132 "xmlscan.rl"
	{
		attr_value_len = p - attr_value_start;

		AttrMarker newAttr;
		newAttr.id = attr_id_start;
		newAttr.idLen = attr_id_len;
		newAttr.value = attr_value_start;
		newAttr.valueLen = attr_value_len;
		attrMkList.append( newAttr );
	}
#line 124 "xmlscan.rl"
	{ attr_id_start = p; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st15;
st15:
	if ( ++p == pe )
		goto _out15;
case 15:
#line 664 "xmlscan.cpp"
	switch( (*p) ) {
		case 9: goto tr32;
		case 10: goto tr33;
		case 13: goto tr32;
		case 32: goto tr32;
		case 61: goto tr35;
		case 95: goto tr34;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr34;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr34;
	} else
		goto tr34;
	goto st0;
tr36:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st16;
tr37:
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st16;
tr32:
#line 125 "xmlscan.rl"
	{ attr_id_len = p - attr_id_start; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st16;
tr33:
#line 125 "xmlscan.rl"
	{ attr_id_len = p - attr_id_start; }
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st16;
st16:
	if ( ++p == pe )
		goto _out16;
case 16:
#line 710 "xmlscan.cpp"
	switch( (*p) ) {
		case 9: goto tr36;
		case 10: goto tr37;
		case 13: goto tr36;
		case 32: goto tr36;
		case 61: goto tr38;
	}
	goto st0;
tr38:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st17;
tr39:
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st17;
tr35:
#line 125 "xmlscan.rl"
	{ attr_id_len = p - attr_id_start; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st17;
st17:
	if ( ++p == pe )
		goto _out17;
case 17:
#line 739 "xmlscan.cpp"
	switch( (*p) ) {
		case 9: goto tr38;
		case 10: goto tr39;
		case 13: goto tr38;
		case 32: goto tr38;
		case 34: goto tr40;
	}
	goto st0;
tr41:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st18;
tr42:
#line 117 "xmlscan.rl"
	{ curcol = 0; curline++; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st18;
tr40:
#line 130 "xmlscan.rl"
	{ attr_value_start = p; }
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st18;
st18:
	if ( ++p == pe )
		goto _out18;
case 18:
#line 768 "xmlscan.cpp"
	switch( (*p) ) {
		case 10: goto tr42;
		case 34: goto tr43;
	}
	goto tr41;
tr43:
#line 115 "xmlscan.rl"
	{ curcol++; }
	goto st19;
st19:
	if ( ++p == pe )
		goto _out19;
case 19:
#line 782 "xmlscan.cpp"
	switch( (*p) ) {
		case 9: goto tr44;
		case 10: goto tr45;
		case 13: goto tr44;
		case 32: goto tr44;
		case 62: goto tr46;
		case 95: goto tr47;
	}
	if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr47;
	} else if ( (*p) >= 65 )
		goto tr47;
	goto st0;
	}
	_out20: cs = 20; goto _out; 
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
	_out10: cs = 10; goto _out; 
	_out11: cs = 11; goto _out; 
	_out12: cs = 12; goto _out; 
	_out13: cs = 13; goto _out; 
	_out14: cs = 14; goto _out; 
	_out15: cs = 15; goto _out; 
	_out16: cs = 16; goto _out; 
	_out17: cs = 17; goto _out; 
	_out18: cs = 18; goto _out; 
	_out19: cs = 19; goto _out; 

	_out: {}
	}
#line 239 "xmlscan.rl"

		if ( cs == Scanner_error )
			return TK_ERR;

		if ( token != TK_NO_TOKEN ) {
			/* fbreak does not advance p, so we do it manually. */
			p = p + 1;
			data_len = p - data;
			return token;
		}
	}
}

int xml_parse( std::istream &input, const char *fileName, 
		bool outputActive, bool wantComplete )
{
	Scanner scanner( fileName, input );
	Parser parser( fileName, outputActive, wantComplete );

	parser.init();

	while ( 1 ) {
		int token = scanner.scan();
		if ( token == TK_NO_TOKEN ) {
			cerr << "xmlscan: interal error: scanner returned NO_TOKEN" << endl;
			exit(1);
		}
		else if ( token == TK_EOF ) {
			parser.token( _eof, scanner.token_col, scanner.token_line );
			break;
		}
		else if ( token == TK_ERR ) {
			scanner.error() << "scanner error" << endl;
			break;
		}
		else if ( token == TK_SPACE ) {
			scanner.error() << "scanner is out of buffer space" << endl;
			break;
		}
		else {
			/* All other tokens are either open or close tags. */
			XMLTagHashPair *tagId = Perfect_Hash::in_word_set( 
					scanner.tag_id_start, scanner.tag_id_len );

			XMLTag *tag = new XMLTag( tagId, token == TK_OpenTag ? 
					XMLTag::Open : XMLTag::Close );

			if ( tagId != 0 ) {
				/* Get attributes for open tags. */
				if ( token == TK_OpenTag && scanner.attrMkList.length() > 0 ) {
					tag->attrList = new AttrList;
					for ( AttrMkList::Iter attr = scanner.attrMkList; 
							attr.lte(); attr++ )
					{
						Attribute newAttr;
						newAttr.id = new char[attr->idLen+1];
						memcpy( newAttr.id, attr->id, attr->idLen );
						newAttr.id[attr->idLen] = 0;

						/* Exclude the surrounding quotes. */
						newAttr.value = new char[attr->valueLen-1];
						memcpy( newAttr.value, attr->value+1, attr->valueLen-2 );
						newAttr.value[attr->valueLen-2] = 0;

						tag->attrList->append( newAttr );
					}
				}

				/* Get content for closing tags. */
				if ( token == TK_CloseTag ) {
					switch ( tagId->id ) {
					case TAG_host: case TAG_arg:
					case TAG_t: case TAG_alphtype:
					case TAG_text: case TAG_goto:
					case TAG_call: case TAG_next:
					case TAG_entry: case TAG_set_tokend:
					case TAG_set_act: case TAG_start_state:
					case TAG_error_state: case TAG_state_actions: 
					case TAG_action_table: case TAG_cond_space: 
					case TAG_c: case TAG_ex:
						tag->content = new char[scanner.buffer.length+1];
						memcpy( tag->content, scanner.buffer.data,
								scanner.buffer.length );
						tag->content[scanner.buffer.length] = 0;
						break;
					}
				}
			}

			#if 0
			cerr << "parser_driver: " << (tag->type == XMLTag::Open ? "open" : "close") <<
					": " << (tag->tagId != 0 ? tag->tagId->name : "<unknown>") << endl;
			if ( tag->attrList != 0 ) {
				for ( AttrList::Iter attr = *tag->attrList; attr.lte(); attr++ )
					cerr << "    " << attr->id << ": " << attr->value << endl;
			}
			if ( tag->content != 0 )
				cerr << "    content: " << tag->content << endl;
			#endif

			parser.token( tag, scanner.token_col, scanner.token_line );
		}
	}

	return 0;
}

std::ostream &Scanner::error()
{
	cerr << fileName << ":" << curline << ":" << curcol << ": ";
	return cerr;
}
