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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#ifndef _WIN32
#   include <unistd.h>
#endif

#include "common.h"
#include "rlgen-cd.h"
#include "xmlparse.h"
#include "pcheck.h"
#include "vector.h"
#include "version.h"

/* Code generators. */
#include "tabcodegen.h"
#include "ftabcodegen.h"
#include "flatcodegen.h"
#include "fflatcodegen.h"
#include "gotocodegen.h"
#include "fgotocodegen.h"
#include "ipgotocodegen.h"
#include "splitcodegen.h"

using std::istream;
using std::ifstream;
using std::ostream;
using std::ios;
using std::cin;
using std::cout;
using std::cerr;
using std::endl;

/* Target language and output style. */
CodeStyleEnum codeStyle = GenTables;

/* Io globals. */
istream *inStream = 0;
ostream *outStream = 0;
output_filter *outFilter = 0;
char *outputFileName = 0;

/* Graphviz dot file generation. */
bool graphvizDone = false;

int numSplitPartitions = 0;
bool noLineDirectives = false;
bool printPrintables = false;

/* Print a summary of the options. */
void usage()
{
	cout <<
"usage: " PROGNAME " [options] file\n"
"general:\n"
"   -h, -H, -?, --help    Print this usage and exit\n"
"   -v, --version         Print version information and exit\n"
"   -o <file>             Write output to <file>\n"
"code generation options:\n"
"   -l                    Inhibit writing of #line directives\n"
"generated code style:\n"
"   -T0                   Table driven FSM (default)\n"
"   -T1                   Faster table driven FSM\n"
"   -F0                   Flat table driven FSM\n"
"   -F1                   Faster flat table-driven FSM\n"
"   -G0                   Goto-driven FSM\n"
"   -G1                   Faster goto-driven FSM\n"
"   -G2                   Really fast goto-driven FSM\n"
"   -P<N>                 N-Way Split really fast goto-driven FSM\n"
	;	
}

/* Print version information. */
void version()
{
	cout << "Ragel Code Generator for C, C++, Objective-C and D" << endl <<
			"Version " VERSION << ", " PUBDATE << endl <<
			"Copyright (c) 2001-2007 by Adrian Thurston" << endl;
}

/* Total error count. */
int gblErrorCount = 0;

ostream &error()
{
	gblErrorCount += 1;
	cerr << PROGNAME ": ";
	return cerr;
}

/*
 * Callbacks invoked by the XML data parser.
 */

/* Invoked by the parser when the root element is opened. */
ostream *openOutput( char *inputFile )
{
	if ( hostLangType != CCode && hostLangType != DCode ) {
		error() << "this code generator is for C and D only" << endl;
		exit(1);
	}

	/* If the output format is code and no output file name is given, then
	 * make a default. */
	if ( outputFileName == 0 ) {
		char *ext = findFileExtension( inputFile );
		if ( ext != 0 && strcmp( ext, ".rh" ) == 0 )
			outputFileName = fileNameFromStem( inputFile, ".h" );
		else {
			const char *defExtension = 0;
			switch ( hostLangType ) {
				case CCode: defExtension = ".c"; break;
				case DCode: defExtension = ".d"; break;
				default: break;
			}
			outputFileName = fileNameFromStem( inputFile, defExtension );
		}
	}

	/* Make sure we are not writing to the same file as the input file. */
	if ( outputFileName != 0 && strcmp( inputFile, outputFileName  ) == 0 ) {
		error() << "output file \"" << outputFileName  << 
				"\" is the same as the input file" << endl;
	}

	if ( outputFileName != 0 ) {
		/* Create the filter on the output and open it. */
		outFilter = new output_filter( outputFileName );
		outFilter->open( outputFileName, ios::out|ios::trunc );
		if ( !outFilter->is_open() ) {
			error() << "error opening " << outputFileName << " for writing" << endl;
			exit(1);
		}

		/* Open the output stream, attaching it to the filter. */
		outStream = new ostream( outFilter );
	}
	else {
		/* Writing out ot std out. */
		outStream = &cout;
	}
	return outStream;
}

/* Invoked by the parser when a ragel definition is opened. */
CodeGenData *makeCodeGen( char *sourceFileName, char *fsmName, 
		ostream &out, bool wantComplete )
{
	CodeGenData *codeGen = 0;
	switch ( hostLangType ) {
	case CCode:
		switch ( codeStyle ) {
		case GenTables:
			codeGen = new CTabCodeGen(out);
			break;
		case GenFTables:
			codeGen = new CFTabCodeGen(out);
			break;
		case GenFlat:
			codeGen = new CFlatCodeGen(out);
			break;
		case GenFFlat:
			codeGen = new CFFlatCodeGen(out);
			break;
		case GenGoto:
			codeGen = new CGotoCodeGen(out);
			break;
		case GenFGoto:
			codeGen = new CFGotoCodeGen(out);
			break;
		case GenIpGoto:
			codeGen = new CIpGotoCodeGen(out);
			break;
		case GenSplit:
			codeGen = new CSplitCodeGen(out);
			break;
		}
		break;

	case DCode:
		switch ( codeStyle ) {
		case GenTables:
			codeGen = new DTabCodeGen(out);
			break;
		case GenFTables:
			codeGen = new DFTabCodeGen(out);
			break;
		case GenFlat:
			codeGen = new DFlatCodeGen(out);
			break;
		case GenFFlat:
			codeGen = new DFFlatCodeGen(out);
			break;
		case GenGoto:
			codeGen = new DGotoCodeGen(out);
			break;
		case GenFGoto:
			codeGen = new DFGotoCodeGen(out);
			break;
		case GenIpGoto:
			codeGen = new DIpGotoCodeGen(out);
			break;
		case GenSplit:
			codeGen = new DSplitCodeGen(out);
			break;
		}
		break;

	default: break;
	}

	codeGen->sourceFileName = sourceFileName;
	codeGen->fsmName = fsmName;
	codeGen->wantComplete = wantComplete;

	return codeGen;
}



/* Main, process args and call yyparse to start scanning input. */
int main(int argc, char **argv)
{
	ParamCheck pc("-:Hh?vlo:T:F:G:P:", argc, argv);
	const char *xmlInputFileName = 0;

	while ( pc.check() ) {
		switch ( pc.state ) {
		case ParamCheck::match:
			switch ( pc.parameter ) {
			/* Output. */
			case 'o':
				if ( *pc.parameterArg == 0 )
					error() << "a zero length output file name was given" << endl;
				else if ( outputFileName != 0 )
					error() << "more than one output file name was given" << endl;
				else {
					/* Ok, remember the output file name. */
					outputFileName = pc.parameterArg;
				}
				break;

			case 'l':
				noLineDirectives = true;
				break;

			/* Code style. */
			case 'T':
				if ( pc.parameterArg[0] == '0' )
					codeStyle = GenTables;
				else if ( pc.parameterArg[0] == '1' )
					codeStyle = GenFTables;
				else {
					error() << "-T" << pc.parameterArg[0] << 
							" is an invalid argument" << endl;
					exit(1);
				}
				break;
			case 'F':
				if ( pc.parameterArg[0] == '0' )
					codeStyle = GenFlat;
				else if ( pc.parameterArg[0] == '1' )
					codeStyle = GenFFlat;
				else {
					error() << "-F" << pc.parameterArg[0] << 
							" is an invalid argument" << endl;
					exit(1);
				}
				break;
			case 'G':
				if ( pc.parameterArg[0] == '0' )
					codeStyle = GenGoto;
				else if ( pc.parameterArg[0] == '1' )
					codeStyle = GenFGoto;
				else if ( pc.parameterArg[0] == '2' )
					codeStyle = GenIpGoto;
				else {
					error() << "-G" << pc.parameterArg[0] << 
							" is an invalid argument" << endl;
					exit(1);
				}
				break;
			case 'P':
				codeStyle = GenSplit;
				numSplitPartitions = atoi( pc.parameterArg );
				break;

			/* Version and help. */
			case 'v':
				version();
				exit(0);
			case 'H': case 'h': case '?':
				usage();
				exit(0);
			case '-':
				if ( strcasecmp(pc.parameterArg, "help") == 0 ) {
					usage();
					exit(0);
				}
				else if ( strcasecmp(pc.parameterArg, "version") == 0 ) {
					version();
					exit(0);
				}
				else {
					error() << "--" << pc.parameterArg << 
							" is an invalid argument" << endl;
					break;
				}
			}
			break;

		case ParamCheck::invalid:
			error() << "-" << pc.parameter << " is an invalid argument" << endl;
			break;

		case ParamCheck::noparam:
			if ( *pc.curArg == 0 )
				error() << "a zero length input file name was given" << endl;
			else if ( xmlInputFileName != 0 )
				error() << "more than one input file name was given" << endl;
			else {
				/* OK, Remember the filename. */
				xmlInputFileName = pc.curArg;
			}
			break;
		}
	}

	/* Bail on above errors. */
	if ( gblErrorCount > 0 )
		exit(1);

	/* Open the input file for reading. */
	if ( xmlInputFileName != 0 ) {
		/* Open the input file for reading. */
		ifstream *inFile = new ifstream( xmlInputFileName );
		inStream = inFile;
		if ( ! inFile->is_open() )
			error() << "could not open " << xmlInputFileName << " for reading" << endl;
	}
	else {
		xmlInputFileName = "<stdin>";
		inStream = &cin;
	}

	/* Bail on above errors. */
	if ( gblErrorCount > 0 )
		exit(1);

	bool wantComplete = true;
	bool outputActive = true;

	/* Parse the input! */
	xml_parse( *inStream, xmlInputFileName, outputActive, wantComplete );

	/* If writing to a file, delete the ostream, causing it to flush.
	 * Standard out is flushed automatically. */
	if ( outputFileName != 0 ) {
		delete outStream;
		delete outFilter;
	}

	/* Finished, final check for errors.. */
	if ( gblErrorCount > 0 ) {
		/* If we opened an output file, remove it. */
		if ( outputFileName != 0 )
			unlink( outputFileName );
		exit(1);
	}
	return 0;
}
