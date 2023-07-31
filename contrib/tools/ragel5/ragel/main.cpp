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
#include <sstream>

/* Parsing. */
#include "ragel.h"
#include "rlscan.h"

/* Parameters and output. */
#include "pcheck.h"
#include "vector.h"
#include "version.h"
#include "common.h"

#ifdef _MSC_VER
#   define strncasecmp _strnicmp
#   define strcasecmp _stricmp
#endif

using std::istream;
using std::ostream;
using std::ifstream;
using std::ofstream;
using std::cin;
using std::cout;
using std::cerr;
using std::endl;

/* Controls minimization. */
MinimizeLevel minimizeLevel = MinimizePartition2;
MinimizeOpt minimizeOpt = MinimizeMostOps;

/* Graphviz dot file generation. */
char *machineSpec = 0, *machineName = 0;
bool machineSpecFound = false;

bool printStatistics = false;

/* Print a summary of the options. */
void usage()
{
	cout <<
"usage: ragel [options] file\n"
"general:\n"
"   -h, -H, -?, --help   Print this usage and exit\n"
"   -v, --version        Print version information and exit\n"
"   -o <file>            Write output to <file>\n"
"   -s                   Print some statistics on stderr\n"
"fsm minimization:\n"
"   -n                   Do not perform minimization\n"
"   -m                   Minimize at the end of the compilation\n"
"   -l                   Minimize after most operations (default)\n"
"   -e                   Minimize after every operation\n"
"machine selection:\n"
"   -S <spec>            FSM specification to output for -V\n"
"   -M <machine>         Machine definition/instantiation to output for -V\n"
"host language:\n"
"   -C                   The host language is C, C++, Obj-C or Obj-C++ (default)\n"
"   -D                   The host language is D\n"
"   -J                   The host language is Java\n"
"   -R                   The host language is Ruby\n"
	;	
}

/* Print version information. */
void version()
{
	cout << "Ragel State Machine Compiler version " VERSION << " " PUBDATE << endl <<
			"Copyright (c) 2001-2006 by Adrian Thurston" << endl;
}

/* Total error count. */
int gblErrorCount = 0;

/* Print the opening to a warning in the input, then return the error ostream. */
ostream &warning( const InputLoc &loc )
{
	assert( loc.fileName != 0 );
	cerr << loc.fileName << ":" << loc.line << ":" << 
			loc.col << ": warning: ";
	return cerr;
}

/* Print the opening to a program error, then return the error stream. */
ostream &error()
{
	gblErrorCount += 1;
	cerr << PROGNAME ": ";
	return cerr;
}

ostream &error( const InputLoc &loc )
{
	gblErrorCount += 1;
	assert( loc.fileName != 0 );
	cerr << loc.fileName << ":" << loc.line << ": ";
	return cerr;
}

void escapeLineDirectivePath( std::ostream &out, char *path )
{
	for ( char *pc = path; *pc != 0; pc++ ) {
		if ( *pc == '\\' )
			out << "\\\\";
		else
			out << *pc;
	}
}

/* Main, process args and call yyparse to start scanning input. */
int main(int argc, char **argv)
{
	ParamCheck pc("o:nmleabjkS:M:CDJRvHh?-:s", argc, argv);
	char *inputFileName = 0;
	char inputFileNameArr[] = "<stdin>";
	char *outputFileName = 0;

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

			/* Minimization, mostly hidden options. */
			case 'n':
				minimizeOpt = MinimizeNone;
				break;
			case 'm':
				minimizeOpt = MinimizeEnd;
				break;
			case 'l':
				minimizeOpt = MinimizeMostOps;
				break;
			case 'e':
				minimizeOpt = MinimizeEveryOp;
				break;
			case 'a':
				minimizeLevel = MinimizeApprox;
				break;
			case 'b':
				minimizeLevel = MinimizeStable;
				break;
			case 'j':
				minimizeLevel = MinimizePartition1;
				break;
			case 'k':
				minimizeLevel = MinimizePartition2;
				break;

			/* Machine spec. */
			case 'S':
				if ( *pc.parameterArg == 0 )
					error() << "please specify an argument to -S" << endl;
				else if ( machineSpec != 0 )
					error() << "more than one -S argument was given" << endl;
				else {
					/* Ok, remember the path to the machine to generate. */
					machineSpec = pc.parameterArg;
				}
				break;

			/* Machine path. */
			case 'M':
				if ( *pc.parameterArg == 0 )
					error() << "please specify an argument to -M" << endl;
				else if ( machineName != 0 )
					error() << "more than one -M argument was given" << endl;
				else {
					/* Ok, remember the machine name to generate. */
					machineName = pc.parameterArg;
				}
				break;

			/* Host language types. */
			case 'C':
				hostLangType = CCode;
				hostLang = &hostLangC;
				break;
			case 'D':
				hostLangType = DCode;
				hostLang = &hostLangD;
				break;
			case 'J':
				hostLangType = JavaCode;
				hostLang = &hostLangJava;
				break;
			case 'R':
				hostLangType = RubyCode;
				hostLang = &hostLangRuby;
				break;

			/* Version and help. */
			case 'v':
				version();
				exit(0);
			case 'H': case 'h': case '?':
				usage();
				exit(0);
			case 's':
				printStatistics = true;
				break;
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
				}
			}
			break;

		case ParamCheck::invalid:
			error() << "-" << pc.parameter << " is an invalid argument" << endl;
			break;

		case ParamCheck::noparam:
			/* It is interpreted as an input file. */
			if ( *pc.curArg == 0 )
				error() << "a zero length input file name was given" << endl;
			else if ( inputFileName != 0 )
				error() << "more than one input file name was given" << endl;
			else {
				/* OK, Remember the filename. */
				inputFileName = pc.curArg;
			}
			break;
		}
	}

	/* Bail on above errors. */
	if ( gblErrorCount > 0 )
		exit(1);

	/* Make sure we are not writing to the same file as the input file. */
	if ( inputFileName != 0 && outputFileName != 0 && 
			strcmp( inputFileName, outputFileName  ) == 0 )
	{
		error() << "output file \"" << outputFileName  << 
				"\" is the same as the input file" << endl;
	}

	/* Open the input file for reading. */
	istream *inStream;
	if ( inputFileName != 0 ) {
		/* Open the input file for reading. */
		ifstream *inFile = new ifstream( inputFileName );
		inStream = inFile;
		if ( ! inFile->is_open() )
			error() << "could not open " << inputFileName << " for reading" << endl;
	}
	else {
		inStream = &cin;
	}


	/* Bail on above errors. */
	if ( gblErrorCount > 0 )
		exit(1);

	std::ostringstream outputBuffer;

	if ( machineSpec == 0 && machineName == 0 )
		outputBuffer << "<host line=\"1\" col=\"1\">";

#if defined _WIN32 || defined _WIN64
	if (inputFileName != 0) {
		NormalizeWinPath(inputFileName);
	}
#endif
	if (inputFileName == 0) {
		inputFileName = inputFileNameArr;
	}

	if (strrchr(inputFileName, '/') == NULL) {
		error() << "input file path should be absolute: " << inputFileName << endl;
		exit(1);
	}

	Scanner scanner( inputFileName, *inStream, outputBuffer, 0, 0, 0, false );
	scanner.do_scan();

	/* Finished, final check for errors.. */
	if ( gblErrorCount > 0 )
		return 1;
	
	/* Now send EOF to all parsers. */
	terminateAllParsers();

	/* Finished, final check for errors.. */
	if ( gblErrorCount > 0 )
		return 1;

	if ( machineSpec == 0 && machineName == 0 )
		outputBuffer << "</host>\n";

	if ( gblErrorCount > 0 )
		return 1;
	
	ostream *outputFile = 0;
	if ( outputFileName != 0 )
		outputFile = new ofstream( outputFileName );
	else
		outputFile = &cout;

	/* Write the machines, then the surrounding code. */
	writeMachines( *outputFile, outputBuffer.str(), inputFileName );

	if ( outputFileName != 0 )
		delete outputFile;

	return 0;
}
