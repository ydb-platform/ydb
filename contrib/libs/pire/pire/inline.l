%{ // -*- mode: c++ -*-

/*
 * inline.lpp -- a tool for inlining Pire regexps into your C++ code
 *
 * Copyright (c) 2007-2010, Dmitry Prokoptsev <dprokoptsev@gmail.com>,
 *                          Alexander Gololobov <agololobov@gmail.com>
 *
 * This file is part of Pire, the Perl Incompatible
 * Regular Expressions library.
 *
 * Pire is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Pire is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser Public License for more details.
 * You should have received a copy of the GNU Lesser Public License
 * along with Pire.  If not, see <http://www.gnu.org/licenses>.
 */

#include <stdio.h>
#include <vector>
#include <string>
#include <stdexcept>

#include <contrib/libs/pire/pire/stub/hacks.h>
#include <contrib/libs/pire/pire/stub/lexical_cast.h>
#include <contrib/libs/pire/pire/stub/saveload.h>
#include <contrib/libs/pire/pire/stub/memstreams.h>
#include <contrib/libs/pire/pire/stub/stl.h>

#include "pire.h"

ystring filename = "";
int line = 1;
TVector<ystring> args;

#ifdef _WIN32
#if _MCS_VER >= 1600
static int isatty(int) { return 0; }
#endif
#endif

class Die {
public:
	Die() {
		Msg = filename.empty() ? "pire_inline" : (filename + ":" + ToString(line) + ":");
	}


	template<class T>
	Die& operator << (const T& t) {
		Msg += ToString(t);
		return *this;
	}


	~Die() {
		fprintf(stderr, "%s\n", Msg.c_str());
		exit(1);
	}
private:
	ystring Msg;
};
Die DieHelper() {
	return Die();
}

void putChar(char c) { putc(c, yyout); }
void suppressChar(char) {}
void eatComment(void (*action)(char));

#define YY_FATAL_ERROR(msg) DieHelper() << msg
%}
%x Regexp
%%


<INITIAL>"/*"            { eatComment(putChar); }
<Regexp>"/*"             { eatComment(suppressChar); }
<INITIAL>"//".*\n        { ++line; fprintf(yyout, "%s", yytext); }
<Regexp>"//".*\n         { ++line; }
"\""([^\"]|\\.)*"\""     { fprintf(yyout, "%s", yytext); }
\n                       { ++line; putc('\n', yyout); }


<INITIAL>"PIRE_REGEXP"[:space:]*"(" { BEGIN(Regexp); args.clear(); args.push_back(ystring()); }
<Regexp>"\""([^\"]|\\.)*"\"" {
	ystring& s = args.back();
	const char* p;
	for (p = yytext + 1; *p && p[1]; ++p) {
		if (*p == '\\') {
			++p;
			if (!*p)
				Die() << "string ends with a backslash";
			else if (*p == '\'' || *p == '\"' || *p == '\\')
				s.push_back(*p);
			else if (*p == 'n')
				s.push_back('\n');
			else if (*p == 't')
				s.push_back('\t');
			else if (isdigit(*p)) {
				const char* beg = p;
				while (isdigit(*p))
					++p;
				s.push_back(strtol(ystring(beg, p).c_str(), 0, 8));
			} else if (*p == 'x') {
				const char* beg = p;
				while (isdigit(*p) || (*p > 'a' && *p <= 'f') || (*p > 'A' && *p < 'F'))
					++p;
				s.push_back(strtol(ystring(beg, p).c_str(), 0, 16));
			} else
				Die() << "unknown escape sequence (\\" <<  *p << ")";
		} else
			s.push_back(*p);
	}
	if (!*p)
		Die() << "string ends with a backslash";
}
<Regexp>[ \t]            {}
<Regexp>\n               { ++line; }
<Regexp>","              { args.push_back(ystring()); }
<Regexp>")" {

	if (args.size() & 1 || args.empty())
		Die() << "Usage: PIRE_REGEXP(\"regexp1\", \"flags1\" [, \"regexp2\", \"flags2\" [,...] ])";

	bool first = true;
	Pire::Scanner sc;
	ystring pattern;
	for (auto i = args.begin(), ie = args.end(); i != ie; i += 2) {

		Pire::Lexer lexer(i->c_str(), i->c_str() + i->size());
		bool surround = false;
		bool greedy = false;
		bool reverse = false;
		for (const char* option = (i+1)->c_str(); *option; ++option) {
			if (*option == 'i')
				lexer.AddFeature(Pire::Features::CaseInsensitive());
			else if (*option == 'u')
				lexer.SetEncoding(Pire::Encodings::Utf8());
			else if (*option == 's')
				surround = true;
			else if (*option == 'a')
				lexer.AddFeature(Pire::Features::AndNotSupport());
			else if (*option == 'g')
				greedy = true;
			else if (*option == 'r')
				reverse = true;
			else
				Die() << "unknown option " <<  *option << "";
		}

		Pire::Fsm fsm;
		try {
			fsm = lexer.Parse();
		}
		catch (std::exception& e) {
			Die() << "" << filename << ":" <<  line << ": " <<  e.what() << "";
		}
		if (reverse)
			fsm.Reverse();
		if (greedy && surround)
			Die() << "greedy and surround options are incompatible";
		if (greedy)
			fsm = ~fsm.Surrounded() + fsm;
		else if (surround)
			fsm.Surround();

		Pire::Scanner tsc(fsm);
		if (first) {
			pattern = *i;
			first = false;
			tsc.Swap(sc);
		} else {
			sc = Pire::Scanner::Glue(sc, tsc);
			pattern += " | ";
			pattern += *i;
		}
	}

	BufferOutput buf;
	AlignedOutput stream(&buf);
	Save(&stream, sc);

	fprintf(yyout, "Pire::MmappedScanner<Pire::Scanner>(PIRE_LITERAL( // %s \n    \"", pattern.c_str());
	size_t pos = 5;
	for (auto i = buf.Buffer().Begin(), ie = buf.Buffer().End(); i != ie; ++i) {
		pos += fprintf(yyout, "\\x%02X", static_cast<unsigned char>(*i));
		if (pos >= 78) {
			fprintf(yyout, "\"\n    \"");
			pos = 5;
		}
	}
	fprintf(yyout, "\"), %u)\n#line %d \"%s\"\n",
		(unsigned int) buf.Buffer().Size(), line, filename.c_str());
	BEGIN(INITIAL);
}
<INITIAL>.               { putc(*yytext, yyout); }




%%

void eatComment(void (*action)(char))
{
	int c;
	action('/'); action('*');
	for (;;) {
		while ((c = yyinput()) != EOF && c != '*') {
			if (c == '\n')
				++line;
			action(c);
		}
		if (c == '*') {
			action(c);
			while ((c = yyinput()) == '*')
				action(c);
			if (c == '/') {
				action(c);
				break;
			}
		}
		if (c == EOF)
			Die() << "EOF in comment";
	}
}

int yywrap() { return 1; }


int main(int argc, char** argv)
{
	// Suppress warnings
	static_cast<void>(&yy_fatal_error);
	static_cast<void>(&yyunput);


	try {
		const char* outfile = 0;
		if (argc >= 3 && !strcmp(argv[1], "-o")) {
			outfile = argv[2];
			argv += 2, argc -= 2;
		}
		if (argc == 2)
			filename = ystring(argv[1]);
		else if (argc > 2)
			Die() << "usage: pire_inline [-o outfile] [infile]";

		yyin = stdin, yyout = stdout;
		if (outfile && (yyout = fopen(outfile, "w")) == NULL)
			Die() << "cannot open file " <<  outfile << " for writing";
		if (!filename.empty()) {
			if ((yyin = fopen(filename.c_str(), "r")) == NULL)
				Die() << "cannot open file " << filename.c_str() << "\n";
	} else
			filename = "(stdin)";


		yylex();
		return 0;
	}
	catch (std::exception& e) {
		fprintf(stderr, "%s\n", e.what());
		return 1;
	}
}
