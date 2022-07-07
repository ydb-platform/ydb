/*
 * common.h --
 *
 * Copyright (c) 2007-2010, Dmitry Prokoptsev <dprokoptsev@gmail.com>,
 *						  Alexander Gololobov <agololobov@gmail.com>
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


#ifndef PIRE_TEST_COMMON_H_INCLUDED
#define PIRE_TEST_COMMON_H_INCLUDED

#include <stdio.h>
#include <pire.h>
#include <stub/stl.h>
#include <stub/defaults.h>
#include <stub/lexical_cast.h>
#include "stub/cppunit.h"

using namespace Pire;

/*****************************************************************************
* Helpers
*****************************************************************************/

inline Pire::Fsm ParseRegexp(const char* str, const char* options = "", const Pire::Encoding** enc = 0)
{
	Pire::Lexer lexer;
	TVector<wchar32> ucs4;

	bool surround = true;
	for (; *options; ++options) {
		if (*options == 'i')
			lexer.AddFeature(Pire::Features::CaseInsensitive());
		else if (*options == 'u')
			lexer.SetEncoding(Pire::Encodings::Utf8());
		else if (*options == 'n')
			surround = false;
		else if (*options == 'a')
			lexer.AddFeature(Pire::Features::AndNotSupport());
		else
			throw std::invalid_argument("Unknown option: " + ystring(1, *options));
	}

	if (enc)
		*enc = &lexer.Encoding();

	lexer.Encoding().FromLocal(str, str + strlen(str), std::back_inserter(ucs4));
	lexer.Assign(ucs4.begin(), ucs4.end());

	Pire::Fsm fsm = lexer.Parse();
	if (surround)
		fsm.Surround();
	return fsm;
}

inline bool HasError(const char* regexp) {
	try {
		ParseRegexp(regexp);
		return false;
	} catch (Pire::Error& ex) {
		return true;
	}
}

struct Scanners {
	Pire::Scanner fast;
	Pire::NonrelocScanner nonreloc;
	Pire::SimpleScanner simple;
	Pire::SlowScanner slow;
	Pire::ScannerNoMask fastNoMask;
	Pire::NonrelocScannerNoMask nonrelocNoMask;
	Pire::HalfFinalScanner halfFinal;
	Pire::HalfFinalScannerNoMask halfFinalNoMask;
	Pire::NonrelocHalfFinalScanner nonrelocHalfFinal;
	Pire::NonrelocHalfFinalScannerNoMask nonrelocHalfFinalNoMask;

	Scanners(const Pire::Fsm& fsm, size_t distance = 0)
		: fast(Pire::Fsm(fsm).Compile<Pire::Scanner>(distance))
 		, nonreloc(Pire::Fsm(fsm).Compile<Pire::NonrelocScanner>(distance))
		, simple(Pire::Fsm(fsm).Compile<Pire::SimpleScanner>(distance))
		, slow(Pire::Fsm(fsm).Compile<Pire::SlowScanner>(distance))
		, fastNoMask(Pire::Fsm(fsm).Compile<Pire::ScannerNoMask>(distance))
 		, nonrelocNoMask(Pire::Fsm(fsm).Compile<Pire::NonrelocScannerNoMask>(distance))
		, halfFinal(Pire::Fsm(fsm).Compile<Pire::HalfFinalScanner>(distance))
		, halfFinalNoMask(Pire::Fsm(fsm).Compile<Pire::HalfFinalScannerNoMask>(distance))
		, nonrelocHalfFinal(Pire::Fsm(fsm).Compile<Pire::NonrelocHalfFinalScanner>(distance))
		, nonrelocHalfFinalNoMask(Pire::Fsm(fsm).Compile<Pire::NonrelocHalfFinalScannerNoMask>(distance))
	{}

	Scanners(const char* str, const char* options = "")
	{
		Pire::Fsm fsm = ParseRegexp(str, options);
		fast = Pire::Fsm(fsm).Compile<Pire::Scanner>();
		nonreloc = Pire::Fsm(fsm).Compile<Pire::NonrelocScanner>();
		simple = Pire::Fsm(fsm).Compile<Pire::SimpleScanner>();
		slow = Pire::Fsm(fsm).Compile<Pire::SlowScanner>();
		fastNoMask = Pire::Fsm(fsm).Compile<Pire::ScannerNoMask>();
 		nonrelocNoMask = Pire::Fsm(fsm).Compile<Pire::NonrelocScannerNoMask>();
		halfFinal = Pire::Fsm(fsm).Compile<Pire::HalfFinalScanner>();
		halfFinalNoMask = Pire::Fsm(fsm).Compile<Pire::HalfFinalScannerNoMask>();
		nonrelocHalfFinal = Pire::Fsm(fsm).Compile<Pire::NonrelocHalfFinalScanner>();
		nonrelocHalfFinalNoMask = Pire::Fsm(fsm).Compile<Pire::NonrelocHalfFinalScannerNoMask>();
	}
};

#ifdef PIRE_DEBUG

template <class Scanner>
inline ystring DbgState(const Scanner& scanner, typename Scanner::State state)
{
	return ToString(scanner.StateIndex(state)) + (scanner.Final(state) ? ystring(" [final]") : ystring());
}
/*
inline ystring DbgState(const Pire::SimpleScanner& scanner, Pire::SimpleScanner::State state)
{
	return ToString(scanner.StateIndex(state)) + (scanner.Final(state) ? ystring(" [final]") : ystring());
}
*/
inline ystring DbgState(const Pire::SlowScanner& scanner, const Pire::SlowScanner::State& state)
{
	return ystring("(") + Join(state.states.begin(), state.states.end(), ", ") + ystring(")") + (scanner.Final(state) ? ystring(" [final]") : ystring());
}

template<class Scanner>
void DbgRun(const Scanner& scanner, typename Scanner::State& state, const char* begin, const char* end)
{
	for (; begin != end; ++begin) {
		char tmp[8];
		if (*begin >= 32) {
			tmp[0] = *begin;
			tmp[1] = 0;
		} else
			snprintf(tmp, sizeof(tmp)-1, "\\%03o", (unsigned char) *begin);
		std::clog << DbgState(scanner, state) << " --[" << tmp << "]--> ";
		scanner.Next(state, (unsigned char) *begin);
		std::clog << DbgState(scanner, state) << "\n";
	}
}

#define Run DbgRun
#endif

template<class Scanner>
typename Scanner::State RunRegexp(const Scanner& scanner, const ystring& str)
{
	PIRE_IFDEBUG(std::clog << "--- checking against " << str << "\n");

	typename Scanner::State state;
	scanner.Initialize(state);
	Step(scanner, state, BeginMark);
	Run(scanner, state, str.c_str(), str.c_str() + str.length());
	Step(scanner, state, EndMark);
	return state;
}

template<class Scanner>
typename Scanner::State RunRegexp(const Scanner& scanner, const char* str)
{
	return RunRegexp(scanner, ystring(str));
}

template<class Scanner>
bool Matches(const Scanner& scanner, const ystring& str)
{
	auto state = RunRegexp(scanner, str);
	auto result = scanner.AcceptedRegexps(state);
	return result.first != result.second;
}

template<class Scanner>
bool Matches(const Scanner& scanner, const char* str)
{
	return Matches(scanner, ystring(str));
}

#define SCANNER(fsm) for (Scanners m_scanners(fsm), *m_flag = &m_scanners; m_flag; m_flag = 0)
#define APPROXIMATE_SCANNER(fsm, distance) for (Scanners m_scanners(fsm, distance), *m_flag = &m_scanners; m_flag; m_flag = 0)
#define REGEXP(pattern) for (Scanners m_scanners(pattern), *m_flag = &m_scanners; m_flag; m_flag = 0)
#define REGEXP2(pattern,flags) for (Scanners m_scanners(pattern, flags), *m_flag = &m_scanners; m_flag; m_flag = 0)
#define ACCEPTS(str) \
	do {\
		UNIT_ASSERT(Matches(m_scanners.fast, str));\
        UNIT_ASSERT(Matches(m_scanners.nonreloc, str));\
		UNIT_ASSERT(Matches(m_scanners.simple, str));\
		UNIT_ASSERT(Matches(m_scanners.slow, str));\
		UNIT_ASSERT(Matches(m_scanners.fastNoMask, str));\
		UNIT_ASSERT(Matches(m_scanners.nonrelocNoMask, str));\
		UNIT_ASSERT(Matches(m_scanners.halfFinal, str));\
		UNIT_ASSERT(Matches(m_scanners.halfFinalNoMask, str));\
		UNIT_ASSERT(Matches(m_scanners.nonrelocHalfFinal, str));\
		UNIT_ASSERT(Matches(m_scanners.nonrelocHalfFinalNoMask, str));\
	} while (false)

#define DENIES(str) \
	do {\
		UNIT_ASSERT(!Matches(m_scanners.fast, str));\
        UNIT_ASSERT(!Matches(m_scanners.nonreloc, str));\
		UNIT_ASSERT(!Matches(m_scanners.simple, str));\
		UNIT_ASSERT(!Matches(m_scanners.slow, str));\
		UNIT_ASSERT(!Matches(m_scanners.fastNoMask, str));\
		UNIT_ASSERT(!Matches(m_scanners.nonrelocNoMask, str));\
		UNIT_ASSERT(!Matches(m_scanners.halfFinal, str));\
		UNIT_ASSERT(!Matches(m_scanners.halfFinalNoMask, str));\
		UNIT_ASSERT(!Matches(m_scanners.nonrelocHalfFinal, str));\
		UNIT_ASSERT(!Matches(m_scanners.nonrelocHalfFinalNoMask, str));\
	} while (false)


#endif
