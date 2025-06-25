/*
 * inline_ut.cpp --
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


#include <stub/hacks.h>
#include "stub/cppunit.h"
#include <pire.h>
#include <iostream>
#include <string.h>

Y_UNIT_TEST_SUITE(TestPireInline) {

template<class Scanner>
typename Scanner::State RunRegexp(const Scanner& scanner, const char* str)
{
	typename Scanner::State state;
	scanner.Initialize(state);
	Step(scanner, state, Pire::BeginMark);
	Run(scanner, state, str, str + strlen(str));
	Step(scanner, state, Pire::EndMark);
	return state;
}

template<class Scanner>
bool Matches(const Scanner& scanner, const char* str)
{
	return scanner.Final(RunRegexp(scanner, str));
}

template<class Scanner>
bool Matches2(const Scanner& scanner, const char* str)
{
	return Pire::Matches(scanner, str);
}

bool ParticularMatch(Pire::Scanner& sc, Pire::Scanner::State st, size_t idx)
{
	std::pair<const size_t*, const size_t*> p = sc.AcceptedRegexps(st);
	return std::distance(p.first, p.second) == 1 && *p.first == idx;
}

Y_UNIT_TEST(Inline)
{
	Pire::Scanner scanner = PIRE_REGEXP("http://([a-z0-9]+\\.)+[a-z]{2,4}/?", "is");
	UNIT_ASSERT(Matches(scanner, "http://domain.vasya.ru/"));
	UNIT_ASSERT(Matches(scanner, "prefix http://domain.vasya.ru/"));
	UNIT_ASSERT(!Matches(scanner, "http://127.0.0.1/"));

	Pire::Scanner scanner2 = PIRE_REGEXP("http://([a-z0-9]+\\.)+[a-z]{2,4}/?", "i");
	UNIT_ASSERT(Matches2(scanner2, "http://domain.vasya.ru/"));
	UNIT_ASSERT(!Matches2(scanner2, "prefix http://domain.vasya.ru/"));
	UNIT_ASSERT(!Matches2(scanner2, "http://127.0.0.1/"));
}
 
Y_UNIT_TEST(InlineGlue)
{
	// Check whether pire_inline handles comments as well:
	
	/* - a C-style comment outside a regexp; */
	Pire::Scanner sc = PIRE_REGEXP(
		"foo", "", /* - a C-style comment inside a regexp; */
		"bar", "", // - a C++-style comment inside a regexp;
		"baz", ""
	);
	// - a C++-style comment outside a regexp.
	UNIT_ASSERT(ParticularMatch(sc, Pire::Runner(sc).Run("foo").State(), 0));
	UNIT_ASSERT(ParticularMatch(sc, Pire::Runner(sc).Run("bar").State(), 1));
	UNIT_ASSERT(ParticularMatch(sc, Pire::Runner(sc).Run("baz").State(), 2));
	UNIT_ASSERT(!Matches2(sc, "xxx"));
}

}
