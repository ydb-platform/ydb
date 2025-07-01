/*
 * easy_ut.cpp -- Unit tests for PireEasy
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
#include <stub/defaults.h>
#include "stub/cppunit.h"
#include <stdexcept>
#include "common.h"

#undef Run

#include <easy.h>

Y_UNIT_TEST_SUITE(TestPireEasy) {
	
Y_UNIT_TEST(Match)
{
	Pire::Regexp re("(foo|bar)+", Pire::I);
	UNIT_ASSERT("prefix fOoBaR suffix" ==~ re);
	UNIT_ASSERT(!("bla bla bla" ==~ re));
}

Y_UNIT_TEST(Utf8)
{
	Pire::Regexp re("^.$", Pire::I | Pire::UTF8);
	UNIT_ASSERT("\x41" ==~ re);
	UNIT_ASSERT(!("\x81" ==~ re));
}

Y_UNIT_TEST(TwoFeatures)
{
	Pire::Regexp re("^(a.c&.b.)$", Pire::I | Pire::ANDNOT);
	UNIT_ASSERT("abc" ==~ re);
	UNIT_ASSERT("ABC" ==~ re);
	UNIT_ASSERT(!("adc" ==~ re));
}
	
}
