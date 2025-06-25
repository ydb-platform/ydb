/*
 * glyph_ut.cpp --
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


#include <pire.h>
#include <extra/glyphs.h>
#include "stub/cppunit.h"
#include "common.h"

Y_UNIT_TEST_SUITE(Glyphs) {

	Pire::Fsm ParseFsm(const char* regexp)
	{
		TVector<wchar32> ucs4;
		Pire::Encodings::Utf8().FromLocal(regexp, regexp + strlen(regexp), std::back_inserter(ucs4));
		return Pire::Lexer(ucs4).SetEncoding(Pire::Encodings::Utf8()).AddFeature(Pire::Features::GlueSimilarGlyphs()).Parse().Surround();
	}

#define NOGL_REGEXP(str) REGEXP2(str, "u")
#define GL_REGEXP(str) SCANNER(ParseFsm(str))

	Y_UNIT_TEST(Glyphs)
	{
		NOGL_REGEXP("regexp") {
			ACCEPTS("regexp");
			DENIES("r\xD0\xB5g\xD0\xB5\xD1\x85\xD1\x80");
		}

		GL_REGEXP("regexp") {
			ACCEPTS("regexp");
			ACCEPTS("r\xD0\xB5g\xD0\xB5\xD1\x85\xD1\x80");
		}

		NOGL_REGEXP("r\xD0\xB5g\xD0\xB5\xD1\x85\xD1\x80") {
			DENIES("regexp");
			ACCEPTS("r\xD0\xB5g\xD0\xB5\xD1\x85\xD1\x80");
		}

		GL_REGEXP("r\xD0\xB5g\xD0\xB5\xD1\x85\xD1\x80") {
			ACCEPTS("regexp");
			ACCEPTS("r\xD0\xB5g\xD0\xB5\xD1\x85\xD1\x80");
		}
	}
}
