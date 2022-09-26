/*
 * unicode_range_ut.cpp --
 *
 * Copyright (c) 2019 YANDEX LLC
 * Author: Karina Usmanova <usmanova.karin@yandex.ru>
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
#include "stub/cppunit.h"
#include "common.h"

Y_UNIT_TEST_SUITE(ReadUnicodeTest) {
	ystring CreateStringWithZeroSymbol(const char* str, size_t pos) {
		ystring result = str;
		Y_ASSERT(pos < result.size());
		result[pos] = '\0';
		return result;
	}

	Y_UNIT_TEST(ZeroSymbol)
	{
		REGEXP("\\x{0}") {
			ACCEPTS(CreateStringWithZeroSymbol("a", 0));
			ACCEPTS(CreateStringWithZeroSymbol("some text", 3));
			DENIES("string without zero");
		}

		REGEXP("the\\x00middle") {
			ACCEPTS(CreateStringWithZeroSymbol("in the middle", 6));
			DENIES(CreateStringWithZeroSymbol("in the middle", 5));
			DENIES("in the middle");
		}
	}

	Y_UNIT_TEST(SymbolsByCodes)
	{
		REGEXP("\\x{41}") {
			ACCEPTS("A");
			ACCEPTS("tAst string");
			DENIES("test string");
		}

		REGEXP("\\x26abc") {
			ACCEPTS("&abc;");
			DENIES("test &ab");
			DENIES("without");
		}
	}

	Y_UNIT_TEST(ErrorsWhileCompiling)
	{
		UNIT_ASSERT(HasError("\\x"));
		UNIT_ASSERT(HasError("\\x0"));
		UNIT_ASSERT(HasError("\\xfu"));
		UNIT_ASSERT(HasError("\\xs1"));
		UNIT_ASSERT(HasError("\\x 0"));
		UNIT_ASSERT(HasError("\\x0 "));

		UNIT_ASSERT(HasError("\\x{2A1"));
		UNIT_ASSERT(HasError("\\x{"));
		UNIT_ASSERT(HasError("\\x}"));
		UNIT_ASSERT(HasError("\\x2}"));
		UNIT_ASSERT(HasError("\\x{{3}"));
		UNIT_ASSERT(HasError("\\x{2a{5}"));

		UNIT_ASSERT(HasError("\\x{}"));
		UNIT_ASSERT(HasError("\\x{+3}"));
		UNIT_ASSERT(HasError("\\x{-3}"));
		UNIT_ASSERT(HasError("\\x{ 2F}"));
		UNIT_ASSERT(HasError("\\x{2A F}"));
		UNIT_ASSERT(HasError("\\x{2Arft}"));
		UNIT_ASSERT(HasError("\\x{110000}"));

		UNIT_ASSERT(!HasError("\\x{fB1}"));
		UNIT_ASSERT(!HasError("\\x00"));
		UNIT_ASSERT(!HasError("\\x{10FFFF}"));
	}

	Y_UNIT_TEST(OneCharacterRange)
	{
		SCANNER("[\\x{61}]") {
			ACCEPTS("a");
			ACCEPTS("bac");
			DENIES("test");
		}

		SCANNER("[\\x3f]") {
			ACCEPTS("?");
			ACCEPTS("test?");
			DENIES("test");
		}
	}

	Y_UNIT_TEST(CharacterRange) {
		REGEXP("[\\x{61}\\x62\\x{3f}\\x26]") {
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("?");
			ACCEPTS("acd");
			ACCEPTS("bcd");
			ACCEPTS("cd?");
			ACCEPTS("ab?");
			DENIES("cd");
		}

		REGEXP("[\\x{61}-\\x{63}]") {
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("c");
			ACCEPTS("qwertya");
			DENIES("d");
		}

		REGEXP("[\\x61-\\x61]") {
			ACCEPTS("a");
			ACCEPTS("qwertya");
			DENIES("b");
		}

		REGEXP("[\\x26\\x{61}-\\x{62}\\x{3f}]") {
			ACCEPTS("&");
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("?");
			ACCEPTS("ade");
			ACCEPTS("ab?");
			DENIES("d");
		}

		REGEXP("[\\x{41}-\\x{42}\\x{61}-\\x{62}]") {
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("A");
			ACCEPTS("B");
			DENIES("c");
			DENIES("C");
		}

		REGEXP("[\\x{41}-\\x{42}][\\x{61}-\\x{62}]") {
			ACCEPTS("Aa");
			ACCEPTS("Ab");
			ACCEPTS("Ba");
			ACCEPTS("Bb");
			DENIES("a");
			DENIES("b");
			DENIES("A");
			DENIES("B");
			DENIES("ab");
			DENIES("AB");
			DENIES("Ca");
		}
	}

	Y_UNIT_TEST(RangeExcludeCharacters) {
		REGEXP("[^\\x{61}]") {
			ACCEPTS("b");
			ACCEPTS("c");
			ACCEPTS("aba");
			DENIES("a");
			DENIES("aaa");
		}

		REGEXP("[^\\x{61}-\\x{7a}]") {
			ACCEPTS("A");
			ACCEPTS("123");
			ACCEPTS("acb1");
			DENIES("a");
			DENIES("abcxyz");
		}
	}

	Y_UNIT_TEST(MixedRange) {
		REGEXP("[\\x{61}B]") {
			ACCEPTS("a");
			ACCEPTS("B");
			ACCEPTS("atestB");
			DENIES("test");
		}

		REGEXP("[^\\x{61}A]") {
			ACCEPTS("b");
			ACCEPTS("B");
			ACCEPTS("atestB");
			DENIES("a");
			DENIES("A");
			DENIES("aaAA");
		}

		REGEXP("[0-9][\\x{61}-\\x{62}A-B]") {
			ACCEPTS("0a");
			ACCEPTS("1A");
			ACCEPTS("5b");
			ACCEPTS("9B");
			ACCEPTS("1atestB");
			ACCEPTS("2Atest");
			DENIES("aB");
			DENIES("testb");
			DENIES("test");
		}

		REGEXP("[\\x{61}-c]") {
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("c");
			ACCEPTS("testb");
			DENIES("d");
		}

		REGEXP("[^a-\\x{7a}]") {
			ACCEPTS("A");
			ACCEPTS("123");
			ACCEPTS("acb1");
			DENIES("a");
			DENIES("abcxyz");
		}

		REGEXP("[\\x{41}-Ba-\\x{62}]") {
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("A");
			ACCEPTS("B");
			DENIES("c");
			DENIES("C");
		}
	}

	Y_UNIT_TEST(CompilingRange)
	{
		UNIT_ASSERT(HasError("[\\x41"));
		UNIT_ASSERT(HasError("[\\xfq]"));
		UNIT_ASSERT(HasError("[\\x{01}-]"));

		UNIT_ASSERT(!HasError("[\\x{10FFFF}]"));
		UNIT_ASSERT(!HasError("[\\x{00}]"));
		UNIT_ASSERT(!HasError("[\\x{abc}-\\x{FFF}]"));

		UNIT_ASSERT(!HasError("[^\\xFF]"));
		UNIT_ASSERT(!HasError("[^\\x{FF}-\\x{FF0}]"));
		UNIT_ASSERT(!HasError("[-\\x{01}]"));
	}

	Y_UNIT_TEST(UnicodeRepetition)
	{
		REGEXP("^\\x{78}{3,6}$") {
			DENIES ("xx");
			ACCEPTS("xxx");
			ACCEPTS("xxxx");
			ACCEPTS("xxxxx");
			ACCEPTS("xxxxxx");
			DENIES ("xxxxxxx");
		}

		REGEXP("^x{3,}$") {
			DENIES ("xx");
			ACCEPTS("xxx");
			ACCEPTS("xxxx");
			ACCEPTS("xxxxxxxxxxx");
			ACCEPTS("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		}

		REGEXP("^\\x{78}{3}$") {
			DENIES ("x");
			DENIES ("xx");
			ACCEPTS("xxx");
			DENIES ("xxxx");
			DENIES ("xxxxx");
			DENIES ("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		}

		REGEXP("^([\\x{78}-\\x{79}]){2}$") {
			DENIES("x");
			DENIES("y");
			ACCEPTS("xx");
			ACCEPTS("xy");
			ACCEPTS("yx");
			ACCEPTS("yy");
			DENIES("xxy");
			DENIES("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		}
	}

	Y_UNIT_TEST(AnyUnicodeCodepointIsAllowed)
	{
		REGEXP("[\\x{0}-\\x{77}\\x{79}-\\x{10ffff}]") {
			ACCEPTS("w");
			DENIES ("x");
			ACCEPTS("y");
		}
	}

}
