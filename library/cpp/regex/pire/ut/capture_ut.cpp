/*
 * capture_ut.cpp --
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
#include <stub/saveload.h>
#include <stub/utf8.h>
#include <stub/memstreams.h>
#include "stub/cppunit.h"
#include <pire.h>
#include <extra.h>
#include <string.h>

Y_UNIT_TEST_SUITE(TestPireCapture) {

	using Pire::CapturingScanner;
	using Pire::SlowCapturingScanner;
	typedef Pire::CapturingScanner::State State;

	CapturingScanner Compile(const char* regexp, int index)
	{
		Pire::Lexer lexer;

		lexer.Assign(regexp, regexp + strlen(regexp));
		lexer.AddFeature(Pire::Features::CaseInsensitive());
		lexer.AddFeature(Pire::Features::Capture((size_t) index));

		Pire::Fsm fsm = lexer.Parse();

		fsm.Surround();
		fsm.Determine();
		return fsm.Compile<Pire::CapturingScanner>();
	}

	SlowCapturingScanner SlowCompile(const char* regexp, int index, const Pire::Encoding& encoding = Pire::Encodings::Utf8())
	{
		Pire::Lexer lexer;
		lexer.AddFeature(Pire::Features::Capture(static_cast<size_t>(index)));
		lexer.SetEncoding(encoding);
		TVector<wchar32> ucs4;
		encoding.FromLocal(regexp, regexp + strlen(regexp), std::back_inserter(ucs4));
		lexer.Assign(ucs4.begin(), ucs4.end());
		Pire::Fsm fsm = lexer.Parse();
		fsm.Surround();
		return fsm.Compile<Pire::SlowCapturingScanner>();
	}

	State RunRegexp(const CapturingScanner& scanner, const char* str)
	{
		State state;
		scanner.Initialize(state);
		Step(scanner, state, Pire::BeginMark);
		Run(scanner, state, str, str + strlen(str));
		Step(scanner, state, Pire::EndMark);
		return state;
	}

	SlowCapturingScanner::State RunRegexp(const SlowCapturingScanner& scanner, const char* str)
	{
		SlowCapturingScanner::State state;
		scanner.Initialize(state);
		Run(scanner, state, str, str + strlen(str));
		return state;
	}

	ystring Captured(const State& state, const char* str)
	{
		if (state.Captured())
			return ystring(str + state.Begin() - 1, str + state.End() - 1);
		else
			return ystring();
	}

	Y_UNIT_TEST(Trivial)
	{
		CapturingScanner scanner = Compile("google_id\\s*=\\s*[\'\"]([a-z0-9]+)[\'\"]\\s*;", 1);
		State state;
		const char* str;

		str = "google_id = 'abcde';";
		state = RunRegexp(scanner, str);
		UNIT_ASSERT(state.Captured());
		UNIT_ASSERT_EQUAL(Captured(state, str), ystring("abcde"));

		str = "var google_id = 'abcde'; eval(google_id);";
		state = RunRegexp(scanner, str);
		UNIT_ASSERT(state.Captured());
		UNIT_ASSERT_EQUAL(Captured(state, str), ystring("abcde"));

		str = "google_id != 'abcde';";
		state = RunRegexp(scanner, str);
		UNIT_ASSERT(!state.Captured());
	}

	Y_UNIT_TEST(Sequential)
	{
		CapturingScanner scanner = Compile("google_id\\s*=\\s*[\'\"]([a-z0-9]+)[\'\"]\\s*;", 1);
		State state;
		const char* str;

		str = "google_id = 'abcde'; google_id = 'xyz';";
		state = RunRegexp(scanner, str);
		UNIT_ASSERT(state.Captured());
		UNIT_ASSERT_VALUES_EQUAL(Captured(state, str), ystring("abcde"));

		str = "var google_id = 'abc de'; google_id = 'xyz';";
		state = RunRegexp(scanner, str);
		UNIT_ASSERT(state.Captured());
		UNIT_ASSERT_VALUES_EQUAL(Captured(state, str), ystring("xyz"));
	}

	Y_UNIT_TEST(NegatedTerminator)
	{
		CapturingScanner scanner = Compile("=(\\d+)[^\\d]", 1);
		State state;
		const char* str;

		str = "=12345;";
		state = RunRegexp(scanner, str);
		UNIT_ASSERT(state.Captured());
		UNIT_ASSERT_EQUAL(Captured(state, str), ystring("12345"));
	}

	Y_UNIT_TEST(Serialization)
	{
		const char* regex = "google_id\\s*=\\s*[\'\"]([a-z0-9]+)[\'\"]\\s*;";
		CapturingScanner scanner2 = Compile(regex, 1);
		SlowCapturingScanner slowScanner2 = SlowCompile(regex, 1);
		BufferOutput wbuf, wbuf2;
		::Save(&wbuf, scanner2);
		::Save(&wbuf2, slowScanner2);

		MemoryInput rbuf(wbuf.Buffer().Data(), wbuf.Buffer().Size());
		MemoryInput rbuf2(wbuf2.Buffer().Data(), wbuf2.Buffer().Size());
		CapturingScanner scanner;
		SlowCapturingScanner slowScanner;
		::Load(&rbuf, scanner);
		::Load(&rbuf2, slowScanner);

		State state;
		SlowCapturingScanner::State slowState;
		const char* str;

		str = "google_id = 'abcde';";
		state = RunRegexp(scanner, str);
		slowState = RunRegexp(slowScanner, str);
		UNIT_ASSERT(state.Captured());
		UNIT_ASSERT_EQUAL(Captured(state, str), ystring("abcde"));
		SlowCapturingScanner::SingleState final;
		UNIT_ASSERT(slowScanner.GetCapture(slowState, final));
		ystring ans(str, final.GetBegin(), final.GetEnd() - final.GetBegin());
		UNIT_ASSERT_EQUAL(ans, ystring("abcde"));

		str = "google_id != 'abcde';";
		state = RunRegexp(scanner, str);
		slowState = RunRegexp(slowScanner, str);
		UNIT_ASSERT(!state.Captured());
		UNIT_ASSERT(!slowScanner.GetCapture(slowState, final));

		CapturingScanner scanner3;
		const size_t MaxTestOffset = 2 * sizeof(Pire::Impl::MaxSizeWord);
		TVector<char> buf2(wbuf.Buffer().Size() + sizeof(size_t) + MaxTestOffset);
		const void* ptr = Pire::Impl::AlignUp(&buf2[0], sizeof(size_t));
		memcpy((void*) ptr, wbuf.Buffer().Data(), wbuf.Buffer().Size());
		const void* tail = scanner3.Mmap(ptr, wbuf.Buffer().Size());
		UNIT_ASSERT_EQUAL(tail, (const void*) ((size_t)ptr + wbuf.Buffer().Size()));

		str = "google_id = 'abcde';";
		state = RunRegexp(scanner3, str);
		UNIT_ASSERT(state.Captured());
		UNIT_ASSERT_EQUAL(Captured(state, str), ystring("abcde"));

		str = "google_id != 'abcde';";
		state = RunRegexp(scanner3, str);
		UNIT_ASSERT(!state.Captured());

		ptr = (const void*) ((const char*) wbuf.Buffer().Data() + 1);
		try {
			scanner3.Mmap(ptr, wbuf.Buffer().Size());
			UNIT_ASSERT(!"CapturingScanner failed to check for misaligned mmaping");
		}
		catch (Pire::Error&) {}

		for (size_t offset = 1; offset < MaxTestOffset; ++offset) {
			ptr = Pire::Impl::AlignUp(&buf2[0], sizeof(size_t)) + offset;
			memcpy((void*) ptr, wbuf.Buffer().Data(), wbuf.Buffer().Size());
			try {
				scanner3.Mmap(ptr, wbuf.Buffer().Size());
				if (offset % sizeof(size_t) != 0) {
					UNIT_ASSERT(!"CapturingScanner failed to check for misaligned mmaping");
				} else {
					str = "google_id = 'abcde';";
					state = RunRegexp(scanner3, str);
					UNIT_ASSERT(state.Captured());
				}
			}
			catch (Pire::Error&) {}
		}
	}

	Y_UNIT_TEST(Empty)
	{
		Pire::CapturingScanner sc;
		UNIT_ASSERT(sc.Empty());
		
		UNIT_CHECKPOINT(); RunRegexp(sc, "a string"); // Just should not crash

		// Test Save/Load/Mmap
		BufferOutput wbuf;
		::Save(&wbuf, sc);

		MemoryInput rbuf(wbuf.Buffer().Data(), wbuf.Buffer().Size());
		Pire::CapturingScanner sc3;
		::Load(&rbuf, sc3);
		UNIT_CHECKPOINT(); RunRegexp(sc3, "a string");

		const size_t MaxTestOffset = 2 * sizeof(Pire::Impl::MaxSizeWord);
		TVector<char> buf2(wbuf.Buffer().Size() + sizeof(size_t) + MaxTestOffset);
		const void* ptr = Pire::Impl::AlignUp(&buf2[0], sizeof(size_t));
		memcpy((void*) ptr, wbuf.Buffer().Data(), wbuf.Buffer().Size());

		Pire::CapturingScanner sc4;
		const void* tail = sc4.Mmap(ptr, wbuf.Buffer().Size());
		UNIT_ASSERT_EQUAL(tail, (const void*) ((size_t)ptr + wbuf.Buffer().Size()));
		UNIT_CHECKPOINT(); RunRegexp(sc4, "a string");
	}

	void MakeSlowCapturingTest(const char* regexp, const char* text, size_t position, bool ans, const ystring& captured = ystring(""), const Pire::Encoding& encoding = Pire::Encodings::Utf8())
	{
		Pire::SlowCapturingScanner sc = SlowCompile(regexp, position, encoding);
		SlowCapturingScanner::State st = RunRegexp(sc, text);
		SlowCapturingScanner::SingleState fin;
		bool ifCaptured = sc.GetCapture(st, fin);
		if (ans) {
			UNIT_ASSERT(ifCaptured);
			ystring answer(text, fin.GetBegin(), fin.GetEnd() - fin.GetBegin());
			UNIT_ASSERT_EQUAL(answer, captured);
		} else  {
			UNIT_ASSERT(!ifCaptured);
		}
	}

	Y_UNIT_TEST(SlowCapturingNonGreedy)
	{
		const char* regexp = ".*?(pref.*suff)";
		const char* text = "pref ala bla pref cla suff dla";
		MakeSlowCapturingTest(regexp, text, 1, true, ystring("pref ala bla pref cla suff"));
	}

	Y_UNIT_TEST(SlowCaptureGreedy)
	{
		const char* regexp = ".*(pref.*suff)";
		const char* text = "pref ala bla pref cla suff dla";
		MakeSlowCapturingTest(regexp, text, 1, true, ystring("pref cla suff"));
	}

	Y_UNIT_TEST(SlowCaptureInOr)
	{
		const char* regexp = "(A)|A";
		const char* text = "A";
		MakeSlowCapturingTest(regexp, text, 1, true, ystring("A"));
		const char* regexp2 = "A|(A)";
		MakeSlowCapturingTest(regexp2, text, 1, false);
	}

	Y_UNIT_TEST(SlowCapturing)
	{
		const char* regexp = "^http://vk(ontakte[.]ru|[.]com)/id(\\d+)([^0-9]|$)";
		const char* text = "http://vkontakte.ru/id100500";
		MakeSlowCapturingTest(regexp, text, 2, true, ystring("100500"));
	}

	Y_UNIT_TEST(Utf_8)
	{
		const char* regexp = "\xd0\x97\xd0\xb4\xd1\x80\xd0\xb0\xd0\xb2\xd1\x81\xd1\x82\xd0\xb2\xd1\x83\xd0\xb9\xd1\x82\xd0\xb5, ((\\s|\\w|[()]|-)+)!";
		const char* text ="    \xd0\x97\xd0\xb4\xd1\x80\xd0\xb0\xd0\xb2\xd1\x81\xd1\x82\xd0\xb2\xd1\x83\xd0\xb9\xd1\x82\xd0\xb5, \xd0\xa3\xd0\xb2\xd0\xb0\xd0\xb6\xd0\xb0\xd0\xb5\xd0\xbc\xd1\x8b\xd0\xb9 (-\xd0\xb0\xd1\x8f)!   ";
		const char* ans = "\xd0\xa3\xd0\xb2\xd0\xb0\xd0\xb6\xd0\xb0\xd0\xb5\xd0\xbc\xd1\x8b\xd0\xb9 (-\xd0\xb0\xd1\x8f)";
		MakeSlowCapturingTest(regexp, text, 1, true, ystring(ans));
	}
}
