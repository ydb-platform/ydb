/*
 * count_ut.cpp --
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


Y_UNIT_TEST_SUITE(TestCount) {

	Pire::Fsm MkFsm(const char* regexp, const Pire::Encoding& encoding)
	{
		Pire::Lexer lex;
		lex.SetEncoding(encoding);
		TVector<wchar32> ucs4;
		encoding.FromLocal(regexp, regexp + strlen(regexp), std::back_inserter(ucs4));
		lex.Assign(ucs4.begin(), ucs4.end());
		return lex.Parse();
	}

	template<class Scanner>
	typename Scanner::State InitializedState(const Scanner& scanner)
	{
		typename Scanner::State state;
		scanner.Initialize(state);
		return state;
	}

	template<class Scanner>
	typename Scanner::State Run(const Scanner& scanner, const char* text, size_t len =-1)
	{
		if (len == (size_t)-1) len = strlen(text);
		auto state = InitializedState(scanner);
		Pire::Step(scanner, state, Pire::BeginMark);
		Pire::Run(scanner, state, text, text + len);
		Pire::Step(scanner, state, Pire::EndMark);
		return state;
	}

	template<class Scanner>
	size_t CountOne(const char* regexp, const char* separator, const char* text, size_t len = -1, const Pire::Encoding& encoding = Pire::Encodings::Utf8())
	{
		const auto regexpFsm = MkFsm(regexp, encoding);
		const auto separatorFsm = MkFsm(separator, encoding);
		return Run(Scanner{regexpFsm, separatorFsm}, text, len).Result(0);
	}

	size_t Count(const char* regexp, const char* separator, const char* text, size_t len = -1, const Pire::Encoding& encoding = Pire::Encodings::Utf8())
	{
		const auto regexpFsm = MkFsm(regexp, encoding);
		const auto separatorFsm = MkFsm(separator, encoding);
		auto countingResult = Run(Pire::CountingScanner{regexpFsm, separatorFsm}, text, len).Result(0);
		auto newResult = Run(Pire::AdvancedCountingScanner{regexpFsm, separatorFsm}, text, len).Result(0);
		if (strcmp(separator, ".*") == 0) {
			HalfFinalFsm fsm(regexpFsm);
			fsm.MakeGreedyCounter(true);
			auto halfFinalSimpleResult = Run(Pire::HalfFinalScanner{fsm}, text, len).Result(0);
			fsm = HalfFinalFsm(regexpFsm);
			fsm.MakeGreedyCounter(false);
			auto halfFinalCorrectResult = Run(Pire::HalfFinalScanner{fsm}, text, len).Result(0);
			UNIT_ASSERT_EQUAL(halfFinalSimpleResult, halfFinalCorrectResult);
			UNIT_ASSERT_EQUAL(halfFinalSimpleResult, countingResult);
		}
		UNIT_ASSERT_EQUAL(countingResult, newResult);
		auto noGlueLimitResult = Run(Pire::NoGlueLimitCountingScanner{regexpFsm, separatorFsm}, text, len).Result(0);
		UNIT_ASSERT_EQUAL(countingResult, noGlueLimitResult);
		return newResult;
	}

	Y_UNIT_TEST(Count)
	{
		UNIT_ASSERT_EQUAL(Count("[a-z]+", "\\s",  "abc def, abc def ghi, abc"), size_t(3));
		char aaa[] = "abc def\0 abc\0 def ghi, abc";
		UNIT_ASSERT_EQUAL(Count("[a-z]+", ".*", aaa, sizeof(aaa), Pire::Encodings::Latin1()), size_t(6));
		UNIT_ASSERT_EQUAL(Count("[a-z]+", ".*", aaa, sizeof(aaa)), size_t(6));
		UNIT_ASSERT_EQUAL(Count("\\w", "", "abc abcdef abcd abcdefgh ac"), size_t(8));
		UNIT_ASSERT_EQUAL(Count("http", ".*", "http://aaa, http://bbb, something in the middle, http://ccc, end"), size_t(3));
		UNIT_ASSERT_EQUAL(Count("abc", ".*", "abcabcabcabc"), size_t(4));
		UNIT_ASSERT_EQUAL(Count("[\320\220-\320\257\320\260-\321\217]+", "\\s+", " \320\257\320\275\320\264\320\265\320\272\321\201      "
			"\320\237\320\276\320\262\320\265\321\200\320\275\321\203\321\202\321\214  \320\222\320\276\320\271\321\202\320\270\302\240"
			"\320\262\302\240\320\277\320\276\321\207\321\202\321\203                       \302\251\302\240" "1997\342\200\224" "2008 "
			"\302\253\320\257\320\275\320\264\320\265\320\272\321\201\302\273    \320\224\320\270\320\267\320\260\320\271\320\275\302"
			"\240\342\200\224\302\240\320\241\321\202\321\203\320\264\320\270\321\217 \320\220\321\200\321\202\320\265\320\274\320\270"
			"\321\217\302\240\320\233\320\265\320\261\320\265\320\264\320\265\320\262\320\260\012\012"), size_t(5));
		UNIT_ASSERT_EQUAL(Count("\321\201\320\265\320\272\321\201", ".*",
			"\320\277\320\276\321\200\320\275\320\276, \320\273\320\265\321\202 10 \320\263\320\276\320\273\321\213\320\265 12 "
			"\320\264\320\265\321\202\320\270, \320\264\320\265\321\202\320\270 \320\277\320\276\321\200\320\275\320\276 "
			"\320\262\320\270\320\264\320\265\320\276 \320\261\320\265\321\201\320\277\320\273\320\260\321\202\320\275\320\276\320\265. "
			"\320\261\320\265\321\201\320\277\320\273\320\260\321\202\320\275\320\276\320\265 \320\262\320\270\320\264\320\265\320\276 "
			"\320\277\320\276\321\200\320\275\320\276 \320\264\320\265\321\202\320\270. \320\264\320\265\321\202\320\270 "
			"\320\277\320\276\321\200\320\275\320\276 \320\262\320\270\320\264\320\265\320\276 "
			"\320\261\320\265\321\201\320\277\320\273\320\260\321\202\320\275\320\276\320\265!<br> "
			"\320\264\320\265\320\262\321\203\321\210\320\272\321\203 \320\264\320\273\321\217 \320\277\320\276\320\264 "
			"\321\201\320\265\320\272\321\201\320\260 \320\277\320\260\321\200\320\276\320\271 "
			"\321\201\320\265\320\274\320\265\320\271\320\275\320\276\320\271 \321\201 \320\270\321\211\320\265\320\274 "
			"\320\272\320\260\320\271\321\204\320\276\320\274. \321\201\320\265\320\274\320\265\320\271\320\275\320\276\320\271 "
			"\320\277\320\276\320\264 \320\272\320\260\320\271\321\204\320\276\320\274 "
			"\320\264\320\265\320\262\321\203\321\210\320\272\321\203 \320\277\320\260\321\200\320\276\320\271 "
			"\320\270\321\211\320\265\320\274 \321\201 \320\264\320\273\321\217 \321\201\320\265\320\272\321\201\320\260!<br> "
			"\321\202\320\270\321\202\321\214\320\272\320\270 \320\261\320\276\320\273\321\214\321\210\320\270\320\265. "
			"\320\273\320\265\321\202 10 \320\263\320\276\320\273\321\213\320\265 12 \320\264\320\265\321\202\320\270!<br> "
			"\320\270\321\211\320\265\320\274 \321\201 \320\277\320\276\320\264 \320\272\320\260\320\271\321\204\320\276\320\274 "
			"\321\201\320\265\320\272\321\201\320\260\320\277\320\260\321\200\320\276\320\271 \320\264\320\273\321\217 "
			"\320\264\320\265\320\262\321\203\321\210\320\272\321\203 \321\201\320\265\320\274\320\265\320\271\320\275\320\276\320\271! "
			"\320\261\320\276\320\273\321\214\321\210\320\270\320\265 \321\202\320\270\321\202\321\214\320\272\320\270, "
			"\320\273\320\265\320\272\320\260\321\200\321\201\321\202\320\262\320\260 \321\201\320\270\321\201\321\202\320\265\320\274\320\260 "
			"\320\264\320\273\321\217 \320\276\320\277\320\276\321\200\320\275\320\276-\320\264\320\262\320\270\320\263\320\260\321\202"
			"\320\265\320\273\321\214\320\275\320\260\321\217 \320\266\320\270\320\262\320\276\321\202\320\275\321\213\321\205, \320\264"
			"\320\273\321\217 \320\270\321\211\320\265\320\274 \321\201\320\265\320\272\321\201\320\260 \320\272\320\260\320\271\321\204"
			"\320\276\320\274 \320\264\320\265\320\262\321\203\321\210\320\272\321\203 \321\201\320\265\320\274\320\265\320\271\320\275"
			"\320\276\320\271 \320\277\320\276\320\264 \320\277\320\260\321\200\320\276\320\271 \321\201. \320\276\320\277\320\276\321"
			"\200\320\275\320\276-\320\264\320\262\320\270\320\263\320\260\321\202\320\265\320\273\321\214\320\275\320\260\321\217 \321"
			"\201\320\270\321\201\321\202\320\265\320\274\320\260 \320\273\320\265\320\272\320\260\321\200\321\201\321\202\320\262\320\260 "
			"\320\264\320\273\321\217 \320\266\320\270\320\262\320\276\321\202\320\275\321\213\321\205, \320\261\320\265\321\201\320\277"
			"\320\273\320\260\321\202\320\275\320\276\320\265 \320\277\320\276\321\200\320\275\320\276 \320\262\320\270\320\264\320\265"
			"\320\276 \320\264\320\265\321\202\320\270. \320\276\321\204\320\270\321\206\320\265\321\200\321\213 \320\277\320\276\321"
			"\200\320\275\320\276 \321\204\320\276\321\202\320\276 \320\263\320\265\320\270, \320\270\321\211\320\265\320\274 \321\201"
			"\320\265\320\274\320\265\320\271\320\275\320\276\320\271 \320\264\320\265\320\262\321\203\321\210\320\272\321\203 \320\277"
			"\320\276 \320\277\320\260\321\200\320\276\320\271 \321\201\320\265\320\272\321\201\320\260 \320\264\320\273\321\217 \321\201 "
			"\320\272\320\260\320\271\321\204\320\276\320\274. \320\277\320\276\320\264 \320\264\320\273\321\217 \320\272\320\260\320\271"
			"\321\204\320\276\320\274 \321\201\320\265\320\274\320\265\320\271\320\275\320\276\320\271 \321\201\320\265\320\272\321\201"
			"\320\260 \320\277\320\260\321\200\320\276\320\271 \321\201 \320\264\320\265\320\262\321\203\321\210\320\272\321\203 \320\270"
			"\321\211\320\265\320\274? \320\262\320\270\320\264\320\265\320\276 \320\261\320\265\321\201\320\277\320\273\320\260\321\202"
			"\320\275\320\276\320\265 \320\277\320\276\321\200\320\275\320\276 \320\264\320\265\321\202\320\270, \320\264\320\265\321\202"
			"\320\270 \320\261\320\265\321\201\320\277\320\273\320\260\321\202\320\275\320\276\320\265"),
			size_t(6));
		UNIT_ASSERT_EQUAL(Count("<a[^>]*>[^<]*</a>", "([^<]|<br\\s?/?>)*", "\321\200\320\275\320\276</a><br />"
			"<a href=\"http://wapspzk.1sweethost.com//22.html\">\320\264\320\265\321\210\320\265\320\262\321\213\320\265 \320\277\320\276"
			"\321\200\320\275\320\276 \321\204\320\270\320\273\321\214\320\274\321\213</a><br /><a href=\"http://wapspzk.1sweethost.com//23.html\">"
			"\321\201\320\265\320\272\321\201 \321\210\320\276\320\277 \321\200\320\276\321\201\320\270\321\202\320\260</a><br />"
			"<a href=\"http://wapspzk.1sweethost.com//24.html\">\320\263\320\276\320\273\321\213\320\265 \320\264\320\265\320\262\321\203"
			"\321\210\320\272\320\270 \321\203\320\273\320\270\321\206\320\260</a><br /><a href=\"http://wapspzk.1sweethost.com//25.html\">"
			"\321\202\321\200\320\260\321\205\320\275\321\203\321\202\321\214 \320\274\320\260\320\274\320\260\321\210\320\270</a><br />"
			"<a href=\"http://wapspzk.1sweethost.com//26.html\">\320\277\320\270\320\267\320\264\320\260 \321\204\321\200\320\270\321\201"
			"\320\272\320\265</a><br /><a href=\"http://wapspzk.1sweethost.com//27.html\">\320\261\320\265\321\201\320\277\320\273\320\260"
			"\321\202\320\275\320\276</a><br /><a href=\"http://wapspzk.1sweethost.com//33.html\">\321\201\320\276\321\206\320\270\320\276"
			"\320\273\320\276\320\263\320\270\321\207\320\265\321\201\320\272\320\270\320\271 \320\260\320\275\320\260\320\273\320\270\320"
			"\267 \320\274\320\276\320\264\320\265\320\273\320\265\320\271 \321\201\320\265\320\272\321\201\321\203\320\260\320\273\321\214"
			"\320\275\320\276\320\263\320\276 \320\277\320\276\320\262\320\265\320\264\320\265\320\275\320\270\321\217</a>\321\217"), size_t(7));
		UNIT_ASSERT(CountOne<Pire::CountingScanner>("a", "b", "aaa") != size_t(3));
		UNIT_ASSERT_EQUAL(CountOne<Pire::AdvancedCountingScanner>("a", "b", "aaa"), size_t(1));
		UNIT_ASSERT_EQUAL(CountOne<Pire::AdvancedCountingScanner>("[a-z\320\260-\321\217]+", " +",
			" \320\260\320\260\320\220 abc def \320\260  cd"),
			size_t(4)); // Pire::CountingScanner returns 1 here, since it enters a dead state
	}

	Y_UNIT_TEST(CountWithoutSeparator)
	{
		UNIT_ASSERT_EQUAL(Count("a", "",  "aa aaa"), size_t(3));
	}

	Y_UNIT_TEST(CountGreedy)
	{
		const auto& enc = Pire::Encodings::Latin1();
		char text[] = "wwwsswwwsssswwws";
		UNIT_ASSERT_EQUAL(CountOne<Pire::AdvancedCountingScanner>("www", ".{1,6}", text, sizeof(text), enc), size_t(3));
		UNIT_ASSERT_EQUAL(CountOne<Pire::NoGlueLimitCountingScanner>("www", ".{1,6}", text, sizeof(text), enc), size_t(3));
		UNIT_ASSERT_EQUAL(CountOne<Pire::AdvancedCountingScanner>("www.{1,6}", "", text, sizeof(text), enc), size_t(3));
		UNIT_ASSERT_EQUAL(CountOne<Pire::NoGlueLimitCountingScanner>("www.{1,6}", "", text, sizeof(text), enc), size_t(3));
	}

	Y_UNIT_TEST(CountRepeating)
	{
		char text[] = "abbabbabbabbat";
		UNIT_ASSERT_EQUAL(Count("abba", ".*", text, sizeof(text), Pire::Encodings::Latin1()), size_t(2));
	}

	template<class Scanner>
	void CountGlueOne()
	{
		const auto& enc = Pire::Encodings::Utf8();
		auto sc1 = Scanner(MkFsm("[a-z]+", enc), MkFsm(".*", enc));
		auto sc2 = Scanner(MkFsm("[0-9]+", enc), MkFsm(".*", enc));
		auto sc  = Scanner::Glue(sc1, sc2);
		auto st = Run(sc, "abc defg 123 jklmn 4567 opqrst");
		UNIT_ASSERT_EQUAL(st.Result(0), size_t(4));
		UNIT_ASSERT_EQUAL(st.Result(1), size_t(2));
	}

	Y_UNIT_TEST(CountGlue)
	{
		CountGlueOne<Pire::CountingScanner>();
		CountGlueOne<Pire::AdvancedCountingScanner>();
		CountGlueOne<Pire::NoGlueLimitCountingScanner>();
	}

	template <class Scanner>
	void CountManyGluesOne(size_t maxRegexps) {
		const auto& encoding = Pire::Encodings::Utf8();
		auto text = "abcdbaa aa";
		TVector<ypair<std::string, std::string>> tasks = {
			{"a", ".*"},
			{"b", ".*"},
			{"c", ".*"},
			{"ba", ".*"},
			{"ab",".*"},
		};
		TVector<size_t> answers = {5, 2, 1, 1, 1};
		Scanner scanner;
		size_t regexpsCount = 0;
		for (; regexpsCount < maxRegexps; ++regexpsCount) {
			const auto& task = tasks[regexpsCount % tasks.size()];
			const auto regexpFsm = MkFsm(task.first.c_str(), encoding);
			const auto separatorFsm = MkFsm(task.second.c_str(), encoding);
			Scanner nextScanner(regexpFsm, separatorFsm);
			auto glue = Scanner::Glue(scanner, nextScanner);
			if (glue.Empty()) {
				break;
			}
			scanner = std::move(glue);
		}
		auto state = Run(scanner, text);
		for (size_t i = 0; i < regexpsCount; ++i) {
			UNIT_ASSERT_EQUAL(state.Result(i), answers[i % answers.size()]);
		}
	}

	Y_UNIT_TEST(CountManyGlues)
	{
		CountManyGluesOne<Pire::CountingScanner>(20);
		CountManyGluesOne<Pire::AdvancedCountingScanner>(20);
		CountManyGluesOne<Pire::NoGlueLimitCountingScanner>(50);
	}

	template<class Scanner>
	void CountBoundariesOne()
	{
		const char* strings[] = { "abcdef", "abc def", "defcba", "wxyz abc", "a", "123" };

		const auto& enc = Pire::Encodings::Utf8();
		Scanner sc(MkFsm("^[a-z]+$", enc), MkFsm("(.|^|$)*", enc));
		auto st = InitializedState(sc);
		for (size_t i = 0; i < sizeof(strings) / sizeof(*strings); ++i) {
			Pire::Step(sc, st, Pire::BeginMark);
			Pire::Run(sc, st, strings[i], strings[i] + strlen(strings[i]));
			Pire::Step(sc, st, Pire::EndMark);
		}
		UNIT_ASSERT_EQUAL(st.Result(0), size_t(3));

		const auto& enc2 = Pire::Encodings::Latin1();
		Scanner sc2(MkFsm("[a-z]", enc2), MkFsm(".*", enc2));
		auto st2 = InitializedState(sc2);
		for (size_t i = 0; i < sizeof(strings) / sizeof(*strings); ++i) {
			Pire::Step(sc2, st2, Pire::BeginMark);
			Pire::Run(sc2, st2, strings[i], strings[i] + strlen(strings[i]));
			Pire::Step(sc2, st2, Pire::EndMark);
		}
		UNIT_ASSERT_EQUAL(st2.Result(0), size_t(7));
	}

	Y_UNIT_TEST(CountBoundaries)
	{
		CountBoundariesOne<Pire::CountingScanner>();
		CountBoundariesOne<Pire::AdvancedCountingScanner>();
		CountBoundariesOne<Pire::NoGlueLimitCountingScanner>();
	}

	template<class Scanner>
	void SerializationOne()
	{
		const auto& enc = Pire::Encodings::Latin1();
		auto sc1 = Scanner(MkFsm("[a-z]+", enc), MkFsm(".*", enc));
		auto sc2 = Scanner(MkFsm("[0-9]+", enc), MkFsm(".*", enc));
		auto sc  = Scanner::Glue(sc1, sc2);

		BufferOutput wbuf;
		::Save(&wbuf, sc);

		MemoryInput rbuf(wbuf.Buffer().Data(), wbuf.Buffer().Size());
		Scanner sc3;
		::Load(&rbuf, sc3);

		auto st = Run(sc3, "abc defg 123 jklmn 4567 opqrst");
		UNIT_ASSERT_EQUAL(st.Result(0), size_t(4));
		UNIT_ASSERT_EQUAL(st.Result(1), size_t(2));

		const size_t MaxTestOffset = 2 * sizeof(Pire::Impl::MaxSizeWord);
		TVector<char> buf2(wbuf.Buffer().Size() + sizeof(size_t) + MaxTestOffset);

		// Test mmap-ing at various alignments
		for (size_t offset = 0; offset < MaxTestOffset; ++offset) {
			const void* ptr = Pire::Impl::AlignUp(&buf2[0], sizeof(size_t)) + offset;
			memcpy((void*) ptr, wbuf.Buffer().Data(), wbuf.Buffer().Size());
			try {
				Scanner sc4;
				const void* tail = sc4.Mmap(ptr, wbuf.Buffer().Size());

				if (offset % sizeof(size_t) != 0) {
					UNIT_ASSERT(!"CountingScanner failed to check for misaligned mmaping");
				} else {
					UNIT_ASSERT_EQUAL(tail, (const void*) ((size_t)ptr + wbuf.Buffer().Size()));

					st = Run(sc4, "abc defg 123 jklmn 4567 opqrst");
					UNIT_ASSERT_EQUAL(st.Result(0), size_t(4));
					UNIT_ASSERT_EQUAL(st.Result(1), size_t(2));
				}
			}
			catch (Pire::Error&) {}
		}
	}

	Y_UNIT_TEST(Serialization)
	{
		SerializationOne<Pire::CountingScanner>();
		SerializationOne<Pire::AdvancedCountingScanner>();
		SerializationOne<Pire::NoGlueLimitCountingScanner>();
	}

	template<class Scanner>
	void Serialization_v6_compatibilityOne()
	{
		const auto& enc = Pire::Encodings::Latin1();
		auto sc1 = Scanner(MkFsm("[a-z]+", enc), MkFsm(".*", enc));
		auto sc2 = Scanner(MkFsm("[0-9]+", enc), MkFsm(".*", enc));
		auto sc  = Scanner::Glue(sc1, sc2);

		BufferOutput wbuf;
		::Save(&wbuf, sc);

		// Patched scanner is a scanner of RE_VERSION 6.
		// The patched scanner is concatenated with original scanner to
		// make sure all content of patched scanner is consumed.

		const size_t ALIGNMENT = sizeof(size_t);
		size_t actions_size =
			sc.Size() *
			sc.LettersCount() *
			sizeof(typename Scanner::Action);
		UNIT_ASSERT_EQUAL(actions_size % ALIGNMENT, 0);
		size_t tags_size = sc.Size() * sizeof(typename Scanner::Tag);
		const char* src = wbuf.Buffer().Data();
		size_t src_size = wbuf.Buffer().Size();
		size_t patched_size = src_size + actions_size;
		size_t bytes_before_actions = src_size - tags_size;
		const int fill_char = 0x42;

		TVector<char> buf2(patched_size + src_size + 2 * ALIGNMENT);
		char* dst = reinterpret_cast<char*>(Pire::Impl::AlignUp(&buf2[0], ALIGNMENT));
		char* patched = dst;

		// Insert dummy m_actions between m_jumps and m_tags.
		memcpy(patched, src, bytes_before_actions); // copy members before m_actions
		memset(patched + bytes_before_actions, fill_char, actions_size); // m_actions
		memcpy(patched + bytes_before_actions + actions_size,
			src + bytes_before_actions,
			tags_size); // m_tags
		// Set version to 6
		// order of fields in header: magic, version, ...
		ui32* version_in_patched = reinterpret_cast<ui32*>(patched) + 1;
		UNIT_ASSERT_EQUAL(*version_in_patched, Pire::Header::RE_VERSION);
		*version_in_patched = Pire::Header::RE_VERSION_WITH_MACTIONS;

		// write normal scanner after patched one
		char* normal = Pire::Impl::AlignUp(patched + patched_size, ALIGNMENT);
		memcpy(normal, src, src_size);
		char* dst_end = Pire::Impl::AlignUp(normal + src_size, ALIGNMENT);
		size_t dst_size = dst_end - dst;

		// test loading from stream
		{
			MemoryInput rbuf(dst, dst_size);
			Scanner sc_patched, sc_normal;
			::Load(&rbuf, sc_patched);
			::Load(&rbuf, sc_normal);
			auto st_patched = Run(sc_patched,
					"abc defg 123 jklmn 4567 opqrst");
			UNIT_ASSERT_EQUAL(st_patched.Result(0), size_t(4));
			UNIT_ASSERT_EQUAL(st_patched.Result(1), size_t(2));
			auto st_normal = Run(sc_normal,
					"abc defg 123 jklmn 4567 opqrst");
			UNIT_ASSERT_EQUAL(st_normal.Result(0), size_t(4));
			UNIT_ASSERT_EQUAL(st_normal.Result(1), size_t(2));
		}

		// test loading using Mmap
		{
			Scanner sc_patched, sc_normal;
			const void* tail = sc_patched.Mmap(patched, patched_size);
			UNIT_ASSERT_EQUAL(tail, normal);
			const void* tail2 = sc_normal.Mmap(tail, src_size);
			UNIT_ASSERT_EQUAL(tail2, dst_end);
			auto st_patched = Run(sc_patched,
					"abc defg 123 jklmn 4567 opqrst");
			UNIT_ASSERT_EQUAL(st_patched.Result(0), size_t(4));
			UNIT_ASSERT_EQUAL(st_patched.Result(1), size_t(2));
			auto st_normal = Run(sc_normal,
					"abc defg 123 jklmn 4567 opqrst");
			UNIT_ASSERT_EQUAL(st_normal.Result(0), size_t(4));
			UNIT_ASSERT_EQUAL(st_normal.Result(1), size_t(2));
		}
	}

	Y_UNIT_TEST(Serialization_v6_compatibility)
	{
		Serialization_v6_compatibilityOne<Pire::CountingScanner>();
		Serialization_v6_compatibilityOne<Pire::AdvancedCountingScanner>();
		// NoGlueLimitCountingScanner is not v6_compatible
	}

	Y_UNIT_TEST(NoGlueLimitScannerCompatibilityWithAdvancedScanner) {
		const auto& enc = Pire::Encodings::Latin1();
		auto sc1 = AdvancedCountingScanner(MkFsm("[a-z]+", enc), MkFsm(".*", enc));
		auto sc2 = AdvancedCountingScanner(MkFsm("[0-9]+", enc), MkFsm(".*", enc));
		auto sc = AdvancedCountingScanner::Glue(sc1, sc2);

		BufferOutput wbuf;
		::Save(&wbuf, sc);

		TVector<char> buf2(wbuf.Buffer().Size());
		memcpy(buf2.data(), wbuf.Buffer().Data(), wbuf.Buffer().Size());

		// test loading from stream
		{
			MemoryInput rbuf(buf2.data(), buf2.size());
			NoGlueLimitCountingScanner scanner;
			::Load(&rbuf, scanner);
			auto state = Run(scanner,
						 "abc defg 123 jklmn 4567 opqrst");
			UNIT_ASSERT_EQUAL(state.Result(0), size_t(4));
			UNIT_ASSERT_EQUAL(state.Result(1), size_t(2));
		}

		// test loading using Mmap
		{
			NoGlueLimitCountingScanner scanner;
			const void* tail = scanner.Mmap(buf2.data(), buf2.size());
			UNIT_ASSERT_EQUAL(tail, buf2.data() + buf2.size());
			auto state = Run(scanner,
						 "abc defg 123 jklmn 4567 opqrst");
			UNIT_ASSERT_EQUAL(state.Result(0), size_t(4));
			UNIT_ASSERT_EQUAL(state.Result(1), size_t(2));
		}
	}

	template<class Scanner>
	void EmptyOne()
	{
		Scanner sc;
		UNIT_ASSERT(sc.Empty());

		UNIT_CHECKPOINT(); Run(sc, "a string"); // Just should not crash

		// Test glueing empty
		const auto& enc = Pire::Encodings::Latin1();
		auto sc1 = Scanner(MkFsm("[a-z]+", enc), MkFsm(".*", enc));
		auto sc2 = Scanner::Glue(sc, Scanner::Glue(sc, sc1));
		auto st = Run(sc2, "abc defg 123 jklmn 4567 opqrst");
		UNIT_ASSERT_EQUAL(st.Result(0), size_t(4));

		// Test Save/Load/Mmap
		BufferOutput wbuf;
		::Save(&wbuf, sc);

		MemoryInput rbuf(wbuf.Buffer().Data(), wbuf.Buffer().Size());
		Pire::CountingScanner sc3;
		::Load(&rbuf, sc3);
		UNIT_CHECKPOINT(); Run(sc3, "a string");

		const size_t MaxTestOffset = 2 * sizeof(Pire::Impl::MaxSizeWord);
		TVector<char> buf2(wbuf.Buffer().Size() + sizeof(size_t) + MaxTestOffset);
		const void* ptr = Pire::Impl::AlignUp(&buf2[0], sizeof(size_t));
		memcpy((void*) ptr, wbuf.Buffer().Data(), wbuf.Buffer().Size());

		Scanner sc4;
		const void* tail = sc4.Mmap(ptr, wbuf.Buffer().Size());
		UNIT_ASSERT_EQUAL(tail, (const void*) ((size_t)ptr + wbuf.Buffer().Size()));
		UNIT_CHECKPOINT(); Run(sc4, "a string");
	}

	Y_UNIT_TEST(Empty)
	{
		EmptyOne<Pire::CountingScanner>();
		EmptyOne<Pire::AdvancedCountingScanner>();
		EmptyOne<Pire::NoGlueLimitCountingScanner>();
	}

	template<typename Scanner>
	TVector<Scanner> MakeHalfFinalCount(const char* regexp, const Pire::Encoding& encoding = Pire::Encodings::Utf8()) {
		TVector<Scanner> scanners(6);
		const auto regexpFsm = MkFsm(regexp, encoding);
		HalfFinalFsm fsm(regexpFsm);
		fsm.MakeGreedyCounter(true);
		scanners[0] = Scanner(fsm);
		fsm = HalfFinalFsm(regexpFsm);
		fsm.MakeGreedyCounter(false);
		scanners[1] = Scanner(fsm);
		fsm = HalfFinalFsm(regexpFsm);
		fsm.MakeNonGreedyCounter(true, true);
		scanners[2] = Scanner(fsm);
		fsm = HalfFinalFsm(regexpFsm);
		fsm.MakeNonGreedyCounter(true, false);
		scanners[3] = Scanner(fsm);
		fsm = HalfFinalFsm(regexpFsm);
		fsm.MakeNonGreedyCounter(false);
		scanners[4] = Scanner(fsm);
		scanners[5] = scanners[0];
		for (size_t i = 1; i < 5; i++) {
			scanners[5] = Scanner::Glue(scanners[5], scanners[i]);
		}
		return scanners;
	}

	template<typename Scanner>
	void HalfFinalCount(TVector<Scanner> scanners, const char* text, TVector<size_t> result) {
		for (size_t i = 0; i < 5; i++) {
			UNIT_ASSERT_EQUAL(Run(scanners[i], text, -1).Result(0), result[i]);
		}
		auto state = Run(scanners[5], text, -1);
		for (size_t i = 0; i < 5; i++) {
			UNIT_ASSERT_EQUAL(state.Result(i), result[i]);
		}
	}

	template<typename Scanner>
	void TestHalfFinalCount() {
		HalfFinalCount(MakeHalfFinalCount<Scanner>("ab+"), "abbabbbabbbbbb", {3, 3, 3, 11, 3});
		HalfFinalCount(MakeHalfFinalCount<Scanner>("(ab)+"), "ababbababbab", {3, 3, 5, 5, 5});
		HalfFinalCount(MakeHalfFinalCount<Scanner>("(abab)+"), "ababababab", {1, 1, 4, 4, 2});
		HalfFinalCount(MakeHalfFinalCount<Scanner>("ab+c|b"), "abbbbbbbbbb", {1, 10, 10, 10, 10});
		HalfFinalCount(MakeHalfFinalCount<Scanner>("ab+c|b"), "abbbbbbbbbbb", {1, 10, 11, 11, 11});
		HalfFinalCount(MakeHalfFinalCount<Scanner>("ab+c|b"), "abbbbbbbbbbc", {1, 1, 10, 11, 10});
		HalfFinalCount(MakeHalfFinalCount<Scanner>("ab+c|b"), "abbbbbbbbbbbc", {1, 1, 11, 12, 11});
		HalfFinalCount(MakeHalfFinalCount<Scanner>("a\\w+c|b"), "abbbdbbbdbbc", {1, 1, 8, 9, 8});
		HalfFinalCount(MakeHalfFinalCount<Scanner>("a\\w+c|b"), "abbbdbbbdbb", {1, 8, 8, 8, 8});
		HalfFinalCount(MakeHalfFinalCount<Scanner>("a[a-z]+c|b"), "abeeeebeeeeeeeeeceeaeebeeeaeecceebeeaeebeeb", {2, 4, 7, 9, 7});
	}

	Y_UNIT_TEST(HalfFinal)
	{
		TestHalfFinalCount<Pire::HalfFinalScanner>();
		TestHalfFinalCount<Pire::NonrelocHalfFinalScanner>();
		TestHalfFinalCount<Pire::HalfFinalScannerNoMask>();
		TestHalfFinalCount<Pire::NonrelocHalfFinalScannerNoMask>();
	}

	template<typename Scanner>
	void TestHalfFinalSerialization() {
		auto oldScanners = MakeHalfFinalCount<Scanner>("(\\w\\w)+");
		BufferOutput wbuf;
		for (size_t i = 0; i < 6; i++) {
			::Save(&wbuf, oldScanners[i]);
		}

		MemoryInput rbuf(wbuf.Buffer().Data(), wbuf.Buffer().Size());
		TVector<Scanner> scanners(6);
		for (size_t i = 0; i < 6; i++) {
			::Load(&rbuf, scanners[i]);
		}

		HalfFinalCount(scanners, "ab abbb ababa a", {3, 3, 8, 8, 5});
	}

	Y_UNIT_TEST(HalfFinalSerialization)
	{
		TestHalfFinalSerialization<Pire::HalfFinalScanner>();
		TestHalfFinalSerialization<Pire::HalfFinalScannerNoMask>();
	}
}
