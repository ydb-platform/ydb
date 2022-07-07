/*
 * approx_matching_ut.cpp --
 *
 * Copyright (c) 2019 YANDEX LLC, Karina Usmanova <usmanova.karin@yandex.ru>
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


#include <contrib/libs/pire/pire/pire.h>
#include "common.h"

Y_UNIT_TEST_SUITE(ApproxMatchingTest) {
	Pire::Fsm BuildFsm(const char *str)
	{
		Pire::Lexer lexer;
		TVector<wchar32> ucs4;

		lexer.Encoding().FromLocal(str, str + strlen(str), std::back_inserter(ucs4));
		lexer.Assign(ucs4.begin(), ucs4.end());
		return lexer.Parse();
	}

	Y_UNIT_TEST(Simple) {
		auto fsm = BuildFsm("^ab$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("ab");
			ACCEPTS("ax");
			ACCEPTS("xb");
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("xab");
			ACCEPTS("axb");
			ACCEPTS("abx");
			ACCEPTS("aab");
			DENIES("xy");
			DENIES("abcd");
			DENIES("xabx");
			DENIES("");
		}

		fsm = BuildFsm("^ab$");
		APPROXIMATE_SCANNER(fsm, 2) {
			ACCEPTS("ab");
			ACCEPTS("xy");
			ACCEPTS("");
			ACCEPTS("axbx");
			DENIES("xxabx");
			DENIES("xbxxx");
		}
	}

	Y_UNIT_TEST(SpecialSymbols) {
		auto fsm = BuildFsm("^.*ab$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("ab");
			ACCEPTS("xxxxab");
			ACCEPTS("xxxxabab");
			DENIES("xxxx");
			DENIES("abxxxx");
		}

		fsm = BuildFsm("^[a-c]$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("c");
			ACCEPTS("/");
			ACCEPTS("");
			ACCEPTS("ax");
			DENIES("xx");
			DENIES("abc");
		}

		fsm = BuildFsm("^x{4}$");
		APPROXIMATE_SCANNER(fsm, 2) {
			DENIES ("x");
			ACCEPTS("xx");
			ACCEPTS("xxx");
			ACCEPTS("xxxx");
			ACCEPTS("xxxxx");
			ACCEPTS("xxxxxx");
			DENIES ("xxxxxxx");
			ACCEPTS("xxyy");
			ACCEPTS("xxyyx");
			ACCEPTS("xxxxyz");
			DENIES("xyyy");
		}

		fsm = BuildFsm("^(a|b)$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("x");
			ACCEPTS("");
			ACCEPTS("ax");
			DENIES("abc");
			DENIES("xx");
		}

		fsm = BuildFsm("^(ab|cd)$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("ab");
			ACCEPTS("cd");
			ACCEPTS("ax");
			ACCEPTS("xd");
			ACCEPTS("abx");
			ACCEPTS("a");
			DENIES("abcd");
			DENIES("xx");
			DENIES("");
		}

		fsm = BuildFsm("^[a-c]{3}$");
		APPROXIMATE_SCANNER(fsm, 2) {
			ACCEPTS("abc");
			ACCEPTS("aaa");
			ACCEPTS("a");
			ACCEPTS("ax");
			ACCEPTS("abxcx");
			DENIES("x");
			DENIES("");
			DENIES("xaxx");
		}

		fsm = BuildFsm("^\\x{61}$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("a");
			ACCEPTS("x");
			ACCEPTS("");
			ACCEPTS("ax");
			DENIES("axx");
			DENIES("xx");
		}

		fsm = BuildFsm("^a.bc$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("axxbc");
			ACCEPTS("abc");
			ACCEPTS("xabc");
			ACCEPTS("xaxbc");
			DENIES("bc");
			DENIES("abcx");
		}
	}

	Y_UNIT_TEST(TestSurrounded) {
		auto fsm = BuildFsm("abc").Surround();
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("abc");
			ACCEPTS("xabcx");
			ACCEPTS("xabx");
			ACCEPTS("axc");
			ACCEPTS("bac");
			DENIES("a");
			DENIES("xaxxxx");
		}

		fsm = BuildFsm("^abc$").Surround();
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("abc");
			ACCEPTS("abcx");
			ACCEPTS("xabc");
			ACCEPTS("axc");
			ACCEPTS("bac");
			DENIES("xabx");
			DENIES("axx");
		}
	}

	Y_UNIT_TEST(GlueFsm) {
		auto fsm = BuildFsm("^a$") | BuildFsm("^b$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("");
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("x");
			ACCEPTS("ab");
			DENIES("abb");
		}

		fsm = BuildFsm("^[a-b]$") | BuildFsm("^c{2}$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("a");
			ACCEPTS("b");
			ACCEPTS("cc");
			ACCEPTS("x");
			ACCEPTS("xa");
			ACCEPTS("c");
			ACCEPTS("xc");
			ACCEPTS("cxc");
			ACCEPTS("");
		}
	}

	enum MutateOperation {
		Begin,
		Substitute = Begin,
		Delete,
		Insert,
		End
	};

	ystring ChangeText(const ystring& text, int operation, int pos)
	{
		auto changedText = text;
		switch (operation) {
			case MutateOperation::Substitute:
				changedText[pos] = 'x';
				break;
			case MutateOperation::Delete:
				changedText.erase(pos, 1);
				break;
			case MutateOperation::Insert:
				changedText.insert(pos, 1, 'x');
				break;
		}

		return changedText;
	}

	Y_UNIT_TEST(StressTest) {
		ystring text;
		for (size_t letter = 0; letter < 10; ++letter) {
			text += ystring(3, letter + 'a');
		}
		const ystring regexp = "^" + text + "$";
		auto fsm = BuildFsm(regexp.Data());

		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS(text);

			for (size_t pos = 0; pos < regexp.size() - 2; ++pos) {
				for (int operation = MutateOperation::Begin; operation < MutateOperation::End; ++operation) {
					auto changedText = ChangeText(text, operation, pos);
					ACCEPTS(changedText);
				}
			}
		}

		APPROXIMATE_SCANNER(fsm, 0) {
			ACCEPTS(text);

			for (size_t pos = 0; pos < regexp.size() - 2; ++pos) {
				for (int operation = MutateOperation::Begin; operation < MutateOperation::End; ++operation) {
					auto changedText = ChangeText(text, operation, pos);
					DENIES(changedText);
				}
			}
		}

		APPROXIMATE_SCANNER(fsm, 2) {
			ACCEPTS(text);

			for (size_t posLeft = 0; posLeft < text.size() / 2 - 1; ++posLeft) { // Subtract 1 to avoid interaction of operationLeft and operationRight
				size_t posRight = text.size() - posLeft - 1;
				for (int operationLeft = MutateOperation::Begin; operationLeft < MutateOperation::End; ++operationLeft) {
					for (int operationRight  = MutateOperation::Begin; operationRight < MutateOperation::End; ++operationRight) {
						auto changedText = ChangeText(text, operationRight, posRight);
						changedText = ChangeText(changedText, operationLeft, posLeft);
						ACCEPTS(changedText);
					}
				}
			}
		}

		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS(text);

			for (size_t posLeft = 0; posLeft < text.size() / 2 - 1; ++posLeft) { // Subtract 1 to avoid interaction of operationLeft and operationRight
				size_t posRight = text.size() - posLeft - 1;
				for (int operationLeft = MutateOperation::Begin; operationLeft < MutateOperation::End; ++operationLeft) {
					for (int operationRight  = MutateOperation::Begin; operationRight < MutateOperation::End; ++operationRight) {
						auto changedText = ChangeText(text, operationRight, posRight);
						changedText = ChangeText(changedText, operationLeft, posLeft);
						DENIES(changedText);
					}
				}
			}
		}
	}

	Y_UNIT_TEST(SwapLetters) {
		auto fsm = BuildFsm("^abc$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("bac");
			ACCEPTS("acb");
			DENIES("cba");
			DENIES("bax");
		}

		fsm = BuildFsm("^abcd$");
		APPROXIMATE_SCANNER(fsm, 2) {
			ACCEPTS("bacd");
			ACCEPTS("acbd");
			ACCEPTS("baxd");
			ACCEPTS("badc");
			ACCEPTS("bcad");
			ACCEPTS("bcda");
			DENIES("xcbx");
			DENIES("baxx");
			DENIES("ba");
			DENIES("cdab");
		}

		fsm = BuildFsm("^abc$");
		APPROXIMATE_SCANNER(fsm, 0) {
			ACCEPTS("abc");
			DENIES("bac");
		}

		fsm = BuildFsm("^[a-c][1-3]$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("a3");
			ACCEPTS("c");
			ACCEPTS("1");
			ACCEPTS("1a");
			ACCEPTS("3b");
			DENIES("4a");
		}

		fsm = BuildFsm("^.*abc$");
		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS("ab");
			ACCEPTS("xxxxbac");
			DENIES("xxxxa");
			DENIES("xxxxcb");
		}
	}

	Y_UNIT_TEST(SwapStressTest){
		ystring text;
		for (size_t letter = 0; letter < 30; ++letter) {
			text += ystring(1, (letter % 26) + 'a');
		}
		const ystring regexp = "^" + text + "$";
		auto fsm = BuildFsm(regexp.Data());
		auto changedText = text;

		APPROXIMATE_SCANNER(fsm, 1) {
			ACCEPTS(text);

			for (size_t pos = 0; pos < text.size() - 1; ++pos) {
				changedText[pos] = text[pos + 1];
				changedText[pos + 1] = text[pos];
				ACCEPTS(changedText);
				changedText[pos] = text[pos];
				changedText[pos + 1] = text[pos + 1];
			}
		}

		APPROXIMATE_SCANNER(fsm, 0) {
			ACCEPTS(text);

			for (size_t pos = 0; pos < text.size() - 1; ++pos) {
				changedText[pos] = text[pos + 1];
				changedText[pos + 1] = text[pos];
				DENIES(changedText);
				changedText[pos] = text[pos];
				changedText[pos + 1] = text[pos + 1];
			}
		}
	}
}
