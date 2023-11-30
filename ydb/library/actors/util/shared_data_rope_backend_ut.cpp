#include <ydb/library/actors/util/rope.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>

#include "shared_data_rope_backend.h"

namespace NActors {

    namespace {

        TRope CreateRope(TString s, size_t sliceSize) {
            TRope res;
            for (size_t i = 0; i < s.size(); ) {
                size_t len = std::min(sliceSize, s.size() - i);
                if (i % 2) {
                    auto str = s.substr(i, len);
                    res.Insert(res.End(), TRope(MakeIntrusive<TRopeSharedDataBackend>(
                        TSharedData::Copy(str.data(), str.size()))));
                } else {
                    res.Insert(res.End(), TRope(s.substr(i, len)));
                }
                i += len;
            }
            return res;
        }

        TString RopeToString(const TRope& rope) {
            TString res;
            auto iter = rope.Begin();
            while (iter != rope.End()) {
                res.append(iter.ContiguousData(), iter.ContiguousSize());
                iter.AdvanceToNextContiguousBlock();
            }

            UNIT_ASSERT_VALUES_EQUAL(rope.GetSize(), res.size());

            TString temp = TString::Uninitialized(rope.GetSize());
            rope.Begin().ExtractPlainDataAndAdvance(temp.Detach(), temp.size());
            UNIT_ASSERT_VALUES_EQUAL(temp, res);

            return res;
        }

        TString Text = "No elements are copied or moved, only the internal pointers of the list nodes are re-pointed.";

    }

    Y_UNIT_TEST_SUITE(TRopeSharedDataBackend) {

        // Same tests as in TRope but with new CreateRope using TSharedData backend

        Y_UNIT_TEST(Leak) {
            const size_t begin = 10, end = 20;
            TRope rope = CreateRope(Text, 10);
            rope.Erase(rope.Begin() + begin, rope.Begin() + end);
        }

        Y_UNIT_TEST(BasicRange) {
            TRope rope = CreateRope(Text, 10);
            for (size_t begin = 0; begin < Text.size(); ++begin) {
                for (size_t end = begin; end <= Text.size(); ++end) {
                    TRope::TIterator rBegin = rope.Begin() + begin;
                    TRope::TIterator rEnd = rope.Begin() + end;
                    UNIT_ASSERT_VALUES_EQUAL(RopeToString(TRope(rBegin, rEnd)), Text.substr(begin, end - begin));
                }
            }
        }

        Y_UNIT_TEST(Erase) {
            for (size_t begin = 0; begin < Text.size(); ++begin) {
                for (size_t end = begin; end <= Text.size(); ++end) {
                    TRope rope = CreateRope(Text, 10);
                    rope.Erase(rope.Begin() + begin, rope.Begin() + end);
                    TString text = Text;
                    text.erase(text.begin() + begin, text.begin() + end);
                    UNIT_ASSERT_VALUES_EQUAL(RopeToString(rope), text);
                }
            }
        }

        Y_UNIT_TEST(Insert) {
            TRope rope = CreateRope(Text, 10);
            for (size_t begin = 0; begin < Text.size(); ++begin) {
                for (size_t end = begin; end <= Text.size(); ++end) {
                    TRope part = TRope(rope.Begin() + begin, rope.Begin() + end);
                    for (size_t where = 0; where <= Text.size(); ++where) {
                        TRope x(rope);
                        x.Insert(x.Begin() + where, TRope(part));
                        UNIT_ASSERT_VALUES_EQUAL(x.GetSize(), rope.GetSize() + part.GetSize());
                        TString text = Text;
                        text.insert(text.begin() + where, Text.begin() + begin, Text.begin() + end);
                        UNIT_ASSERT_VALUES_EQUAL(RopeToString(x), text);
                    }
                }
            }
        }

        Y_UNIT_TEST(Extract) {
            for (size_t begin = 0; begin < Text.size(); ++begin) {
                for (size_t end = begin; end <= Text.size(); ++end) {
                    TRope rope = CreateRope(Text, 10);
                    TRope part = rope.Extract(rope.Begin() + begin, rope.Begin() + end);
                    TString text = Text;
                    text.erase(text.begin() + begin, text.begin() + end);
                    UNIT_ASSERT_VALUES_EQUAL(RopeToString(rope), text);
                    UNIT_ASSERT_VALUES_EQUAL(RopeToString(part), Text.substr(begin, end - begin));
                }
            }
        }

        Y_UNIT_TEST(EraseFront) {
            for (size_t pos = 0; pos <= Text.size(); ++pos) {
                TRope rope = CreateRope(Text, 10);
                rope.EraseFront(pos);
                UNIT_ASSERT_VALUES_EQUAL(RopeToString(rope), Text.substr(pos));
            }
        }

        Y_UNIT_TEST(EraseBack) {
            for (size_t pos = 0; pos <= Text.size(); ++pos) {
                TRope rope = CreateRope(Text, 10);
                rope.EraseBack(pos);
                UNIT_ASSERT_VALUES_EQUAL(RopeToString(rope), Text.substr(0, Text.size() - pos));
            }
        }

        Y_UNIT_TEST(ExtractFront) {
            for (size_t step = 1; step <= Text.size(); ++step) {
                TRope rope = CreateRope(Text, 10);
                TRope out;
                while (const size_t len = Min(step, rope.GetSize())) {
                    rope.ExtractFront(len, &out);
                    UNIT_ASSERT(rope.GetSize() + out.GetSize() == Text.size());
                    UNIT_ASSERT_VALUES_EQUAL(RopeToString(out), Text.substr(0, out.GetSize()));
                }
            }
        }

        Y_UNIT_TEST(ExtractFrontPlain) {
            for (size_t step = 1; step <= Text.size(); ++step) {
                TRope rope = CreateRope(Text, 10);
                TString buffer = Text;
                auto it = rope.Begin();
                size_t remain = rope.GetSize();
                while (const size_t len = Min(step, remain)) {
                    TString data = TString::Uninitialized(len);
                    it.ExtractPlainDataAndAdvance(data.Detach(), data.size());
                    UNIT_ASSERT_VALUES_EQUAL(data, buffer.substr(0, len));
                    UNIT_ASSERT_VALUES_EQUAL(RopeToString(TRope(it, rope.End())), buffer.substr(len));
                    buffer = buffer.substr(len);
                    remain -= len;
                }
            }
        }

        Y_UNIT_TEST(Glueing) {
            TRope rope = CreateRope(Text, 10);
            for (size_t begin = 0; begin <= Text.size(); ++begin) {
                for (size_t end = begin; end <= Text.size(); ++end) {
                    TString repr = rope.DebugString();
                    TRope temp = rope.Extract(rope.Position(begin), rope.Position(end));
                    rope.Insert(rope.Position(begin), std::move(temp));
                    UNIT_ASSERT_VALUES_EQUAL(repr, rope.DebugString());
                    UNIT_ASSERT_VALUES_EQUAL(RopeToString(rope), Text);
                }
            }
        }

        Y_UNIT_TEST(IterWalk) {
            TRope rope = CreateRope(Text, 10);
            for (size_t step1 = 0; step1 <= rope.GetSize(); ++step1) {
                for (size_t step2 = 0; step2 <= step1; ++step2) {
                    TRope::TConstIterator iter = rope.Begin();
                    iter += step1;
                    iter -= step2;
                    UNIT_ASSERT(iter == rope.Position(step1 - step2));
                }
            }
        }

        Y_UNIT_TEST(Compare) {
            auto check = [](const TString& x, const TString& y) {
                const TRope xRope = CreateRope(x, 7);
                const TRope yRope = CreateRope(y, 11);
                UNIT_ASSERT_VALUES_EQUAL(xRope == yRope, x == y);
                UNIT_ASSERT_VALUES_EQUAL(xRope != yRope, x != y);
                UNIT_ASSERT_VALUES_EQUAL(xRope <  yRope, x <  y);
                UNIT_ASSERT_VALUES_EQUAL(xRope <= yRope, x <= y);
                UNIT_ASSERT_VALUES_EQUAL(xRope >  yRope, x >  y);
                UNIT_ASSERT_VALUES_EQUAL(xRope >= yRope, x >= y);
            };

            TVector<TString> pool;
            for (size_t k = 0; k < 10; ++k) {
                size_t len = RandomNumber<size_t>(100) + 100;
                TString s = TString::Uninitialized(len);
                char *p = s.Detach();
                for (size_t j = 0; j < len; ++j) {
                    *p++ = RandomNumber<unsigned char>();
                }
                pool.push_back(std::move(s));
            }

            for (const TString& x : pool) {
                for (const TString& y : pool) {
                    check(x, y);
                }
            }
        }

        // Specific TSharedDataRopeBackend tests

        Y_UNIT_TEST(RopeOnlyBorrows) {
            TSharedData data = TSharedData::Copy(Text.data(), Text.size());
            {
                TRope rope;
                rope.Insert(rope.End(), TRope(MakeIntrusive<TRopeSharedDataBackend>(data)));
                UNIT_ASSERT(data.IsShared());
                TSharedData dataCopy = data;
                UNIT_ASSERT(dataCopy.IsShared());
                UNIT_ASSERT_EQUAL(dataCopy.data(), data.data());
                rope.Insert(rope.End(), TRope(MakeIntrusive<TRopeSharedDataBackend>(data)));
                rope.Insert(rope.End(), TRope(MakeIntrusive<TRopeSharedDataBackend>(data)));
                dataCopy.TrimBack(10);
                UNIT_ASSERT_EQUAL(rope.GetSize(), data.size() * 3);
            }
            UNIT_ASSERT(data.IsPrivate());
        }
    }

} // namespace NActors
