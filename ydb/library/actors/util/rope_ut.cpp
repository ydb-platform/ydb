#include "rope.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include "ut_helpers.h"

class TRopeStringBackend : public IContiguousChunk {
    TString Buffer;

public:
    TRopeStringBackend(TString buffer)
        : Buffer(std::move(buffer))
    {}

    TContiguousSpan GetData() const override {
        return {Buffer.data(), Buffer.size()};
    }

    TMutableContiguousSpan GetDataMut() override {
        return {Buffer.Detach(), Buffer.size()};
    }

    TMutableContiguousSpan UnsafeGetDataMut() override {
        return {const_cast<char*>(Buffer.data()), Buffer.size()};
    }

    size_t GetOccupiedMemorySize() const override {
        return Buffer.capacity();
    }
};

TRope CreateRope(TString s, size_t sliceSize) {
    TRope res;
    for (size_t i = 0; i < s.size(); ) {
        size_t len = std::min(sliceSize, s.size() - i);
        if (i % 2) {
            res.Insert(res.End(), TRope(MakeIntrusive<TRopeStringBackend>(s.substr(i, len))));
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

Y_UNIT_TEST_SUITE(TRope) {
    Y_UNIT_TEST(StringCompare) {
        TRope rope = CreateRope(Text, 10);
        UNIT_ASSERT_EQUAL(rope, Text);
        UNIT_ASSERT_EQUAL(Text, rope);
        rope.Erase(rope.Begin() + 10, rope.Begin() + 11);
        UNIT_ASSERT_UNEQUAL(rope, Text);
        UNIT_ASSERT_UNEQUAL(Text, rope);
        TString str("aa");
        rope = TRope(TString("ab"));
        UNIT_ASSERT_LT(str, rope);
        UNIT_ASSERT_GT(rope, str);
        str = TString("aa");
        rope = TRope(TString("a"));
        UNIT_ASSERT_LT(rope, str);
        UNIT_ASSERT_GT(str, rope);
        str = TString("a");
        rope = TRope(TString("aa"));
        UNIT_ASSERT_LT(str, rope);
        UNIT_ASSERT_GT(rope, str);
    }

    Y_UNIT_TEST(Leak) {
        const size_t begin = 10, end = 20;
        TRope rope = CreateRope(Text, 10);
        rope.Erase(rope.Begin() + begin, rope.Begin() + end);
    }

    Y_UNIT_TEST(Compacted) {
        TRope rope = CreateRope(Text, 10);
        UNIT_ASSERT_EQUAL(rope.UnsafeGetContiguousSpanMut(), Text);
        UNIT_ASSERT(rope.IsContiguous());
    }

#ifndef TSTRING_IS_STD_STRING
    Y_UNIT_TEST(ExtractZeroCopy) {
        TString str = Text;
        TRope packed(str);
        TString extracted = packed.ExtractUnderlyingContainerOrCopy<TString>();
        UNIT_ASSERT_EQUAL(str.data(), extracted.data());
    }

    Y_UNIT_TEST(ExtractZeroCopySlice) {
        TString str = Text;
        TRope sliced(str);
        sliced.EraseFront(1);
        TString extracted = sliced.ExtractUnderlyingContainerOrCopy<TString>();
        UNIT_ASSERT_UNEQUAL(str.data(), extracted.data());
        TRope sliced2(str);
        sliced2.EraseBack(1);
        TString extracted2 = sliced2.ExtractUnderlyingContainerOrCopy<TString>();
        UNIT_ASSERT_UNEQUAL(str.data(), extracted2.data());
    }

    Y_UNIT_TEST(TStringDetach) {
        TRope pf;
        TRope rope;
        TString string = TString(Text.data(), Text.size());
        rope = TRope(string);
        pf = rope;
        pf.GetContiguousSpanMut();
        UNIT_ASSERT(!string.IsDetached());
        rope.GetContiguousSpanMut();
        UNIT_ASSERT(string.IsDetached());
    }

    Y_UNIT_TEST(TStringUnsafeShared) {
        TRope pf;
        TRope rope;
        TString string = TString(Text.data(), Text.size());
        rope = TRope(string);
        pf = rope;
        UNIT_ASSERT(pf.IsContiguous());
        UNIT_ASSERT_EQUAL(pf.UnsafeGetContiguousSpanMut().data(), string.data());
        UNIT_ASSERT(!string.IsDetached());
    }

    Y_UNIT_TEST(ContiguousDataInterop) {
        TString string = "Some long-long text needed for not sharing data and testing";
        TRcBuf data(string);
        UNIT_ASSERT_EQUAL(data.UnsafeGetDataMut(), &(*string.cbegin()));
        TRope rope(data); // check operator TRope
        UNIT_ASSERT_EQUAL(rope.UnsafeGetContiguousSpanMut().data(), &(*string.cbegin()));
        TRcBuf otherData(rope);
        UNIT_ASSERT_EQUAL(otherData.UnsafeGetDataMut(), &(*string.cbegin()));
        TString extractedBack = otherData.ExtractUnderlyingContainerOrCopy<TString>();
        UNIT_ASSERT_EQUAL(extractedBack.data(), &(*string.cbegin()));
    }
#endif
    Y_UNIT_TEST(CrossCompare) {
        TString str = "some very long string";
        const TString constStr(str);
        TStringBuf strbuf = str;
        const TStringBuf constStrbuf = str;
        TContiguousSpan span(str);
        const TContiguousSpan constSpan(str);
        TMutableContiguousSpan mutableSpan(const_cast<char*>(str.data()), str.size());
        const TMutableContiguousSpan constMutableSpan(const_cast<char*>(str.data()), str.size());
        TRcBuf data(str);
        const TRcBuf constData(str);
        TArrayRef<char> arrRef(const_cast<char*>(str.data()), str.size());
        const TArrayRef<char> constArrRef(const_cast<char*>(str.data()), str.size());
        TArrayRef<const char> arrConstRef(const_cast<char*>(str.data()), str.size());
        const TArrayRef<const char> constArrConstRef(const_cast<char*>(str.data()), str.size());
        NActors::TSharedData sharedData = NActors::TSharedData::Copy(str.data(), str.size());
        const NActors::TSharedData constSharedData(sharedData);
        TRope rope(str);
        const TRope constRope(str);

        Permutate(
            [](auto& arg1, auto& arg2) {
                UNIT_ASSERT(arg1 == arg2);
            },
            str,
            constStr,
            strbuf,
            constStrbuf,
            span,
            constSpan,
            mutableSpan,
            constMutableSpan,
            data,
            constData,
            arrRef,
            constArrRef,
            arrConstRef,
            constArrConstRef,
            sharedData,
            constSharedData,
            rope,
            constRope);
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

    Y_UNIT_TEST(EraseThenInsert) {
        TRope text = CreateRope(Text, 10);
        for (size_t begin = 0; begin < Text.size(); ++begin) {
            for (size_t end = begin; end <= Text.size(); ++end) {
                for (size_t offset = 0; offset < Text.size(); offset += 11) {
                    for (size_t endOffset = offset; endOffset <= Min(offset + 20, Text.size()); ++endOffset) {
                        TRope rope = text;
                        const auto beginIt = rope.Position(begin);
                        const auto endIt = rope.Position(end);
                        const auto insertIt = rope.Erase(beginIt, endIt);
                        rope.Insert(insertIt, {text.Position(offset), text.Position(endOffset)});
                        TString reference = Text;
                        reference.erase(reference.begin() + begin, reference.begin() + end);
                        reference.insert(reference.begin() + begin, Text.begin() + offset, Text.begin() + endOffset);
                        UNIT_ASSERT_VALUES_EQUAL(RopeToString(rope), reference);
                    }
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
            size_t remain = rope.GetSize();
            while (const size_t len = Min(step, remain)) {
                TString data = TString::Uninitialized(len);
                rope.ExtractFrontPlain(data.Detach(), data.size());
                UNIT_ASSERT_VALUES_EQUAL(data, buffer.substr(0, len));
                UNIT_ASSERT_VALUES_EQUAL(RopeToString(rope), buffer.substr(len));
                buffer = buffer.substr(len);
                remain -= len;
            }
        }
    }

    Y_UNIT_TEST(FetchFrontPlain) {
        char s[10];
        char *data = s;
        size_t remain = sizeof(s);
        TRope rope = TRope(TString("HELLO"));
        UNIT_ASSERT(!rope.FetchFrontPlain(&data, &remain));
        UNIT_ASSERT(!rope);
        rope.Insert(rope.End(), TRope(TString("WORLD!!!")));
        UNIT_ASSERT(rope.FetchFrontPlain(&data, &remain));
        UNIT_ASSERT(!remain);
        UNIT_ASSERT(rope.GetSize() == 3);
        UNIT_ASSERT_VALUES_EQUAL(rope.ConvertToString(), "!!!");
        UNIT_ASSERT(!strncmp(s, "HELLOWORLD", 10));
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
            UNIT_ASSERT_VALUES_EQUAL(xRope == y, x == y);
            UNIT_ASSERT_VALUES_EQUAL(x == yRope, x == y);
            UNIT_ASSERT_VALUES_EQUAL(xRope != yRope, x != y);
            UNIT_ASSERT_VALUES_EQUAL(xRope != y, x != y);
            UNIT_ASSERT_VALUES_EQUAL(x != yRope, x != y);
            UNIT_ASSERT_VALUES_EQUAL(xRope <  yRope, x <  y);
            UNIT_ASSERT_VALUES_EQUAL(xRope <  y, x <  y);
            UNIT_ASSERT_VALUES_EQUAL(x <  yRope, x <  y);
            UNIT_ASSERT_VALUES_EQUAL(xRope <= yRope, x <= y);
            UNIT_ASSERT_VALUES_EQUAL(xRope <= y, x <= y);
            UNIT_ASSERT_VALUES_EQUAL(x <= yRope, x <= y);
            UNIT_ASSERT_VALUES_EQUAL(xRope >  yRope, x >  y);
            UNIT_ASSERT_VALUES_EQUAL(xRope >  y, x >  y);
            UNIT_ASSERT_VALUES_EQUAL(x >  yRope, x >  y);
            UNIT_ASSERT_VALUES_EQUAL(xRope >= yRope, x >= y);
            UNIT_ASSERT_VALUES_EQUAL(xRope >= y, x >= y);
            UNIT_ASSERT_VALUES_EQUAL(x >= yRope, x >= y);
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

    Y_UNIT_TEST(RopeZeroCopyInputBasic) {
        TRope rope = CreateRope(Text, 3);
        TRopeZeroCopyInput input(rope.Begin());

        TString result;
        TStringOutput output(result);
        TransferData(&input, &output);
        UNIT_ASSERT_EQUAL(result, Text);
    }

    Y_UNIT_TEST(RopeZeroCopyInput) {
        TRope rope;
        rope.Insert(rope.End(), TRope{"abc"});
        rope.Insert(rope.End(), TRope{TString{}});
        rope.Insert(rope.End(), TRope{"de"});
        rope.Insert(rope.End(), TRope{TString{}});
        rope.Insert(rope.End(), TRope{TString{}});
        rope.Insert(rope.End(), TRope{"fghi"});

        TRopeZeroCopyInput input(rope.Begin());

        const char* data = nullptr;
        size_t len;

        len = input.Next(&data, 2);
        UNIT_ASSERT_EQUAL("ab", TStringBuf(data, len));

        len = input.Next(&data, 3);
        UNIT_ASSERT_EQUAL("c", TStringBuf(data, len));

        len = input.Next(&data, 3);
        UNIT_ASSERT_EQUAL("de", TStringBuf(data, len));

        len = input.Next(&data);
        UNIT_ASSERT_EQUAL("fghi", TStringBuf(data, len));

        len = input.Next(&data);
        UNIT_ASSERT_EQUAL(len, 0);

        len = input.Next(&data);
        UNIT_ASSERT_EQUAL(len, 0);
    }
}
