#include <library/cpp/scheme/tests/fuzz_ops/lib/vm_parse.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TestParseNextAction) {
    using namespace NSc::NUt;

    Y_UNIT_TEST(TestWidth) {
        UNIT_ASSERT_VALUES_EQUAL(GetCountWidth(TIdx::ValueCount), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetCountWidth(TPos::ValueCount), 4);
        UNIT_ASSERT_VALUES_EQUAL(GetCountWidth(TRef::TypeCount), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetCountWidth(TSrc::TypeCount), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetCountWidth(TDst::TypeCount), 4);
        UNIT_ASSERT_VALUES_EQUAL(GetCountWidth(TPath::MaxLength), 5);
        UNIT_ASSERT_VALUES_EQUAL(GetCountWidth(TVMAction::TypeCount), 6);
    }

    Y_UNIT_TEST(TestParseIdx) {
        {
            TVMState st{"", 1, 0};
            UNIT_ASSERT(!ParseIdx(st));
        }
        {
            TVMState st{"\x03", 1, 0};
            auto idx = ParseIdx(st);
            UNIT_ASSERT(idx);
            UNIT_ASSERT_VALUES_EQUAL(idx->Idx, 3);
        }
    }

    void DoTestParsePosFailure(const TStringBuf inp, const ui32 memSz, const ui32 curPos) {
        TVMState st{inp, memSz, curPos};
        UNIT_ASSERT(!ParsePos(st));
    }

    [[nodiscard]]
    ui32 DoTestParsePosSuccess(TVMState& st) {
        const auto pos = ParsePos(st);
        UNIT_ASSERT(pos);
        return pos->Pos;
    }

    [[nodiscard]]
    ui32 DoTestParsePosSuccess(const TStringBuf inp, const ui32 memSz, const ui32 curPos) {
        TVMState st{inp, memSz, curPos};
        return DoTestParsePosSuccess(st);
    }

    Y_UNIT_TEST(TestParsePos) {
        DoTestParsePosFailure("", 1, 0);

        UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(TStringBuf("\x00"sv), 1, 0), 0);
        UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(TStringBuf("\x01"sv), 1, 0), 0);

        DoTestParsePosFailure(TStringBuf("\x02"sv), 1, 0);
        DoTestParsePosFailure(TStringBuf("\x03"sv), 2, 0);

        UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(TStringBuf("\x02"sv), 2, 0), 1);
        UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(TStringBuf("\x03"sv), 2, 1), 0);

        UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(TStringBuf("\x0E"sv), 8, 0), 7);
        UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(TStringBuf("\x0F"sv), 8, 7), 0);

        {
            TVMState st{TStringBuf("\xDE\x7B"), 16, 0};
            UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(st), 15);
            UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(st), 15);
            UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(st), 15);
            UNIT_ASSERT(!ParsePos(st));
        }
        {
            TVMState st{TStringBuf("\xFF\x7F"), 16, 15};
            UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(st), 0);
            UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(st), 0);
            UNIT_ASSERT_VALUES_EQUAL(DoTestParsePosSuccess(st), 0);
            UNIT_ASSERT(!ParsePos(st));
        }
    }

    void DoTestParseRefFailure(const TStringBuf inp, const ui32 memSz, const ui32 curPos) {
        TVMState st{inp, memSz, curPos};
        UNIT_ASSERT(!ParseRef(st));
    }

    [[nodiscard]]
    auto DoTestParseRefSuccess(TVMState& st) {
        const auto ref = ParseRef(st);
        UNIT_ASSERT(ref);
        return std::make_pair(ref->Pos, ref->Type);
    }

    [[nodiscard]]
    auto DoTestParseRefSuccess(const TStringBuf inp, const ui32 memSz, const ui32 curPos) {
        TVMState st{inp, memSz, curPos};
        return DoTestParseRefSuccess(st);
    }

    Y_UNIT_TEST(TestParseRef) {
        DoTestParseRefFailure("", 1, 0);

        UNIT_ASSERT_VALUES_EQUAL(DoTestParseRefSuccess(TStringBuf("\x00"sv), 1, 0), std::make_pair((ui32)-1, TRef::T_CREATE_FRONT));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseRefSuccess(TStringBuf("\x01"sv), 1, 0), std::make_pair((ui32)-1, TRef::T_CREATE_BACK));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseRefSuccess(TStringBuf("\x0A"sv), 2, 0), std::make_pair(1u, TRef::T_REF__POS));

        DoTestParseRefFailure(TStringBuf("\x12"), 1, 0);
        DoTestParseRefFailure(TStringBuf("\x03"sv), 1, 0);

        {
            TVMState st{TStringBuf("\x7A\x7D"), 16, 0};
            UNIT_ASSERT_VALUES_EQUAL(DoTestParseRefSuccess(st), std::make_pair(15u, TRef::T_REF__POS));
            UNIT_ASSERT_VALUES_EQUAL(DoTestParseRefSuccess(st), std::make_pair(15u, TRef::T_REF__POS));
            UNIT_ASSERT_VALUES_EQUAL(DoTestParseRefSuccess(st), std::make_pair((ui32)-1, TRef::T_CREATE_BACK));
            UNIT_ASSERT(!ParseRef(st));
        }
    }

    void DoTestParseSrcFailure(const TStringBuf inp, const ui32 memSz, const ui32 curPos) {
        TVMState st{inp, memSz, curPos};
        UNIT_ASSERT(!ParseSrc(st));
    }

    [[nodiscard]]
    auto DoTestParseSrcSuccess(TVMState& st) {
        const auto src = ParseSrc(st);
        UNIT_ASSERT(src);
        return std::make_pair(src->Pos, src->Type);
    }

    [[nodiscard]]
    auto DoTestParseSrcSuccess(const TStringBuf inp, const ui32 memSz, const ui32 curPos) {
        TVMState st{inp, memSz, curPos};
        return DoTestParseSrcSuccess(st);
    }

    Y_UNIT_TEST(TestParseSrc) {
        DoTestParseSrcFailure("", 1, 0);

        UNIT_ASSERT_VALUES_EQUAL(DoTestParseSrcSuccess(TStringBuf("\x08"sv), 2, 0), std::make_pair(1u, TSrc::T_LREF__POS));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseSrcSuccess(TStringBuf("\x09"sv), 2, 0), std::make_pair(1u, TSrc::T_CREF__POS));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseSrcSuccess(TStringBuf("\x0A"sv), 2, 0), std::make_pair(1u, TSrc::T_RREF__POS));

        DoTestParseSrcFailure(TStringBuf("\x03"sv), 1, 0);

        {
            TVMState st{TStringBuf("\x7A\x7D"), 16, 0};
            UNIT_ASSERT_VALUES_EQUAL(DoTestParseSrcSuccess(st), std::make_pair(15u, TSrc::T_RREF__POS));
            UNIT_ASSERT_VALUES_EQUAL(DoTestParseSrcSuccess(st), std::make_pair(15u, TSrc::T_RREF__POS));
            UNIT_ASSERT(!ParseSrc(st));
        }
    }

    void DoTestParseDstFailure(const TStringBuf inp, const ui32 memSz, const ui32 curPos) {
        TVMState st{inp, memSz, curPos};
        UNIT_ASSERT(!ParseDst(st));
    }

    [[nodiscard]]
    auto DoTestParseDstSuccess(TVMState& st) {
        const auto dst = ParseDst(st);
        UNIT_ASSERT(dst);
        return std::make_pair(dst->Pos, dst->Type);
    }

    [[nodiscard]]
    auto DoTestParseDstSuccess(const TStringBuf inp, const ui32 memSz, const ui32 curPos) {
        TVMState st{inp, memSz, curPos};
        return DoTestParseDstSuccess(st);
    }

    Y_UNIT_TEST(TestParseDst) {
        DoTestParseDstFailure("", 1, 0);

        UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(TStringBuf("\x00"sv), 1, 0), std::make_pair((ui32)-1, TDst::T_CREATE_FRONT_LREF));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(TStringBuf("\x01"sv), 1, 0), std::make_pair((ui32)-1, TDst::T_CREATE_FRONT_CREF));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(TStringBuf("\x02"sv), 1, 0), std::make_pair((ui32)-1, TDst::T_CREATE_FRONT_RREF));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(TStringBuf("\x03"sv), 1, 0), std::make_pair((ui32)-1, TDst::T_CREATE_BACK_LREF));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(TStringBuf("\x04"sv), 1, 0), std::make_pair((ui32)-1, TDst::T_CREATE_BACK_CREF));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(TStringBuf("\x05"sv), 1, 0), std::make_pair((ui32)-1, TDst::T_CREATE_BACK_RREF));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(TStringBuf("\x26\x00"sv), 2, 0), std::make_pair(1u, TDst::T_LREF__POS));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(TStringBuf("\x27\x00"sv), 2, 0), std::make_pair(1u, TDst::T_CREF__POS));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(TStringBuf("\x28\x00"sv), 2, 0), std::make_pair(1u, TDst::T_RREF__POS));

        DoTestParseDstFailure(TStringBuf("\x06"sv), 1, 0);
        DoTestParseDstFailure(TStringBuf("\x09\x00"sv), 1, 0);

        {
            TVMState st{TStringBuf("\x14\xE7\x09"sv), 16, 0};
            // 4=4
            UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(st), std::make_pair((ui32)-1, TDst::T_CREATE_BACK_CREF));
            // 4=8
            UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(st), std::make_pair((ui32)-1, TDst::T_CREATE_FRONT_CREF));
            // 4+1+4=17
            UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(st), std::make_pair(15u, TDst::T_CREF__POS));
            // 4=21
            UNIT_ASSERT_VALUES_EQUAL(DoTestParseDstSuccess(st), std::make_pair((ui32)-1, TDst::T_CREATE_BACK_CREF));
            UNIT_ASSERT(!ParseDst(st));
        }
    }

    void DoTestParsePathFailure(const TStringBuf inp, const ui32 memSz, const ui32 curPos) {
        TVMState st{inp, memSz, curPos};
        UNIT_ASSERT(!ParsePath(st));
    }

    [[nodiscard]]
    auto DoTestParsePathSuccess(TVMState& st) {
        const auto path = ParsePath(st);
        UNIT_ASSERT(path);
        return path->Path;
    }

    [[nodiscard]]
    auto DoTestParsePathSuccess(const TStringBuf inp, const ui32 memSz, const ui32 curPos) {
        TVMState st{inp, memSz, curPos};
        return DoTestParsePathSuccess(st);
    }

    Y_UNIT_TEST(TestParsePath) {
        DoTestParsePathFailure("", 1, 0);

        UNIT_ASSERT_VALUES_EQUAL(DoTestParsePathSuccess(TStringBuf("\x00"sv), 1, 0), TStringBuf(""));
        UNIT_ASSERT_VALUES_EQUAL(DoTestParsePathSuccess(TStringBuf("\x21\x0C"sv), 1, 0), TStringBuf("a"));

        DoTestParsePathFailure("\x22\x0C", 1, 0);
    }
}
