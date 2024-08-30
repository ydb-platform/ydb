#include "stlog.h"
#include <ydb/core/base/logoblob.h>
#include <ydb/core/protos/base.pb.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(StLog) {

    Y_UNIT_TEST(Basic) {
        std::optional<int> x1(1);
        std::optional<int> y1;
        TMaybe<int> x2(2);
        TMaybe<int> y2;
        int z = 3;
        const int *x3 = &z;
        const int *y3 = nullptr;
        std::vector<int> v{{1, 2, 3}};
        NKikimrProto::EReplyStatus status = NKikimrProto::RACE;
        NKikimrProto::TLogoBlobID id;
        LogoBlobIDFromLogoBlobID(NKikimr::TLogoBlobID(1, 2, 3, 4, 5, 6), &id);
        struct {
            TString ToString() const { return "yep"; }
        } s;
        NKikimr::NStLog::OutputLogJson = false;
        STLOG_STREAM(stream, MARKER1, "hello, world", (Param1, 1), (Param2, 'c'), (Param3, 1.1f), (Param4, "abcdef"),
            (Param5, TString("abcdef")), (Param6, x1), (Param7, x2), (Param8, x3), (Param9, y1),
            (Param10, y2), (Param11, y3), (Param12, true), (Param13, v), (Param14, status), (Param15, id),
            (Param16, &s));
        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), "{MARKER1@stlog_ut.cpp:27} hello, world Param1# 1 Param2# c Param3# 1.1 "
            "Param4# abcdef Param5# abcdef Param6# 1 Param7# 2 Param8# 3 Param9# <null> Param10# <null> Param11# <null> "
            "Param12# true Param13# [1 2 3] Param14# RACE Param15# {RawX1: 1 RawX2: 288230376185266176 "
            "RawX3: 216172807883587664 } Param16# yep");
        NKikimr::NStLog::OutputLogJson = true;
        STLOG_STREAM(stream2, MARKER2, "hello, world", (Param1, 1), (Param2, 'c'), (Param3, 1.1f), (Param4, "abcdef"),
            (Param5, TString("abcdef")), (Param6, x1), (Param7, x2), (Param8, x3), (Param9, y1),
            (Param10, y2), (Param11, y3), (Param12, true), (Param13, v), (Param14, status), (Param15, id),
            (Param16, &s));
        UNIT_ASSERT_VALUES_EQUAL(stream2.Str(), R"({"marker":"MARKER2","file":"stlog_ut.cpp","line":36,"message":"hello, world",)"
            R"("Param1":1,"Param2":99,"Param3":1.1,"Param4":"abcdef","Param5":"abcdef","Param6":1,"Param7":2,"Param8":3,)"
            R"("Param9":null,"Param10":null,"Param11":null,"Param12":true,"Param13":[1,2,3],"Param14":"RACE","Param15":)"
            R"({"RawX1":"1","RawX2":"288230376185266176","RawX3":"216172807883587664"},"Param16":"yep"})");
    }

}
