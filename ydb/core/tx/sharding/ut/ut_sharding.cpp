#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/sharding/sharding.h>
#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <ydb/library/actors/core/av_bootstrapped.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(Sharding) {

    Y_UNIT_TEST(XXUsage) {
        NArrow::NHash::NXX64::TStreamStringHashCalcer hCalcer(0);
        for (ui32 a = 1; a < 10; ++a) {
            TString ss;
            hCalcer.Start();
            for (ui32 i = 0; i < 10000; ++i) {
                const ui8 c = RandomNumber<ui8>();
                hCalcer.Update(&c, 1);
                ss += (char)c;
                UNIT_ASSERT(hCalcer.Finish() == XXH64(ss.data(), ss.size(), 0));
                if (i % 1000 == 0) {
                    Cerr << hCalcer.Finish() << Endl;
                }
            }
        }
    }
}
}
