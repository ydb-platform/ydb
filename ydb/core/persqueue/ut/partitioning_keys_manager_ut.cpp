#include <library/cpp/testing/unittest/registar.h>

#include <cstdio>
#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <ydb/core/persqueue/public/partitioning_keys_manager.h>

namespace NKikimr::NPQ {

namespace {

    constexpr ui64 kMsgSize = 1024;

    /** Large windows avoid time-based sketch rotation / expiry during the test. */
    TDuration HugeWindow() {
        return TDuration::Days(365 * 1000);
    }

} // namespace

Y_UNIT_TEST_SUITE(TPartitioningKeysManagerTest) {

    Y_UNIT_TEST(GetMedianKey_Empty) {
        TPartitioningKeysManager m(1, HugeWindow());
        UNIT_ASSERT(m.GetMedianKey().empty());
    }

    Y_UNIT_TEST(GetMedianKey_SingleKey) {
        TPartitioningKeysManager m(1, HugeWindow());
        m.Add(TString{"partition-alpha"}, kMsgSize);
        UNIT_ASSERT_VALUES_EQUAL(m.GetMedianKey(), TString{"partition-alpha"});
    }

    Y_UNIT_TEST(GetMedianKey_SameKeyManyTimes) {
        TPartitioningKeysManager m(1, HugeWindow());
        for (int i = 0; i < 500; ++i) {
            m.Add(TString{"stable-key"}, kMsgSize);
        }
        UNIT_ASSERT_VALUES_EQUAL(m.GetMedianKey(), TString{"stable-key"});
    }

    Y_UNIT_TEST(GetMedianKey_HeavyKeyBiasesMedian) {
        TPartitioningKeysManager m(1, HugeWindow());
        m.Add(TString{"a"}, kMsgSize);
        m.Add(TString{"c"}, kMsgSize);
        for (int i = 0; i < 4000; ++i) {
            m.Add(TString{"b"}, kMsgSize);
        }
        UNIT_ASSERT_VALUES_EQUAL(m.GetMedianKey(), TString{"b"});
    }

    Y_UNIT_TEST(GetMedianKey_StreamingPaddedKeys) {
        TPartitioningKeysManager m(1, HugeWindow());
        for (int i = 0; i < 3000; ++i) {
            char buf[24];
            std::snprintf(buf, sizeof(buf), "k%07d", i);
            m.Add(TString{buf}, kMsgSize);
        }
        TString med = m.GetMedianKey();
        UNIT_ASSERT(med.StartsWith("k"));
        UNIT_ASSERT(med >= TString{"k0000000"} && med <= TString{"k0002999"});
    }

    Y_UNIT_TEST(AddZeroMsgSize_ThenGetMedianKeyThrows) {
        TPartitioningKeysManager m(1, HugeWindow());
        m.Add(TString{"x"}, 0);
        UNIT_ASSERT_EXCEPTION(m.GetMedianKey(), yexception);
    }

} // Y_UNIT_TEST_SUITE(TPartitioningKeysManagerTest)

} // namespace NKikimr::NPQ
