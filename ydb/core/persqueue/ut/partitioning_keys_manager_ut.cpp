#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <cstdio>
#include <public/partition_key_range/partition_key_range.h>
#include <random>
#include <vector>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <ydb/core/persqueue/pqtablet/partition/partitioning_keys_manager.h>
#include <ydb/services/lib/sharding/sharding.h>


#include <library/cpp/digest/md5/md5.h>

namespace NKikimr::NPQ {

namespace {

    constexpr ui64 kMsgSize = 1024;

    /** yutil has no ToString / IOutputStream for native unsigned __int128 — avoid << TUint128 in asserts. */
    TString Uint128ToDiagString(TUint128 v) {
        const ui64 hi = static_cast<ui64>(v >> 64);
        const ui64 lo = static_cast<ui64>(v);
        return TStringBuilder() << hi << ':' << lo;
    }

    TUint128 RandomUint128() {
        return NYql::NDecimal::TUint128(std::random_device{}());
    }

    /** Large windows avoid time-based sketch rotation / expiry during the test. */
    TDuration HugeWindow() {
        return TDuration::Days(365 * 1000);
    }

    TDuration SmallWindow() {
        return TDuration::Seconds(5);
    }

    template <typename T>
    void AddRandomKeys(TPartitioningKeysManager& m, size_t N, T& keys) {
        for (size_t i = 0; i < N; ++i) {
            auto decimal = NDataStreams::V1::HexBytesToDecimal(MD5::Calc(CreateGuidAsString()));
            keys.emplace_back(decimal);
        }
        for (const auto& key : keys) {m.Add(key, kMsgSize, Now());}
    }

} // namespace

Y_UNIT_TEST_SUITE(TPartitioningKeysManagerTest) {

    Y_UNIT_TEST(GetMedianKey_Empty) {
        TPartitioningKeysManager m(1, HugeWindow());
        UNIT_ASSERT(m.GetMedianKey() == 0);
    }

    Y_UNIT_TEST(GetMedianKey_SingleKey) {
        TPartitioningKeysManager m(1, HugeWindow());
        auto key = RandomUint128();
        m.Add(key, kMsgSize);
        const TUint128 got = m.GetMedianKey();
        UNIT_ASSERT_C(got == key, "median " << Uint128ToDiagString(got) << " expected " << Uint128ToDiagString(key));
    }

    Y_UNIT_TEST(GetMedianKey_SameKeyManyTimes) {
        TPartitioningKeysManager m(1, HugeWindow());
        auto key = RandomUint128();
        for (int i = 0; i < 500; ++i) {
            m.Add(key, kMsgSize);
        }
        const TUint128 got = m.GetMedianKey();
        UNIT_ASSERT_C(got == key, "median " << Uint128ToDiagString(got) << " expected " << Uint128ToDiagString(key));
    }

    Y_UNIT_TEST(GetMedianKey_StreamingPaddedKeys) {
        TPartitioningKeysManager m(1, HugeWindow());
        std::vector<TUint128> keys;
        for (int i = 0; i < 3000; ++i) {
            auto key = RandomUint128();
            keys.emplace_back(key);
            m.Add(key, kMsgSize);
        }
        TUint128 med = m.GetMedianKey();
        
        std::sort(keys.begin(), keys.end());
        const size_t mid = keys.size() / 2;
        // Allow ~10% rank slack around the empirical median (KLL is approximate).
        const size_t tol = keys.size() / 10;
        const size_t loIdx = mid > tol ? mid - tol : 0;
        const size_t hiIdx = std::min(keys.size() - 1, mid + tol);
        UNIT_ASSERT_C(keys[loIdx] <= med && med <= keys[hiIdx],
            "median " << Uint128ToDiagString(med) << " outside [" << Uint128ToDiagString(keys[loIdx]) << ", "
                      << Uint128ToDiagString(keys[hiIdx]) << "]");
    }

    Y_UNIT_TEST(GetMedianKey_RandomKeys) {
        constexpr size_t N = 1'000'000;
        TPartitioningKeysManager m(1, HugeWindow());
        std::vector<TUint128> keys;
        keys.reserve(N);
        AddRandomKeys(m, N, keys);
        TUint128 med = m.GetMedianKey();
        UNIT_ASSERT(med != 0);

        std::sort(keys.begin(), keys.end());
        const size_t mid = N / 2;
        // Allow ~10% rank slack around the empirical median (KLL is approximate).
        const size_t tol = N / 10;
        const size_t loIdx = mid > tol ? mid - tol : 0;
        const size_t hiIdx = std::min(N - 1, mid + tol);
        UNIT_ASSERT_C(keys[loIdx] <= med && med <= keys[hiIdx],
            "median " << Uint128ToDiagString(med) << " outside [" << Uint128ToDiagString(keys[loIdx]) << ", "
                      << Uint128ToDiagString(keys[hiIdx]) << "]");
    }

    Y_UNIT_TEST(GetMedianKey_MultipleSketches) {
        constexpr size_t N = 1'000'000;
        TPartitioningKeysManager m(5, SmallWindow());
        std::deque<TUint128> keys;

        for (int i = 0; i < 10; ++i) {
            AddRandomKeys(m, N / 10, keys);
            Sleep(TDuration::Seconds(1));
        }

        keys.erase(keys.begin(), keys.begin() + N / 2);
        auto start = Now();
        TUint128 med = m.GetMedianKey();
        auto duration = Now() - start;
        UNIT_ASSERT_C(duration.MilliSeconds() < 100, "duration " << duration.MilliSeconds() << " milliseconds");
        UNIT_ASSERT(med != 0);

        std::sort(keys.begin(), keys.end());
        const size_t mid = keys.size() / 2;
        // Allow ~10% rank slack around the empirical median (KLL is approximate).
        const size_t tol = keys.size() / 10;
        const size_t loIdx = mid > tol ? mid - tol : 0;
        const size_t hiIdx = std::min(keys.size() - 1, mid + tol);
        UNIT_ASSERT_C(keys[loIdx] <= med && med <= keys[hiIdx],
            TStringBuilder() << "median " << Uint128ToDiagString(med) << " outside ["
                             << Uint128ToDiagString(keys[loIdx]) << ", " << Uint128ToDiagString(keys[hiIdx]) << "]");
    }

} // Y_UNIT_TEST_SUITE(TPartitioningKeysManagerTest)

} // namespace NKikimr::NPQ
