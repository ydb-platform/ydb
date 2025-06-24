#include "cached.h"

#include <yql/essentials/sql/v1/complete/name/cache/local/cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/time_provider/monotonic_provider.h>

using namespace NSQLComplete;

class TFailableCache: public ICache<TString, TString> {
public:
    NThreading::TFuture<TEntry> Get(const TString& key) const try {
        if (IsGetFailing) {
            ythrow yexception() << "O_O";
        }
        return NThreading::MakeFuture<TEntry>({.Value = key, .IsExpired = IsExpired});
    } catch (...) {
        return NThreading::MakeErrorFuture<TEntry>(std::current_exception());
    }

    NThreading::TFuture<void> Update(const TString& /* key */, TString /* value */) const try {
        if (IsUpdateFailing) {
            ythrow yexception() << "O_O";
        }
        return NThreading::MakeFuture();
    } catch (...) {
        return NThreading::MakeErrorFuture<void>(std::current_exception());
    }

    bool IsGetFailing = false;
    bool IsExpired = false;
    bool IsUpdateFailing = false;
};

Y_UNIT_TEST_SUITE(CachedQueryTests) {

    Y_UNIT_TEST(OnEmpty_WhenGet_ThenWaitUntilReceived) {
        auto cache = MakeLocalCache<TString, TString>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {.TTL = TDuration::Zero()});
        auto cached = TCachedQuery<TString, TString>(cache, [&](const TString& key) {
            return NThreading::MakeFuture<TString>(key);
        });

        TString value = cached("1").GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(value, "1");
    }

    Y_UNIT_TEST(OnExpired_WhenApplied_ThenDefferedUpdateAndReturnOld) {
        size_t queried = 0;
        auto cache = MakeLocalCache<TString, TString>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {.TTL = TDuration::Zero()});
        auto cached = TCachedQuery<TString, TString>(cache, [&](const TString& key) {
            queried += 1;
            return NThreading::MakeFuture<TString>(key);
        });
        cache->Update("1", "2");

        TString value = cached("1").GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(value, "2");
        UNIT_ASSERT_VALUES_EQUAL(queried, 1);
        UNIT_ASSERT_VALUES_EQUAL(cached("1").GetValueSync(), "1");
    }

    Y_UNIT_TEST(OnQueryError_WhenApplied_ThenNoDeadlock) {
        size_t queried = 0;
        auto cache = MakeLocalCache<TString, TString>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {.TTL = TDuration::Zero()});
        auto cached = TCachedQuery<TString, TString>(cache, [&](const TString&) {
            queried += 1;
            try {
                ythrow yexception() << "T_T";
            } catch (...) {
                return NThreading::MakeErrorFuture<TString>(std::current_exception());
            }
        });

        UNIT_ASSERT_EXCEPTION(cached("1").GetValueSync(), yexception);
        UNIT_ASSERT_EXCEPTION(cached("1").GetValueSync(), yexception);
        UNIT_ASSERT_VALUES_EQUAL(queried, 2);
    }

    Y_UNIT_TEST(OnFailingCacheGet_WhenApplied_ThenNoDeadlock) {
        size_t queried = 0;
        auto cache = MakeIntrusive<TFailableCache>();
        auto cached = TCachedQuery<TString, TString>(cache, [&](const TString& key) {
            queried += 1;
            return NThreading::MakeFuture<TString>(key);
        });

        cache->IsGetFailing = true;

        UNIT_ASSERT_EXCEPTION(cached("1").GetValueSync(), yexception);
        UNIT_ASSERT_VALUES_EQUAL(queried, 0);
    }

    Y_UNIT_TEST(OnFailingCacheUpdate_WhenApplied_ThenNoErrorAndNotCached) {
        size_t queried = 0;
        auto cache = MakeIntrusive<TFailableCache>();
        auto cached = TCachedQuery<TString, TString>(cache, [&](const TString& key) {
            queried += 1;
            return NThreading::MakeFuture<TString>(key);
        });

        cache->IsExpired = true;
        cache->IsUpdateFailing = true;

        UNIT_ASSERT_VALUES_EQUAL(cached("1").GetValueSync(), "1");
        UNIT_ASSERT_VALUES_EQUAL(cached("1").GetValueSync(), "1");
        UNIT_ASSERT_VALUES_EQUAL(queried, 2);
    }

} // Y_UNIT_TEST_SUITE(CachedQueryTests)
