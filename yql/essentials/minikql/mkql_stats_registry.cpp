#include "mkql_stats_registry.h"

#include <util/generic/yexception.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>


namespace NKikimr {
namespace NMiniKQL {
namespace {

/**
 * vector based implementation of stats registry
 */
class TDefaultStatsRegistry: public IStatsRegistry {
public:
    i64 GetStat(const TStatKey& key) const override {
        return key.GetId() < Values_.size() ? Values_[key.GetId()] : 0;
    }

    void SetStat(const TStatKey& key, i64 value) override {
        EnsureSize(key);
        Values_[key.GetId()] = value;
    }

    void SetMaxStat(const TStatKey& key, i64 value) override {
        EnsureSize(key);
        i64& oldValue = Values_[key.GetId()];
        oldValue = Max(value, oldValue);
    }

    void SetMinStat(const TStatKey& key, i64 value) override {
        EnsureSize(key);
        i64& oldValue = Values_[key.GetId()];
        oldValue = Min(value, oldValue);
    }

    void AddStat(const TStatKey& key, i64 value) override {
        EnsureSize(key);
        Values_[key.GetId()] += value;
    }

private:
    void EnsureSize(const TStatKey& key) {
        if (Y_UNLIKELY(Values_.size() <= key.GetId())) {
            Values_.resize(key.GetId() + 10); // try to avoid too often resizing
        }
    }

private:
    TVector<i64> Values_;
};

/**
 * Key names set to prevent keys accidental duplication.
 */
class TKeyNames: private THashSet<TStringBuf> {
public:
    bool AddIfNotExists(TStringBuf name) {
        return insert(name).second;
    }
};

} // namespace


ui32 TStatKey::IdSequence_ = 0;
TStatKey* TStatKey::KeysChain_ = nullptr;


TStatKey::TStatKey(TStringBuf name, bool deriv)
    : Name_(name)
    , Deriv_(deriv)
    , Id_(IdSequence_++)
{
    bool newOne = Singleton<TKeyNames>()->AddIfNotExists(name);
    Y_ENSURE(newOne, "duplicated stat key: " << name);

    SetNext(KeysChain_);
    KeysChain_ = this;
}

IStatsRegistryPtr CreateDefaultStatsRegistry() {
    return new TDefaultStatsRegistry;
}

} // namespace NMiniKQL
} // namespace NKikimr
