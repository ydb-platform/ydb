#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/intrlist.h>
#include <util/system/datetime.h>
#include <util/system/hp_timer.h>
#include <util/system/yassert.h>

namespace NKikimr {
namespace NMiniKQL {

struct IStatsRegistry;

/**
 * @brief Statistic unique named key.
 *
 * NOTE: Instances of this class must be static objects.
 */
class TStatKey: public TIntrusiveSListItem<TStatKey> {
    friend struct IStatsRegistry;

// -- static part --
    static ui32 IdSequence_;
    static TStatKey* KeysChain_;

// -- instance part --
public:
    TStatKey(TStringBuf name, bool deriv);

    TStringBuf GetName() const noexcept { return Name_; }
    bool IsDeriv() const noexcept { return Deriv_; }
    ui32 GetId() const noexcept { return Id_; }
    static ui32 GetMaxId() { return IdSequence_; }

    template <typename F>
    static void ForEach(F f) {
        for (TStatKey* p = KeysChain_; p;) {
            f(*p);
            auto n = p->Next();
            p = n ? n->Node() : nullptr;
        }
    }

private:
    TStringBuf Name_;
    bool Deriv_;
    ui32 Id_;
};

/**
 * @brief Sase interface for statistics registry implementations.
 */
struct IStatsRegistry {
    virtual ~IStatsRegistry() = default;

    virtual i64 GetStat(const TStatKey& key) const = 0;

    virtual void SetStat(const TStatKey& key, i64 value) = 0;
    virtual void SetMaxStat(const TStatKey& key, i64 value) = 0;
    virtual void SetMinStat(const TStatKey& key, i64 value) = 0;

    virtual void AddStat(const TStatKey& key, i64 value) = 0;

    void IncStat(const TStatKey& key) {
        AddStat(key, 1);
    }

    void DecStat(const TStatKey& key) {
        AddStat(key, -1);
    }

    // TConsumer = void(const TStatKey& key, i64 value)
    template <typename TConsumer>
    void ForEachStat(TConsumer c) const {
        TStatKey::ForEach([this, &c](const auto& key) {
            c(key, GetStat(key));
        });
    }
};

using IStatsRegistryPtr = TAutoPtr<IStatsRegistry>;

IStatsRegistryPtr CreateDefaultStatsRegistry();


#define MKQL_STAT_MOD_IMPL(method, nullableStatsRegistry, key, value) \
    do { \
        if (nullableStatsRegistry) \
            nullableStatsRegistry->method(key, value); \
    } while (0)

#define MKQL_SET_STAT(statRegistry, key, value) \
    MKQL_STAT_MOD_IMPL(SetStat, statRegistry, key, value)

#define MKQL_SET_MIN_STAT(statRegistry, key, value) \
    MKQL_STAT_MOD_IMPL(SetMinStat, statRegistry, key, value)

#define MKQL_SET_MAX_STAT(statRegistry, key, value) \
    MKQL_STAT_MOD_IMPL(SetMaxStat, statRegistry, key, value)

#define MKQL_ADD_STAT(statRegistry, key, value) \
    MKQL_STAT_MOD_IMPL(AddStat, statRegistry, key, value)

#define MKQL_INC_STAT(statRegistry, key) MKQL_ADD_STAT(statRegistry, key, 1)
#define MKQL_DEC_STAT(statRegistry, key) MKQL_ADD_STAT(statRegistry, key, -1)

class TStatTimerBase {
public:
    TStatTimerBase(const TStatKey& key)
        : Key_(key)
        , Total_(0)
        , Start_(0)
    {}

    void Reset() {
        Total_ = 0;
    }

    void Report(IStatsRegistry* statRegistry) const {
        auto value = (i64)(1e3 * Total_ / NHPTimer::GetClockRate());
        MKQL_ADD_STAT(statRegistry, Key_, value);
    }

protected:
    const TStatKey& Key_;
    i64 Total_;
    i64 Start_;
};

class TStatTimer: public TStatTimerBase {
public:
    TStatTimer(const TStatKey& key)
        : TStatTimerBase(key)
    {}

    void Acquire() {
        Start_ = GetCycleCount();
    }

    void Release() {
        Total_ += GetCycleCount() - Start_;
    }
};

class TSamplingStatTimer: public TStatTimerBase {
public:
    TSamplingStatTimer(const TStatKey& key, ui64 frequency = 100)
        : TStatTimerBase(key)
        , Frequency_(frequency)
    {
        Y_ASSERT(Frequency_ > 0);
    }

    void Acquire() {
        if (Counter_ == 0) {
            Start_ = GetCycleCount();
        }
    }

    void Release() {
        if (Counter_ == 0) {
            Total_ += (GetCycleCount() - Start_) * Frequency_;
        }
        if (++Counter_ == Frequency_) {
            Counter_ = 0;
        }
    }
private:
    const ui64 Frequency_;
    ui64 Counter_ = 0;
};

} // namespace NMiniKQL
} // namespace NKikimr
