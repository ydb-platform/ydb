#include "access.h"
#include "variable.h"

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <array>

namespace NYT::NGlobal {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

inline constexpr int MaxTrackedGlobalVariables = 32;

////////////////////////////////////////////////////////////////////////////////

class TGlobalVariablesRegistry
{
public:
    static TGlobalVariablesRegistry* Get()
    {
        return LeakySingleton<TGlobalVariablesRegistry>();
    }

    void RegisterAccessor(const TVariableTag& tag, TAccessor accessor)
    {
        if (!tag.Initialized_.exchange(true, std::memory_order::relaxed)) { // (a)
            DoRegisterAccessor(tag, accessor);
            return;
        }

        TryVerifyingExistingAccessor(tag, accessor);
    }

    std::optional<TErasedStorage> GetVariable(const TVariableTag& tag)
    {
        auto key = tag.Key_.load(std::memory_order::acquire); // (e)
        if (key != -1) {
            return Accessors_[key](); // (f)
        }

        return std::nullopt;
    }

private:
    std::atomic<int> KeyGenerator_ = 0;
    std::array<TAccessor, MaxTrackedGlobalVariables> Accessors_;

    void DoRegisterAccessor(const TVariableTag& tag, TAccessor accessor)
    {
        // Get id -> place accessor -> store id
        auto key = KeyGenerator_.fetch_add(1, std::memory_order::relaxed); // (b)

        YT_VERIFY(key < MaxTrackedGlobalVariables);

        Accessors_[key] = accessor; // (c)

        tag.Key_.store(key, std::memory_order::release); // (d)
    }

    void TryVerifyingExistingAccessor(const TVariableTag& tag, TAccessor accessor)
    {
        auto key = tag.Key_.load(std::memory_order::acquire); // (e')
        if (key == -1) {
            // Accessor is about to be set.

            // In order to avoid deadlock caused by forks
            // we just leave. We could try acquiring fork
            // locks here but this makes our check too expensive
            // to be bothered.
            return;
        }

        // Accessor has been already set -> safe to read it.
        YT_VERIFY(Accessors_[key] == accessor); // (f')
    }
};

// (arkady-e1ppa): Memory orders:
/*
    We have two scenarios: 2 writes and write & read:

    2 writes: Accessors_ is protected via Initialized_ flag
    and KeyGenerator_ counter.
    1) RMW (a) reads the last value in modification order
    thus relaxed is enough to ensure <= 1 threads registering
    per Tag.
    2) KeyGenerator_ uses the same logic (see (b))
    to ensure <= 1 threads registering per index in array.

    If there are two writes per tag, then there is a "losing"
    thread which read Initialized_ // true. For all intents
    and purposes TryVerifyingExistingAccessor call is identical
    to GetVariable call.

    write & read: Relevant execution is below
                    W^na(Accessors_[id], 0x0) // Ctor
        T1(Register)                    T2(Read)
    W^na(Accessors_[id], 0x42) (c)  R^acq(Key_, id)              (e)
    W^rel(Key_, id)             (d)  R^na(Accessors_[id], 0x42)  (f)

    (d) -rf-> (e) => (d) -SW-> (e). Since (c) -SB-> (d) and (e) -SB-> (f)
    we have (c) -strongly HB-> (f) (strongly happens before). Thus we must
    read 0x42 from Accessors_[id] (and not 0x0 which was written in ctor).
 */

////////////////////////////////////////////////////////////////////////////////

void RegisterVariable(const TVariableTag& tag, TAccessor accessor)
{
    TGlobalVariablesRegistry::Get()->RegisterAccessor(tag, accessor);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

std::optional<TErasedStorage> GetErasedVariable(const TVariableTag& tag)
{
    return NDetail::TGlobalVariablesRegistry::Get()->GetVariable(tag);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGlobal
