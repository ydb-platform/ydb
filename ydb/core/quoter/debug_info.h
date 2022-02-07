#pragma once
#define QUOTER_SYSTEM_DEBUG_INFO // uncomment this or build with -DQUOTER_SYSTEM_DEBUG_INFO

#ifdef QUOTER_SYSTEM_DEBUG_INFO
#define QUOTER_SYSTEM_DEBUG(action) action
#else // QUOTER_SYSTEM_DEBUG_INFO
#define QUOTER_SYSTEM_DEBUG(action)
#endif // QUOTER_SYSTEM_DEBUG_INFO

#ifdef QUOTER_SYSTEM_DEBUG_INFO

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

namespace NKikimr::NQuoter {

// Structure for using in debug purpuses with gdb.
// Can show all main Quoter System objects in coredump.
struct TDebugInfo {
    class TQuoterService* QuoterService = nullptr;
    THashMap<TString, class TKesusQuoterProxy*> KesusQuoterProxies;
};

// Helper for safe access to debug info.
class TDebugInfoHolder {
public:
    class TAutoGuarder {
        friend class TDebugInfoHolder;

        TAutoGuarder(TDebugInfoHolder& parent)
            : Parent(parent)
            , Guard(Parent.Lock)
        {
        }

    public:
        TDebugInfo* operator->() {
            return &Parent.DebugInfo;
        }

    private:
        TDebugInfoHolder& Parent;
        TGuard<TAdaptiveLock> Guard;
    };

    // Returns safe (guarded) debug info to write to.
    TAutoGuarder operator->() {
        return { *this };
    }

private:
    TDebugInfo DebugInfo;
    TAdaptiveLock Lock;
};

extern TDebugInfoHolder DebugInfo;

} // namespace NKikimr::NQuoter

#endif // QUOTER_SYSTEM_DEBUG_INFO
