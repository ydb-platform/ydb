#pragma once

#include <ydb/library/actors/core/subsystem.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/system/mutex.h>
#include <util/system/types.h>

#include <set>

namespace NActors {
    class TMon;
}

namespace NKikimr {

inline constexpr TStringBuf BlobStorageGroupSelectorRulesPath = "/etc/ydb/blobstorage-emergency-recovery.conf";

// The UI reloads and saves the file. VDisks take a snapshot of the cached rules on restart.
class TBlobStorageGroupSelector : public NActors::ISubSystem {
public:
    // An empty file path keeps the selector in memory.
    explicit TBlobStorageGroupSelector(TString rulesFilePath = TString(BlobStorageGroupSelectorRulesPath),
        NActors::TMon* mon = nullptr, ui32 poolId = 0);

    bool Matches(ui32 groupId, TStringBuf storagePoolName) const;
    TString GetRules() const;
    bool ReloadRules(TString* error);
    bool AddRules(TStringBuf rules, TString* error);
    const TString& GetRulesFilePath() const;

    void OnAfterStart(NActors::TActorSystem& actorSystem) override;

private:
    struct TRules {
        bool AllGroups = false;
        THashSet<ui32> Groups;
        std::set<TString> PoolPrefixes;

        TString ToString() const;
    };

    static TRules ParseRules(TStringBuf text);
    TRules ReadRulesFile() const;
    void SaveRulesFile(const TRules& rules) const;

    const TString RulesFilePath;
    NActors::TMon* const Mon;
    const ui32 PoolId;
    // Serialize file updates without holding the readers' mutex during I/O.
    TMutex UpdateMutex;
    mutable TMutex Mutex;
    TRules Rules;
};

} // namespace NKikimr
