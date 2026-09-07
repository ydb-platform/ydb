#pragma once

#include "schemeshard_info_types_base.h"

namespace NKikimr {
namespace NSchemeShard {

struct TIncrementalBackupInfo : public TSimpleRefCount<TIncrementalBackupInfo> {
    using TPtr = TIntrusivePtr<TIncrementalBackupInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        Transferring = 1,
        Done = 240,
        Cancellation = 250,
        Cancelled = 251,
    };

    struct TItem {
        enum class EState: ui8 {
            Invalid = 0,
            Transferring = 1,
            Dropping = 230,
            Done = 240,
            Cancellation = 250,
            Cancelled = 251,
        };

        TPathId PathId;
        EState State;

        bool IsDone() const {
            return State == EState::Done;
        }
    };

    ui64 Id;
    EState State;
    TPathId DomainPathId;

    THashMap<TPathId, TItem> Items;

    TMaybe<TString> UserSID;
    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    explicit TIncrementalBackupInfo(
            const ui64 id,
            const TPathId domainPathId)
        : Id(id)
        , DomainPathId(domainPathId)
    {}

    bool IsDone() const {
        return State == EState::Done;
    }

    bool IsCancelled() const {
        return State == EState::Cancelled;
    }

    bool IsFinished() const {
        return IsDone() || IsCancelled();
    }

    bool IsAllItemsDone() const {
        for (const auto& item : Items) {
            if (!item.second.IsDone()) {
                return false;
            }
        }
        return true;
    }
};

// Trackable full backup op (the aggregator over a BackupBackupCollection's
// CCT). Mirrors TIncrementalBackupInfo with three changes:
//   - Adds Failed terminal state (header + item).
//   - Stores BackupCollectionPathId so reboot can rebuild
//     Self->BCPathToFullBackup from non-terminal rows.
//   - Stores FinalIssues for hard-fail diagnostics surfaced via GET.

}
}
