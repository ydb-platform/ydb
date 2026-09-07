#pragma once

#include "schemeshard_info_types_base.h"

namespace NKikimr {
namespace NSchemeShard {

struct TFullBackupInfo : public TSimpleRefCount<TFullBackupInfo> {
    using TPtr = TIntrusivePtr<TFullBackupInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        Transferring = 1,
        Failed = 230,
        Done = 240,
    };

    struct TItem {
        enum class EState: ui8 {
            Invalid = 0,
            Transferring = 1,
            Failed = 230,
            Done = 240,
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
    TPathId BackupCollectionPathId;

    // Planned number of items (base tables + non-omitted index impl tables).
    // Advisory only (drives the progress percentage); the terminal state is
    // decided by control op completion, not by this count.
    ui32 ExpectedItemCount = 0;

    THashMap<TPathId, TItem> Items;

    TMaybe<TString> UserSID;
    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();
    TString FinalIssues;

    explicit TFullBackupInfo(
            const ui64 id,
            const TPathId domainPathId)
        : Id(id)
        , State(EState::Invalid)
        , DomainPathId(domainPathId)
    {}

    bool IsDone() const {
        return State == EState::Done;
    }

    bool IsFailed() const {
        return State == EState::Failed;
    }

    bool IsFinished() const {
        return IsDone() || IsFailed();
    }

    bool IsAllItemsDone() const {
        for (const auto& item : Items) {
            if (!item.second.IsDone()) {
                return false;
            }
        }
        return true;
    }

    // True if any observed item is in the Failed terminal state. Drives the
    // Done-vs-Failed choice when operation completion finalizes the header
    // (see FinalizeFullBackupOnOpComplete).
    bool HasAnyFailed() const {
        for (const auto& [_, item] : Items) {
            if (item.State == TItem::EState::Failed) {
                return true;
            }
        }
        return false;
    }
};

}
}
