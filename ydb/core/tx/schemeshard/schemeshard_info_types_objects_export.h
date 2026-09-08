#pragma once

#include "schemeshard_info_types_base.h"

namespace NKikimr::NSchemeShard {

struct TExportInfo: public TSimpleRefCount<TExportInfo> {
    using TPtr = TIntrusivePtr<TExportInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        Waiting = 1,
        CreateExportDir = 2,
        CopyTables = 3,
        Transferring = 4,
        UploadExportMetadata = 5,
        Done = 240,
        Dropping = 241,
        Dropped = 242,
        AutoDropping = 243,
        Cancellation = 250,
        Cancelled = 251,
    };

    enum class EKind: ui8 {
        YT = 0,
        S3,
        FS,
    };

    struct TItem {
        enum class ESubState: ui8 {
            AllocateTxId = 0,
            Proposed,
            Subscribed,
        };

        TString SourcePathName;
        TPathId SourcePathId;
        NKikimrSchemeOp::EPathType SourcePathType;
        ui32 ParentIdx;

        EState State = EState::Waiting;
        ESubState SubState = ESubState::AllocateTxId;
        TTxId WaitTxId = InvalidTxId;
        TActorId SchemeUploader;
        TString Issue;

        TItem() = default;

        explicit TItem(
                const TString& sourcePathName,
                const TPathId sourcePathId,
                NKikimrSchemeOp::EPathType sourcePathType,
                ui32 parentIdx = Max<ui32>())
            : SourcePathName(sourcePathName)
            , SourcePathId(sourcePathId)
            , SourcePathType(sourcePathType)
            , ParentIdx(parentIdx)
        {
        }

        TString ToString(ui32 idx) const;

        static bool IsDone(const TItem& item);
        static bool IsDropped(const TItem& item);
    };

    ui64 Id;
    TString Uid;
    EKind Kind;
    TString Settings;
    TPathId DomainPathId;
    TMaybe<TString> UserSID;
    TString PeerName;
    TString SanitizedToken;

    TVector<TItem> Items;

    TPathId ExportPathId = InvalidPathId;
    EState State = EState::Invalid;
    TTxId WaitTxId = InvalidTxId;
    THashSet<TTxId> DependencyTxIds;
    TString Issue;

    TDeque<ui32> PendingItems;
    TDeque<ui32> PendingDropItems;

    TSet<TActorId> Subscribers;

    ui64 SnapshotStep = 0;
    ui64 SnapshotTxId = 0;

    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    bool EnableChecksums = false;
    bool EnablePermissions = false;
    bool IncludeIndexData = false;

    NKikimrSchemeOp::TExportMetadata ExportMetadata;
    TActorId ExportMetadataUploader;

    explicit TExportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const TString& settings,
            const TPathId domainPathId,
            const TString& peerName)
        : Id(id)
        , Uid(uid)
        , Kind(kind)
        , Settings(settings)
        , DomainPathId(domainPathId)
        , PeerName(peerName)
    {
    }

    template <typename TSettingsPB>
    explicit TExportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const TSettingsPB& settingsPb,
            const TPathId domainPathId,
            const TString& peerName)
        : TExportInfo(id, uid, kind, SerializeSettings(settingsPb), domainPathId, peerName)
    {
    }

    bool IsValid() const {
        return State != EState::Invalid;
    }

    bool IsPreparing() const {
        return State == EState::CreateExportDir || State == EState::CopyTables || State == EState::UploadExportMetadata;
    }

    bool IsWorking() const {
        return State == EState::Transferring;
    }

    bool IsDropping() const {
        return State == EState::Dropping;
    }

    bool IsAutoDropping() const {
        return State == EState::AutoDropping;
    }

    bool IsCancelling() const {
        return State == EState::Cancellation;
    }

    bool IsInProgress() const {
        return IsPreparing() || IsWorking() || IsDropping() || IsAutoDropping() || IsCancelling();
    }

    bool IsDone() const {
        return State == EState::Done;
    }

    bool IsCancelled() const {
        return State == EState::Cancelled;
    }

    bool IsFinished() const {
        return IsDone() || IsCancelled();
    }

    bool AllItemsAreDropped() const;
    void AddNotifySubscriber(const TActorId& actorId);

    TString ToString() const;
};

bool IsPathTypeTable(const TExportInfo::TItem& item);

} // namespace NKikimr::NSchemeShard
