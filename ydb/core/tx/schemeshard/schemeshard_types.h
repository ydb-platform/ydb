#pragma once

#include "schemeshard_identificators.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/fwd.h>

namespace NKikimr::NSchemeShard {

// Concept for TEventBase::TPtr
template <class TEvPtr>
concept EventBasePtr = requires { typename TEvPtr::TValueType; } && std::is_base_of_v<IEventHandle, typename TEvPtr::TValueType>;

// Deduce TEventType from its own TEventType::TPtr.
template <EventBasePtr TEvPtr>
struct EventTypeFromTEvPtr {
    // TEventType::TPtr is TAutoPtr<TEventHandle*<TEventType>>.
    // Retrieve TEventType through return type of TEventHandle*::Get().
    using type = typename std::remove_pointer<decltype(std::declval<typename TEvPtr::TValueType>().Get())>::type;
    // It would help if TEventHandle* had an explicit type alias to wrapped TEventType
};

// Defaults for TSchemeLimits are runtime-tunable via YDB_TEST_SCHEMESHARD_* env vars
// (see schemeshard_types.cpp). Defaults match historical compile-time values when env unset.
// TODO(YDBAPPTEAM-773): revert this override after the ticket is closed.
ui64 SchemeLimitsDefaultMaxObjectsInBackup();
ui64 SchemeLimitsDefaultMaxTableColumns();
ui64 SchemeLimitsDefaultMaxTableIndices();

struct TSchemeLimits {
    // Used for backward compatability in case of old databases without explicit limits
    static constexpr ui64 MaxPathsCompat = 200*1000;
    // Function (not data member) to avoid static-init ordering issues with
    // TSchemeShard::DefaultLimits, which is itself a global.
    static ui64 MaxObjectsInBackup() { return SchemeLimitsDefaultMaxObjectsInBackup(); }

    // path
    ui64 MaxDepth = 32;
    ui64 MaxPaths = MaxObjectsInBackup();
    ui64 MaxChildrenInDir = 100*1000;
    ui64 MaxAclBytesSize = 10 << 10;
    ui64 MaxPathElementLength = 255;
    TString ExtraPathSymbolsAllowed = "!\"#$%&'()*+,-.:;<=>?@[\\]^_`{|}~";

    // table
    ui64 MaxTableColumns = SchemeLimitsDefaultMaxTableColumns();
    ui64 MaxColumnTableColumns = 10000;
    ui64 MaxTableColumnNameLength = 255;
    ui64 MaxTableKeyColumns = 30;
    ui64 MaxTableIndices = SchemeLimitsDefaultMaxTableIndices();
    ui64 MaxTableCdcStreams = 5;
    ui64 MaxShards = 200*1000; // In each database
    ui64 MaxShardsInPath = 35*1000; // In each path in database
    ui64 MaxConsistentCopyTargets = MaxObjectsInBackup();

    // pq group
    ui64 MaxPQPartitions = 1000000;

    // export & import
    ui64 MaxExports = 10;
    ui64 MaxImports = 10;

    static TSchemeLimits FromProto(const NKikimrSubDomains::TSchemeLimits& proto);
    void MergeFromProto(const NKikimrSubDomains::TSchemeLimits& proto);
    NKikimrSubDomains::TSchemeLimits AsProto() const;
};

using ETabletType = TTabletTypes;

struct TVirtualTimestamp
    : public TRowVersion
{
    using TRowVersion::TRowVersion;

    TVirtualTimestamp(TStepId step, TTxId txId)
        : TRowVersion(step.GetValue(), txId.GetValue())
    {}

    bool Empty() const {
        return !bool(Step);
    }

    explicit operator bool () const {
        return !Empty();
    }

    TStepId GetStep() const {
        return TStepId(Step);
    }

    void SetStep(TStepId step) {
        Step = step.GetValue();
    }

    TTxId GetTxId() const {
        return TTxId(TxId);
    }

    void SetTxId(TTxId txid) {
        TxId = txid.GetValue();
    }

    bool operator<(const TVirtualTimestamp& ts) const {
        Y_DEBUG_ABORT_UNLESS(Step, "Comparing with unset timestamp");
        Y_DEBUG_ABORT_UNLESS(ts.Step, "Comparing with unset timestamp");
        return static_cast<const TRowVersion&>(*this) < ts;
    }

    TString ToString() const {
        if (Empty()) {
            return "unset";
        }

        return TStringBuilder()
                << "[" << Step << ":" <<  TxId << "]";
    }
};

enum class ETableColumnDefaultKind : ui32 {
    None = 0,
    FromSequence = 1,
    FromLiteral = 2,
};

enum class EAttachChildResult : ui32 {
    Undefined = 0,

    AttachedAsOnlyOne,

    AttachedAsNewerDeleted,
    RejectAsOlderDeleted,

    AttachedAsActual,
    RejectAsDeleted,

    AttachedAsNewerActual,
    RejectAsOlderActual,

    AttachedAsCreatedActual,
    RejectAsInactive,

    AttachedAsOlderUnCreated,
    RejectAsNewerUnCreated
};

using EServerlessComputeResourcesMode = NKikimrSubDomains::EServerlessComputeResourcesMode;

struct TTempDirsState {

    struct TRetryState {
        bool IsScheduled = false;
        NMonotonic::TMonotonic LastRetryAt = TMonotonic::Zero();
        TDuration CurrentDelay = TDuration::Zero();
        ui32 RetryNumber = 0;
    };

    struct TNodeState {
        THashSet<TActorId> Owners;
        TRetryState RetryState;
    };

    THashMap<TActorId, THashSet<TPathId>> TempDirsByOwner; // OwnerActorId -> [ TPathId ]
    THashMap<ui32, TNodeState> NodeStates; // NodeId -> TNodeState
};

struct TTempDirInfo {
    TString WorkingDir;
    TString Name;
    TActorId TempDirOwnerActorId;
};

enum class EShredStatus : ui32 {
    UNSPECIFIED = 0,
    COMPLETED = 1,
    IN_PROGRESS = 2,
    IN_PROGRESS_BSC = 3,
};

}
