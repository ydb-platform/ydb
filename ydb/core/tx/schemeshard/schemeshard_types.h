#pragma once

#include "schemeshard_identificators.h"

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <util/generic/fwd.h>

namespace NKikimr::NSchemeShard {

struct TSchemeLimits {
    // Used for backward compatability in case of old databases without explicit limits
    static constexpr ui64 MaxPathsCompat = 200*1000;
    static constexpr ui64 MaxObjectsInBackup = 10*1000;

    // path
    ui64 MaxDepth = 32;
    ui64 MaxPaths = MaxObjectsInBackup;
    ui64 MaxChildrenInDir = 100*1000;
    ui64 MaxAclBytesSize = 10 << 10;
    ui64 MaxPathElementLength = 255;
    TString ExtraPathSymbolsAllowed = "!\"#$%&'()*+,-.:;<=>?@[\\]^_`{|}~";

    // table
    ui64 MaxTableColumns = 200;
    ui64 MaxTableColumnNameLength = 255;
    ui64 MaxTableKeyColumns = 20;
    ui64 MaxTableIndices = 20;
    ui64 MaxTableCdcStreams = 5;
    ui64 MaxShards = 200*1000; // In each database
    ui64 MaxShardsInPath = 35*1000; // In each path in database
    ui64 MaxConsistentCopyTargets = MaxObjectsInBackup;

    // pq group
    ui64 MaxPQPartitions = 1000000;

    // export & import
    ui64 MaxExports = 10;
    ui64 MaxImports = 10;

    static TSchemeLimits FromProto(const NKikimrScheme::TSchemeLimits& proto);
    NKikimrScheme::TSchemeLimits AsProto() const;
};

using ETabletType = TTabletTypes;

struct TGlobalTimestamp {
    TStepId Step = InvalidStepId;
    TTxId TxId = InvalidTxId;

    TGlobalTimestamp(TStepId step, TTxId txId)
        : Step(step)
        , TxId(txId)
    {}

    bool Empty() const {
        return !bool(Step);
    }

    explicit operator bool () const {
        return !Empty();
    }

    bool operator < (const TGlobalTimestamp& ts) const {
        Y_VERIFY_DEBUG(Step, "Comparing with unset timestamp");
        Y_VERIFY_DEBUG(ts.Step, "Comparing with unset timestamp");
        return Step < ts.Step || Step == ts.Step && TxId < ts.TxId;
    }

    bool operator == (const TGlobalTimestamp& ts) const {
        return Step == ts.Step && TxId == ts.TxId;
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

}
