#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>

namespace NFq {

class THoppingRecoveryState {
    static constexpr ui32 STATE_VERSION = 3;

    YDB_READONLY_DEF(ui64, MinWindowStartIndex);
    YDB_READONLY_DEF(ui32, KeysCount);

public:
    static THoppingRecoveryState Read(const TStringBuf state);

    static TString MakeRecoveryState(const ui64 minWindowStartIndex);
};

struct TStageStateRecoveryContext {
    NKikimr::NMiniKQL::TScopedAlloc Alloc{__LOCATION__};
    NKikimr::NMiniKQL::TTypeEnvironment Env{Alloc};
};

struct TStageStateRecoveryInfo {
    struct THoppingSettings {
        ui64 HopTimeUs = 0;
        ui64 WindowSizeUs = 0;
    };

    TMaybe<THoppingSettings> Hopping;
    bool HasWatermarkGenerator = false;

    TStageStateRecoveryInfo() = default;

    TStageStateRecoveryInfo(const ui32 runtimeVersion, const TString& program, TStageStateRecoveryContext& context);

    // Earliest input needed to produce all hop ends at or after outputStartTimeUs.
    ui64 InputStartForOutput(const ui64 outputStartTimeUs) const;
};

} // namespace NFq
