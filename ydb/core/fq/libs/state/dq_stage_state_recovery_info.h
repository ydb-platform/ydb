#pragma once

#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <memory>

namespace NFq {

class THoppingRecoveryState {
    static constexpr ui32 STATE_VERSION = 3;

    YDB_READONLY_DEF(ui64, MinWindowStartIndex);
    YDB_READONLY_DEF(ui32, KeysCount);

public:
    static THoppingRecoveryState Read(const TStringBuf state);

    static TString MakeRecoveryState(const ui64 minWindowStartIndex);
};

struct TStageStateInfo {
    ui32 StageId = 0;
    ui32 RuntimeVersion = 0;
    TVector<const NYql::NDqProto::TDqTask*> Tasks;
    TVector<const NKikimr::NMiniKQL::TCallable*> StatefulOperators;
    TVector<const NKikimr::NMiniKQL::TType*> OutputTypes;
    bool HasWatermarkGenerator = false;
};

class TGraphStateContext {
public:
    TGraphStateContext();

    ~TGraphStateContext();

    const NKikimr::NMiniKQL::TTypeEnvironment& GetTypeEnvironment() const;

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() const;

private:
    NKikimr::NMiniKQL::TScopedAlloc Alloc{__LOCATION__, NKikimr::TAlignedPagePoolCounters(), /* supportsSizedAllocators */ false, /* initiallyAcquired */ false};
    std::unique_ptr<NKikimr::NMiniKQL::TTypeEnvironment> Env;
};

class TGraphStateInfo {
    using TGraphPtr = const NProto::TGraphParams*;
    using TContextPtr = const TGraphStateContext*;

    YDB_READONLY_DEF(TGraphPtr, Graph);
    YDB_READONLY_DEF(TContextPtr, Context);
    YDB_READONLY_DEF(TVector<TStageStateInfo>, Stages);

public:
    TGraphStateInfo(const NProto::TGraphParams& graph, const TGraphStateContext& context);

    bool HasHopping() const;

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() const;
};

struct TStageStateRecoveryInfo {
    enum class EMode {
        Analyze,
        HistoryReplay,
    };

    struct THoppingSettings {
        ui64 HopTimeUs = 0;
        ui64 WindowSizeUs = 0;
    };

    TMaybe<THoppingSettings> Hopping;
    bool HasWatermarkGenerator = false;
    bool HasState = false;

    TStageStateRecoveryInfo() = default;;

    explicit TStageStateRecoveryInfo(const TStageStateInfo& stage, EMode mode = EMode::HistoryReplay);

    // Earliest input needed to produce all hop ends at or after outputStartTimeUs.
    ui64 InputStartForOutput(const ui64 outputStartTimeUs) const;
};

} // namespace NFq
