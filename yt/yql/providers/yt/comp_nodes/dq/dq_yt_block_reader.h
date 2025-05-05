#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <yt/yql/providers/yt/comp_nodes/yql_mkql_file_input_state.h>
#include <yt/yql/providers/yt/codec/yt_codec.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE

namespace NYql::NDqs {
NKikimr::NMiniKQL::IComputationNode* CreateDqYtReadBlockWrapper(
        const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups, NKikimr::NMiniKQL::TType* itemType, const TVector<TString>& tableNames,
        TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables, NKikimr::NMiniKQL::IStatsRegistry* jobStats,
        size_t inflight, size_t timeout, const TVector<ui64>& tableOffsets);
}
