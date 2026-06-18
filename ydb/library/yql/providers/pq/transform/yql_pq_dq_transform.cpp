#include "yql_pq_dq_transform.h"

#include <ydb/library/yql/providers/pq/common/pq_partitions.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/string/split.h>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;

class TPqDqTaskTransform {
public:
    TPqDqTaskTransform(THashMap<TString, TString> taskParams, TVector<TString> readRanges, const IFunctionRegistry& functionRegistry)
        : TaskParams(std::move(taskParams))
        , ReadRanges(std::move(readRanges))
        , FunctionRegistry(functionRegistry)
    {
    }

    TCallableVisitFunc operator()(TInternName name) {
        if (name == "DqWatermarkGenerator") {
            return [this](TCallable& callable, const TTypeEnvironment& env) {
                TProgramBuilder pgmBuilder(env, FunctionRegistry);

                TCallableBuilder callableBuilder(env, callable.GetType()->GetName(), callable.GetType()->GetReturnType(), false);
                callableBuilder.Add(callable.GetInput(0));
                callableBuilder.Add(callable.GetInput(1));
                callableBuilder.Add(callable.GetInput(2));
                callableBuilder.Add(callable.GetInput(3));
                callableBuilder.Add(callable.GetInput(4));

                if (callable.GetInput(5).GetStaticType()->IsVoid()) {
                    const auto watermarkSettingsNode = AS_VALUE(TListLiteral, callable.GetInput(4));
                    const auto watermarkSettings = TConstArrayRef<TRuntimeNode>(watermarkSettingsNode->GetItems(), watermarkSettingsNode->GetItemsCount());

                    std::vector<TPartitionKey> federatedClusters;
                    for (ui32 i = 0; i + 2 <= watermarkSettings.size(); i += 2) {
                        const auto  name = AS_VALUE(TDataLiteral, watermarkSettings[i + 0])->AsValue().AsStringRef();
                        const auto value = AS_VALUE(TDataLiteral, watermarkSettings[i + 1])->AsValue().AsStringRef();

                        if ("FederatedClusters" == std::string_view{name}) {
                            TVector<TString> federatedClustersStr;
                            Split(value.data(), ",", federatedClustersStr);

                            for (const auto& federatedClusterStr : federatedClustersStr) {
                                TPartitionKey federatedCluster;
                                TStringStream ss(federatedClusterStr);
                                ss >> federatedCluster;
                                federatedClusters.push_back(federatedCluster);
                            }
                        }
                    }

                    auto readTaskParams = ExtractReadTaskParams(TaskParams, ReadRanges);
                    auto partitionKeys = GetPartitionsToRead(readTaskParams, federatedClusters);

                    std::vector<TRuntimeNode> items;
                    for (const auto& partitionKey : partitionKeys) {
                        TStringStream ss;
                        ss << partitionKey;

                        items.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(ss.Str()));
                    }
                    callableBuilder.Add(pgmBuilder.NewList(items.front().GetStaticType(), items));
                } else {
                    callableBuilder.Add(callable.GetInput(5));
                }

                return TRuntimeNode(callableBuilder.Build(), false);
            };
        }

        return TCallableVisitFunc();
    }

private:
    THashMap<TString, TString> TaskParams;
    TVector<TString> ReadRanges;
    const IFunctionRegistry& FunctionRegistry;
};

TTaskTransformFactory CreatePqDqTaskTransformFactory() {
    return [](const TTaskTransformArguments& args, const IFunctionRegistry* funcRegistry) -> TCallableVisitFuncProvider {
        return TPqDqTaskTransform(args.TaskParams, args.ReadRanges, *funcRegistry);
    };
}

} // namespace NYql::NDq
