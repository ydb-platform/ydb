#include "yql_yt_dq_transform.h"

#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_printer.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/cpp/mapreduce/interface/serialize.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/yson/node/serialize.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/vector.h>
#include <util/generic/guid.h>


namespace NYql {

using namespace NKikimr;

class TYtDqTaskTransform {
    using TPartitionParams = THashMap<TString, NYT::TRichYPath>;

public:
    TYtDqTaskTransform(THashMap<TString, TString> taskParams, TVector<TString> readRanges, const NMiniKQL::IFunctionRegistry& functionRegistry, bool enableReadRanges)
        : TaskParams(std::move(taskParams))
        , ReadRanges(std::move(readRanges))
        , FunctionRegistry(functionRegistry)
        , EnableReadRanges(enableReadRanges)
    {
    }

    NMiniKQL::TCallableVisitFunc operator()(NMiniKQL::TInternName name) {
        bool hasReadRanges = EnableReadRanges && !ReadRanges.empty();
        if ((hasReadRanges || TaskParams.contains("yt")) && (name == "DqYtRead" || name == "DqYtBlockRead")) {
            return [this](NMiniKQL::TCallable& callable, const NMiniKQL::TTypeEnvironment& env) {
                using namespace NMiniKQL;

                TProgramBuilder pgmBuilder(env, FunctionRegistry);

                YQL_ENSURE(callable.GetInputsCount() == 8 || callable.GetInputsCount() == 9, "Expected 8 or 9 arguments.");

                TCallableBuilder callableBuilder(env, callable.GetType()->GetName(), callable.GetType()->GetReturnType(), false);
                callableBuilder.Add(callable.GetInput(0));
                callableBuilder.Add(callable.GetInput(1));
                callableBuilder.Add(callable.GetInput(2));
                callableBuilder.Add(callable.GetInput(3));

                if (callable.GetInputsCount() == 8U)
                    callableBuilder.Add(callable.GetInput(4));
                else {
                    auto params = GetPartitionParams();

                    TVector<TRuntimeNode> newGrpList;
                    TListLiteral* groupList = AS_VALUE(TListLiteral, callable.GetInput(4));
                    for (ui32 grp = 0; grp < groupList->GetItemsCount(); ++grp) {
                        TListLiteral* tableList = AS_VALUE(TListLiteral, groupList->GetItems()[grp]);
                        TVector<TRuntimeNode> newTableList;
                        for (ui32 i = 0; i < tableList->GetItemsCount(); ++i) {
                            TString paramsKey = TStringBuilder() << grp << "/" << i;

                            TTupleLiteral* tableTuple = AS_VALUE(TTupleLiteral, tableList->GetItems()[i]);
                            YQL_ENSURE(tableTuple->GetValuesCount() == 4);

                            NYT::TRichYPath richYPath;
                            NYT::Deserialize(richYPath, NYT::NodeFromYsonString(TString(AS_VALUE(TDataLiteral, tableTuple->GetValue(1))->AsValue().AsStringRef())));

                            if (const auto it = params.find(paramsKey); it != params.end()) {
                                richYPath.MutableRanges() = it->second.GetRanges();
                            } else {
                                richYPath.MutableRanges().ConstructInPlace();
                            }

                            newTableList.push_back(pgmBuilder.NewTuple({
                                tableTuple->GetValue(0),
                                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(NYT::PathToNode(richYPath))),
                                tableTuple->GetValue(2),
                                tableTuple->GetValue(3)
                            }));
                        }
                        newGrpList.push_back(pgmBuilder.NewList(newTableList.front().GetStaticType(), newTableList));
                    }
                    callableBuilder.Add(pgmBuilder.NewList(newGrpList.front().GetStaticType(), newGrpList));
                }
                callableBuilder.Add(callable.GetInput(5));
                callableBuilder.Add(callable.GetInput(6));
                callableBuilder.Add(callable.GetInput(7));
                return TRuntimeNode(callableBuilder.Build(), false);
            };
        }
        if (name == "YtDqRowsWideWrite") {
            YQL_ENSURE(TaskParams.contains("yt.write.tx"), "Expected nested transaction");
            return [this](NMiniKQL::TCallable& callable, const NMiniKQL::TTypeEnvironment& env) -> NMiniKQL::TRuntimeNode {
                using namespace NMiniKQL;

                TProgramBuilder pgmBuilder(env, FunctionRegistry);

                YQL_ENSURE(callable.GetInputsCount() == 6, "Expected six arguments.");

                TCallableBuilder callableBuilder(env, callable.GetType()->GetName(), callable.GetType()->GetReturnType(), false);
                callableBuilder.Add(callable.GetInput(0));
                callableBuilder.Add(callable.GetInput(1));
                callableBuilder.Add(callable.GetInput(2));

                NYT::TRichYPath richYPath;
                NYT::Deserialize(richYPath, NYT::NodeFromYsonString(TString(AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().AsStringRef())));
                richYPath.TransactionId(GetGuid(TaskParams.Value("yt.write.tx", TString())));
                callableBuilder.Add(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(NYT::PathToNode(richYPath))));

                callableBuilder.Add(callable.GetInput(4));
                callableBuilder.Add(callable.GetInput(5));

                return TRuntimeNode(callableBuilder.Build(), false);
            };
        }

        return NMiniKQL::TCallableVisitFunc();
    }

private:
    TPartitionParams GetPartitionParams() const {
        TPartitionParams result;
        if (!EnableReadRanges || ReadRanges.empty()) {
            FillPartitionParams(result, NYT::NodeFromYsonString(TaskParams.Value("yt", TString())).AsMap());
            return result;
        }

        for (const auto& partition : ReadRanges) {
            FillPartitionParams(result, NYT::NodeFromYsonString(partition).AsMap());
        }

        return result;
    }

    static void FillPartitionParams(TPartitionParams& partitionParams, const NYT::TNode::TMapType& partitionMap) {
        for (const auto& [key, value] : partitionMap) {
            NYT::TRichYPath newRichPath;
            NYT::Deserialize(newRichPath, value);

            const auto [it, inserted] = partitionParams.emplace(key, newRichPath);
            if (inserted) {
                continue;
            }

            auto& ranges = it->second.MutableRanges();
            YQL_ENSURE(ranges, "Found intersecting read ranges, current range already cover up all table");

            const auto& newRanges = newRichPath.GetRanges();
            YQL_ENSURE(newRanges, "Found intersecting read ranges, new range cover up all table when another range exists");
            ranges->insert(ranges->end(), newRanges->begin(), newRanges->end());
        }
    }

private:
    const THashMap<TString, TString> TaskParams;
    const TVector<TString> ReadRanges;
    const NMiniKQL::IFunctionRegistry& FunctionRegistry;
    const bool EnableReadRanges;
};

TTaskTransformFactory CreateYtDqTaskTransformFactory(bool enableReadRanges) {
    return [enableReadRanges] (const TTaskTransformArguments& args, const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry) -> NKikimr::NMiniKQL::TCallableVisitFuncProvider {
        return TYtDqTaskTransform(args.TaskParams, args.ReadRanges, *funcRegistry, enableReadRanges);
    };
}

} // NYql
