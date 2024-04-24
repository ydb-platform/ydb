#include "yql_yt_dq_transform.h"

#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <yt/cpp/mapreduce/interface/serialize.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/yson/node/serialize.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/vector.h>
#include <util/generic/guid.h>


namespace NYql {

using namespace NKikimr;

class TYtDqTaskTransform {
public:
    TYtDqTaskTransform(THashMap<TString, TString> taskParams, const NMiniKQL::IFunctionRegistry& functionRegistry)
        : TaskParams(std::move(taskParams))
        , FunctionRegistry(functionRegistry)
    {
    }

    NMiniKQL::TCallableVisitFunc operator()(NMiniKQL::TInternName name) {
        if (TaskParams.contains("yt") && (name == "DqYtRead" || name == "DqYtBlockRead")) {
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
                    auto params = NYT::NodeFromYsonString(TaskParams.Value("yt", TString())).AsMap();

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

                            if (params.contains(paramsKey)) {
                                NYT::TRichYPath ranges;
                                NYT::Deserialize(ranges, params[paramsKey]);
                                richYPath.MutableRanges() = ranges.GetRanges();
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
    THashMap<TString, TString> TaskParams;
    const NMiniKQL::IFunctionRegistry& FunctionRegistry;
};

TTaskTransformFactory CreateYtDqTaskTransformFactory() {
    return [] (const THashMap<TString, TString>& taskParams, const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry) -> NKikimr::NMiniKQL::TCallableVisitFuncProvider {
        return TYtDqTaskTransform(taskParams, *funcRegistry);
    };
}

} // NYql
