#include "yql_mkql_table_content.h"
#include "yql_mkql_file_input_state.h"
#include "yql_mkql_file_list.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/defs.h>

#include <ydb/library/yql/public/udf/udf_value.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/size_literals.h>

#include <type_traits>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

class TYtTableContentWrapper : public TMutableComputationNode<TYtTableContentWrapper> {
    typedef TMutableComputationNode<TYtTableContentWrapper> TBaseComputation;
public:
    TYtTableContentWrapper(TComputationMutables& mutables, NCommon::TCodecContext& codecCtx,
        TVector<TString>&& files, const TString& inputSpec, TType* listType, bool useSkiff, bool decompress, const TString& optLLVM,
        std::optional<ui64> length)
        : TBaseComputation(mutables)
        , Files_(std::move(files))
        , Decompress_(decompress)
        , Length_(std::move(length))
    {
        if (useSkiff) {
            Spec_.SetUseSkiff(optLLVM);
        }
        Spec_.Init(codecCtx, inputSpec, {}, {}, AS_TYPE(TListType, listType)->GetItemType(), {}, TString());
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TFileListValue>(Spec_, ctx.HolderFactory, Files_, Decompress_, 4, 1_MB, Length_);
    }

private:
    void RegisterDependencies() const final {}

    TMkqlIOSpecs Spec_;
    TVector<TString> Files_;
    const bool Decompress_;
    const std::optional<ui64> Length_;
};

IComputationNode* WrapYtTableContent(NCommon::TCodecContext& codecCtx,
    TComputationMutables& mutables, TCallable& callable, const TString& optLLVM, TStringBuf pathPrefix)
{
    MKQL_ENSURE(callable.GetInputsCount() == 6, "Expected 6 arguments");
    TString uniqueId(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
    const ui32 tablesCount = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    TString inputSpec(AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().AsStringRef());
    const bool useSkiff = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<bool>();
    const bool decompress = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().Get<bool>();

    std::optional<ui64> length;
    TTupleLiteral* lengthTuple = AS_VALUE(TTupleLiteral, callable.GetInput(5));
    if (lengthTuple->GetValuesCount() > 0) {
        YQL_ENSURE(lengthTuple->GetValuesCount() == 1, "Expect 1 element in the length tuple");
        length = AS_VALUE(TDataLiteral, lengthTuple->GetValue(0))->AsValue().Get<ui64>();
    }

    TVector<TString> files;
    for (ui32 index = 0; index < tablesCount; ++index) {
        files.push_back(TStringBuilder() << pathPrefix << uniqueId << '_' << index);
    }

    return new TYtTableContentWrapper(mutables, codecCtx, std::move(files), inputSpec,
        callable.GetType()->GetReturnType(), useSkiff, decompress, optLLVM, length);
}

} // NYql
