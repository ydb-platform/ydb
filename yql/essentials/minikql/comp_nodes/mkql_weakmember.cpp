#include "mkql_fromyson.h"
#include <library/cpp/yson/varint.h>
#include <library/cpp/yson/detail.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_unboxed_value_stream.h>

namespace NKikimr::NMiniKQL {
namespace {

class TTryWeakMemberFromDictWrapper: public TMutableComputationNode<TTryWeakMemberFromDictWrapper> {
    using TBaseComputation = TMutableComputationNode<TTryWeakMemberFromDictWrapper>;

public:
    TTryWeakMemberFromDictWrapper(TComputationMutables& mutables, IComputationNode* otherDict, IComputationNode* restDict, NUdf::TDataTypeId schemeType,
                                  NUdf::TUnboxedValue&& memberName, NUdf::TUnboxedValue&& otherIsStrMemberName)
        : TBaseComputation(mutables)
        , OtherDict_(otherDict)
        , RestDict_(restDict)
        , SchemeType_(NUdf::GetDataSlot(schemeType))
        , MemberName_(std::move(memberName))
        , OtherIsStringMemberName_(std::move(otherIsStrMemberName))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto result = DoCalculateImpl(ctx);
        return result.Release();
    }

    NUdf::TUnboxedValue DoCalculateImpl(TComputationContext& ctx) const {
        if (const auto& restDict = RestDict_->GetValue(ctx)) {
            if (const auto& tryMember = restDict.Lookup(MemberName_)) {
                return SimpleValueFromYson(SchemeType_, tryMember.AsStringRef());
            }
        }

        if (const auto& otherDict = OtherDict_->GetValue(ctx)) {
            if (auto tryMember = otherDict.Lookup(MemberName_)) {
                const bool isString = otherDict.Contains(OtherIsStringMemberName_);
                if (isString) {
                    if (SchemeType_ == NUdf::EDataSlot::Yson) {
                        const auto& ref = tryMember.AsStringRef();
                        const auto size = ref.Size();
                        MKQL_ENSURE(size <= std::numeric_limits<i32>::max(), "TryWeakMemberFromDict: Unable to fit string to i32");
                        TUnboxedValueStream stringStream;
                        stringStream.DoWrite(&NYson::NDetail::StringMarker, 1);
                        NYson::WriteVarInt32(&stringStream, size);
                        stringStream.DoWrite(ref.Data(), size);
                        return stringStream.Value();
                    } else if (SchemeType_ == NUdf::EDataSlot::String) {
                        return tryMember;
                    } else {
                        return {};
                    }
                } else {
                    return SimpleValueFromYson(SchemeType_, tryMember.AsStringRef());
                }
            }
        }

        return NUdf::TUnboxedValuePod();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(OtherDict_);
        DependsOn(RestDict_);
    }

    IComputationNode* const OtherDict_;
    IComputationNode* const RestDict_;
    const NUdf::EDataSlot SchemeType_;
    const NUdf::TUnboxedValue MemberName_;
    const NUdf::TUnboxedValue OtherIsStringMemberName_;
};

} // namespace

IComputationNode* WrapTryWeakMemberFromDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");

    auto otherType = AS_TYPE(TOptionalType, callable.GetInput(0));
    auto otherDictType = AS_TYPE(TDictType, otherType->GetItemType());
    auto otherDictKeyType = AS_TYPE(TDataType, otherDictType->GetKeyType());
    auto otherDictPayloadType = AS_TYPE(TDataType, otherDictType->GetPayloadType());
    MKQL_ENSURE(otherDictKeyType->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected String");
    MKQL_ENSURE(otherDictPayloadType->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected String");

    auto restType = AS_TYPE(TOptionalType, callable.GetInput(1));
    auto restDictType = AS_TYPE(TDictType, restType->GetItemType());
    auto restDictKeyType = AS_TYPE(TDataType, restDictType->GetKeyType());
    auto restDictPayloadType = AS_TYPE(TDataType, restDictType->GetPayloadType());
    MKQL_ENSURE(restDictKeyType->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected String");
    MKQL_ENSURE(restDictPayloadType->GetSchemeType() == NUdf::TDataType<NUdf::TYson>::Id, "Expected String");

    TDataLiteral* schemeTypeData = AS_VALUE(TDataLiteral, callable.GetInput(2));
    auto schemeType = schemeTypeData->AsValue().Get<ui32>();

    auto memberNameValue = AS_VALUE(TDataLiteral, callable.GetInput(3));
    const TString memberName(memberNameValue->AsValue().AsStringRef());

    auto otherDict = LocateNode(ctx.NodeLocator, callable, 0);
    auto restDict = LocateNode(ctx.NodeLocator, callable, 1);
    auto memberNameStr = MakeString(memberName);
    auto otherIsStringMemberNameStr = MakeString("_yql_" + memberName);
    return new TTryWeakMemberFromDictWrapper(ctx.Mutables, otherDict, restDict, static_cast<NUdf::TDataTypeId>(schemeType),
                                             std::move(memberNameStr), std::move(otherIsStringMemberNameStr));
}

} // namespace NKikimr::NMiniKQL
