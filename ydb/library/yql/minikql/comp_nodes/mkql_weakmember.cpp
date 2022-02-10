#include "mkql_fromyson.h"
#include <library/cpp/yson/varint.h>
#include <library/cpp/yson/detail.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_unboxed_value_stream.h>

namespace NKikimr {
namespace NMiniKQL {
namespace {

class TTryWeakMemberFromDictWrapper : public TMutableComputationNode<TTryWeakMemberFromDictWrapper> {
    typedef TMutableComputationNode<TTryWeakMemberFromDictWrapper> TBaseComputation;
public:
    TTryWeakMemberFromDictWrapper(TComputationMutables& mutables, IComputationNode* otherDict, IComputationNode* restDict, NUdf::TDataTypeId schemeType,
            NUdf::TUnboxedValue&& memberName, NUdf::TUnboxedValue&& otherIsStrMemberName)
        : TBaseComputation(mutables)
        , OtherDict(otherDict)
        , RestDict(restDict)
        , SchemeType(NUdf::GetDataSlot(schemeType))
        , MemberName(std::move(memberName))
        , OtherIsStringMemberName(std::move(otherIsStrMemberName))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (const auto& restDict = RestDict->GetValue(ctx)) {
            if (const auto& tryMember = restDict.Lookup(MemberName)) {
                return SimpleValueFromYson(SchemeType, tryMember.AsStringRef());
            }
        }

        if (const auto& otherDict = OtherDict->GetValue(ctx)) {
            if (auto tryMember = otherDict.Lookup(MemberName)) {
                const bool isString = otherDict.Contains(OtherIsStringMemberName);
                if (isString) {
                    if (SchemeType == NUdf::EDataSlot::Yson) {
                        const auto& ref = tryMember.AsStringRef();
                        const auto size = ref.Size();
                        MKQL_ENSURE(size <= std::numeric_limits<i32>::max(), "TryWeakMemberFromDict: Unable to fit string to i32");
                        TUnboxedValueStream stringStream;
                        stringStream.DoWrite(&NYson::NDetail::StringMarker, 1);
                        NYson::WriteVarInt32(&stringStream, size);
                        stringStream.DoWrite(ref.Data(), size);
                        return stringStream.Value();
                    } else if (SchemeType == NUdf::EDataSlot::String) {
                        return tryMember.Release();
                    } else {
                        return {};
                    }
                } else {
                    return SimpleValueFromYson(SchemeType, tryMember.AsStringRef());
                }
            }
        }

        return NUdf::TUnboxedValuePod();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(OtherDict);
        DependsOn(RestDict);
    }

    IComputationNode* const OtherDict;
    IComputationNode* const RestDict;
    const NUdf::EDataSlot SchemeType;
    const NUdf::TUnboxedValue MemberName;
    const NUdf::TUnboxedValue OtherIsStringMemberName;
};

}

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

}
}
