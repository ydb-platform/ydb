#include "mkql_frombytes.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/public/udf/tz/udf_tz.h>

#include <ydb/library/yql/utils/swap_bytes.h>

#include <ydb/library/binary_json/read.h>

#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

using NYql::SwapBytes;

template <bool IsOptional>
class TFromBytesWrapper : public TMutableComputationNode<TFromBytesWrapper<IsOptional>> {
    typedef TMutableComputationNode<TFromBytesWrapper<IsOptional>> TBaseComputation;
public:
    TFromBytesWrapper(TComputationMutables& mutables, IComputationNode* data, NUdf::TDataTypeId schemeType)
        : TBaseComputation(mutables)
        , Data(data)
        , SchemeType(NUdf::GetDataSlot(schemeType))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto data = Data->GetValue(ctx);
        if (IsOptional && !data) {
            return NUdf::TUnboxedValuePod();
        }

        switch (SchemeType) {
        case NUdf::EDataSlot::TzDate: {
            if (!data) {
                return NUdf::TUnboxedValuePod();
            }

            const auto& ref = data.AsStringRef();
            if (ref.Size() != 4) {
                return NUdf::TUnboxedValuePod();
            }

            auto tzId = SwapBytes(ReadUnaligned<ui16>(ref.Data() + ref.Size() - sizeof(ui16)));
            auto value = SwapBytes(data.Get<ui16>());
            if (value < NUdf::MAX_DATE && tzId < NUdf::GetTimezones().size()) {
                auto ret = NUdf::TUnboxedValuePod(value);
                ret.SetTimezoneId(tzId);
                return ret;
            }

            return NUdf::TUnboxedValuePod();
        }

        case NUdf::EDataSlot::TzDatetime: {
            if (!data) {
                return NUdf::TUnboxedValuePod();
            }

            const auto& ref = data.AsStringRef();
            if (ref.Size() != 6) {
                return NUdf::TUnboxedValuePod();
            }

            auto tzId = SwapBytes(ReadUnaligned<ui16>(ref.Data() + ref.Size() - sizeof(ui16)));
            auto value = SwapBytes(data.Get<ui32>());
            if (value < NUdf::MAX_DATETIME && tzId < NUdf::GetTimezones().size()) {
                auto ret = NUdf::TUnboxedValuePod(value);
                ret.SetTimezoneId(tzId);
                return ret;
            }

            return NUdf::TUnboxedValuePod();
        }

        case NUdf::EDataSlot::TzTimestamp: {
            if (!data) {
                return NUdf::TUnboxedValuePod();
            }

            const auto& ref = data.AsStringRef();
            if (ref.Size() != 10) {
                return NUdf::TUnboxedValuePod();
            }

            auto tzId = SwapBytes(ReadUnaligned<ui16>(ref.Data() + ref.Size() - sizeof(ui16)));
            auto value = SwapBytes(data.Get<ui64>());
            if (value < NUdf::MAX_TIMESTAMP && tzId < NUdf::GetTimezones().size()) {
                auto ret = NUdf::TUnboxedValuePod(value);
                ret.SetTimezoneId(tzId);
                return ret;
            }

            return NUdf::TUnboxedValuePod();
        }

        case NUdf::EDataSlot::JsonDocument: {
            if (!NBinaryJson::IsValidBinaryJson(TStringBuf(data.AsStringRef()))) {
                return NUdf::TUnboxedValuePod();
            }
            return data.Release();
        }

        default:
            if (IsValidValue(SchemeType, data)) {
                return data.Release();
            } else {
                return NUdf::TUnboxedValuePod();
            }
        }
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Data);
    }

    IComputationNode* const Data;
    const NUdf::EDataSlot SchemeType;
};

}

IComputationNode* WrapFromBytes(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(0), isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected String");

    const auto schemeTypeData = AS_VALUE(TDataLiteral, callable.GetInput(1));
    const auto schemeType = schemeTypeData->AsValue().Get<ui32>();

    const auto data = LocateNode(ctx.NodeLocator, callable, 0);
    if (isOptional) {
        return new TFromBytesWrapper<true>(ctx.Mutables, data, static_cast<NUdf::TDataTypeId>(schemeType));
    } else {
        return new TFromBytesWrapper<false>(ctx.Mutables, data, static_cast<NUdf::TDataTypeId>(schemeType));
    }
}

}
}
