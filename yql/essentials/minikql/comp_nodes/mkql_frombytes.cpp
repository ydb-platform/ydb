#include "mkql_frombytes.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <yql/essentials/utils/swap_bytes.h>

#include <yql/essentials/types/binary_json/read.h>

#include <library/cpp/type_info/tz/tz.h>

#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

using NYql::SwapBytes;

template <typename T>
NUdf::TUnboxedValuePod MakeTzFromBytes(const NUdf::TUnboxedValue& data) {
    static_assert(NUdf::TDataType<T>::Features & NUdf::EDataTypeFeatures::TzDateType);
    using TLayout = NUdf::TDataType<T>::TLayout;

    const auto& ref = data.AsStringRef();
    if (ref.Size() != sizeof(TLayout) + sizeof(ui16)) {
        return NUdf::TUnboxedValuePod();
    }

    TLayout value = SwapBytes(ReadUnaligned<TLayout>(ref.Data()));
    auto tzId = SwapBytes(ReadUnaligned<ui16>(ref.Data() + ref.Size() - sizeof(ui16)));
    if (NUdf::IsValidLayoutValue<T>(value) && IsValidTimezoneId(tzId)) {
        auto ret = NUdf::TUnboxedValuePod(value);
        ret.SetTimezoneId(tzId);
        return ret;
    }

    return NUdf::TUnboxedValuePod();
}

template <bool IsOptional>
class TFromBytesWrapper: public TMutableComputationNode<TFromBytesWrapper<IsOptional>> {
    typedef TMutableComputationNode<TFromBytesWrapper<IsOptional>> TBaseComputation;

public:
    TFromBytesWrapper(TComputationMutables& mutables, IComputationNode* data, NUdf::TDataTypeId schemeType, ui32 param1, ui32 param2)
        : TBaseComputation(mutables)
        , Data(data)
        , SchemeType(NUdf::GetDataSlot(schemeType))
        , Param1(param1)
        , Param2(param2)
    {
        if (SchemeType == NUdf::EDataSlot::Decimal) {
            DecimalBound = NYql::NDecimal::TInt128(1);
            NYql::NDecimal::TInt128 ten(10U);
            for (ui32 i = 0; i < Param1; ++i) {
                DecimalBound *= ten;
            }

            NegDecimalBound = -DecimalBound;
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto data = Data->GetValue(ctx);
        if (IsOptional && !data) {
            return NUdf::TUnboxedValuePod();
        }

        switch (SchemeType) {
            case NUdf::EDataSlot::TzDate:
                return MakeTzFromBytes<NUdf::TTzDate>(data);
            case NUdf::EDataSlot::TzDatetime:
                return MakeTzFromBytes<NUdf::TTzDatetime>(data);
            case NUdf::EDataSlot::TzTimestamp:
                return MakeTzFromBytes<NUdf::TTzTimestamp>(data);
            case NUdf::EDataSlot::TzDate32:
                return MakeTzFromBytes<NUdf::TTzDate32>(data);
            case NUdf::EDataSlot::TzDatetime64:
                return MakeTzFromBytes<NUdf::TTzDatetime64>(data);
            case NUdf::EDataSlot::TzTimestamp64:
                return MakeTzFromBytes<NUdf::TTzTimestamp64>(data);

            case NUdf::EDataSlot::JsonDocument: {
                if (!NBinaryJson::IsValidBinaryJson(TStringBuf(data.AsStringRef()))) {
                    return NUdf::TUnboxedValuePod();
                }
                return data.Release();
            }

            case NUdf::EDataSlot::Decimal: {
                const auto& ref = data.AsStringRef();
                if (ref.Size() != 15) {
                    return NUdf::TUnboxedValuePod();
                }

                NYql::NDecimal::TInt128 v = 0;
                ui8* p = (ui8*)&v;
                memcpy(p, ref.Data(), 15);
                p[0xF] = (p[0xE] & 0x80) ? 0xFF : 0x00;
                if (NYql::NDecimal::IsError(v)) {
                    return NUdf::TUnboxedValuePod();
                }

                if (!NYql::NDecimal::IsNormal(v)) {
                    return NUdf::TUnboxedValuePod(v);
                }

                if (v >= DecimalBound) {
                    return NUdf::TUnboxedValuePod(NYql::NDecimal::Inf());
                }

                if (v <= NegDecimalBound) {
                    return NUdf::TUnboxedValuePod(-NYql::NDecimal::Inf());
                }

                return NUdf::TUnboxedValuePod(v);
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
    const ui32 Param1;
    const ui32 Param2;
    NYql::NDecimal::TInt128 DecimalBound, NegDecimalBound;
};

} // namespace

IComputationNode* WrapFromBytes(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2 || callable.GetInputsCount() == 4, "Expected 2 or 4 args");

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(0), isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected String");

    const auto schemeTypeData = AS_VALUE(TDataLiteral, callable.GetInput(1));
    const auto schemeType = schemeTypeData->AsValue().Get<ui32>();
    ui32 param1 = 0;
    ui32 param2 = 0;
    if (schemeType == NUdf::TDataType<NUdf::TDecimal>::Id) {
        MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");
        const auto param1Data = AS_VALUE(TDataLiteral, callable.GetInput(2));
        param1 = param1Data->AsValue().Get<ui32>();
        const auto param2Data = AS_VALUE(TDataLiteral, callable.GetInput(3));
        param2 = param2Data->AsValue().Get<ui32>();
    } else {
        MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    }

    const auto data = LocateNode(ctx.NodeLocator, callable, 0);
    if (isOptional) {
        return new TFromBytesWrapper<true>(ctx.Mutables, data, static_cast<NUdf::TDataTypeId>(schemeType), param1, param2);
    } else {
        return new TFromBytesWrapper<false>(ctx.Mutables, data, static_cast<NUdf::TDataTypeId>(schemeType), param1, param2);
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
