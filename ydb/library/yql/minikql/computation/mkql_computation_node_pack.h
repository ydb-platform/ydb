#pragma once

#include "mkql_computation_node.h"
#include "mkql_computation_node_holders.h"
#include "mkql_optional_usage_mask.h"

#include <ydb/library/yql/public/udf/udf_value.h>

#include <library/cpp/enumbitset/enumbitset.h>

#include <util/stream/output.h>
#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>

#include <utility>

namespace NKikimr {
namespace NMiniKQL {

template<bool Fast>
class TValuePackerImpl {
private:
    enum EProps {
        Begin,
        UseOptionalMask = Begin,
        UseTopLength,
        SingleOptional,
        End
    };
    using TProperties = TEnumBitSet<EProps, EProps::Begin, EProps::End>;
public:
    TValuePackerImpl(bool stable, const TType* type);
    TValuePackerImpl(const TValuePackerImpl& other);

    // Returned buffer is temporary and should be copied before next Pack() call
    TStringBuf Pack(const NUdf::TUnboxedValuePod& value) const;
    NUdf::TUnboxedValue Unpack(TStringBuf buf, const THolderFactory& holderFactory) const;

private:
    void PackImpl(const TType* type, const NUdf::TUnboxedValuePod& value) const;
    NUdf::TUnboxedValue UnpackImpl(const TType* type, TStringBuf& buf, ui32 topLength, const THolderFactory& holderFactory) const;
    static TProperties ScanTypeProperties(const TType* type);
    static bool HasOptionalFields(const TType* type);
    // Returns length and empty single optional flag
    static std::pair<ui32, bool> SkipEmbeddedLength(TStringBuf& buf);

    const bool Stable_;
    const TType* Type_;
    // TODO: real thread safety with external state
    mutable TBuffer Buffer_;
    TProperties Properties_;
    mutable size_t OptionalMaskReserve_;
    mutable NDetails::TOptionalUsageMask OptionalUsageMask_;
    mutable TPlainContainerCache TopStruct_;
    mutable TVector<TVector<std::pair<NUdf::TUnboxedValue, NUdf::TUnboxedValue>>> DictBuffers_;
    mutable TVector<TVector<std::tuple<NUdf::TUnboxedValue, NUdf::TUnboxedValue, NUdf::TUnboxedValue>>> EncodedDictBuffers_;
};

using TValuePacker = TValuePackerImpl<false>;
using TValuePackerFast = TValuePackerImpl<true>;

class TValuePackerBoxed : public TComputationValue<TValuePackerBoxed>, public TValuePacker {
    typedef TComputationValue<TValuePackerBoxed> TBase;
public:
    TValuePackerBoxed(TMemoryUsageInfo* memInfo, bool stable, const TType* type);
    TValuePackerBoxed(TMemoryUsageInfo* memInfo, const TValuePacker& other);
};

}
}
