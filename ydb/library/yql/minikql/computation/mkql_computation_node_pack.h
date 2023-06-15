#pragma once

#include "mkql_computation_node.h"
#include "mkql_computation_node_holders.h"
#include "mkql_optional_usage_mask.h"

#include <ydb/library/yql/minikql/mkql_buffer.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <library/cpp/enumbitset/enumbitset.h>
#include <library/cpp/actors/util/rope.h>

#include <util/stream/output.h>
#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>

#include <utility>

namespace NKikimr {
namespace NMiniKQL {

namespace NDetails {

enum EPackProps {
  Begin,
  UseOptionalMask = Begin,
  UseTopLength,
  SingleOptional,
  End
};

using TPackProperties = TEnumBitSet<EPackProps, EPackProps::Begin, EPackProps::End>;

struct TPackerState {
    explicit TPackerState(TPackProperties&& properties)
        : Properties(std::move(properties))
        , OptionalMaskReserve(Properties.Test(EPackProps::UseOptionalMask) ? 1 : 0)
    {
    }

    const TPackProperties Properties;

    TPlainContainerCache TopStruct;
    TVector<TVector<std::pair<NUdf::TUnboxedValue, NUdf::TUnboxedValue>>> DictBuffers;
    TVector<TVector<std::tuple<NUdf::TUnboxedValue, NUdf::TUnboxedValue, NUdf::TUnboxedValue>>> EncodedDictBuffers;
    size_t OptionalMaskReserve;
    NDetails::TOptionalUsageMask OptionalUsageMask;
};

} // namespace NDetails

template<bool Fast>
class TValuePackerGeneric {
public:
    using TSelf = TValuePackerGeneric<Fast>;

    TValuePackerGeneric(bool stable, const TType *type);

    // reference is valid till the next call to Pack()
    TStringBuf Pack(const NUdf::TUnboxedValuePod& value) const;
    NUdf::TUnboxedValue Unpack(TStringBuf buf, const THolderFactory& holderFactory) const;

private:
    const bool Stable_;
    const TType* const Type_;
    // TODO: real thread safety with external state
    mutable TBuffer Buffer_;
    mutable NDetails::TPackerState State_;
};

template<bool Fast>
class TValuePackerTransport {
public:
    using TSelf = TValuePackerTransport<Fast>;

    explicit TValuePackerTransport(const TType* type);
    // for compatibility with TValuePackerGeneric - stable packing is not supported
    TValuePackerTransport(bool stable, const TType* type);

    // AddItem()/UnpackBatch() will perform incremental packing - type T is processed as list item type. Will produce List<T> layout
    TSelf& AddItem(const NUdf::TUnboxedValuePod& value);
    TSelf& AddWideItem(const NUdf::TUnboxedValuePod* values, ui32 count);
    size_t PackedSizeEstimate() const {
        return Buffer_ ? (Buffer_->Size() + Buffer_->ReservedHeaderSize()) : 0;
    }
    void Clear();
    TPagedBuffer::TPtr Finish();

    // Pack()/Unpack() will pack/unpack single value of type T
    TPagedBuffer::TPtr Pack(const NUdf::TUnboxedValuePod& value) const;
    NUdf::TUnboxedValue Unpack(TStringBuf buf, const THolderFactory& holderFactory) const;
    NUdf::TUnboxedValue Unpack(TRope&& buf, const THolderFactory& holderFactory) const;
    void UnpackBatch(TStringBuf buf, const THolderFactory& holderFactory, TUnboxedValueBatch& result) const;
    void UnpackBatch(TRope&& buf, const THolderFactory& holderFactory, TUnboxedValueBatch& result) const;
private:
    void BuildMeta(TPagedBuffer::TPtr& buffer, bool addItemCount) const;
    void StartPack();

    const TType* const Type_;
    ui64 ItemCount_ = 0;
    TPagedBuffer::TPtr Buffer_;
    mutable NDetails::TPackerState State_;
    mutable NDetails::TPackerState IncrementalState_;
};

using TValuePacker = TValuePackerGeneric<false>;

class TValuePackerBoxed : public TComputationValue<TValuePackerBoxed>, public TValuePacker {
    typedef TComputationValue<TValuePackerBoxed> TBase;
public:
    TValuePackerBoxed(TMemoryUsageInfo* memInfo, bool stable, const TType* type);
};

}
}
