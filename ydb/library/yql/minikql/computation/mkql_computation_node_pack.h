#pragma once

#include "mkql_computation_node.h"
#include "mkql_computation_node_holders.h"
#include "mkql_optional_usage_mask.h"
#include "mkql_block_transport.h"
#include "mkql_block_reader.h"

#include <ydb/library/yql/minikql/mkql_buffer.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <library/cpp/enumbitset/enumbitset.h>
#include <ydb/library/actors/util/rope.h>

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

    explicit TValuePackerTransport(const TType* type, arrow::MemoryPool* pool = nullptr);
    // for compatibility with TValuePackerGeneric - stable packing is not supported
    TValuePackerTransport(bool stable, const TType* type, arrow::MemoryPool* ppol = nullptr);

    // AddItem()/UnpackBatch() will perform incremental packing - type T is processed as list item type. Will produce List<T> layout
    TSelf& AddItem(const NUdf::TUnboxedValuePod& value);
    TSelf& AddWideItem(const NUdf::TUnboxedValuePod* values, ui32 count);
    size_t PackedSizeEstimate() const {
        return IsBlock_ ? BlockBuffer_.size() : (Buffer_ ? (Buffer_->Size() + Buffer_->ReservedHeaderSize()) : 0);
    }

    bool IsEmpty() const {
        return !ItemCount_;
    }

    void Clear();
    TRope Finish();

    // Pack()/Unpack() will pack/unpack single value of type T
    TRope Pack(const NUdf::TUnboxedValuePod& value) const;
    NUdf::TUnboxedValue Unpack(TRope&& buf, const THolderFactory& holderFactory) const;
    void UnpackBatch(TRope&& buf, const THolderFactory& holderFactory, TUnboxedValueBatch& result) const;
private:
    void BuildMeta(TPagedBuffer::TPtr& buffer, bool addItemCount) const;
    void StartPack();

    void InitBlocks();
    TSelf& AddWideItemBlocks(const NUdf::TUnboxedValuePod* values, ui32 count);
    TRope FinishBlocks();
    void UnpackBatchBlocks(TRope&& buf, const THolderFactory& holderFactory, TUnboxedValueBatch& result) const;

    const TType* const Type_;
    ui64 ItemCount_ = 0;
    TPagedBuffer::TPtr Buffer_;
    mutable NDetails::TPackerState State_;
    mutable NDetails::TPackerState IncrementalState_;

    arrow::MemoryPool& ArrowPool_;
    bool IsBlock_ = false;
    bool IsLegacyBlock_ = false;
    ui32 BlockLenIndex_ = 0;

    TVector<std::unique_ptr<IBlockSerializer>> BlockSerializers_;
    TVector<std::unique_ptr<IBlockReader>> BlockReaders_;
    TVector<std::shared_ptr<arrow::ArrayData>> ConvertedScalars_;
    TRope BlockBuffer_;

    TVector<std::unique_ptr<IBlockDeserializer>> BlockDeserializers_;
};

using TValuePacker = TValuePackerGeneric<false>;

class TValuePackerBoxed : public TComputationValue<TValuePackerBoxed>, public TValuePacker {
    typedef TComputationValue<TValuePackerBoxed> TBase;
public:
    TValuePackerBoxed(TMemoryUsageInfo* memInfo, bool stable, const TType* type);
};

}
}
