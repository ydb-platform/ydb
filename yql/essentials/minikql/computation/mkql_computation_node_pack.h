#pragma once

#include "mkql_computation_node.h"
#include "mkql_computation_node_holders.h"
#include "mkql_optional_usage_mask.h"
#include "mkql_block_transport.h"
#include "mkql_block_reader.h"

#include <yql/essentials/minikql/mkql_buffer.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <library/cpp/enumbitset/enumbitset.h>
#include <yql/essentials/utils/chunked_buffer.h>

#include <util/stream/output.h>
#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>

#include <utility>

namespace NKikimr::NMiniKQL {

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

template <bool Fast>
class TValuePackerGeneric {
public:
    using TSelf = TValuePackerGeneric<Fast>;

    TValuePackerGeneric(bool stable, const TType* type);

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

// This version specify exactly how data will be packed and unpacked.
enum class EValuePackerVersion {
    V0 = 0, // Initial version.
    V1 = 1, // Fixed Block type |child_data| serialization/deserialization.
            // Remove the invariant of equality of offsets for all recursive children.
};

template <bool Fast>
class TValuePackerTransport {
public:
    using TSelf = TValuePackerTransport<Fast>;

    explicit TValuePackerTransport(const TType* type, EValuePackerVersion valuePackerVersion,
                                   TMaybe<size_t> bufferPageAllocSize = Nothing(),
                                   arrow::MemoryPool* pool = nullptr, TMaybe<ui8> minFillPercentage = Nothing());

    // Deprecated: For YDB sync only.
    explicit TValuePackerTransport(const TType* type,
                                   TMaybe<size_t> bufferPageAllocSize = Nothing(),
                                   arrow::MemoryPool* pool = nullptr, TMaybe<ui8> minFillPercentage = Nothing());

    // for compatibility with TValuePackerGeneric - stable packing is not supported
    TValuePackerTransport(bool stable, const TType* type, EValuePackerVersion valuePackerVersion,
                          TMaybe<size_t> bufferPageAllocSize = Nothing(),
                          arrow::MemoryPool* ppol = nullptr, TMaybe<ui8> minFillPercentage = Nothing());

    // Deprecated: For YDB sync only.
    TValuePackerTransport(bool stable, const TType* type,
                          TMaybe<size_t> bufferPageAllocSize = Nothing(),
                          arrow::MemoryPool* ppol = nullptr, TMaybe<ui8> minFillPercentage = Nothing());

    // AddItem()/UnpackBatch() will perform incremental packing - type T is processed as list item type. Will produce List<T> layout
    TSelf& AddItem(const NUdf::TUnboxedValuePod& value);
    TSelf& AddWideItem(const NUdf::TUnboxedValuePod* values, ui32 count);
    size_t PackedSizeEstimate() const {
        return IsBlock_ ? BlockBuffer_.Size() : (Buffer_ ? (Buffer_->Size() + Buffer_->ReservedHeaderSize()) : 0);
    }

    bool IsEmpty() const {
        return !ItemCount_;
    }

    bool IsBlock() const {
        return IsBlock_;
    }

    void Clear();
    NYql::TChunkedBuffer Finish();

    void SetMinFillPercentage(TMaybe<ui8> minFillPercentage);

    // Pack()/Unpack() will pack/unpack single value of type T
    NYql::TChunkedBuffer Pack(const NUdf::TUnboxedValuePod& value) const;
    NUdf::TUnboxedValue Unpack(NYql::TChunkedBuffer&& buf, const THolderFactory& holderFactory) const;
    void UnpackBatch(NYql::TChunkedBuffer&& buf, const THolderFactory& holderFactory, TUnboxedValueBatch& result) const;

private:
    void BuildMeta(TPagedBuffer::TPtr& buffer, bool addItemCount) const;
    void StartPack();

    void InitBlocks(TMaybe<ui8> minFillPercentage);
    TSelf& AddWideItemBlocks(const NUdf::TUnboxedValuePod* values, ui32 count);
    NYql::TChunkedBuffer FinishBlocks();
    void UnpackBatchBlocks(NYql::TChunkedBuffer&& buf, const THolderFactory& holderFactory, TUnboxedValueBatch& result) const;

    const TType* const Type_;
    ui64 ItemCount_ = 0;
    TPagedBuffer::TPtr Buffer_;
    const size_t BufferPageAllocSize_;
    mutable NDetails::TPackerState State_;
    mutable NDetails::TPackerState IncrementalState_;

    arrow::MemoryPool& ArrowPool_;
    bool IsBlock_ = false;
    bool IsLegacyBlock_ = false;
    ui32 BlockLenIndex_ = 0;
    EValuePackerVersion ValuePackerVersion_;
    TVector<std::unique_ptr<IBlockSerializer>> BlockSerializers_;
    TVector<std::unique_ptr<IBlockReader>> BlockReaders_;
    TVector<std::shared_ptr<arrow::ArrayData>> ConvertedScalars_;
    NYql::TChunkedBuffer BlockBuffer_;

    TVector<std::unique_ptr<IBlockDeserializer>> BlockDeserializers_;
};

using TValuePacker = TValuePackerGeneric<false>;

class TValuePackerBoxed: public TComputationValue<TValuePackerBoxed>, public TValuePacker {
    typedef TComputationValue<TValuePackerBoxed> TBase;

public:
    TValuePackerBoxed(TMemoryUsageInfo* memInfo, bool stable, const TType* type);
};

bool IsLegacyStructBlock(const TType* type, ui32& blockLengthIndex, TVector<const TBlockType*>& items);
bool IsMultiBlock(const TType* type, ui32& blockLengthIndex, TVector<const TBlockType*>& items);

} // namespace NKikimr::NMiniKQL
