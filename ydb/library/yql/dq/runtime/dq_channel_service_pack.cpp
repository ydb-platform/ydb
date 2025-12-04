#include <queue>
#include <mutex>

#include "dq_arrow_helpers.h"
#include "dq_channel_service_impl.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>
#include <ydb/library/yql/dq/runtime/dq_packer_version_helper.h>

namespace NYql::NDq {

class TPackedSerializer : public TOutputSerializer {
public:
    TPackedSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize)
        : TOutputSerializer(buffer, rowType, transportVersion, packerVersion, bufferPageAllocSize)
        , Packer(rowType, packerVersion, bufferPageAllocSize) {
    }

    NKikimr::NMiniKQL::TValuePackerTransport<true> Packer;
};

class TBuferredSerializer : public TPackedSerializer {
public:
    TBuferredSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize, ui64 maxChunkBytes)
        : TPackedSerializer(buffer, rowType, transportVersion, packerVersion, bufferPageAllocSize)
        , MaxChunkBytes(maxChunkBytes) {

    }

    ui64 MaxChunkBytes;
    ui64 Rows = 0;
};

class TNarrowSerializer : public TBuferredSerializer {
public:
    TNarrowSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize, ui64 maxChunkBytes)
        : TBuferredSerializer(buffer, rowType, transportVersion, packerVersion, bufferPageAllocSize, maxChunkBytes) {
    }

    void Flush(bool finished) override {
        if (Packer.PackedSizeEstimate() > 0) {
            Buffer->Push(TDataChunk(Packer.Finish(), Rows, TransportVersion, PackerVersion, Buffer->GetLeading(), finished));
        } else if (finished) {
            Buffer->SendFinish();
        }
        Rows = 0;
    }

    void Push(NUdf::TUnboxedValue&& value) override {
        Packer.AddItem(value);
        Rows++;
        if (Packer.PackedSizeEstimate() > MaxChunkBytes) {
            Buffer->Push(TDataChunk(Packer.Finish(), Rows, TransportVersion, PackerVersion, Buffer->GetLeading(), false));
            Rows = 0;
        }
    }

    void WidePush(NUdf::TUnboxedValue*, ui32) override {
        YQL_ENSURE(false, "WidePush to Narrow Channel");
    }
};

class TWideSerializer : public TBuferredSerializer {
public:
    TWideSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize, ui64 maxChunkBytes)
        : TBuferredSerializer(buffer, rowType, transportVersion, packerVersion, bufferPageAllocSize, maxChunkBytes) {
    }

    void Flush(bool finished) override {
        if (Packer.PackedSizeEstimate() > 0) {
            Buffer->Push(TDataChunk(Packer.Finish(), Rows, TransportVersion, PackerVersion, Buffer->GetLeading(), finished));
        } else if (finished) {
            Buffer->SendFinish();
        }
        Rows = 0;
    }

    void Push(NUdf::TUnboxedValue&&) override {
        YQL_ENSURE(false, "Push to Wide Channel");
    }

    void WidePush(NUdf::TUnboxedValue* values, ui32 width) override {
        Packer.AddWideItem(values, width);
        Rows++;
        for (ui32 i = 0; i < width; ++i) {
            values[i] = {};
        }
        if (Packer.PackedSizeEstimate() > MaxChunkBytes) {
            Buffer->Push(TDataChunk(Packer.Finish(), Rows, TransportVersion, PackerVersion, Buffer->GetLeading(), false));
            Rows = 0;
        }
    }
};

class TBlockSerializer : public TPackedSerializer {
public:
    TBlockSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize)
        : TPackedSerializer(buffer, rowType, transportVersion, packerVersion, bufferPageAllocSize) {

    }

    void Flush(bool finished) override {
        if (finished) {
            Buffer->SendFinish();
        }
    }

    void Push(NUdf::TUnboxedValue&&) override {
        YQL_ENSURE(false, "Push to Wide Channel");
    }

    void WidePush(NUdf::TUnboxedValue* values, ui32 width) override {
        ui32 rows = NKikimr::NMiniKQL::TArrowBlock::From(values[width - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
        Packer.AddWideItem(values, width);
        for (ui32 i = 0; i < width; ++i) {
            values[i] = {};
        }
        Buffer->Push(TDataChunk(Packer.Finish(), rows, TransportVersion, PackerVersion, Buffer->GetLeading(), false));
    }
};

class TChunkedSerializer : public TBlockSerializer {
public:
    TChunkedSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion,
        TMaybe<size_t> bufferPageAllocSize, const NKikimr::NMiniKQL::THolderFactory& holderFactory, NArrow::IBlockSplitter::TPtr splitter)
        : TBlockSerializer(buffer, rowType, transportVersion, packerVersion, bufferPageAllocSize)
        , HolderFactory(holderFactory)
        , Splitter(splitter) {
    }

    void WidePush(NUdf::TUnboxedValue* values, ui32 width) override {
        if (Splitter->ShouldSplitItem(values, width)) {
            for (auto&& block : Splitter->SplitItem(values, width)) {
                NKikimr::NMiniKQL::TUnboxedValueVector outputValues;
                outputValues.reserve(block.size());
                for (auto& datum : block) {
                    outputValues.emplace_back(HolderFactory.CreateArrowBlock(std::move(datum)));
                }
                TBlockSerializer::WidePush(outputValues.data(), outputValues.size());
            }
        } else {
            TBlockSerializer::WidePush(values, width);
        }
    }

    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    NArrow::IBlockSplitter::TPtr Splitter;
};

class TPackedDeserializer : public TInputDeserializer {
public:
    using TInputDeserializer::HolderFactory;

    TPackedDeserializer(NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize, const NKikimr::NMiniKQL::THolderFactory& holderFactory)
        : TInputDeserializer(rowType, transportVersion, packerVersion, holderFactory)
        , Packer(rowType, packerVersion, bufferPageAllocSize) {
    }

    void Deserialize(TChunkedBuffer&& data, NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override {
        Packer.UnpackBatch(std::move(data), HolderFactory, batch);
    }

    NKikimr::NMiniKQL::TValuePackerTransport<true> Packer;
};

std::unique_ptr<TOutputSerializer> CreateSerializer(const TDqChannelParams& params, std::shared_ptr<IChannelBuffer> buffer, bool local) {

    if (params.RowType->IsMulti()) {
        ui32 blockLengthIndex;
        TVector<const NKikimr::NMiniKQL::TBlockType*> items;
        if (IsLegacyStructBlock(params.RowType, blockLengthIndex, items) || IsMultiBlock(params.RowType, blockLengthIndex, items)) {
            if (local) {
                return std::make_unique<TBlockSerializer>(buffer, params.RowType, params.TransportVersion, params.PackerVersion, params.BufferPageAllocSize);
            } else {
                auto splitter = NArrow::CreateBlockSplitter(params.RowType, 36_MB);
                return std::make_unique<TChunkedSerializer>(buffer, params.RowType, params.TransportVersion, params.PackerVersion, params.BufferPageAllocSize, *params.HolderFactory, splitter);
            }
        } else {
            return std::make_unique<TWideSerializer>(buffer, params.RowType, params.TransportVersion, params.PackerVersion, params.BufferPageAllocSize, 48_MB);
        }
    } else {
        return std::make_unique<TNarrowSerializer>(buffer, params.RowType, params.TransportVersion, params.PackerVersion, params.BufferPageAllocSize, 48_MB);
    }

}

std::unique_ptr<TOutputSerializer> ConvertToLocalSerializer(std::unique_ptr<TOutputSerializer>&& serializer) {
    ui32 blockLengthIndex;
    TVector<const NKikimr::NMiniKQL::TBlockType*> items;
    auto rowType = serializer->RowType;
    if (IsLegacyStructBlock(rowType, blockLengthIndex, items) || IsMultiBlock(rowType, blockLengthIndex, items)) {
        return std::make_unique<TBlockSerializer>(serializer->Buffer, rowType, serializer->TransportVersion, serializer->PackerVersion, serializer->BufferPageAllocSize);
    } else {
        return serializer;
    }
}

std::unique_ptr<TInputDeserializer> CreateDeserializer(NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize, const NKikimr::NMiniKQL::THolderFactory& holderFactory) {
    Y_ENSURE(transportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0
          || transportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0);
    return std::make_unique<TPackedDeserializer>(rowType, transportVersion, packerVersion, bufferPageAllocSize, holderFactory);
}

} // namespace NYql::NDq
