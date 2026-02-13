#include <queue>
#include <mutex>

#include "dq_arrow_helpers.h"
#include "dq_channel_service_impl.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>
#include <ydb/library/yql/dq/runtime/dq_packer_version_helper.h>

namespace NYql::NDq {

template<bool fast>
class TPackedSerializer : public TOutputSerializer {
public:
    TPackedSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize)
        : TOutputSerializer(buffer, rowType, transportVersion, packerVersion, bufferPageAllocSize)
        , Packer(rowType, packerVersion, bufferPageAllocSize) {
    }

    NKikimr::NMiniKQL::TValuePackerTransport<fast> Packer;
};

template<bool fast>
class TBuferredSerializer : public TPackedSerializer<fast> {
public:
    TBuferredSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, ui64 maxChunkBytes, TMaybe<size_t> bufferPageAllocSize)
        : TPackedSerializer<fast>(buffer, rowType, transportVersion, packerVersion, bufferPageAllocSize)
        , MaxChunkBytes(maxChunkBytes) {

    }

    ui64 MaxChunkBytes;
    ui64 Rows = 0;
};

template<bool fast>
class TNarrowSerializer : public TBuferredSerializer<fast> {
public:
    using TOutputSerializer::Buffer;
    using TOutputSerializer::RowType;
    using TOutputSerializer::TransportVersion;
    using TOutputSerializer::PackerVersion;
    using TOutputSerializer::BufferPageAllocSize;
    using TBuferredSerializer<fast>::Packer;
    using TBuferredSerializer<fast>::MaxChunkBytes;
    using TBuferredSerializer<fast>::Rows;

    TNarrowSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, ui64 maxChunkBytes, TMaybe<size_t> bufferPageAllocSize)
        : TBuferredSerializer<fast>(buffer, rowType, transportVersion, packerVersion, maxChunkBytes, bufferPageAllocSize) {
    }

    void Flush(bool finished) override {
        if (Packer.PackedSizeEstimate() > 0 && (finished || Buffer->IsEmpty())) {
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

template<bool fast>
class TWideSerializer : public TBuferredSerializer<fast> {
public:
    using TOutputSerializer::Buffer;
    using TOutputSerializer::RowType;
    using TOutputSerializer::TransportVersion;
    using TOutputSerializer::PackerVersion;
    using TOutputSerializer::BufferPageAllocSize;
    using TPackedSerializer<fast>::Packer;
    using TBuferredSerializer<fast>::MaxChunkBytes;
    using TBuferredSerializer<fast>::Rows;

    TWideSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, ui64 maxChunkBytes, TMaybe<size_t> bufferPageAllocSize)
        : TBuferredSerializer<fast>(buffer, rowType, transportVersion, packerVersion, maxChunkBytes, bufferPageAllocSize) {
    }

    void Flush(bool finished) override {
        if (Packer.PackedSizeEstimate() > 0 && (finished || Buffer->IsEmpty())) {
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

template<bool fast>
class TBlockSerializer : public TPackedSerializer<fast> {
public:
    using TOutputSerializer::Buffer;
    using TOutputSerializer::RowType;
    using TOutputSerializer::TransportVersion;
    using TOutputSerializer::PackerVersion;
    using TOutputSerializer::BufferPageAllocSize;
    using TPackedSerializer<fast>::Packer;

    TBlockSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize, TMaybe<ui8> arrayBufferMinFillPercentage)
        : TPackedSerializer<fast>(buffer, rowType, transportVersion, packerVersion, bufferPageAllocSize) {
        Packer.SetMinFillPercentage(arrayBufferMinFillPercentage);
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

template<bool fast>
class TChunkedSerializer : public TBlockSerializer<fast> {
public:
    TChunkedSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion,
        TMaybe<size_t> bufferPageAllocSize, TMaybe<ui8> arrayBufferMinFillPercentage, const NKikimr::NMiniKQL::THolderFactory& holderFactory, NArrow::IBlockSplitter::TPtr splitter)
        : TBlockSerializer<fast>(buffer, rowType, transportVersion, packerVersion, bufferPageAllocSize, arrayBufferMinFillPercentage)
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
                TBlockSerializer<fast>::WidePush(outputValues.data(), outputValues.size());
            }
        } else {
            TBlockSerializer<fast>::WidePush(values, width);
        }
    }

    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    NArrow::IBlockSplitter::TPtr Splitter;
};

template<bool fast>
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

    NKikimr::NMiniKQL::TValuePackerTransport<fast> Packer;
};

template<bool fast>
std::unique_ptr<TOutputSerializer> CreateSerializer(const TDqChannelSettings& settings, std::shared_ptr<IChannelBuffer> buffer, bool local) {
    if (settings.RowType->IsMulti()) {
        ui32 blockLengthIndex;
        TVector<const NKikimr::NMiniKQL::TBlockType*> items;
        if (IsLegacyStructBlock(settings.RowType, blockLengthIndex, items) || IsMultiBlock(settings.RowType, blockLengthIndex, items)) {

            auto chunkSizeLimit = settings.ChunkSizeLimit;
            if (settings.ArrayBufferMinFillPercentage && *settings.ArrayBufferMinFillPercentage > 0) {
                chunkSizeLimit = chunkSizeLimit * *settings.ArrayBufferMinFillPercentage / 100;
            }

            if (local || chunkSizeLimit == 0) {
                return std::make_unique<TBlockSerializer<fast>>(buffer, settings.RowType, settings.TransportVersion, settings.PackerVersion,
                    settings.BufferPageAllocSize, local ? Nothing() : settings.ArrayBufferMinFillPercentage);
            } else {
                auto splitter = NArrow::CreateBlockSplitter(settings.RowType, chunkSizeLimit);
                return std::make_unique<TChunkedSerializer<fast>>(buffer, settings.RowType, settings.TransportVersion, settings.PackerVersion, settings.BufferPageAllocSize, settings.ArrayBufferMinFillPercentage, *settings.HolderFactory, splitter);
            }
        } else {
            return std::make_unique<TWideSerializer<fast>>(buffer, settings.RowType, settings.TransportVersion, settings.PackerVersion, std::min(settings.MaxStoredBytes, settings.MaxChunkBytes), settings.BufferPageAllocSize);
        }
    } else {
        return std::make_unique<TNarrowSerializer<fast>>(buffer, settings.RowType, settings.TransportVersion, settings.PackerVersion, std::min(settings.MaxStoredBytes, settings.MaxChunkBytes), settings.BufferPageAllocSize);
    }
}

std::unique_ptr<TOutputSerializer> CreateSerializer(const TDqChannelSettings& settings, std::shared_ptr<IChannelBuffer> buffer, bool local) {
    if (settings.TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0
            || settings.TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0) {
        return CreateSerializer<true>(settings, buffer, local);
    } else {
        return CreateSerializer<false>(settings, buffer, local);
    }
}

std::unique_ptr<TOutputSerializer> ConvertToLocalSerializer(std::unique_ptr<TOutputSerializer>&& serializer) {
    ui32 blockLengthIndex;
    TVector<const NKikimr::NMiniKQL::TBlockType*> items;
    auto rowType = serializer->RowType;
    if (IsLegacyStructBlock(rowType, blockLengthIndex, items) || IsMultiBlock(rowType, blockLengthIndex, items)) {
        if (serializer->TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0
                || serializer->TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0) {
            return std::make_unique<TBlockSerializer<true>>(serializer->Buffer, rowType, serializer->TransportVersion, serializer->PackerVersion, serializer->BufferPageAllocSize, Nothing());
        } else {
            return std::make_unique<TBlockSerializer<false>>(serializer->Buffer, rowType, serializer->TransportVersion, serializer->PackerVersion, serializer->BufferPageAllocSize, Nothing());
        }
    } else {
        return serializer;
    }
}

std::unique_ptr<TInputDeserializer> CreateDeserializer(NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize, const NKikimr::NMiniKQL::THolderFactory& holderFactory) {
    if (transportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0
          || transportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0) {
        return std::make_unique<TPackedDeserializer<true>>(rowType, transportVersion, packerVersion, bufferPageAllocSize, holderFactory);
    } else {
        return std::make_unique<TPackedDeserializer<false>>(rowType, transportVersion, packerVersion, bufferPageAllocSize, holderFactory);
    }
}

} // namespace NYql::NDq
