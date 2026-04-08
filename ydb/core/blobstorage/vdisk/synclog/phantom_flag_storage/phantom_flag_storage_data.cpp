#include "phantom_flag_storage_data.h"

#include <util/generic/overloaded.h>

namespace NKikimr::NSyncLog {

TPhantomFlagStorageItem TPhantomFlagStorageItem::CreateSkip(ui32 skipSize) {
    TPhantomFlagStorageItem res;
    res.Data.emplace<TSkip>(skipSize);
    return res;
}

TPhantomFlagStorageItem TPhantomFlagStorageItem::CreateFlag(const TLogoBlobRec* blobRec) {
    TPhantomFlagStorageItem res;
    res.Data.emplace<TFlag>(*blobRec);
    return res;
}

TPhantomFlagStorageItem TPhantomFlagStorageItem::CreateThreshold(ui32 orderNumber,
        ui64 tabletId, ui8 channel, ui32 generation, ui32 step) {
    TPhantomFlagStorageItem res;
    res.Data.emplace<TThreshold>(tabletId, channel, generation, step, orderNumber);
    return res;
}

TPhantomFlagStorageItem TPhantomFlagStorageItem::CreateThreshold(ui32 orderNumber,
        const TLogoBlobID& blobId) {
    TPhantomFlagStorageItem res;
    res.Data.emplace<TThreshold>(blobId.TabletID(), blobId.Channel(), blobId.Generation(),
            blobId.Step(), orderNumber);
    return res;
}

EPhantomFlagStorageItem TPhantomFlagStorageItem::GetType() const {
    EPhantomFlagStorageItem res = EPhantomFlagStorageItem::Unknown;
    std::visit(TOverloaded{
        [&](const std::monostate&) {},
        [&](const TSkip&) {},
        [&](const TFlag&) { res = EPhantomFlagStorageItem::Flag; },
        [&](const TThreshold&) { res = EPhantomFlagStorageItem::Threshold; }
    }, Data);
    return res;
}

TPhantomFlagStorageItem::TSkip TPhantomFlagStorageItem::GetSkip() const {
    return std::get<TSkip>(Data);
}

TPhantomFlagStorageItem::TThreshold TPhantomFlagStorageItem::GetThreshold() const {
    return std::get<TThreshold>(Data);
}

TPhantomFlagStorageItem::TFlag TPhantomFlagStorageItem::GetFlag() const {
    return std::get<TFlag>(Data);
}

void TPhantomFlagStorageItem::Serialize(TString* buffer) const {
    std::visit(TOverloaded{
        [&](const std::monostate&) {},
        [&](const TSkip&) {},
        [&](const TFlag& flag) {
            constexpr static EPhantomFlagStorageItem type = EPhantomFlagStorageItem::Flag;
            buffer->append(reinterpret_cast<const char*>(&type), sizeof(type));
            buffer->append(reinterpret_cast<const char*>(&flag), sizeof(flag));
        },
        [&](const TThreshold& threshold) {
            constexpr static EPhantomFlagStorageItem type = EPhantomFlagStorageItem::Threshold;
            buffer->append(reinterpret_cast<const char*>(&type), sizeof(type));
            buffer->append(reinterpret_cast<const char*>(&threshold), sizeof(threshold));
        },
    }, Data);
}

TPhantomFlagStorageItem TPhantomFlagStorageItem::DeserializeFromRaw(const char* data) {
    const EPhantomFlagStorageItem type = *reinterpret_cast<const EPhantomFlagStorageItem*>(data);
    data += sizeof(type);
    switch (type) {
    case EPhantomFlagStorageItem::Flag: {
        const TLogoBlobRec rec = *reinterpret_cast<const TLogoBlobRec*>(data);
        return TPhantomFlagStorageItem::CreateFlag(&rec);
    }
    case EPhantomFlagStorageItem::Threshold: {
        const TThreshold threshold = *reinterpret_cast<const TThreshold*>(data);
        return TPhantomFlagStorageItem::CreateThreshold(threshold.OrderNumber, threshold.TabletId,
                threshold.Channel, threshold.Generation, threshold.Step);
    }
    default: {
        const ui32 size = *reinterpret_cast<const ui32*>(data);
        return TPhantomFlagStorageItem::CreateSkip(size);
    }
    }
}

ui32 TPhantomFlagStorageItem::SerializedSize() const {
    ui32 res = 0;
    std::visit(TOverloaded{
        [&](const std::monostate&) { },
        [&](const TSkip& skip) { res = skip.Size; },
        [&](const TFlag&) { res = sizeof(EPhantomFlagStorageItem) + sizeof(TFlag); },
        [&](const TThreshold&) { res = sizeof(EPhantomFlagStorageItem) + sizeof(TThreshold); },
    }, Data);
    return res;
}

void TPhantomFlagStorageData::Deserialize(const TPhantomFlagStorageDataProto& proto) {
    ChunkSize = proto.GetChunkSize();
    for (const auto& chunk : proto.GetChunks()) {
        Chunks[chunk.GetChunkIdx()] = TChunk{
            .DataSize = chunk.GetDataSize(),
        };
    }
}

void TPhantomFlagStorageData::Serialize(TPhantomFlagStorageDataProto* proto) const {
    for (const auto& [chunkIdx, chunk] : Chunks) {
        auto* chunkProto = proto->AddChunks();
        chunkProto->SetChunkIdx(chunkIdx);
        chunkProto->SetDataSize(chunk.DataSize);
    }
    proto->SetChunkSize(ChunkSize);
}

}
