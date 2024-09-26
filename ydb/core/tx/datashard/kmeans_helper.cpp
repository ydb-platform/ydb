#include "kmeans_helper.h"

#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimr::NDataShard::NKMeans {

TTableRange CreateRangeFrom(const TUserTable& table, ui32 parent, TCell& from, TCell& to) {
    if (parent == 0) {
        return table.GetTableRange();
    }
    from = TCell::Make(parent - 1);
    to = TCell::Make(parent);
    TTableRange range{{&from, 1}, false, {&to, 1}, true};
    return Intersect(table.KeyColumnTypes, range, table.GetTableRange());
}

NTable::TLead CreateLeadFrom(const TTableRange& range) {
    NTable::TLead lead;
    if (range.From) {
        lead.To(range.From, range.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper);
    } else {
        lead.To({}, NTable::ESeek::Lower);
    }
    if (range.To) {
        lead.Until(range.To, range.InclusiveTo);
    }
    return lead;
}

void AddRowMain2Tmp(TBufferData& buffer, ui32 parent, TArrayRef<const TCell> key, const NTable::TRowState& row) {
    std::array<TCell, 1> cells;
    cells[0] = TCell::Make(parent);
    auto pk = TSerializedCellVec::Serialize(cells);
    TSerializedCellVec::UnsafeAppendCells(key, pk);
    buffer.AddRow(TSerializedCellVec{key}, TSerializedCellVec{std::move(pk)}, TSerializedCellVec::Serialize(*row));
}

void AddRowMain2Posting(TBufferData& buffer, ui32 parent, TArrayRef<const TCell> key, const NTable::TRowState& row,
                        ui32 dataPos)
{
    std::array<TCell, 1> cells;
    cells[0] = TCell::Make(parent);
    auto pk = TSerializedCellVec::Serialize(cells);
    TSerializedCellVec::UnsafeAppendCells(key, pk);
    buffer.AddRow(TSerializedCellVec{key}, TSerializedCellVec{std::move(pk)},
                  TSerializedCellVec::Serialize((*row).Slice(dataPos)));
}

void AddRowTmp2Tmp(TBufferData& buffer, ui32 parent, TArrayRef<const TCell> key, const NTable::TRowState& row) {
    std::array<TCell, 1> cells;
    cells[0] = TCell::Make(parent);
    auto pk = TSerializedCellVec::Serialize(cells);
    TSerializedCellVec::UnsafeAppendCells(key.Slice(1), pk);
    buffer.AddRow(TSerializedCellVec{key}, TSerializedCellVec{std::move(pk)}, TSerializedCellVec::Serialize(*row));
}

void AddRowTmp2Posting(TBufferData& buffer, ui32 parent, TArrayRef<const TCell> key, const NTable::TRowState& row,
                       ui32 dataPos)
{
    std::array<TCell, 1> cells;
    cells[0] = TCell::Make(parent);
    auto pk = TSerializedCellVec::Serialize(cells);
    TSerializedCellVec::UnsafeAppendCells(key.Slice(1), pk);
    buffer.AddRow(TSerializedCellVec{key}, TSerializedCellVec{std::move(pk)},
                  TSerializedCellVec::Serialize((*row).Slice(dataPos)));
}

TTags MakeUploadTags(const TUserTable& table, const TProtoStringType& embedding,
                     const google::protobuf::RepeatedPtrField<TProtoStringType>& data, ui32& embeddingPos,
                     ui32& dataPos, NTable::TTag& embeddingTag)
{
    auto tags = GetAllTags(table);
    TTags uploadTags;
    uploadTags.reserve(1 + data.size());
    embeddingTag = tags.at(embedding);
    if (auto it = std::find(data.begin(), data.end(), embedding); it != data.end()) {
        embeddingPos = it - data.begin();
        dataPos = 0;
    } else {
        uploadTags.push_back(embeddingTag);
    }
    for (const auto& column : data) {
        uploadTags.push_back(tags.at(column));
    }
    return uploadTags;
}

std::shared_ptr<NTxProxy::TUploadTypes>
MakeUploadTypes(const TUserTable& table, NKikimrTxDataShard::TEvLocalKMeansRequest::EState uploadState,
                const TProtoStringType& embedding, const google::protobuf::RepeatedPtrField<TProtoStringType>& data)
{
    auto types = GetAllTypes(table);

    auto uploadTypes = std::make_shared<NTxProxy::TUploadTypes>();
    uploadTypes->reserve(1 + 1 + std::min(table.KeyColumnTypes.size() + data.size(), types.size()));

    Ydb::Type type;
    type.set_type_id(Ydb::Type::UINT32);
    uploadTypes->emplace_back(NTableIndex::NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn, type);

    auto addType = [&](const auto& column) {
        auto it = types.find(column);
        Y_ABORT_UNLESS(it != types.end());
        ProtoYdbTypeFromTypeInfo(&type, it->second);
        uploadTypes->emplace_back(it->first, type);
        types.erase(it);
    };
    for (const auto& column : table.KeyColumnIds) {
        addType(table.Columns.at(column).Name);
    }
    switch (uploadState) {
        case NKikimrTxDataShard::TEvLocalKMeansRequest::UPLOAD_MAIN_TO_TMP:
        case NKikimrTxDataShard::TEvLocalKMeansRequest::UPLOAD_TMP_TO_TMP:
            addType(embedding);
            [[fallthrough]];
        case NKikimrTxDataShard::TEvLocalKMeansRequest::UPLOAD_MAIN_TO_POSTING:
        case NKikimrTxDataShard::TEvLocalKMeansRequest::UPLOAD_TMP_TO_POSTING: {
            for (const auto& column : data) {
                addType(column);
            }
        } break;
        default:
            Y_UNREACHABLE();
    }
    return uploadTypes;
}

}
