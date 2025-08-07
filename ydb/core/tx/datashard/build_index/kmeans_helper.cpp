#include "kmeans_helper.h"

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NDataShard::NKMeans {

TTableRange CreateRangeFrom(const TUserTable& table, TClusterId parent, TCell& from, TCell& to) {
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

void AddRowToLevel(TBufferData& buffer, TClusterId parent, TClusterId child, const TString& embedding, bool isPostingLevel) {
    if (isPostingLevel) {
        child = SetPostingParentFlag(child);
    } else {
        EnsureNoPostingParentFlag(child);
    }

    std::array<TCell, 2> pk;
    pk[0] = TCell::Make(parent);
    pk[1] = TCell::Make(child);

    std::array<TCell, 1> data;
    data[0] = TCell{embedding};

    buffer.AddRow(pk, data);
}

void AddRowToData(TBufferData& buffer, TClusterId parent, TArrayRef<const TCell> sourcePk,
    TArrayRef<const TCell> dataColumns, TArrayRef<const TCell> origKey, bool isPostingLevel) {
    if (isPostingLevel) {
        parent = SetPostingParentFlag(parent);
    } else {
        EnsureNoPostingParentFlag(parent);
    }

    TVector<TCell> pk(::Reserve(sourcePk.size() + 1));
    pk.push_back(TCell::Make(parent));
    pk.insert(pk.end(), sourcePk.begin(), sourcePk.end());

    buffer.AddRow(pk, dataColumns, origKey);
}

TTags MakeScanTags(const TUserTable& table, const TProtoStringType& embedding, 
    const google::protobuf::RepeatedPtrField<TProtoStringType>& data, ui32& embeddingPos,
    ui32& dataPos, NTable::TTag& embeddingTag)
{
    auto tags = GetAllTags(table);
    TTags result;
    result.reserve(1 + data.size());
    embeddingTag = tags.at(embedding);
    if (auto it = std::find(data.begin(), data.end(), embedding); it != data.end()) {
        embeddingPos = it - data.begin();
        dataPos = 0;
    } else {
        result.push_back(embeddingTag);
    }
    for (const auto& column : data) {
        result.push_back(tags.at(column));
    }
    return result;
}

std::shared_ptr<NTxProxy::TUploadTypes> MakeOutputTypes(const TUserTable& table, NKikimrTxDataShard::EKMeansState uploadState,
    const TProtoStringType& embedding, const google::protobuf::RepeatedPtrField<TProtoStringType>& data,
    const google::protobuf::RepeatedPtrField<TProtoStringType>& pkColumns)
{
    auto types = GetAllTypes(table);

    auto result = std::make_shared<NTxProxy::TUploadTypes>();

    Ydb::Type type;
    type.set_type_id(NTableIndex::ClusterIdType);
    result->emplace_back(NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn, type);

    auto addType = [&](const auto& column) {
        auto it = types.find(column);
        if (it != types.end()) {
            NScheme::ProtoFromTypeInfo(it->second, type);
            result->emplace_back(it->first, type);
            types.erase(it);
        }
    };
    if (pkColumns.size()) {
        for (const auto& column : pkColumns) {
            addType(column);
        }
    } else {
        for (const auto& column : table.KeyColumnIds) {
            addType(table.Columns.at(column).Name);
        }
    }
    switch (uploadState) {
        case NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD:
        case NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD:
            if (auto it = std::find(data.begin(), data.end(), embedding); it == data.end()) {
                addType(embedding);
            }
            [[fallthrough]];
        case NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING:
        case NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING: {
            for (const auto& column : data) {
                addType(column);
            }
            break;
        }
        default:
            Y_ENSURE(false);

    }
    return result;
}

}
