#pragma once

#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap {

inline THashMap<ui32, TString> BuildColumnIdToNameMap(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    THashMap<ui32, TString> result;
    for (const auto& col : schema.GetColumns()) {
        result[col.GetId()] = col.GetName();
    }
    return result;
}

inline bool ConvertOlapIndexToCreationConfig(
    const NKikimrSchemeOp::TOlapIndexDescription& indexProto,
    const THashMap<ui32, TString>& columnIdToName,
    NKikimrSchemeOp::TIndexCreationConfig& config)
{
    config.SetName(indexProto.GetName());
    config.SetState(NKikimrSchemeOp::EIndexStateReady);

    if (indexProto.HasBloomFilter()) {
        config.SetType(NKikimrSchemeOp::EIndexTypeLocalBloomFilter);
        for (ui32 colId : indexProto.GetBloomFilter().GetColumnIds()) {
            if (auto it = columnIdToName.find(colId); it != columnIdToName.end()) {
                config.AddKeyColumnNames(it->second);
            }
        }
        *config.MutableBloomFilterDescription() = indexProto.GetBloomFilter();
        return true;
    } else if (indexProto.HasBloomNGrammFilter()) {
        config.SetType(NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter);
        if (auto it = columnIdToName.find(indexProto.GetBloomNGrammFilter().GetColumnId()); it != columnIdToName.end()) {
            config.AddKeyColumnNames(it->second);
        }
        *config.MutableBloomNGrammFilterDescription() = indexProto.GetBloomNGrammFilter();
        return true;
    }

    return false;
}

inline bool ConvertRequestedIndexToCreationConfig(
    const NKikimrSchemeOp::TOlapIndexRequested& indexProto,
    NKikimrSchemeOp::TIndexCreationConfig& config)
{
    config.SetName(indexProto.GetName());
    config.SetState(NKikimrSchemeOp::EIndexStateReady);

    if (indexProto.HasBloomFilter()) {
        config.SetType(NKikimrSchemeOp::EIndexTypeLocalBloomFilter);
        const auto& bf = indexProto.GetBloomFilter();
        for (const auto& colName : bf.GetColumnNames()) {
            config.AddKeyColumnNames(colName);
        }
        auto* desc = config.MutableBloomFilterDescription();
        desc->SetFalsePositiveProbability(bf.GetFalsePositiveProbability());
        if (bf.HasDataExtractor()) {
            *desc->MutableDataExtractor() = bf.GetDataExtractor();
        }
        if (bf.HasBitsStorage()) {
            *desc->MutableBitsStorage() = bf.GetBitsStorage();
        }
        return true;
    } else if (indexProto.HasBloomNGrammFilter()) {
        config.SetType(NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter);
        const auto& nf = indexProto.GetBloomNGrammFilter();
        if (!nf.GetColumnName().empty()) {
            config.AddKeyColumnNames(nf.GetColumnName());
        }
        auto* desc = config.MutableBloomNGrammFilterDescription();
        if (nf.HasNGrammSize()) desc->SetNGrammSize(nf.GetNGrammSize());
        if (nf.HasFilterSizeBytes()) desc->SetFilterSizeBytes(nf.GetFilterSizeBytes());
        if (nf.HasHashesCount()) desc->SetHashesCount(nf.GetHashesCount());
        if (nf.HasRecordsCount()) desc->SetRecordsCount(nf.GetRecordsCount());
        if (nf.HasCaseSensitive()) desc->SetCaseSensitive(nf.GetCaseSensitive());
        if (nf.HasDataExtractor()) {
            *desc->MutableDataExtractor() = nf.GetDataExtractor();
        }
        if (nf.HasBitsStorage()) {
            *desc->MutableBitsStorage() = nf.GetBitsStorage();
        }
        return true;
    }

    return false;
}

} // namespace NKikimr::NSchemeShard::NOlap
