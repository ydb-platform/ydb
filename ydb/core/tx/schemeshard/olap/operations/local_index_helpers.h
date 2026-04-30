#pragma once

#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/library/actors/core/log.h>

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
            auto it = columnIdToName.find(colId);
            if (it == columnIdToName.end()) {
                return false;
            }
            config.AddKeyColumnNames(it->second);
        }
        *config.MutableBloomFilterDescription() = indexProto.GetBloomFilter();
        return true;
    } else if (indexProto.HasBloomNGrammFilter()) {
        config.SetType(NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter);
        auto it = columnIdToName.find(indexProto.GetBloomNGrammFilter().GetColumnId());
        if (it == columnIdToName.end()) {
            return false;
        }
        config.AddKeyColumnNames(it->second);
        *config.MutableBloomNGrammFilterDescription() = indexProto.GetBloomNGrammFilter();
        return true;
    }

    return false;
}

inline bool ConvertOlapIndexToRequested(
    const NKikimrSchemeOp::TOlapIndexDescription& src,
    const THashMap<ui32, TString>& columnIdToName,
    NKikimrSchemeOp::TOlapIndexRequested& dst)
{
    dst.SetName(src.GetName());
    if (src.HasClassName()) {
        dst.SetClassName(src.GetClassName());
    }

    switch (src.Implementation_case()) {
        case NKikimrSchemeOp::TOlapIndexDescription::kBloomFilter: {
            auto* bf = dst.MutableBloomFilter();
            if (src.GetBloomFilter().HasFalsePositiveProbability()) {
                bf->SetFalsePositiveProbability(src.GetBloomFilter().GetFalsePositiveProbability());
            }
            for (ui32 colId : src.GetBloomFilter().GetColumnIds()) {
                auto it = columnIdToName.find(colId);
                if (it == columnIdToName.end()) {
                    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "ConvertOlapIndexToRequested: BloomFilter column ID " << colId
                        << " not found in columnIdToName map for index '" << src.GetName() << "'");
                    return false;
                }
                bf->AddColumnNames(it->second);
            }
            if (src.GetBloomFilter().HasDataExtractor()) {
                *bf->MutableDataExtractor() = src.GetBloomFilter().GetDataExtractor();
            }
            if (src.GetBloomFilter().HasBitsStorage()) {
                *bf->MutableBitsStorage() = src.GetBloomFilter().GetBitsStorage();
            }
            return true;
        }
        case NKikimrSchemeOp::TOlapIndexDescription::kBloomNGrammFilter: {
            auto* nf = dst.MutableBloomNGrammFilter();
            if (src.GetBloomNGrammFilter().HasColumnId()) {
                auto it = columnIdToName.find(src.GetBloomNGrammFilter().GetColumnId());
                if (it == columnIdToName.end()) {
                    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "ConvertOlapIndexToRequested: BloomNGrammFilter column ID " << src.GetBloomNGrammFilter().GetColumnId()
                        << " not found in columnIdToName map for index '" << src.GetName() << "'");
                    return false;
                }
                nf->SetColumnName(it->second);
            }
            if (src.GetBloomNGrammFilter().HasNGrammSize()) {
                nf->SetNGrammSize(src.GetBloomNGrammFilter().GetNGrammSize());
            }
            if (src.GetBloomNGrammFilter().HasHashesCount()) {
                nf->SetHashesCount(src.GetBloomNGrammFilter().GetHashesCount());
            }
            if (src.GetBloomNGrammFilter().HasFilterSizeBytes()) {
                nf->SetFilterSizeBytes(src.GetBloomNGrammFilter().GetFilterSizeBytes());
            }
            if (src.GetBloomNGrammFilter().HasRecordsCount()) {
                nf->SetRecordsCount(src.GetBloomNGrammFilter().GetRecordsCount());
            }
            if (src.GetBloomNGrammFilter().HasCaseSensitive()) {
                nf->SetCaseSensitive(src.GetBloomNGrammFilter().GetCaseSensitive());
            }
            if (src.GetBloomNGrammFilter().HasDataExtractor()) {
                *nf->MutableDataExtractor() = src.GetBloomNGrammFilter().GetDataExtractor();
            }
            if (src.GetBloomNGrammFilter().HasBitsStorage()) {
                *nf->MutableBitsStorage() = src.GetBloomNGrammFilter().GetBitsStorage();
            }
            return true;
        }
        case NKikimrSchemeOp::TOlapIndexDescription::kMaxIndex:
        case NKikimrSchemeOp::TOlapIndexDescription::kCountMinSketch:
        case NKikimrSchemeOp::TOlapIndexDescription::kMinMaxIndex:
        case NKikimrSchemeOp::TOlapIndexDescription::IMPLEMENTATION_NOT_SET:
            return false;
    }

    return false;
}

inline bool ConvertRequestedIndexToCreationConfig(
    const NKikimrSchemeOp::TOlapIndexRequested& indexProto,
    NKikimrSchemeOp::TIndexCreationConfig& config)
{
    config.SetName(indexProto.GetName());
    config.SetState(NKikimrSchemeOp::EIndexStateReady);

    switch (indexProto.GetImplementationCase()) {
        case NKikimrSchemeOp::TOlapIndexRequested::kBloomFilter: {
            config.SetType(NKikimrSchemeOp::EIndexTypeLocalBloomFilter);
            const auto& bf = indexProto.GetBloomFilter();
            for (const auto& colName : bf.GetColumnNames()) {
                config.AddKeyColumnNames(colName);
            }
            auto* desc = config.MutableBloomFilterDescription();
            if (bf.HasFalsePositiveProbability()) {
                desc->SetFalsePositiveProbability(bf.GetFalsePositiveProbability());
            }
            if (bf.HasDataExtractor()) {
                *desc->MutableDataExtractor() = bf.GetDataExtractor();
            }
            if (bf.HasBitsStorage()) {
                *desc->MutableBitsStorage() = bf.GetBitsStorage();
            }
            return true;
        }
        case NKikimrSchemeOp::TOlapIndexRequested::kBloomNGrammFilter: {
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
            if (nf.HasFalsePositiveProbability()) desc->SetFalsePositiveProbability(nf.GetFalsePositiveProbability());
            if (nf.HasDataExtractor()) {
                *desc->MutableDataExtractor() = nf.GetDataExtractor();
            }
            if (nf.HasBitsStorage()) {
                *desc->MutableBitsStorage() = nf.GetBitsStorage();
            }
            return true;
        }
        case NKikimrSchemeOp::TOlapIndexRequested::IMPLEMENTATION_NOT_SET:
        case NKikimrSchemeOp::TOlapIndexRequested::kMaxIndex:
        case NKikimrSchemeOp::TOlapIndexRequested::kCountMinSketch:
        case NKikimrSchemeOp::TOlapIndexRequested::kMinMaxIndex:
            return false;
    }

    return false;
}

} // namespace NKikimr::NSchemeShard::NOlap
