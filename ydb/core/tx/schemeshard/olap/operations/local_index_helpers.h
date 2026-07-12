#pragma once

#include <ydb/core/tx/columnshard/engines/storage/indexes/min_max/misc/misc.h>
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
                LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "ConvertOlapIndexToCreationConfig: BloomFilter column ID " << colId
                    << " not found in columnIdToName map for index '" << indexProto.GetName() << "'");
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
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "ConvertOlapIndexToCreationConfig: BloomNGrammFilter column ID " << indexProto.GetBloomNGrammFilter().GetColumnId()
                << " not found in columnIdToName map for index '" << indexProto.GetName() << "'");
            return false;
        }
        config.AddKeyColumnNames(it->second);
        *config.MutableBloomNGrammFilterDescription() = indexProto.GetBloomNGrammFilter();
        return true;
    } else if (indexProto.HasMinMaxIndex()) {
        config.SetType(NKikimrSchemeOp::EIndexTypeLocalMinMax);
        auto it = columnIdToName.find(indexProto.GetMinMaxIndex().GetColumnId());
        
        if (it == columnIdToName.end()) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "ConvertOlapIndexToCreationConfig: MinMaxIndex column ID " << indexProto.GetMinMaxIndex().GetColumnId()
                << " not found in columnIdToName map for index '" << indexProto.GetName() << "'");
            return false;
        }
        config.AddKeyColumnNames(it->second);
        return true;
    }
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "ConvertOlapIndexToCreationConfig: Unrecognized index type for index '" << indexProto.GetName() << "'");
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
        case NKikimrSchemeOp::TOlapIndexDescription::kMinMaxIndex: {
            NKikimrSchemeOp::TRequestedMinMaxIndex* min_max = dst.MutableMinMaxIndex();
            if (src.GetMinMaxIndex().HasColumnId()) {
                auto it = columnIdToName.find(src.GetMinMaxIndex().GetColumnId());
                if (it == columnIdToName.end()) {
                    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "ConvertOlapIndexToRequested: MinMaxIndex column ID " << src.GetMinMaxIndex().GetColumnId()
                        << " not found in columnIdToName map for index '" << src.GetName() << "'");
                    return false;
                }
                min_max->SetColumnName(it->second);
            }
            return true;
        }
        case NKikimrSchemeOp::TOlapIndexDescription::kMaxIndex:
        case NKikimrSchemeOp::TOlapIndexDescription::kCountMinSketch:
        case NKikimrSchemeOp::TOlapIndexDescription::IMPLEMENTATION_NOT_SET:
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                Sprintf("ConvertOlapIndexToRequested: unimplemented olap index type '%s'", src.GetClassName().c_str()));
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
        case NKikimrSchemeOp::TOlapIndexRequested::kMinMaxIndex: {
            config.SetType(NKikimrSchemeOp::EIndexTypeLocalMinMax);
            const auto& min_max = indexProto.GetMinMaxIndex();
            if (!min_max.GetColumnName().empty()) {
                config.AddKeyColumnNames(min_max.GetColumnName());
            }
            return true;
        }        case NKikimrSchemeOp::TOlapIndexRequested::IMPLEMENTATION_NOT_SET:
        case NKikimrSchemeOp::TOlapIndexRequested::kMaxIndex:
        case NKikimrSchemeOp::TOlapIndexRequested::kCountMinSketch:
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                Sprintf("ConvertRequestedIndexToCreationConfig: unimplemented olap index type '%s'", indexProto.GetClassName().c_str()));
            return false;
    }

    return false;
}

inline bool ConvertRequestedIndexToAlteringConfig(
    const NKikimrSchemeOp::TOlapIndexRequested& indexProto,
    NKikimrSchemeOp::TIndexAlteringConfig& config)
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
            if (nf.HasFalsePositiveProbability()) {
                desc->SetFalsePositiveProbability(nf.GetFalsePositiveProbability());
            }
            if (nf.HasDataExtractor()) {
                *desc->MutableDataExtractor() = nf.GetDataExtractor();
            }
            if (nf.HasBitsStorage()) {
                *desc->MutableBitsStorage() = nf.GetBitsStorage();
            }
            if (nf.HasNGrammSize()) {
                desc->SetNGrammSize(nf.GetNGrammSize());
            }
            if (nf.HasCaseSensitive()) {
                desc->SetCaseSensitive(nf.GetCaseSensitive());
            }
            return true;
        }
        case NKikimrSchemeOp::TOlapIndexRequested::kMinMaxIndex: {
            config.SetType(NKikimrSchemeOp::EIndexTypeLocalMinMax);
            const auto& min_max = indexProto.GetMinMaxIndex();
            if (!min_max.GetColumnName().empty()) {
                config.AddKeyColumnNames(min_max.GetColumnName());
            }
            return true;
        }

        default: {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                Sprintf("ConvertRequestedIndexToAlteringConfig: unimplemented olap index type '%s'", indexProto.GetClassName().c_str()));
            return false;
        }
    }

    return false;
}

inline bool ConvertAlteringConfigToCreationConfig(
    const NKikimrSchemeOp::TIndexAlteringConfig& alterConfig,
    NKikimrSchemeOp::TIndexCreationConfig& creationConfig)
{
    creationConfig.SetName(alterConfig.GetName());
    creationConfig.SetType(alterConfig.GetType());
    creationConfig.SetState(alterConfig.GetState());

    for (const auto& keyCol : alterConfig.GetKeyColumnNames()) {
        creationConfig.AddKeyColumnNames(keyCol);
    }

    // Note: DataColumnNames is not supported in TIndexAlteringConfig for local indexes

    if (alterConfig.HasBloomFilterDescription()) {
        *creationConfig.MutableBloomFilterDescription() = alterConfig.GetBloomFilterDescription();
    } else if (alterConfig.HasBloomNGrammFilterDescription()) {
        *creationConfig.MutableBloomNGrammFilterDescription() = alterConfig.GetBloomNGrammFilterDescription();
    }

    return true;
}

inline bool ConvertCreationConfigToRequested(
    const NKikimrSchemeOp::TIndexCreationConfig& config,
    NKikimrSchemeOp::TOlapIndexRequested& requested)
{
    requested.SetName(config.GetName());

    switch (config.GetType()) {
        case NKikimrSchemeOp::EIndexTypeLocalBloomFilter: {
            requested.SetClassName("BLOOM_FILTER");
            auto* bf = requested.MutableBloomFilter();
            if (config.HasBloomFilterDescription()) {
                const auto& desc = config.GetBloomFilterDescription();
                if (desc.HasFalsePositiveProbability()) {
                    bf->SetFalsePositiveProbability(desc.GetFalsePositiveProbability());
                }
                if (desc.HasDataExtractor()) {
                    *bf->MutableDataExtractor() = desc.GetDataExtractor();
                }
                if (desc.HasBitsStorage()) {
                    *bf->MutableBitsStorage() = desc.GetBitsStorage();
                }
            }
            for (const auto& colName : config.GetKeyColumnNames()) {
                bf->AddColumnNames(colName);
            }
            return true;
        }
        case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter: {
            requested.SetClassName("BLOOM_NGRAMM_FILTER");
            auto* ng = requested.MutableBloomNGrammFilter();
            if (config.HasBloomNGrammFilterDescription()) {
                const auto& desc = config.GetBloomNGrammFilterDescription();
                if (desc.HasNGrammSize()) {
                    ng->SetNGrammSize(desc.GetNGrammSize());
                }
                if (desc.HasFilterSizeBytes()) {
                    ng->SetFilterSizeBytes(desc.GetFilterSizeBytes());
                }
                if (desc.HasHashesCount()) {
                    ng->SetHashesCount(desc.GetHashesCount());
                }
                if (desc.HasRecordsCount()) {
                    ng->SetRecordsCount(desc.GetRecordsCount());
                }
                if (desc.HasCaseSensitive()) {
                    ng->SetCaseSensitive(desc.GetCaseSensitive());
                }
                if (desc.HasFalsePositiveProbability()) {
                    ng->SetFalsePositiveProbability(desc.GetFalsePositiveProbability());
                }
                if (desc.HasDataExtractor()) {
                    *ng->MutableDataExtractor() = desc.GetDataExtractor();
                }
                if (desc.HasBitsStorage()) {
                    *ng->MutableBitsStorage() = desc.GetBitsStorage();
                }
            }
            for (const auto& colName : config.GetKeyColumnNames()) {
                ng->SetColumnName(colName);
                break; // Only one column for ngram filter
            }
            return true;
        }
        case NKikimrSchemeOp::EIndexTypeLocalMinMax: {
            requested.SetClassName(NKikimr::NOlap::NIndexes::NMinMax::kMinMaxClassName);
            auto* min_max = requested.MutableMinMaxIndex();
            for (const auto& colName : config.GetKeyColumnNames()) {
                min_max->SetColumnName(colName);
                break; // Only one column for min_max index
            }
            return true;
        }
        default:
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                Sprintf("ConvertCreationConfigToRequested: unimplemented index type '%s'", EIndexType_Name(config.GetType()).c_str()));
            return false;
    }

    return false;
}

} // namespace NKikimr::NSchemeShard::NOlap
