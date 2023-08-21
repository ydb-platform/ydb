#include "config.h"

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/tablet_client/config.h>
#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/library/quantile_digest/config.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TRetentionConfigPtr& obj)
{
    static const TString NullPtrName("<nullptr>");
    return obj
        ? NYson::ConvertToYsonString(obj, NYson::EYsonFormat::Text).ToString()
        : NullPtrName;
}

////////////////////////////////////////////////////////////////////////////////

void TRetentionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("min_data_versions", &TThis::MinDataVersions)
        .GreaterThanOrEqual(0)
        .Default(1);
    registrar.Parameter("max_data_versions", &TThis::MaxDataVersions)
        .GreaterThanOrEqual(0)
        .Default(1);
    registrar.Parameter("min_data_ttl", &TThis::MinDataTtl)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("max_data_ttl", &TThis::MaxDataTtl)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("ignore_major_timestamp", &TThis::IgnoreMajorTimestamp)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TChunkReaderConfigPtr TChunkReaderConfig::GetDefault()
{
    return LeakyRefCountedSingleton<TChunkReaderConfig>();
}

void TChunkReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sampling_mode", &TThis::SamplingMode)
        .Default();

    registrar.Parameter("sampling_rate", &TThis::SamplingRate)
        .Default()
        .InRange(0, 1);

    registrar.Parameter("sampling_seed", &TThis::SamplingSeed)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->SamplingRate && !config->SamplingMode) {
            config->SamplingMode = ESamplingMode::Row;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void THashTableChunkIndexWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("load_factor", &TThis::LoadFactor)
        .Default(0.5)
        .GreaterThan(0.)
        .LessThanOrEqual(1.);
    registrar.Parameter("rehash_trial_count", &TThis::RehashTrialCount)
        .Default(3)
        .GreaterThan(0);
    registrar.Parameter("enable_group_reordering", &TThis::EnableGroupReordering)
        .Default(false);
    registrar.Parameter("max_block_size", &TThis::MaxBlockSize)
        .Default(128_KB);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkIndexesWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("hash_table", &TThis::HashTable)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TSlimVersionedWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("top_value_quantile", &TThis::TopValueQuantile)
        .Default(0.1)
        .InRange(0.0, 1.0);
    registrar.Parameter("enable_per_value_dictionary_encoding", &TThis::EnablePerValueDictionaryEncoding)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkWriterTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("add_unsupported_feature", &TThis::AddUnsupportedFeature)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkWriterConfig::Register(TRegistrar registrar)
{
    // Allow very small blocks for testing purposes.
    registrar.Parameter("block_size", &TThis::BlockSize)
        .GreaterThan(0)
        .Default(16_MB);

    registrar.Parameter("max_segment_value_count", &TThis::MaxSegmentValueCount)
        .GreaterThan(0)
        .Default(128 * 1024);

    registrar.Parameter("max_buffer_size", &TThis::MaxBufferSize)
        .GreaterThan(0)
        .Default(16_MB);

    registrar.Parameter("max_row_weight", &TThis::MaxRowWeight)
        .GreaterThanOrEqual(5_MB)
        .LessThanOrEqual(MaxRowWeightLimit)
        .Default(16_MB);

    registrar.Parameter("max_key_weight", &TThis::MaxKeyWeight)
        .GreaterThan(0)
        .LessThanOrEqual(MaxKeyWeightLimit)
        .Default(16_KB);

    registrar.Parameter("max_data_weight_between_blocks", &TThis::MaxDataWeightBetweenBlocks)
        .GreaterThan(0)
        .Default(2_GB);

    registrar.Parameter("sample_rate", &TThis::SampleRate)
        .InRange(0.0, 0.001)
        .Default(0.0001);

    registrar.Parameter("chunk_indexes", &TThis::ChunkIndexes)
        .DefaultNew();

    registrar.Parameter("slim", &TThis::Slim)
        .DefaultNew();

    registrar.Parameter("versioned_row_digest", &TThis::VersionedRowDigest)
        .DefaultNew();

    registrar.Parameter("testing_options", &TThis::TestingOptions)
        .DefaultNew();

    registrar.Parameter("key_filter", &TThis::KeyFilter)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TKeyFilterWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    registrar.Parameter("block_size", &TThis::BlockSize)
        .GreaterThan(0)
        .Default(64_KB);

    registrar.Parameter("trial_count", &TThis::TrialCount)
        .GreaterThan(0)
        .Default(100);

    registrar.Parameter("bits_per_key", &TThis::BitsPerKey)
        .InRange(0, 62)
        .Optional();

    registrar.Parameter("false_positive_rate", &TThis::FalsePositiveRate)
        .InRange(0, 1.0 / (1ll << 62))
        .Default()
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        if (config->BitsPerKey && config->FalsePositiveRate) {
            THROW_ERROR_EXCEPTION("At most one of \"bits_per_key\" and "
                "\"false_positive_rate\" can be specified");
        }

        if (config->FalsePositiveRate) {
            int bitsPerKey = 1;

            while ((1ll << bitsPerKey) * *config->FalsePositiveRate < 1) {
                ++bitsPerKey;
            }

            config->EffectiveBitsPerKey = bitsPerKey;
        } else {
            config->EffectiveBitsPerKey = config->BitsPerKey.value_or(DefaultBitsPerKey);
        }
    });
}

///////////////////////////////////////////////////////////////////////////////

void TBatchHunkReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_hunk_count_per_read", &TThis::MaxHunkCountPerRead)
        .GreaterThan(0)
        .Default(10'000);
    registrar.Parameter("max_total_hunk_length_per_read", &TThis::MaxTotalHunkLengthPerRead)
        .GreaterThan(0)
        .Default(16_MB);
}

////////////////////////////////////////////////////////////////////////////////

void TTableReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("suppress_access_tracking", &TThis::SuppressAccessTracking)
        .Default(false);
    registrar.Parameter("suppress_expiration_timeout_renewal", &TThis::SuppressExpirationTimeoutRenewal)
        .Default(false);
    registrar.Parameter("unavailable_chunk_strategy", &TThis::UnavailableChunkStrategy)
        .Default(EUnavailableChunkStrategy::Restore);
    registrar.Parameter("chunk_availability_policy", &TThis::ChunkAvailabilityPolicy)
        .Default(EChunkAvailabilityPolicy::Repairable);
    registrar.Parameter("max_read_duration", &TThis::MaxReadDuration)
        .Default();
    registrar.Parameter("dynamic_store_reader", &TThis::DynamicStoreReader)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TTypeConversionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_type_conversion", &TThis::EnableTypeConversion)
        .Default(false);
    registrar.Parameter("enable_string_to_all_conversion", &TThis::EnableStringToAllConversion)
        .Default(false);
    registrar.Parameter("enable_all_to_string_conversion", &TThis::EnableAllToStringConversion)
        .Default(false);
    registrar.Parameter("enable_integral_type_conversion", &TThis::EnableIntegralTypeConversion)
        .Default(true);
    registrar.Parameter("enable_integral_to_double_conversion", &TThis::EnableIntegralToDoubleConversion)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->EnableTypeConversion) {
            config->EnableStringToAllConversion = true;
            config->EnableAllToStringConversion = true;
            config->EnableIntegralTypeConversion = true;
            config->EnableIntegralToDoubleConversion = true;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TInsertRowsFormatConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_null_to_yson_entity_conversion", &TThis::EnableNullToYsonEntityConversion)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TChunkReaderOptionsPtr TChunkReaderOptions::GetDefault()
{
    return LeakyRefCountedSingleton<TChunkReaderOptions>();
}

void TChunkReaderOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_table_index", &TThis::EnableTableIndex)
        .Default(false);

    registrar.Parameter("enable_range_index", &TThis::EnableRangeIndex)
        .Default(false);

    registrar.Parameter("enable_row_index", &TThis::EnableRowIndex)
        .Default(false);

    registrar.Parameter("enable_tablet_index", &TThis::EnableTabletIndex)
        .Default(false);

    registrar.Parameter("dynamic_table", &TThis::DynamicTable)
        .Default(false);

    registrar.Parameter("enable_key_widening", &TThis::EnableKeyWidening)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->EnableRangeIndex && !config->EnableRowIndex) {
            THROW_ERROR_EXCEPTION("\"enable_row_index\" must be set when \"enable_range_index\" is set");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TChunkWriterOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("validate_sorted", &TThis::ValidateSorted)
        .Default(true);
    registrar.Parameter("validate_row_weight", &TThis::ValidateRowWeight)
        .Default(false);
    registrar.Parameter("validate_key_weight", &TThis::ValidateKeyWeight)
        .Default(false);
    registrar.Parameter("validate_duplicate_ids", &TThis::ValidateDuplicateIds)
        .Default(false);
    registrar.Parameter("validate_column_count", &TThis::ValidateColumnCount)
        .Default(false);
    registrar.Parameter("validate_any_is_valid_yson", &TThis::ValidateAnyIsValidYson)
        .Default(false);
    registrar.Parameter("validate_unique_keys", &TThis::ValidateUniqueKeys)
        .Default(false);
    registrar.Parameter("explode_on_validation_error", &TThis::ExplodeOnValidationError)
        .Default(false);
    registrar.Parameter("optimize_for", &TThis::OptimizeFor)
        .Default(EOptimizeFor::Lookup);
    registrar.Parameter("chunk_format", &TThis::ChunkFormat)
        .Default();
    registrar.Parameter("evaluate_computed_columns", &TThis::EvaluateComputedColumns)
        .Default(true);
    registrar.Parameter("enable_skynet_sharing", &TThis::EnableSkynetSharing)
        .Default(false);
    registrar.Parameter("return_boundary_keys", &TThis::ReturnBoundaryKeys)
        .Default(true);
    registrar.Parameter("cast_any_to_composite", &TThis::CastAnyToCompositeNode)
        .Default();
    registrar.Parameter("single_column_group_by_default", &TThis::SingleColumnGroupByDefault)
        .Default();

    registrar.Parameter("schema_modification", &TThis::SchemaModification)
        .Default(ETableSchemaModification::None);
    registrar.Parameter("max_heavy_columns", &TThis::MaxHeavyColumns)
        .Default(0);

    registrar.Postprocessor([] (TThis* config) {
        if (config->ValidateUniqueKeys && !config->ValidateSorted) {
            THROW_ERROR_EXCEPTION("\"validate_unique_keys\" is allowed to be true only if \"validate_sorted\" is true");
        }

        if (config->CastAnyToCompositeNode) {
            try {
                config->CastAnyToComposite = NYTree::ConvertTo<bool>(config->CastAnyToCompositeNode);
            } catch (const std::exception&) {
                // COMPAT: Do nothing for backward compatibility.
            }
        }

        switch (config->SchemaModification) {
            case ETableSchemaModification::None:
                break;

            case ETableSchemaModification::UnversionedUpdate:
                if (!config->ValidateSorted || !config->ValidateUniqueKeys) {
                    THROW_ERROR_EXCEPTION(
                        "\"schema_modification\" is allowed to be %Qlv only if "
                        "\"validate_sorted\" and \"validate_unique_keys\" are true",
                        config->SchemaModification);
                }
                break;

            case ETableSchemaModification::UnversionedUpdateUnsorted:
                THROW_ERROR_EXCEPTION("\"schema_modification\" is not allowed to be %Qlv",
                    config->SchemaModification);

            default:
                YT_ABORT();
        }

        if (config->ChunkFormat) {
            ValidateTableChunkFormatAndOptimizeFor(*config->ChunkFormat, config->OptimizeFor);
        }
    });
}

EChunkFormat TChunkWriterOptions::GetEffectiveChunkFormat(bool versioned) const
{
    return ChunkFormat.value_or(DefaultFormatFromOptimizeFor(OptimizeFor, versioned));
}

void TChunkWriterOptions::EnableValidationOptions(bool validateAnyIsValidYson)
{
    ValidateDuplicateIds = true;
    ValidateRowWeight = true;
    ValidateKeyWeight = true;
    ValidateColumnCount = true;
    ValidateAnyIsValidYson = validateAnyIsValidYson;
}

////////////////////////////////////////////////////////////////////////////////

void TVersionedRowDigestConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("t_digest", &TThis::TDigest)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
