#include "yql_yt_settings.h"

#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/regex/pcre/regexp.h>

#include <util/generic/yexception.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/compiler.h>

namespace NYql {

using namespace NCommon;

namespace {

// See https://wiki.yandex-team.ru/yt/userdoc/compression/#podderzhivaemyealgoritmyszhatija
const TRegExMatch CODECS("none|snappy|zlib_[1-9]|lz4(_high_compression)?|quick_lz|zstd_([1-9]|1[0-9]|2[0-1])|brotli_([1-9]|1[0-1])|lzma_[0-9]|bzip2_[1-9]");

} // namespace

bool ValidateCompressionCodecValue(const TStringBuf& codec) {
    return CODECS.Match(codec.data());
}

void MediaValidator(const NYT::TNode& value) {
    if (!value.IsMap()) {
        throw yexception() << "Expected yson map, but got " << value.GetType();
    }
    for (auto& p: value.AsMap()) {
        if (!p.second.IsMap()) {
            throw yexception() << "Expected yson map, but got " << p.second.GetType() << "\" for the " << p.first.Quote() << " key";
        }
        for (auto& pp: p.second.AsMap()) {
            if (pp.first == "replication_factor") {
                if (!pp.second.IsInt64()) {
                    throw yexception() << "Expected int64, but got \"" << pp.second.GetType() << "\" for the " << p.first << "." << pp.first << " key";
                }
            } else if (pp.first == "data_parts_only") {
                if (!pp.second.IsBool()) {
                    throw yexception() << "Expected bool, but got \"" << pp.second.GetType() << "\" for the " << p.first << "." << pp.first << " key";
                }
            } else {
                throw yexception() << "Expected key in "<< p.first <<".[replication_factor, data_parts_only], but got " << p.first << "." << pp.first << " key";
            }
        }
    }
}

TYtConfiguration::TYtConfiguration()
{
    const auto codecValidator = [] (const TString&, TString str) {
        if (!ValidateCompressionCodecValue(str)) {
            throw yexception() << "Bad codec value";
        }
    };

    const auto mediaValidator = [] (const TString&, const NYT::TNode& value) {
        MediaValidator(value);
    };

    REGISTER_SETTING(*this, Auth)
        .ValueSetter([this](const TString&, const TString& value) {
            Auth = value;
            for (auto& x: Tokens) {
                x.second = value;
            }
        });
    REGISTER_SETTING(*this, ExternalTx);
    REGISTER_SETTING(*this, TmpFolder);
    REGISTER_SETTING(*this, TablesTmpFolder);
    REGISTER_SETTING(*this, TempTablesTtl);
    REGISTER_SETTING(*this, KeepTempTables)
        .ValueSetter([this](const TString& cluster, bool value) {
            Y_UNUSED(cluster);
            if (value) {
                ReleaseTempData = EReleaseTempDataMode::Never;
            }
        });
    REGISTER_SETTING(*this, InflightTempTablesLimit);
    REGISTER_SETTING(*this, ReleaseTempData).Parser([](const TString& v) { return FromString<EReleaseTempDataMode>(v); });
    REGISTER_SETTING(*this, IgnoreYamrDsv);
    REGISTER_SETTING(*this, IgnoreWeakSchema);
    REGISTER_SETTING(*this, InferSchema)
        .Lower(1)
        .Upper(1000)
        .Parser([](const TString& v) { return v.empty() ? 1 : Max(1u, FromString<ui32>(v)); })
        .Validator([this](const TString&, ui32) {
            if (ForceInferSchema.Get()) {
                throw yexception() << "InferSchema cannot be used together with ForceInferSchema";
            }
        });

    REGISTER_SETTING(*this, ForceInferSchema)
        .Lower(1)
        .Upper(1000)
        .Parser([](const TString& v) { return v.empty() ? 1 : Max(1u, FromString<ui32>(v)); })
        .Validator([this](const TString&, ui32) {
            if (InferSchema.Get()) {
                throw yexception() << "ForceInferSchema cannot be used together with InferSchema";
            }
        });

    REGISTER_SETTING(*this, InferSchemaTableCountThreshold);

    REGISTER_SETTING(*this, QueryCacheMode).Parser([](const TString& v) { return FromString<EQueryCacheMode>(v); });
    REGISTER_SETTING(*this, QueryCacheIgnoreTableRevision);
    REGISTER_SETTING(*this, QueryCacheSalt);
    REGISTER_SETTING(*this, QueryCacheTtl);
    REGISTER_SETTING(*this, QueryCacheUseForCalc);
    REGISTER_SETTING(*this, QueryCacheUseExpirationTimeout);

    REGISTER_SETTING(*this, DefaultMemoryLimit);
    REGISTER_SETTING(*this, DefaultMemoryReserveFactor).Lower(0.0).Upper(1.0);
    REGISTER_SETTING(*this, DefaultMemoryDigestLowerBound).Lower(0.0).Upper(1.0);
    REGISTER_SETTING(*this, DefaultMaxJobFails);
    REGISTER_SETTING(*this, MaxRowWeight).Lower(1).Upper(128_MB);
    REGISTER_SETTING(*this, MaxKeyWeight).Lower(1).Upper(256_KB);
    REGISTER_SETTING(*this, BufferRowCount).Lower(1);
    REGISTER_SETTING(*this, DataSizePerJob);
    REGISTER_SETTING(*this, DataSizePerSortJob).Lower(10_MB);
    REGISTER_SETTING(*this, DataSizePerMapJob);
    REGISTER_SETTING(*this, DataSizePerPartition);
    REGISTER_SETTING(*this, DefaultLocalityTimeout);
    REGISTER_SETTING(*this, MapLocalityTimeout);
    REGISTER_SETTING(*this, ReduceLocalityTimeout);
    REGISTER_SETTING(*this, SortLocalityTimeout);
    REGISTER_SETTING(*this, MinLocalityInputDataWeight);
    REGISTER_SETTING(*this, MaxJobCount).Lower(1);
    REGISTER_SETTING(*this, UserSlots).Lower(1);
    REGISTER_SETTING(*this, Pool).NonEmpty();
    REGISTER_SETTING(*this, DefaultOperationWeight).Lower(0.0);
    REGISTER_SETTING(*this, DefaultMapSelectivityFactor).Lower(0.0);
    REGISTER_SETTING(*this, NightlyCompress);
    REGISTER_SETTING(*this, PublishedCompressionCodec).Validator(codecValidator);
    REGISTER_SETTING(*this, TemporaryCompressionCodec).Validator(codecValidator);
    // See https://wiki.yandex-team.ru/yt/userdoc/chunkowners/#replikacija
    REGISTER_SETTING(*this, PublishedErasureCodec).Parser([](const TString& v) { return FromString<NYT::EErasureCodecAttr>(v); });
    REGISTER_SETTING(*this, TemporaryErasureCodec).Parser([](const TString& v) { return FromString<NYT::EErasureCodecAttr>(v); });
    REGISTER_SETTING(*this, ClientMapTimeout).Deprecated();
    REGISTER_SETTING(*this, CoreDumpPath).NonEmpty();
    REGISTER_SETTING(*this, UseTmpfs);
    REGISTER_SETTING(*this, SuspendIfAccountLimitExceeded);
    REGISTER_SETTING(*this, ExtraTmpfsSize);
    REGISTER_SETTING(*this, OptimizeFor)
        .Parser([](const TString& v) {
            return FromString<NYT::EOptimizeForAttr>(v);
        });

    REGISTER_SETTING(*this, DefaultCluster)
        .Validator([this] (const TString&, TString value) {
            if (!ValidClusters.contains(value)) {
                throw yexception() << "Unknown cluster name: " << value;
            }
        });
    REGISTER_SETTING(*this, UseTypeV2)
        .ValueSetter([this](const TString& cluster, bool value) {
            Y_UNUSED(cluster);
            UseNativeYtTypes = value;
        })
        .Warning("Pragma UseTypeV2 is deprecated. Use UseNativeYtTypes instead");
    REGISTER_SETTING(*this, UseNativeYtTypes);
    REGISTER_SETTING(*this, UseNativeDescSort);
    REGISTER_SETTING(*this, UseIntermediateSchema);
    REGISTER_SETTING(*this, StaticPool);
    REGISTER_SETTING(*this, UseFlow)
        .ValueSetter([this](const TString&, bool value) {
            UseFlow = value;
            if (value) {
                UseSystemColumns = true;
            } else {
                MapJoinUseFlow = false;
            }
        });
    REGISTER_SETTING(*this, WideFlowLimit)
        .ValueSetter([this](const TString&, ui16 value) {
            WideFlowLimit = value;
            if (value > 0) {
                UseSystemColumns = true;
            }
        });
    REGISTER_SETTING(*this, ExpirationDeadline)
        .Lower(Now())
        .ValueSetter([this] (const TString& cluster, TInstant value) {
            ExpirationDeadline[cluster] = value;
        });
    REGISTER_SETTING(*this, ExpirationInterval)
        .ValueSetter([this] (const TString& cluster, TDuration value) {
            ExpirationInterval[cluster] = value;
        });
    REGISTER_SETTING(*this, ScriptCpu).Lower(1.0).GlobalOnly();
    REGISTER_SETTING(*this, PythonCpu).Lower(1.0).GlobalOnly();
    REGISTER_SETTING(*this, JavascriptCpu).Lower(1.0).GlobalOnly();
    REGISTER_SETTING(*this, ErasureCodecCpu).Lower(1.0);
    REGISTER_SETTING(*this, ErasureCodecCpuForDq).Lower(1.0);

    REGISTER_SETTING(*this, Owners)
        .NonEmpty()
        .ValueSetterWithRestore([this] (const TString& cluster, TSet<TString> owners) {
            if (ALL_CLUSTERS == cluster) {
                Owners.UpdateAll([&owners] (const TString&, TSet<TString>& val) {
                    val.insert(owners.begin(), owners.end());
                });
            } else {
                Owners[cluster].insert(owners.begin(), owners.end());
            }
        });
    REGISTER_SETTING(*this, OperationReaders).NonEmpty();
    REGISTER_SETTING(*this, SchedulingTag);
    REGISTER_SETTING(*this, SchedulingTagFilter);
    REGISTER_SETTING(*this, PoolTrees)
        .NonEmpty()
        .ValueSetter([this] (const TString& cluster, TSet<TString> trees) {
            HybridDqExecution = false;
            PoolTrees[cluster] = trees;
        });
    REGISTER_SETTING(*this, TentativePoolTrees).NonEmpty();
    REGISTER_SETTING(*this, TentativeTreeEligibilitySampleJobCount);
    REGISTER_SETTING(*this, TentativeTreeEligibilityMaxJobDurationRatio);
    REGISTER_SETTING(*this, TentativeTreeEligibilityMinJobDuration);
    REGISTER_SETTING(*this, UseDefaultTentativePoolTrees);
    REGISTER_SETTING(*this, IntermediateAccount).NonEmpty();
    REGISTER_SETTING(*this, IntermediateReplicationFactor).Lower(1).Upper(10);
    REGISTER_SETTING(*this, PublishedReplicationFactor).Lower(1).Upper(10);
    REGISTER_SETTING(*this, TemporaryReplicationFactor).Lower(1).Upper(10);
    REGISTER_SETTING(*this, AutoMerge).Enum({"relaxed", "economy", "disabled"})
        .ValueSetter([this](const TString& cluster, const TString& value) {
            PublishedAutoMerge[cluster] = value;
            TemporaryAutoMerge[cluster] = value;
        });

    REGISTER_SETTING(*this, PublishedAutoMerge).Enum({ "relaxed", "economy", "disabled" });
    REGISTER_SETTING(*this, TemporaryAutoMerge).Enum({ "relaxed", "economy", "disabled" });
    REGISTER_SETTING(*this, UseSkiff)
        .ValueSetter([this](const TString& cluster, bool value) {
            UseSkiff[cluster] = value;
            if (!value) {
                UseNativeYtTypes = false;
            }
        });
    REGISTER_SETTING(*this, TableContentCompressLevel).Upper(11);
    REGISTER_SETTING(*this, TableContentDeliveryMode).Parser([](const TString& v) { return FromString<ETableContentDeliveryMode>(v); });
    REGISTER_SETTING(*this, TableContentMaxChunksForNativeDelivery).Upper(1000);
    REGISTER_SETTING(*this, TableContentTmpFolder);
    REGISTER_SETTING(*this, TableContentColumnarStatistics);
    REGISTER_SETTING(*this, TableContentUseSkiff);
    REGISTER_SETTING(*this, TableContentLocalExecution);
    REGISTER_SETTING(*this, DisableJobSplitting);
    REGISTER_SETTING(*this, UseColumnarStatistics)
        .Parser([](const TString& v) {
            // backward compatible parse from bool
            bool value = true;
            if (!v || TryFromString<bool>(v, value)) {
                return value ? EUseColumnarStatisticsMode::Force : EUseColumnarStatisticsMode::Disable;
            } else {
                return FromString<EUseColumnarStatisticsMode>(v);
            }
        })
        ;
    REGISTER_SETTING(*this, ParallelOperationsLimit).Lower(1);
    REGISTER_SETTING(*this, DefaultCalcMemoryLimit);
    REGISTER_SETTING(*this, LayerPaths).NonEmpty()
        .ValueSetter([this](const TString& cluster, const TVector<TString>& value) {
            LayerPaths[cluster] = value;
            HybridDqExecution = false;
        });
    REGISTER_SETTING(*this, DockerImage).NonEmpty()
        .ValueSetter([this](const TString& cluster, const TString& value) {
            DockerImage[cluster] = value;
            HybridDqExecution = false;
        });
    REGISTER_SETTING(*this, _EnableDq);
    // Deprecated. Use MaxInputTables instead
    REGISTER_SETTING(*this, ExtendTableLimit).Lower(2).Upper(3000)
        .ValueSetter([this] (const TString& cluster, ui32 value) {
            Y_UNUSED(cluster);
            MaxInputTables = value;
        })
        .Warning("Pragma ExtendTableLimit is deprecated. Use MaxInputTables instead");
    REGISTER_SETTING(*this, CommonJoinCoreLimit);
    REGISTER_SETTING(*this, CombineCoreLimit).Lower(1_MB); // Min 1Mb
    REGISTER_SETTING(*this, SwitchLimit).Lower(1_MB); // Min 1Mb
    REGISTER_SETTING(*this, JoinMergeTablesLimit);
    REGISTER_SETTING(*this, JoinMergeUseSmallAsPrimary);
    REGISTER_SETTING(*this, JoinMergeReduceJobMaxSize).Lower(1); // YT requires max_data_size_per_job to be > 0, YT default is 200GB
    REGISTER_SETTING(*this, JoinMergeUnsortedFactor).Lower(0.0);
    REGISTER_SETTING(*this, JoinMergeForce);
    REGISTER_SETTING(*this, MapJoinLimit);
    REGISTER_SETTING(*this, MapJoinShardMinRows);
    REGISTER_SETTING(*this, MapJoinShardCount).Lower(1).Upper(10);
    REGISTER_SETTING(*this, MapJoinUseFlow);
    REGISTER_SETTING(*this, EvaluationTableSizeLimit).Upper(10_MB); // Max 10Mb
    REGISTER_SETTING(*this, LookupJoinLimit).Upper(10_MB); // Same as EvaluationTableSizeLimit
    REGISTER_SETTING(*this, LookupJoinMaxRows).Upper(1000);
    REGISTER_SETTING(*this, DisableOptimizers);
    REGISTER_SETTING(*this, MaxInputTables).Lower(2).Upper(3000); // 3000 - default max limit on YT clusters
    REGISTER_SETTING(*this, MaxOutputTables).Lower(1).Upper(100); // https://ml.yandex-team.ru/thread/yt/166633186212752141/
    REGISTER_SETTING(*this, MaxInputTablesForSortedMerge).Lower(2).Upper(1000); // https://st.yandex-team.ru/YTADMINREQ-16742
    REGISTER_SETTING(*this, MaxExtraJobMemoryToFuseOperations);
    REGISTER_SETTING(*this, MaxReplicationFactorToFuseOperations).Lower(1.0);
    REGISTER_SETTING(*this, MaxOperationFiles).Lower(2).Upper(1000);
    REGISTER_SETTING(*this, GeobaseDownloadUrl);
    REGISTER_SETTING(*this, MinPublishedAvgChunkSize);
    REGISTER_SETTING(*this, MinTempAvgChunkSize);
    REGISTER_SETTING(*this, TopSortMaxLimit);
    REGISTER_SETTING(*this, TopSortSizePerJob).Lower(1);
    REGISTER_SETTING(*this, TopSortRowMultiplierPerJob).Lower(1);
    REGISTER_SETTING(*this, JoinUseColumnarStatistics)
        .ValueSetter([this](const TString& arg, bool value) {
            Y_UNUSED(arg);
            if (!value) {
                JoinCollectColumnarStatistics = EJoinCollectColumnarStatisticsMode::Disable;
            }
        })
        .Warning("Pragma JoinUseColumnarStatistics is deprecated. Use JoinCollectColumnarStatistics instead");
    REGISTER_SETTING(*this, JoinCollectColumnarStatistics)
        .Parser([](const TString& v) { return FromString<EJoinCollectColumnarStatisticsMode>(v); });
    REGISTER_SETTING(*this, JoinColumnarStatisticsFetcherMode)
        .Parser([](const TString& v) { return FromString<NYT::EColumnarStatisticsFetcherMode>(v); });
    REGISTER_SETTING(*this, JoinWaitAllInputs);
    REGISTER_SETTING(*this, JoinAllowColumnRenames);
    REGISTER_SETTING(*this, JoinMergeSetTopLevelFullSort);
    REGISTER_SETTING(*this, JoinEnableStarJoin);
    REGISTER_SETTING(*this, JobEnv)
        .Parser([](const TString& v) { return NYT::NodeFromYsonString(v, ::NYson::EYsonType::Node); })
        .Validator([] (const TString&, const NYT::TNode& value) {
            if (!value.IsMap()) {
                throw yexception() << "Expected yson map, but got " << value.GetType();
            }
            for (auto& p: value.AsMap()) {
                if (!p.second.IsString()) {
                    throw yexception() << "Expected string, but got \"" << p.second.GetType() << "\" for the " << p.first.Quote() << " key";
                }
            }
        });
    REGISTER_SETTING(*this, OperationSpec)
        .Parser([](const TString& v) { return NYT::NodeFromYsonString(v, ::NYson::EYsonType::Node); })
        .Validator([] (const TString&, const NYT::TNode& value) {
            if (!value.IsMap()) {
                throw yexception() << "Expected yson map, but got " << value.GetType();
            }
        })
        .ValueSetter([this](const TString& cluster, const NYT::TNode& spec) {
            OperationSpec[cluster] = spec;
            HybridDqExecution = false;
        });
    REGISTER_SETTING(*this, Annotations)
        .Parser([](const TString& v) { return NYT::NodeFromYsonString(v); })
        .Validator([] (const TString&, const NYT::TNode& value) {
            if (!value.IsMap()) {
                throw yexception() << "Expected yson map, but got " << value.GetType();
            }
        });
    REGISTER_SETTING(*this, Description)
        .Parser([](const TString& v) { return NYT::NodeFromYsonString(v); })
        .Validator([] (const TString&, const NYT::TNode& value) {
            if (!value.IsMap()) {
                throw yexception() << "Expected yson map, but got " << value.GetType();
            }
        });
    REGISTER_SETTING(*this, StartedBy)
        .Parser([](const TString& v) { return NYT::NodeFromYsonString(v); })
        .Validator([] (const TString&, const NYT::TNode& value) {
            if (!value.IsMap()) {
                throw yexception() << "Expected yson map, but got " << value.GetType();
            }
        });
    REGISTER_SETTING(*this, MaxSpeculativeJobCountPerTask);
    REGISTER_SETTING(*this, LLVMMemSize);
    REGISTER_SETTING(*this, LLVMPerNodeMemSize);
    REGISTER_SETTING(*this, LLVMNodeCountLimit);
    REGISTER_SETTING(*this, SamplingIoBlockSize);
    REGISTER_SETTING(*this, BinaryTmpFolder);
    REGISTER_SETTING(*this, BinaryExpirationInterval);
    REGISTER_SETTING(*this, FolderInlineDataLimit);
    REGISTER_SETTING(*this, FolderInlineItemsLimit);
    REGISTER_SETTING(*this, TableContentMinAvgChunkSize);
    REGISTER_SETTING(*this, TableContentMaxInputTables);
    REGISTER_SETTING(*this, UseSystemColumns)
        .ValueSetter([this](const TString&, bool value) {
            UseSystemColumns = value;
            if (!value) {
                UseFlow = false;
                WideFlowLimit = 0;
            }
        });
    REGISTER_SETTING(*this, PublishedMedia)
        .Parser([](const TString& v) { return NYT::NodeFromYsonString(v, ::NYson::EYsonType::Node); })
        .Validator(mediaValidator);
    REGISTER_SETTING(*this, TemporaryMedia)
        .Parser([](const TString& v) { return NYT::NodeFromYsonString(v, ::NYson::EYsonType::Node); })
        .Validator(mediaValidator);
    REGISTER_SETTING(*this, PublishedPrimaryMedium);
    REGISTER_SETTING(*this, TemporaryPrimaryMedium);
    REGISTER_SETTING(*this, IntermediateDataMedium);
    REGISTER_SETTING(*this, PrimaryMedium).ValueSetter([this](const TString& cluster, const TString& value) {
        PublishedPrimaryMedium[cluster] = value;
        TemporaryPrimaryMedium[cluster] = value;
        IntermediateDataMedium[cluster] = value;
    });
    REGISTER_SETTING(*this, QueryCacheChunkLimit);
    REGISTER_SETTING(*this, IgnoreTypeV3);
    REGISTER_SETTING(*this, HybridDqExecution);
    REGISTER_SETTING(*this, HybridDqDataSizeLimitForOrdered);
    REGISTER_SETTING(*this, HybridDqDataSizeLimitForUnordered);
    REGISTER_SETTING(*this, HybridDqExecutionFallback);
    REGISTER_SETTING(*this, NativeYtTypeCompatibility)
        .Parser([](const TString& v) {
            ui64 res = 0;
            TVector<TString> vec;
            StringSplitter(v).SplitBySet(",;| ").AddTo(&vec);
            for (auto& s: vec) {
                if (s.empty()) {
                    throw yexception() << "Empty value item";
                }
                res |= FromString<ENativeTypeCompatFlags>(s);
            }
            return res;
        });
    REGISTER_SETTING(*this, _UseKeyBoundApi);
    REGISTER_SETTING(*this, UseYqlRowSpecCompactForm);
    REGISTER_SETTING(*this, UseNewPredicateExtraction);
    REGISTER_SETTING(*this, PruneKeyFilterLambda);
    REGISTER_SETTING(*this, DqPruneKeyFilterLambda);
    REGISTER_SETTING(*this, MergeAdjacentPointRanges);
    REGISTER_SETTING(*this, KeyFilterForStartsWith);
    REGISTER_SETTING(*this, MaxKeyRangeCount).Upper(1000);
    REGISTER_SETTING(*this, MaxChunksForDqRead).Lower(1);
    REGISTER_SETTING(*this, NetworkProject);
    REGISTER_SETTING(*this, FileCacheTtl);
    REGISTER_SETTING(*this, _ImpersonationUser);
    REGISTER_SETTING(*this, InferSchemaMode).Parser([](const TString& v) { return FromString<EInferSchemaMode>(v); });
    REGISTER_SETTING(*this, BatchListFolderConcurrency).Lower(1); // Upper bound on concurrent batch folder list requests https://yt.yandex-team.ru/docs/api/commands#execute_batch
    REGISTER_SETTING(*this, ForceTmpSecurity);
    REGISTER_SETTING(*this, JoinCommonUseMapMultiOut);
    REGISTER_SETTING(*this, _EnableYtPartitioning);
    REGISTER_SETTING(*this, UseAggPhases);
    REGISTER_SETTING(*this, UsePartitionsByKeysForFinalAgg);
    REGISTER_SETTING(*this, _ForceJobSizeAdjuster);
    REGISTER_SETTING(*this, _EnableWriteReorder);
    REGISTER_SETTING(*this, EnforceJobUtc);
    REGISTER_SETTING(*this, UseRPCReaderInDQ);
    REGISTER_SETTING(*this, DQRPCReaderInflight).Lower(1);
    REGISTER_SETTING(*this, DQRPCReaderTimeout);
    REGISTER_SETTING(*this, BlockReaderSupportedTypes);
    REGISTER_SETTING(*this, BlockReaderSupportedDataTypes)
        .Parser([](const TString& v) {
            TSet<TString> vec;
            StringSplitter(v).SplitBySet(",").AddTo(&vec);
            TSet<NUdf::EDataSlot> res;
            for (auto& s: vec) {
                res.emplace(NUdf::GetDataSlot(s));
            }
            return res;
        });
    REGISTER_SETTING(*this, MaxCpuUsageToFuseMultiOuts).Lower(1.0);
    REGISTER_SETTING(*this, MaxReplicationFactorToFuseMultiOuts).Lower(1.0);
    REGISTER_SETTING(*this, ApplyStoredConstraints)
        .Parser([](const TString& v) {
            ui64 res = 0;
            TVector<TString> vec;
            StringSplitter(v).SplitBySet(",;| ").AddTo(&vec);
            for (auto& s: vec) {
                if (s.empty()) {
                    throw yexception() << "Empty value item";
                }
                res |= ui64(FromString<EStoredConstraint>(s));
            }
            return res;
        });
    REGISTER_SETTING(*this, ViewIsolation);
    REGISTER_SETTING(*this, PartitionByConstantKeysViaMap);
    REGISTER_SETTING(*this, ColumnGroupMode)
        .Parser([](const TString& v) {
            return FromString<EColumnGroupMode>(v);
        });
    REGISTER_SETTING(*this, MinColumnGroupSize).Lower(2);
    REGISTER_SETTING(*this, MaxColumnGroups);
}

EReleaseTempDataMode GetReleaseTempDataMode(const TYtSettings& settings) {
    return settings.ReleaseTempData.Get().GetOrElse(EReleaseTempDataMode::Finish);
}

EJoinCollectColumnarStatisticsMode GetJoinCollectColumnarStatisticsMode(const TYtSettings& settings) {
    return settings.JoinCollectColumnarStatistics.Get().GetOrElse(EJoinCollectColumnarStatisticsMode::Async);
}

TYtSettings::TConstPtr TYtConfiguration::Snapshot() const {
    return std::make_shared<const TYtSettings>(*this);
}

size_t TYtVersionedConfiguration::FindNodeVer(const TExprNode& node) {
    auto it = NodeIdToVer.find(node.UniqueId());
    if (it != NodeIdToVer.end()) {
        return it->second;
    }

    size_t ver = 0;
    for (auto& child: node.Children()) {
        ver = Max<size_t>(ver, FindNodeVer(*child));
    }
    NodeIdToVer.emplace(node.UniqueId(), ver);
    return ver;
}

void TYtVersionedConfiguration::FreezeZeroVersion() {
    if (Y_UNLIKELY(FrozenSettings.empty())) {
        FrozenSettings.push_back(Snapshot());
    }
}

void TYtVersionedConfiguration::PromoteVersion(const TExprNode& node) {
    NodeIdToVer[node.UniqueId()] = FrozenSettings.size();
    FrozenSettings.push_back(Snapshot());
}

TYtSettings::TConstPtr TYtVersionedConfiguration::GetSettingsForNode(const TExprNode& node) {
    FreezeZeroVersion();
    size_t ver = FindNodeVer(node);
    YQL_CLOG(DEBUG, ProviderYt) << "Using settings ver." << ver;
    return FrozenSettings.at(ver);
}

TYtSettings::TConstPtr TYtVersionedConfiguration::GetSettingsVer(size_t ver) {
    FreezeZeroVersion();
    YQL_CLOG(DEBUG, ProviderYt) << "Using settings ver." << ver;
    return FrozenSettings.at(ver);
}

void TYtVersionedConfiguration::ClearVersions() {
    FrozenSettings.clear();
    NodeIdToVer.clear();
}

}
