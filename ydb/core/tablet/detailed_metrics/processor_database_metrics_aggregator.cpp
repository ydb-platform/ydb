#include "processor_database_metrics_aggregator.h"

#include "detailed_metrics_descriptor.h"
#include "detailed_metrics_tree.h"
#include "memory_tags.h"
#include "public_metrics_bucket.h"
#include "ydb_metrics_aggregator.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/prof/tag.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <utility>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr {
    namespace {

        using namespace NDetailedMetrics;

        using TMetricsSettings = NKikimrSchemeOp::TTableDetailedMetricsSettings;
        // Contributions use the relative path identifying their published table group.
        using TContribution = std::pair<TString, TBucketKey>;
        using TContributions = THashSet<TContribution>;
        using TNodeRoleKey = std::pair<ui32, bool>;

        TString SourceId(const TBucketKey& key) {
            return key ? TStringBuilder() << key->first << ':' << key->second : TString("table");
        }

        /**
         * Everything the processor keeps for a single table: the public metric values
         * of every bucket (TABLE partials and PARTITION leaves) and the table rollup.
         */
        struct TTableEntry {
            TTabletTypes::EType Type = TTabletTypes::TypeInvalid;
            const TDetailedMetricsDescriptor* Desc = nullptr;
            NMonitoring::TDynamicCounterPtr PublicGroup;
            TYdbMetricsAggregatorPtr Aggregator;
            THashMap<TBucketKey, THolder<TPublicBucket>> Buckets;
        };

        class TProcessorDatabaseMetricsAggregatorImpl: public TProcessorDatabaseMetricsAggregator {
        public:
            TProcessorDatabaseMetricsAggregatorImpl(
                NMonitoring::TDynamicCounterPtr targetCounterGroup,
                const TString& databasePath)
                : TargetCounterGroup(targetCounterGroup)
                , DatabasePrefix(ChopTrailingSlash(databasePath))
            {
            }

            void ApplyFromNode(
                ui32 nodeId,
                bool isFollowerRole,
                const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables) override {
                NProfiling::TMemoryTagScope memoryScope(ProcessorMemoryTag());
                TContributions contributions;
                for (const auto& table : tables) {
                    // The tablet type defines the slots of the public metric values: an entry without it
                    // is ignored, so the buckets the node reported for it before are retired below
                    if (!table.HasTabletType()) {
                        WarnMissingTabletType(nodeId, table.GetTablePath());
                        continue;
                    }
                    const TString path(MakeRelativeTablePath(DatabasePrefix, table.GetTablePath()));
                    const auto type = table.GetTabletType();
                    if (table.GetLevel() == TMetricsSettings::MetricsLevelTable && !isFollowerRole) {
                        const TContribution contribution(path, Nothing());
                        ApplyContribution(nodeId, contribution, type, table.GetTableMetrics(), contributions);
                    } else if (table.GetLevel() == TMetricsSettings::MetricsLevelPartition) {
                        for (const auto& leaf : table.GetLeaves()) {
                            if ((leaf.GetFollowerId() != 0) == isFollowerRole) {
                                const TContribution contribution(path, TTabletKey(leaf.GetTabletId(), leaf.GetFollowerId()));
                                ApplyContribution(nodeId, contribution, type, leaf.GetMetrics(), contributions);
                            }
                        }
                    }
                }
                // Register the new shape before retiring the old one, keeping the table
                // and its rollup alive throughout a gradual metrics-level change.
                ReconcileContributions({nodeId, isFollowerRole}, std::move(contributions));
            }

            void DropNode(ui32 nodeId) override {
                NProfiling::TMemoryTagScope memoryScope(ProcessorMemoryTag());
                ReconcileContributions({nodeId, false}, {});
                ReconcileContributions({nodeId, true}, {});
            }

            void RecalculateAllCounters() override {
                NProfiling::TMemoryTagScope memoryScope(ProcessorMemoryTag());
                for (auto& [_, table] : Tables) {
                    for (auto& [key, bucket] : table.Buckets) {
                        bucket->Publish();
                    }
                    table.Aggregator->RecalculateAllTargetCounters();
                }
            }

        private:
            /**
             * Apply the public metric values of a bucket reported by a node.
             */
            void ApplyContribution(
                ui32 nodeId,
                const TContribution& contribution,
                TTabletTypes::EType type,
                const NKikimrSysView::TDbCounters& values,
                TContributions& contributions) {
                TPublicBucket* bucket = GetOrCreateBucket(contribution, type);
                if (!bucket) {
                    return;
                }
                bucket->Apply(nodeId, values);
                LogWarnings(nodeId, contribution.first, bucket->TakeWarnings());
                contributions.insert(contribution);
            }

            /**
             * Get the bucket of the contribution, creating its table and the bucket as needed.
             *
             * @return The bucket, or nullptr if the contribution is ignored: no table path,
             *         a tablet type without detailed metrics, or a tablet type other than
             *         the one of the table
             */
            TPublicBucket* GetOrCreateBucket(const TContribution& contribution, TTabletTypes::EType type) {
                const auto& [path, key] = contribution;
                const auto* descriptor = GetDetailedMetricsDescriptor(type);
                if (path.empty() || !descriptor) {
                    return nullptr;
                }
                auto& table = Tables[path];
                if (table.Type == TTabletTypes::TypeInvalid) {
                    ReportDescriptorErrors(*descriptor);
                    table.Type = type;
                    table.Desc = descriptor;
                    table.PublicGroup = TargetCounterGroup->GetSubgroup(TABLE_LABEL, path);
                    table.Aggregator = CreateYdbMetricsAggregatorByTabletType(
                        type, table.PublicGroup, ECumulativeHistoryPolicy::RetainOnSourceRemoval);
                } else if (table.Type != type) {
                    return nullptr;
                }
                auto& bucket = table.Buckets[key];
                if (!bucket) {
                    // The partial's group is detached: only the combined table rollup
                    // is public, so partials and leaves never overwrite each other.
                    const auto group = key
                        ? GetOrCreateTabletGroup(table.PublicGroup, *key)
                        : MakeIntrusive<NMonitoring::TDynamicCounters>();
                    const EYdbMetricNameScope nameScope = key
                        ? EYdbMetricNameScope::Partition
                        : EYdbMetricNameScope::Aggregate;
                    const bool isFollowerSource = key && key->second != 0;
                    // The bucket creates every target, which the rollup looks up below
                    bucket = MakeHolder<TPublicBucket>(*table.Desc, group, key.Defined(), isFollowerSource);
                    table.Aggregator->AddSourceCountersGroup(SourceId(key), group, isFollowerSource, nameScope);
                }
                return bucket.Get();
            }

            /**
             * Report the errors of the descriptor of a tablet type (see TDetailedMetricsDescriptor::Errors),
             * each of which leaves a public metric publishing zero, once per tablet type: on the first
             * table of the type.
             */
            void ReportDescriptorErrors(const TDetailedMetricsDescriptor& descriptor) {
                if (descriptor.Errors.empty() || !TypesReportedDescriptorErrors.insert(descriptor.Type).second) {
                    return;
                }
                for (const auto& error : descriptor.Errors) {
                    YDB_LOG_CRIT("Invalid public detailed metric of the tablet type, it publishes zero",
                        {"database", DatabasePrefix},
                        {"tabletType", TTabletTypes::TypeToStr(descriptor.Type)},
                        {"error", error});
                }
            }

            void LogWarnings(ui32 nodeId, const TString& path, const TVector<TString>& warnings) const {
                for (const auto& warning : warnings) {
                    YDB_LOG_WARN("Problem with the detailed metrics reported by a node",
                        {"database", DatabasePrefix},
                        {"table", path},
                        {"nodeId", nodeId},
                        {"warning", warning});
                }
            }

            /**
             * Warn about a table entry reported without the tablet type, once per node,
             * so every node of an older version is visible in a mixed deployment.
             *
             * @note Every node sets the tablet type since the nodes report the public metric values.
             *       The entries without it carried the low level counters, which were never released,
             *       so such an entry comes from a node of an older trunk version only.
             */
            void WarnMissingTabletType(ui32 nodeId, const TString& tablePath) {
                if (!NodesWarnedMissingTabletType.insert(nodeId).second) {
                    return;
                }
                YDB_LOG_WARN("Ignored the detailed metrics of a table reported without the tablet type",
                    {"database", DatabasePrefix},
                    {"table", tablePath},
                    {"nodeId", nodeId});
            }

            void ReconcileContributions(const TNodeRoleKey& nodeRole, TContributions contributions) {
                auto& previous = ContributionsByNodeRole[nodeRole];
                for (const auto& contribution : previous) {
                    if (!contributions.contains(contribution)) {
                        RemoveContribution(nodeRole.first, contribution);
                    }
                }
                if (contributions.empty()) {
                    ContributionsByNodeRole.erase(nodeRole);
                } else {
                    previous = std::move(contributions);
                }
            }

            void RemoveContribution(ui32 nodeId, const TContribution& contribution) {
                const auto& [path, key] = contribution;
                auto tableIt = Tables.find(path);
                if (tableIt == Tables.end()) {
                    return;
                }
                auto& table = tableIt->second;
                auto bucketIt = table.Buckets.find(key);
                if (bucketIt == table.Buckets.end() || !bucketIt->second->DropNode(nodeId)) {
                    return;
                }
                // The last report may not have been published yet. Retain its cumulative
                // values in the table rollup before detaching this source.
                bucketIt->second->Publish();
                table.Aggregator->RemoveSourceCountersGroup(SourceId(key));
                table.Buckets.erase(bucketIt);
                if (key) {
                    table.PublicGroup->RemoveSubgroupChain(MakeTabletPath(*key));
                }
                if (table.Buckets.empty()) {
                    TargetCounterGroup->RemoveSubgroup(TABLE_LABEL, path);
                    Tables.erase(tableIt);
                }
            }

            NMonitoring::TDynamicCounterPtr TargetCounterGroup;
            const TString DatabasePrefix;
            THashMap<TString, TTableEntry> Tables;
            THashMap<TNodeRoleKey, TContributions> ContributionsByNodeRole;
            THashSet<ui32> NodesWarnedMissingTabletType;
            THashSet<TTabletTypes::EType> TypesReportedDescriptorErrors;
        };

    } // namespace

    TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath) {
        NProfiling::TMemoryTagScope memoryScope(NDetailedMetrics::ProcessorMemoryTag());
        return MakeIntrusive<TProcessorDatabaseMetricsAggregatorImpl>(targetCounterGroup, databasePath);
    }

} // namespace NKikimr
