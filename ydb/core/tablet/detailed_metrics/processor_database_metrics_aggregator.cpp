#include "processor_database_metrics_aggregator.h"

#include "detailed_metrics_tree.h"
#include "memory_tags.h"
#include "ydb_metrics_aggregator.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

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

        // The estimated JSON output of public series, as NMonitoring::ToJson writes them
        struct TJsonSize {
            ui64 Bytes = 0;
            ui64 Series = 0;

            TJsonSize& operator+=(const TJsonSize& other) {
                Bytes += other.Bytes;
                Series += other.Series;
                return *this;
            }

            TJsonSize& operator-=(const TJsonSize& other) {
                Bytes -= other.Bytes;
                Series -= other.Series;
                return *this;
            }
        };

        // The widest ui64: values at max width keep the estimate value-independent
        constexpr ui64 JSON_VALUE_BYTES = 20;

        // ,"name":"value" per label (HEM_UNSAFE escapes no path character)
        ui64 JsonLabelBytes(const TSubgroupPath& labels) {
            ui64 bytes = 0;
            for (const auto& [name, value] : labels) {
                bytes += name.size() + value.size() + 6;
            }
            return bytes;
        }

        // Sizes the series of a group without subgroups as the compact JSON encoder writes them:
        // ,{"kind":"K","labels":{...},"value":V} or ,"hist":{"bounds":[...],"buckets":[...],"inf":V}
        class TJsonSizeEstimator final: public NMonitoring::ICountableConsumer {
        public:
            explicit TJsonSizeEstimator(ui64 groupLabelBytes)
                : GroupLabelBytes(groupLabelBytes)
            {
            }

            void OnCounter(
                const TString& labelName, const TString& labelValue,
                const NMonitoring::TCounterForPtr* counter) override {
                Add(counter->ForDerivative() ? "RATE" : "GAUGE", labelName, labelValue, 9 + JSON_VALUE_BYTES);
            }

            void OnHistogram(
                const TString& labelName, const TString& labelValue,
                NMonitoring::IHistogramSnapshotPtr snapshot, bool derivative) override {
                // The explicit histograms end with +Inf, written as "inf"
                const ui32 bounds = snapshot->Count() - 1;
                ui64 bytes = 39 + 2 * bounds + (bounds + 1) * JSON_VALUE_BYTES;
                for (ui32 i = 0; i < bounds; ++i) {
                    bytes += FloatToString(snapshot->UpperBound(i), PREC_NDIGITS, 10).size();
                }
                Add(derivative ? "HIST_RATE" : "HIST", labelName, labelValue, bytes);
            }

            void OnGroupBegin(const TString&, const TString&, const NMonitoring::TDynamicCounters*) override {
            }

            void OnGroupEnd(const TString&, const TString&, const NMonitoring::TDynamicCounters*) override {
            }

            NMonitoring::TCountableBase::EVisibility Visibility() const override {
                return NMonitoring::TCountableBase::EVisibility::Public;
            }

            TJsonSize Size;

        private:
            void Add(TStringBuf kind, const TString& labelName, const TString& labelValue, ui64 valueBytes) {
                Size.Bytes += 23 + kind.size() + GroupLabelBytes + labelName.size() + labelValue.size() + 6 + valueBytes;
                ++Size.Series;
            }

            const ui64 GroupLabelBytes;
        };

        // @param[in] labels The labels of the group below the target counter group
        TJsonSize EstimateJson(const NMonitoring::TDynamicCounters& group, const TSubgroupPath& labels) {
            TJsonSizeEstimator estimator(JsonLabelBytes(labels));
            group.Accept(TString(), TString(), estimator);
            return estimator.Size;
        }

        // The ydb_detailed output limit of one scrape endpoint, shared by its aggregators across actors.
        // Rollups always publish; members fold by leaf bytes desc, then database path, until the total fits.
        class TOutputBudget: public TThrRefBase {
        public:
            explicit TOutputBudget(NMonitoring::TDynamicCounterPtr scope)
                : Scope(std::move(scope))
            {
            }

            // Joins the budget of the scope, a private one if null; Join and the last Leave are atomic
            static TIntrusivePtr<TOutputBudget> Join(
                const NMonitoring::TDynamicCounterPtr& scope, const TString& database, ui64& memberId);

            void Leave(ui64 memberId);

            void SetLimit(ui64 maxBytes, ui64 restorePercent);

            // Records the unfolded output of the member; true when the member has to fold.
            // A member folded by the budget restores only at or below RestorePercent of the limit.
            bool MustFold(ui64 memberId, ui64 rollupBytes, ui64 leafBytes, bool foldedByBudget);

        private:
            struct TMember {
                TString Database;
                ui64 RollupBytes = 0;
                ui64 LeafBytes = 0;
            };

            // The registry key, held so that its address is not reused while registered
            const NMonitoring::TDynamicCounterPtr Scope;
            ui64 Limit = Max<ui64>();
            ui64 RestorePercent = 100;
            THashMap<ui64, TMember> Members;
        };

        struct TBudgetRegistry {
            // Guards the registry and the state of every budget
            TMutex Mutex;
            THashMap<const NMonitoring::TDynamicCounters*, TIntrusivePtr<TOutputBudget>> Budgets;
            ui64 LastMemberId = 0;
        };

        TBudgetRegistry& GetBudgetRegistry() {
            // Leaky: an aggregator may outlive the static destruction
            static auto* registry = new TBudgetRegistry;
            return *registry;
        }

        TIntrusivePtr<TOutputBudget> TOutputBudget::Join(
            const NMonitoring::TDynamicCounterPtr& scope, const TString& database, ui64& memberId) {
            auto& registry = GetBudgetRegistry();
            auto guard = Guard(registry.Mutex);
            TIntrusivePtr<TOutputBudget> budget;
            if (scope) {
                auto& entry = registry.Budgets[scope.Get()];
                if (!entry) {
                    entry = MakeIntrusive<TOutputBudget>(scope);
                }
                budget = entry;
            } else {
                budget = MakeIntrusive<TOutputBudget>(nullptr);
            }
            memberId = ++registry.LastMemberId;
            budget->Members[memberId].Database = database;
            return budget;
        }

        void TOutputBudget::Leave(ui64 memberId) {
            auto& registry = GetBudgetRegistry();
            auto guard = Guard(registry.Mutex);
            Members.erase(memberId);
            if (Members.empty() && Scope) {
                // The caller still holds this budget
                registry.Budgets.erase(Scope.Get());
            }
        }

        void TOutputBudget::SetLimit(ui64 maxBytes, ui64 restorePercent) {
            auto guard = Guard(GetBudgetRegistry().Mutex);
            Limit = maxBytes;
            RestorePercent = restorePercent;
        }

        bool TOutputBudget::MustFold(ui64 memberId, ui64 rollupBytes, ui64 leafBytes, bool foldedByBudget) {
            auto guard = Guard(GetBudgetRegistry().Mutex);
            auto& self = Members[memberId];
            self.RollupBytes = rollupBytes;
            self.LeafBytes = leafBytes;
            if (!leafBytes) {
                return false;
            }
            // Greedy in rank order: the members ranked before this one fold first, then this one if still over
            ui64 total = 0;
            for (const auto& [id, member] : Members) {
                total += member.RollupBytes + member.LeafBytes;
                const bool rankedBefore = member.LeafBytes != self.LeafBytes ? member.LeafBytes > self.LeafBytes
                    : member.Database != self.Database ? member.Database < self.Database
                    : id < memberId;
                if (rankedBefore) {
                    total -= member.LeafBytes;
                }
            }
            // Limit * RestorePercent / 100 without overflow
            const ui64 threshold = foldedByBudget
                ? Limit / 100 * RestorePercent + Limit % 100 * RestorePercent / 100
                : Limit;
            return total > threshold;
        }

        // A TABLE partial or a PARTITION leaf, fed with public metric values (see NKikimrSysView::TDbCounters).
        // Rate and derivative histogram deltas accumulate and outlive a removed node while the bucket is live.
        // Gauges and non-derivative histograms are the full values of each node, replaced by every report
        // and combined on publish. The input is remote: out of range slots and buckets are ignored, the
        // payload sizes nothing.
        class TPublishedBucket {
        public:
            TPublishedBucket(
                const TDetailedMetricsDescriptor& desc,
                NMonitoring::TDynamicCounterPtr targetGroup,
                EYdbMetricNameScope nameScope,
                bool isFollowerSource)
                : Group(targetGroup)
                , Desc(desc)
                , IsPartitionBucket(nameScope == EYdbMetricNameScope::Partition)
                , RateTotals(desc.Rates.size(), 0)
                , DerivativeTotals(desc.Histograms.size())
            {
                // The same targets as the YDB metrics mapper creates: the rollup looks them up
                const auto getName = [&](const TMetricSpec& spec) {
                    return MakeYdbMetricName(spec.Name, nameScope);
                };
                const auto isSkipped = [&](const TMetricSpec& spec) {
                    return isFollowerSource && spec.LeaderOnly;
                };
                for (const auto& spec : desc.Gauges) {
                    auto& target = Gauges.emplace_back();
                    if (!isSkipped(spec)) {
                        target = targetGroup->GetNamedCounter("name", getName(spec), false /* derivative */);
                    }
                }
                for (const auto& spec : desc.Rates) {
                    auto& target = Rates.emplace_back();
                    if (!isSkipped(spec)) {
                        target = targetGroup->GetNamedCounter("name", getName(spec), true /* derivative */);
                    }
                }
                for (size_t i = 0; i < desc.Histograms.size(); ++i) {
                    const auto& spec = desc.Histograms[i];
                    auto& target = Histograms.emplace_back();
                    if (!isSkipped(spec)) {
                        target = targetGroup->GetNamedHistogram("name", getName(spec),
                            NMonitoring::ExplicitHistogram(NMonitoring::TBucketBounds(spec.Bounds.begin(), spec.Bounds.end())),
                            false /* derivative */);
                    }
                    if (!spec.NonDerivative) {
                        DerivativeTotals[i].resize(spec.BucketCount(), 0);
                    }
                }
            }

            void Apply(ui32 nodeId, const NKikimrSysView::TDbCounters& values) {
                auto& node = PerNode[nodeId];
                node.Gauges.assign(Desc.Gauges.size(), 0);
                for (size_t i = 0; i < node.Gauges.size() && i < static_cast<size_t>(values.SimpleSize()); ++i) {
                    node.Gauges[i] = values.GetSimple(i);
                }
                const auto& cumulative = values.GetCumulative();
                for (int pair = 0; pair + 1 < cumulative.size(); pair += 2) {
                    if (cumulative[pair] < RateTotals.size()) {
                        RateTotals[cumulative[pair]] += cumulative[pair + 1];
                    }
                }
                node.NonDerivativeHists.resize(Desc.Histograms.size());
                for (size_t i = 0; i < Desc.Histograms.size(); ++i) {
                    ApplyHistogram(nodeId, i, i < static_cast<size_t>(values.HistogramSize()) ? &values.GetHistogram(i) : nullptr, node.NonDerivativeHists[i]);
                }
            }

            bool DropNode(ui32 nodeId) {
                PerNode.erase(nodeId);
                return PerNode.empty();
            }

            void Publish() {
                for (size_t i = 0; i < Gauges.size(); ++i) {
                    if (!Gauges[i]) {
                        continue;
                    }
                    // The partials of a table come from different tablets, so they add up unless CombineByMax;
                    // overlapping owners of a leaf use MAX, retaining the partition-move behavior
                    const bool combineByMax = IsPartitionBucket || Desc.Gauges[i].CombineByMax();
                    ui64 value = 0;
                    for (const auto& [_, node] : PerNode) {
                        value = combineByMax ? Max(value, node.Gauges[i]) : value + node.Gauges[i];
                    }
                    Gauges[i]->Set(value);
                }
                for (size_t i = 0; i < Rates.size(); ++i) {
                    if (Rates[i]) {
                        Rates[i]->Set(RateTotals[i]);
                    }
                }
                for (size_t i = 0; i < Histograms.size(); ++i) {
                    if (!Histograms[i]) {
                        continue;
                    }
                    const auto& spec = Desc.Histograms[i];
                    const bool nonDerivative = spec.NonDerivative;
                    Histograms[i]->Reset();
                    for (size_t bucket = 0; bucket < spec.BucketCount(); ++bucket) {
                        ui64 count = nonDerivative ? 0 : DerivativeTotals[i][bucket];
                        if (nonDerivative) {
                            for (const auto& [_, node] : PerNode) {
                                count += node.NonDerivativeHists[i][bucket];
                            }
                        }
                        if (count) {
                            // The upper bound of the bucket, as the YDB metrics mapper collects
                            Histograms[i]->Collect(bucket < spec.Bounds.size() ? spec.Bounds[bucket] : Max<double>(), count);
                        }
                    }
                }
            }

            // The mapped group: public under tablet_id/follower_id unless folded
            const NMonitoring::TDynamicCounterPtr Group;
            // The estimated output of a leaf, empty for the TABLE partial
            TJsonSize Size;

        private:
            struct TNodeSnapshot {
                TVector<ui64> Gauges;
                // Per histogram, empty for a derivative one
                TVector<TVector<ui64>> NonDerivativeHists;
            };

            void ApplyHistogram(
                ui32 nodeId, size_t index, const NKikimrSysView::TDbCounters::THistogram* histogram, TVector<ui64>& nodeHist)
            {
                const auto& spec = Desc.Histograms[index];
                const bool nonDerivative = spec.NonDerivative;
                if (nonDerivative) {
                    nodeHist.assign(spec.BucketCount(), 0);
                }
                if (!histogram) {
                    return;
                }
                if (histogram->GetNonDerivative() != nonDerivative) {
                    // A delta cannot be applied without the baseline it was taken against
                    if (!WarnedAboutNonDerivativeMismatch) {
                        WarnedAboutNonDerivativeMismatch = true;
                        YDB_LOG_WARN("Ignored a histogram, whose NonDerivative mark does not match the metric",
                            {"nodeId", nodeId},
                            {"histogram", spec.Name});
                    }
                    return;
                }
                auto& values = nonDerivative ? nodeHist : DerivativeTotals[index];
                const ui64 bucketCount = Min<ui64>(histogram->GetBucketsCount(), values.size());
                const auto& encoded = histogram->GetBuckets();
                for (int b = 0; b + 1 < encoded.size(); b += 2) {
                    if (encoded[b] >= bucketCount) {
                        continue;
                    }
                    if (nonDerivative) {
                        values[encoded[b]] = encoded[b + 1];
                    } else {
                        values[encoded[b]] += encoded[b + 1];
                    }
                }
            }

            const TDetailedMetricsDescriptor& Desc;
            const bool IsPartitionBucket;
            TVector<NMonitoring::TDynamicCounters::TCounterPtr> Gauges;
            TVector<NMonitoring::TDynamicCounters::TCounterPtr> Rates;
            TVector<NMonitoring::THistogramPtr> Histograms;
            TVector<ui64> RateTotals;
            // Per histogram, empty for a non-derivative one
            TVector<TVector<ui64>> DerivativeTotals;
            THashMap<ui32, TNodeSnapshot> PerNode;
            bool WarnedAboutNonDerivativeMismatch = false;
        };

        struct TTableEntry {
            TTabletTypes::EType Type = TTabletTypes::TypeInvalid;
            NMonitoring::TDynamicCounterPtr PublicGroup;
            TYdbMetricsAggregatorPtr Aggregator;
            THashMap<TBucketKey, THolder<TPublishedBucket>> Buckets;
            TJsonSize RollupSize;
        };

        class TProcessorDatabaseMetricsAggregatorImpl: public TProcessorDatabaseMetricsAggregator {
        public:
            TProcessorDatabaseMetricsAggregatorImpl(
                NMonitoring::TDynamicCounterPtr targetCounterGroup,
                const TString& databasePath,
                TDetailedMetricsDescriptorGetter getDescriptor,
                NMonitoring::TDynamicCounterPtr budgetScope)
                : TargetCounterGroup(targetCounterGroup)
                , DatabasePrefix(ChopTrailingSlash(databasePath))
                , GetDescriptor(getDescriptor)
                , Budget(TOutputBudget::Join(budgetScope, DatabasePrefix, MemberId))
                // A scoped aggregator starts folded: no leaf flash after an SVP restart or move
                , Folded(budgetScope != nullptr)
            {
            }

            ~TProcessorDatabaseMetricsAggregatorImpl() override {
                Budget->Leave(MemberId);
            }

            void ApplyFromNode(
                ui32 nodeId,
                bool isFollowerRole,
                const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables) override {
                NProfiling::TMemoryTagScope memoryScope(ProcessorMemoryTag());
                TContributions contributions;
                for (const auto& table : tables) {
                    if (!table.HasTabletType()) {
                        continue;
                    }
                    const TString path(MakeRelativeTablePath(DatabasePrefix, table.GetTablePath()));
                    const auto type = table.GetTabletType();
                    if (table.GetLevel() == TMetricsSettings::MetricsLevelTable && !isFollowerRole) {
                        ApplyContribution(nodeId, {path, Nothing()}, type, table.GetTableMetrics(), contributions);
                    } else if (table.GetLevel() == TMetricsSettings::MetricsLevelPartition) {
                        for (const auto& leaf : table.GetLeaves()) {
                            if ((leaf.GetFollowerId() != 0) == isFollowerRole) {
                                ApplyContribution(nodeId, {path, TTabletKey(leaf.GetTabletId(), leaf.GetFollowerId())},
                                                  type, leaf.GetMetrics(), contributions);
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
                Fit(true);
                // Folded leaves keep publishing: they feed the rollup
                for (auto& [_, table] : Tables) {
                    for (auto& [key, bucket] : table.Buckets) {
                        bucket->Publish();
                    }
                    table.Aggregator->RecalculateAllTargetCounters();
                }
            }

            void SetOutputLimit(ui64 maxBytes, ui64 restorePercent, const TSubgroupPath& prefixLabels) override {
                NProfiling::TMemoryTagScope memoryScope(ProcessorMemoryTag());
                Budget->SetLimit(maxBytes, restorePercent);
                PrefixLabelBytes = JsonLabelBytes(prefixLabels);
                Fit(false);
            }

            void FitOutput() override {
                NProfiling::TMemoryTagScope memoryScope(ProcessorMemoryTag());
                Fit(false);
            }

            ui64 GetEstimatedOutputBytes() const override {
                return Bytes(Rollups) + Bytes(Leaves);
            }

        private:
            ui64 Bytes(const TJsonSize& size) const {
                return size.Bytes + size.Series * PrefixLabelBytes;
            }

            // Folds as soon as the budget says so; restores after DETAILED_OUTPUT_RESTORE_TICKS fitting ticks in a row
            void Fit(bool tick) {
                if (Budget->MustFold(MemberId, Bytes(Rollups), Bytes(Leaves), FoldedByBudget)) {
                    // A confirmed start fold restores through the dead band too
                    FoldedByBudget = true;
                    FitTicks = 0;
                    if (!Folded) {
                        SetFolded(true);
                    }
                } else if (Folded && (FitTicks += tick) >= DETAILED_OUTPUT_RESTORE_TICKS) {
                    SetFolded(false);
                }
            }

            void SetFolded(bool fold) {
                // Restoring the provisional start fold is routine
                const bool notice = fold || FoldedByBudget;
                Folded = fold;
                FoldedByBudget = fold;
                for (auto& [_, table] : Tables) {
                    for (const auto& [key, bucket] : table.Buckets) {
                        if (!key) {
                            continue;
                        }
                        if (fold) {
                            table.PublicGroup->RemoveSubgroupChain(MakeTabletPath(*key));
                        } else {
                            RegisterTabletGroup(table.PublicGroup, *key, bucket->Group);
                        }
                    }
                }
                YDB_LOG(notice ? PRI_NOTICE : PRI_DEBUG, "Detailed metrics output level changed",
                    {"database", DatabasePrefix},
                    {"level", fold ? "TABLE" : "PARTITION"},
                    {"estimatedBytes", GetEstimatedOutputBytes()});
            }

            void ApplyContribution(
                ui32 nodeId,
                const TContribution& contribution,
                TTabletTypes::EType type,
                const NKikimrSysView::TDbCounters& values,
                TContributions& contributions) {
                const auto& [path, key] = contribution;
                const auto* descriptor = GetDescriptor(type);
                if (path.empty() || !descriptor) {
                    return;
                }
                auto& table = Tables[path];
                if (table.Type == TTabletTypes::TypeInvalid) {
                    table.Type = type;
                    table.PublicGroup = TargetCounterGroup->GetSubgroup(TABLE_LABEL, path);
                    table.Aggregator = CreateYdbMetricsAggregatorByTabletType(
                        type, table.PublicGroup, ECumulativeHistoryPolicy::RetainOnSourceRemoval);
                    table.RollupSize = EstimateJson(*table.PublicGroup, {{TABLE_LABEL, path}});
                    Rollups += table.RollupSize;
                } else if (table.Type != type) {
                    return;
                }
                auto& bucket = table.Buckets[key];
                if (!bucket) {
                    // Detached until published, inheriting the lookup counter as GetSubgroup does
                    auto mappedGroup = MakeIntrusive<NMonitoring::TDynamicCounters>(table.PublicGroup.Get());
                    // The partial's mapped group is detached: only the combined table
                    // rollup is public, so partials and leaves never overwrite each other.
                    const EYdbMetricNameScope nameScope = key
                        ? EYdbMetricNameScope::Partition
                        : EYdbMetricNameScope::Aggregate;
                    const bool isFollowerSource = key && key->second != 0;
                    bucket = MakeHolder<TPublishedBucket>(*descriptor, mappedGroup, nameScope, isFollowerSource);
                    table.Aggregator->AddSourceCountersGroup(SourceId(key), mappedGroup, isFollowerSource, nameScope);
                    if (key) {
                        bucket->Size = EstimateJson(*mappedGroup, MakeTabletPath(*key, {{TABLE_LABEL, path}}));
                        Leaves += bucket->Size;
                        if (!Folded) {
                            RegisterTabletGroup(table.PublicGroup, *key, mappedGroup);
                        }
                    }
                }
                bucket->Apply(nodeId, values);
                contributions.insert(contribution);
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
                Leaves -= bucketIt->second->Size;
                table.Buckets.erase(bucketIt);
                if (key && !Folded) {
                    table.PublicGroup->RemoveSubgroupChain(MakeTabletPath(*key));
                }
                if (table.Buckets.empty()) {
                    Rollups -= table.RollupSize;
                    TargetCounterGroup->RemoveSubgroup(TABLE_LABEL, path);
                    Tables.erase(tableIt);
                }
            }

            NMonitoring::TDynamicCounterPtr TargetCounterGroup;
            const TString DatabasePrefix;
            const TDetailedMetricsDescriptorGetter GetDescriptor;
            THashMap<TString, TTableEntry> Tables;
            THashMap<TNodeRoleKey, TContributions> ContributionsByNodeRole;
            ui64 MemberId = 0;
            const TIntrusivePtr<TOutputBudget> Budget;
            // Always published
            TJsonSize Rollups;
            // Published unless folded
            TJsonSize Leaves;
            ui64 PrefixLabelBytes = 0;
            bool Folded;
            // A provisional start fold restores at the limit, a budget fold at RestorePercent of it
            bool FoldedByBudget = false;
            // Fitting ticks in a row while folded
            ui32 FitTicks = 0;
        };

    } // namespace

    TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        TDetailedMetricsDescriptorGetter getDescriptor,
        NMonitoring::TDynamicCounterPtr budgetScope) {
        NProfiling::TMemoryTagScope memoryScope(NDetailedMetrics::ProcessorMemoryTag());
        return MakeIntrusive<TProcessorDatabaseMetricsAggregatorImpl>(
            targetCounterGroup, databasePath, getDescriptor, std::move(budgetScope));
    }

} // namespace NKikimr
