#pragma once
#include "defs.h"
#include <util/generic/map.h>
#include <util/generic/hash_set.h>
#include <util/generic/list.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <google/protobuf/util/message_differencer.h>

namespace NKikimr {
namespace NLocalDb {

struct TCompactionPolicy : public TThrRefBase {
    struct TBackgroundPolicy {
        ui32 Threshold;
        ui32 PriorityBase;
        double TimeFactor;
        TString ResourceBrokerTask;

        TBackgroundPolicy();
        TBackgroundPolicy(ui32 threshold, ui32 priorityBase, double timeFactor, const TString &resourceBrokerTask);
        TBackgroundPolicy(const NKikimrSchemeOp::TCompactionPolicy::TBackgroundPolicy &policyPb);

        void Serialize(NKikimrSchemeOp::TCompactionPolicy::TBackgroundPolicy& policyPb) const;

        bool operator ==(const TBackgroundPolicy& p) const {
            return (Threshold == p.Threshold
                    && PriorityBase == p.PriorityBase
                    && TimeFactor == p.TimeFactor
                    && ResourceBrokerTask == p.ResourceBrokerTask);
        }
    };

    struct TGenerationPolicy {
        ui64 SizeToCompact;
        ui32 CountToCompact;
        ui32 ForceCountToCompact;
        ui64 ForceSizeToCompact;
        ui32 CompactionBrokerQueue; // TODO: remove deprecated field
        TString ResourceBrokerTask;
        bool KeepInCache;
        TBackgroundPolicy BackgroundCompactionPolicy;
        ui32 ExtraCompactionPercent;
        ui32 ExtraCompactionExpPercent;
        ui64 ExtraCompactionMinSize;
        ui64 ExtraCompactionExpMaxSize;
        ui64 UpliftPartSize;

        TGenerationPolicy(ui64 sizeToCompact, ui32 countToCompact, ui32 forceCountToCompact,
                          ui64 forceSizeToCompact, const TString &resourceBrokerTask,
                          bool keepInCache,
                          const TBackgroundPolicy &backgroundCompactionPolicy = TBackgroundPolicy());
        TGenerationPolicy(const NKikimrSchemeOp::TCompactionPolicy::TGenerationPolicy &policyPb);

        void Serialize(NKikimrSchemeOp::TCompactionPolicy::TGenerationPolicy& policyPb) const;

        bool operator ==(const TGenerationPolicy& p) const {
            return SizeToCompact == p.SizeToCompact
                    && CountToCompact == p.CountToCompact
                    && ForceCountToCompact == p.ForceCountToCompact
                    && ForceSizeToCompact == p.ForceSizeToCompact
                    && CompactionBrokerQueue == p.CompactionBrokerQueue
                    && ResourceBrokerTask == p.ResourceBrokerTask
                    && KeepInCache == p.KeepInCache
                    && BackgroundCompactionPolicy == p.BackgroundCompactionPolicy
                    && ExtraCompactionPercent == p.ExtraCompactionPercent
                    && ExtraCompactionExpPercent == p.ExtraCompactionExpPercent
                    && ExtraCompactionMinSize == p.ExtraCompactionMinSize
                    && ExtraCompactionExpMaxSize == p.ExtraCompactionExpMaxSize
                    && UpliftPartSize == p.UpliftPartSize;
        }
    };

    ui64 InMemSizeToSnapshot;
    ui32 InMemStepsToSnapshot;
    ui32 InMemForceStepsToSnapshot;
    ui64 InMemForceSizeToSnapshot;
    ui32 InMemCompactionBrokerQueue; // TODO: remove deprecated field
    TString InMemResourceBrokerTask;
    ui64 ReadAheadHiThreshold;
    ui64 ReadAheadLoThreshold;
    ui32 MinDataPageSize;
    ui32 MinBTreeIndexNodeSize;
    ui32 MinBTreeIndexNodeKeys;
    ui32 SnapshotCompactionBrokerQueue; // TODO: remove deprecated field
    TString SnapshotResourceBrokerTask;
    ui32 BackupCompactionBrokerQueue; // TODO: remove deprecated field
    TString BackupResourceBrokerTask;
    ui32 DefaultTaskPriority;
    TBackgroundPolicy BackgroundSnapshotPolicy;
    ui64 LogOverheadSizeToSnapshot;
    ui32 LogOverheadCountToSnapshot;
    ui32 DroppedRowsPercentToCompact;
    NKikimrSchemeOp::ECompactionStrategy CompactionStrategy;
    NKikimrSchemeOp::TCompactionPolicy::TShardPolicy ShardPolicy;
    bool KeepEraseMarkers;

    TVector<TGenerationPolicy> Generations;

    TCompactionPolicy();
    explicit TCompactionPolicy(const NKikimrSchemeOp::TCompactionPolicy& policyPb);

    void Serialize(NKikimrSchemeOp::TCompactionPolicy& policyPb) const;

    bool operator ==(const TCompactionPolicy& p) const {
        return InMemSizeToSnapshot == p.InMemSizeToSnapshot
                && InMemStepsToSnapshot == p.InMemStepsToSnapshot
                && InMemForceStepsToSnapshot == p.InMemForceStepsToSnapshot
                && InMemForceSizeToSnapshot == p.InMemForceSizeToSnapshot
                && InMemCompactionBrokerQueue == p.InMemCompactionBrokerQueue
                && InMemResourceBrokerTask == p.InMemResourceBrokerTask
                && ReadAheadHiThreshold == p.ReadAheadHiThreshold
                && ReadAheadLoThreshold == p.ReadAheadLoThreshold
                && MinDataPageSize == p.MinDataPageSize
                && MinBTreeIndexNodeSize == p.MinBTreeIndexNodeSize
                && MinBTreeIndexNodeKeys == p.MinBTreeIndexNodeKeys
                && Generations == p.Generations
                && SnapshotCompactionBrokerQueue == p.SnapshotCompactionBrokerQueue
                && SnapshotResourceBrokerTask == p.SnapshotResourceBrokerTask
                && BackupCompactionBrokerQueue == p.BackupCompactionBrokerQueue
                && BackupResourceBrokerTask == p.BackupResourceBrokerTask
                && DefaultTaskPriority == p.DefaultTaskPriority
                && BackgroundSnapshotPolicy == p.BackgroundSnapshotPolicy
                && LogOverheadSizeToSnapshot == p.LogOverheadSizeToSnapshot
                && LogOverheadCountToSnapshot == p.LogOverheadCountToSnapshot
                && DroppedRowsPercentToCompact == p.DroppedRowsPercentToCompact
                && CompactionStrategy == p.CompactionStrategy
                && KeepEraseMarkers == p.KeepEraseMarkers
                && ::google::protobuf::util::MessageDifferencer::Equals(ShardPolicy, p.ShardPolicy);
    }
};

typedef TIntrusivePtr<TCompactionPolicy> TCompactionPolicyPtr;

TCompactionPolicyPtr CreateDefaultTablePolicy();
TCompactionPolicyPtr CreateDefaultUserTablePolicy();

bool ValidateCompactionPolicyChange(const TCompactionPolicy& oldPolicy, const TCompactionPolicy& newPolicy, TString& err);

// Get Resource Broker task type name by Compaction Broker queue ID.
TString LegacyQueueIdToTaskName(ui32 id);

// Check if Resource Broker task name is for a legacy queue ID.
bool IsLegacyQueueIdTaskName(const TString& taskName);

extern const TString DefaultQueueName;
extern const TString UnknownTaskName;
extern const TString TransactionTaskName;
extern const TString ScanTaskName;
extern const TString BackgroundCompactionTaskName;
extern const TString KqpResourceManagerTaskName;
extern const TString KqpResourceManagerQueue;
extern const TString LegacyQueueIdTaskNamePrefix;

}}
