#pragma once

#include <ydb/core/protos/columnshard_scan_stats.pb.h>

#include <map>
#include <memory>
#include <vector>

namespace NKikimr::NOlap::NStats::NScan {

class TLocalStats;

class TStatsUpdate {
    struct TProducer {
        const std::shared_ptr<const NColumnShardStats::TScanIdentity> Identity;
        const std::shared_ptr<const TProducer> Parent;
    };

    struct TNodeRegistration {
        const ui64 Id;
        const std::shared_ptr<const TNodeRegistration> Parent;
        const ui64 Order;
        const std::shared_ptr<const NColumnShardStats::TOperation> Definition;
    };

    struct TNode {
        std::shared_ptr<const TNodeRegistration> Registration;
        ui64 Revision = 0;
        NColumnShardStats::TOperation Measurements;
    };

    std::shared_ptr<const TProducer> Producer;
    ui64 Revision = 0;
    bool ObservationLost = false;
    std::vector<TNode> Nodes;
    google::protobuf::RepeatedPtrField<NColumnShardStats::TDataSource> DataSources;

    TStatsUpdate() = default;
    friend class TLocalStats;
    friend class TOperationNode;
    friend class TScanStats;

public:
    TStatsUpdate(const TStatsUpdate&) = default;
    TStatsUpdate(TStatsUpdate&&) = default;
    TStatsUpdate& operator=(const TStatsUpdate&) = default;
    TStatsUpdate& operator=(TStatsUpdate&&) = default;
};

bool ValidIdentity(const NColumnShardStats::TScanIdentity& identity) noexcept;

class TScanStats {
    struct TOwner {
        std::shared_ptr<const TStatsUpdate::TProducer> Producer;
        ui64 Revision = 0;
        bool ObservationLost = false;
        std::map<ui64, TStatsUpdate::TNode> Nodes;
        google::protobuf::RepeatedPtrField<NColumnShardStats::TDataSource> DataSources;
    };

    const NColumnShardStats::TScanIdentity Identity;
    const NColumnShardStats::EReaderKind ReaderKind;
    std::map<const TStatsUpdate::TProducer*, TOwner> Owners;

    std::shared_ptr<NColumnShardStats::TReport> BuildReport() const;

public:
    explicit TScanStats(
        const NColumnShardStats::TScanIdentity& identity, NColumnShardStats::EReaderKind readerKind = NColumnShardStats::READER_UNKNOWN);
    TScanStats(const TScanStats&) = delete;
    TScanStats& operator=(const TScanStats&) = delete;
    bool Apply(TStatsUpdate update);
    std::shared_ptr<NColumnShardStats::TReport> MakeReport() const;
    std::shared_ptr<NColumnShardStats::TReport> Finish(NColumnShardStats::EExecutionOutcome outcome) &&;
};

}   // namespace NKikimr::NOlap::NStats::NScan
