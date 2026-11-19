#include "collector.h"
#include "metrics.h"

#include <algorithm>

namespace NKikimr::NOlap::NStats::NScan {

bool ValidIdentity(const NColumnShardStats::TScanIdentity& identity) noexcept {
    return (!identity.HasStage() || (identity.GetStage().HasQueryExecutionId() && !identity.GetStage().GetQueryExecutionId().empty() &&
                                        identity.GetStage().HasPhysicalTxIndex() && identity.GetStage().HasStageIndex())) &&
           identity.HasTabletId() && identity.GetTabletId() != 0 && identity.HasScanAttemptId() && !identity.GetScanAttemptId().empty();
}

TScanStats::TScanStats(const NColumnShardStats::TScanIdentity& identity, NColumnShardStats::EReaderKind readerKind)
    : Identity(identity)
    , ReaderKind(readerKind)
{
}

bool TScanStats::Apply(TStatsUpdate update) {
    const auto registerProducer = [&](const auto& self, const std::shared_ptr<const TStatsUpdate::TProducer>& producer) -> TOwner& {
        auto [position, inserted] = Owners.try_emplace(producer.get());
        if (inserted) {
            position->second.Producer = producer;
            if (producer->Parent) {
                self(self, producer->Parent);
            }
        }
        return position->second;
    };
    auto& owner = registerProducer(registerProducer, update.Producer);
    if (update.Revision > owner.Revision) {
        owner.Revision = update.Revision;
        owner.ObservationLost = update.ObservationLost;
        owner.DataSources.Swap(&update.DataSources);
    }
    const auto registerNode = [&](const auto& self,
                                  const std::shared_ptr<const TStatsUpdate::TNodeRegistration>& registration) -> TStatsUpdate::TNode& {
        auto [position, inserted] = owner.Nodes.try_emplace(registration->Id);
        if (inserted) {
            position->second.Registration = registration;
            if (registration->Parent) {
                self(self, registration->Parent);
            }
        }
        return position->second;
    };
    for (auto& node : update.Nodes) {
        auto& target = registerNode(registerNode, node.Registration);
        if (node.Revision > target.Revision) {
            target = std::move(node);
        }
    }
    return true;
}

std::shared_ptr<NColumnShardStats::TReport> TScanStats::BuildReport() const {
    auto report = std::make_shared<NColumnShardStats::TReport>();
    *report->MutableIdentity() = Identity;
    report->SetReaderKind(ReaderKind);
    report->SetSequence(1);
    report->SetFinality(NColumnShardStats::OPEN);
    report->SetFinalizationCause(NColumnShardStats::FINALIZATION_NONE);
    report->SetExecutionOutcome(NColumnShardStats::OUTCOME_UNKNOWN);

    struct TChildrenIndex {
        std::map<TString, NColumnShardStats::TOperation*> ByKey;
        std::map<const NColumnShardStats::TOperation*, ui64> Order;
    };

    std::map<NColumnShardStats::TOperation*, TChildrenIndex> levels;
    const auto addMeasurements = [](NColumnShardStats::TOperation& destination, const NColumnShardStats::TOperation& values) {
        if (values.HasCallsCount()) {
            destination.SetCallsCount(destination.GetCallsCount() + values.GetCallsCount());
        }
        AddMeasurements(destination, values);
        for (const auto& source : values.GetStatistics().GetDataSources()) {
            AddRead(*destination.MutableStatistics()->MutableDataSources(), source);
        }
    };
    bool observationLost = false;
    for (const auto& [_, owner] : Owners) {
        observationLost |= owner.ObservationLost;
        std::map<ui64, NColumnShardStats::TOperation*> resolved;
        const auto resolve = [&](const auto& self,
                                 const std::shared_ptr<const TStatsUpdate::TNodeRegistration>& registration) -> NColumnShardStats::TOperation* {
            if (const auto found = resolved.find(registration->Id); found != resolved.end()) {
                return found->second;
            }
            auto* parent = registration->Parent ? self(self, registration->Parent) : nullptr;
            auto& children = parent ? *parent->MutableSubOperations() : *report->MutableOperations();
            auto& index = levels[parent];
            auto [position, inserted] = index.ByKey.try_emplace(registration->Definition->GetKey(), nullptr);
            if (inserted) {
                position->second = children.Add();
                *position->second = *registration->Definition;
                index.Order.emplace(position->second, registration->Order);
            } else {
                auto& order = index.Order.at(position->second);
                order = std::min(order, registration->Order);
            }
            auto* destination = position->second;
            resolved.emplace(registration->Id, destination);
            return destination;
        };
        for (const auto& [_, node] : owner.Nodes) {
            if (!node.Revision) {
                continue;
            }
            auto* destination = resolve(resolve, node.Registration);
            destination->MutableTablets()->SetExecution(NColumnShardStats::EXECUTION_OBSERVED);
            addMeasurements(*destination, node.Measurements);
            if (node.Measurements.HasUnfinished()) {
                if (!destination->HasUnfinished()) {
                    *destination->MutableUnfinished() = *node.Registration->Definition;
                }
                destination->MutableUnfinished()->MutableTablets()->SetExecution(NColumnShardStats::EXECUTION_OBSERVED);
                addMeasurements(*destination->MutableUnfinished(), node.Measurements.GetUnfinished());
            }
        }
        for (const auto& source : owner.DataSources) {
            AddRead(*report->MutableDataSources(), source);
        }
    }
    const auto orderChildren = [&](const auto& self, NColumnShardStats::TOperation* parent,
                                   google::protobuf::RepeatedPtrField<NColumnShardStats::TOperation>& children) -> void {
        if (const auto found = levels.find(parent); found != levels.end()) {
            const auto& order = found->second.Order;
            std::stable_sort(children.pointer_begin(), children.pointer_end(), [&](const auto* left, const auto* right) {
                return order.at(left) < order.at(right);
            });
        }
        for (auto& child : children) {
            self(self, &child, *child.MutableSubOperations());
        }
    };
    orderChildren(orderChildren, nullptr, *report->MutableOperations());
    if (observationLost) {
        report->SetFinalizationCause(NColumnShardStats::FINALIZATION_OBSERVATION_LOST);
    }
    return report;
}

std::shared_ptr<NColumnShardStats::TReport> TScanStats::MakeReport() const {
    return BuildReport();
}

std::shared_ptr<NColumnShardStats::TReport> TScanStats::Finish(NColumnShardStats::EExecutionOutcome outcome) && {
    auto report = BuildReport();
    report->SetFinality(NColumnShardStats::FINAL);
    report->SetExecutionOutcome(outcome);
    if (report->GetFinalizationCause() == NColumnShardStats::FINALIZATION_NONE) {
        report->SetFinalizationCause(NColumnShardStats::OUTPUT_SNAPSHOT);
    }
    Owners.clear();
    return report;
}

}   // namespace NKikimr::NOlap::NStats::NScan
