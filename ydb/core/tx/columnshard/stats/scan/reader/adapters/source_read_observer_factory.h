#pragma once

#include "decode_observer.h"
#include "read_delivery_observer.h"
#include "read_observer.h"

namespace NKikimr::NOlap::NStats::NScan {

template <class TSource>
class TSourceReadObserverFactory {
    const TSource& Source;

public:
    explicit TSourceReadObserverFactory(const TSource& source)
        : Source(source)
    {
    }

    std::optional<TDecodeObserver> CreateDecodeObserver() const {
        auto* local = Source.GetLocalStats();
        if (!local) {
            return std::nullopt;
        }
        const auto current = Source.GetCurrentStatisticsCall();
        auto* parent = current.GetNode() ? local->CloneBranch(*current.GetNode()) : &local->GetRoot();
        if (!parent) {
            return std::nullopt;
        }
        std::optional<TDecodeObserver> result(std::in_place, *local);
        result->SetParent(parent);
        result->SetParentCall(current);
        return result;
    }

    TReadDeliveryObserver StartSystemViewRead() const {
        auto* local = Source.GetLocalStats();
        if (!local) {
            return {};
        }
        return TReadDeliveryObserver::ForSystemView(
            local, Source.GetCurrentStatisticsCall(), Source.GetContext()->GetReadMetadata()->TableMetadataAccessor->GetTablePath());
    }

    TReadDeliveryObserver StartInlineIndexRead(ui32 indexId) const {
        auto* local = Source.GetLocalStats();
        if (!local) {
            return {};
        }
        return TReadDeliveryObserver::ForInlineIndex(local, Source.GetCurrentStatisticsCall(), indexId);
    }

    std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> CreateReadAttribution(
        std::function<bool()> canAttributeRead) const {
        if (!Source.GetLocalStats()) {
            return nullptr;
        }
        return MakeReadAttributionCallback(Source.GetCurrentStatisticsCall(), std::move(canAttributeRead));
    }

    std::shared_ptr<NBlobOperations::NRead::IReadObserver> CreateBlobReadObserver(std::function<bool()> canAttributeRead) const {
        auto* local = Source.GetLocalStats();
        if (!local) {
            return nullptr;
        }
        const auto current = Source.GetCurrentStatisticsCall();
        TCallHandle requesting;
        for (auto call = current; call; call = call.GetParent()) {
            if (const auto* node = call.GetNode();
                node && (node->GetDefinition().HasFetchingColumns() || node->GetDefinition().HasFetchingIndexes())) {
                requesting = call;
                break;
            }
        }
        auto attribution = MakeReadAttributionCallback(current, std::move(canAttributeRead));
        return MakeReadObserver(local->Fork(), current.GetNode(), std::move(requesting), std::move(attribution));
    }
};

}   // namespace NKikimr::NOlap::NStats::NScan
