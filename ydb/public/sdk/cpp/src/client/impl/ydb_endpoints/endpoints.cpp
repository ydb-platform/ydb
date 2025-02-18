#include "endpoints.h"

#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/random/random.h>

#include <set>
#include <unordered_set>

namespace NYdb::inline V3 {

using std::string;

class TEndpointElectorSafe::TObjRegistry : public IObjRegistryHandle {
public:
    TObjRegistry(const ui64& nodeId)
        : NodeId_(nodeId)
    {}

    bool Add(TEndpointObj* obj) {
        std::unique_lock lock(Mutex_);
        return Objs_.insert(obj).second;
    }

    void Remove(TEndpointObj* obj) {
        std::unique_lock lock(Mutex_);
        Y_ABORT_UNLESS(Objs_.find(obj) != Objs_.end());
        Objs_.erase(obj);
    }

    void NotifyEndpointRemoved() {
        std::shared_lock lock(Mutex_);
        for (auto obj : Objs_) {
            obj->OnEndpointRemoved();
        }
    }

    size_t Size() const override {
        std::shared_lock lock(Mutex_);
        return Objs_.size();
    }

    ui64 GetNodeId() const {
        return NodeId_;
    }

private:
    std::set<TEndpointObj*> Objs_;
    ui64 NodeId_;

    mutable std::shared_mutex Mutex_;
};

////////////////////////////////////////////////////////////////////////////////

// Returns index of last resord with same priority or -1 in case of empty input
static i32 GetBestK(const std::vector<TEndpointRecord>& records) {
    if (records.empty()) {
        return -1;
    }

    const i32 bestPriority = records[0].Priority;

    size_t pos = 1;
    while (pos < records.size()) {
        if (records[pos].Priority != bestPriority) {
            break;
        }
        ++pos;
    }
    return pos - 1;
}

std::vector<string> TEndpointElectorSafe::SetNewState(std::vector<TEndpointRecord>&& records) {
    std::unordered_set<string> index;
    std::vector<TEndpointRecord> uniqRec;

    for (auto&& record : records) {
        if (index.insert(record.Endpoint).second) {
            uniqRec.emplace_back(std::move(record));
        }
    }

    Sort(uniqRec.begin(), uniqRec.end());

    auto bestK = GetBestK(uniqRec);

    std::vector<string> removed;
    std::vector<std::shared_ptr<TObjRegistry>> notifyRemoved;

    {
        std::unique_lock guard(Mutex_);
        // Find endpoins which were removed
        for (const auto& record : Records_) {
            if (index.find(record.Endpoint) == index.end()) {
                removed.emplace_back(record.Endpoint);

                auto it = KnownEndpoints_.find(record.Endpoint);
                Y_ABORT_UNLESS(it != KnownEndpoints_.end());
                KnownEndpoints_.erase(it);

                auto nodeIdIt = KnownEndpointsByNodeId_.find(record.NodeId);
                if (nodeIdIt != KnownEndpointsByNodeId_.end()) {
                    for (const auto& registry : nodeIdIt->second.TaggedObjs) {
                        notifyRemoved.emplace_back(registry.second);
                    }
                    KnownEndpointsByNodeId_.erase(nodeIdIt);
                }
            }
        }
        // Find endpoints which were added
        Records_ = std::move(uniqRec);
        for (const auto& record : Records_) {
            KnownEndpoints_[record.Endpoint] = record;
            KnownEndpointsByNodeId_[record.NodeId].Record = record;
        }
        Y_ABORT_UNLESS(Records_.size() == KnownEndpoints_.size());
        EndpointCountGauge_.SetValue(Records_.size());
        EndpointActiveGauge_.SetValue(Records_.size());
        BestK_ = bestK;
        PessimizationRatio_.store(0);
        PessimizationRatioGauge_.SetValue(0);
    }

    for (auto& obj : notifyRemoved) {
        obj->NotifyEndpointRemoved();
    }

    return removed;
}

TEndpointRecord TEndpointElectorSafe::GetEndpoint(const TEndpointKey& preferredEndpoint, bool onlyPreferred) const {
    std::shared_lock guard(Mutex_);

    if (preferredEndpoint.GetNodeId()) {
        auto it = KnownEndpointsByNodeId_.find(preferredEndpoint.GetNodeId());
        if (it != KnownEndpointsByNodeId_.end()) {
            return it->second.Record;
        }
    }

    if (!preferredEndpoint.GetEndpoint().empty()) {
        auto it = KnownEndpoints_.find(preferredEndpoint.GetEndpoint());
        if (it != KnownEndpoints_.end()) {
            return it->second;
        }
    }

    if(onlyPreferred)
        return {};

    if (BestK_ == -1) {
        Y_ASSERT(Records_.empty());
        return {};
    } else {
        // returns value in range [0, n)
        auto idx = RandomNumber<size_t>(BestK_ + 1);
        return Records_[idx];
    }
}

// TODO: Suboptimal, but should not be used often
void TEndpointElectorSafe::PessimizeEndpoint(const string& endpoint) {
    std::unique_lock guard(Mutex_);
    for (auto& r : Records_) {
        if (r.Endpoint == endpoint && r.Priority != Max<i32>()) {
            int pessimizationRatio = PessimizationRatio_.load();
            auto newRatio = (pessimizationRatio * Records_.size() + 100) / Records_.size();
            PessimizationRatio_.store(newRatio);
            PessimizationRatioGauge_.SetValue(newRatio);
            EndpointActiveGauge_.Dec();
            r.Priority = Max<i32>();

            auto it = KnownEndpoints_.find(endpoint);
            if (it != KnownEndpoints_.end()) {
                it->second.Priority = Max<i32>();
            }
        }
    }
    Sort(Records_.begin(), Records_.end());
    BestK_ = GetBestK(Records_);
}

// % of endpoints which was pessimized
int TEndpointElectorSafe::GetPessimizationRatio() const {
    return PessimizationRatio_.load();
}

void TEndpointElectorSafe::SetStatCollector(const NSdkStats::TStatCollector::TEndpointElectorStatCollector& endpointStatCollector) {
    EndpointCountGauge_.Set(endpointStatCollector.EndpointCount);
    PessimizationRatioGauge_.Set(endpointStatCollector.PessimizationRatio);
    EndpointActiveGauge_.Set(endpointStatCollector.EndpointActive);
}

bool TEndpointElectorSafe::LinkObjToEndpoint(const TEndpointKey& endpoint, TEndpointObj* obj, const void* tag) {
    {
        std::unique_lock guard(Mutex_);
        // Find obj registry for given endpoint
        // No endpoint - no registry, return false
        auto objIt = KnownEndpointsByNodeId_.find(endpoint.GetNodeId());
        if (objIt == KnownEndpointsByNodeId_.end()) {
            return false;
        }

        TTaggedObjRegistry& taggedObjs = objIt->second.TaggedObjs;
        TTaggedObjRegistry::iterator registryIt = taggedObjs.find(tag);

        if (registryIt == taggedObjs.end()) {
            registryIt = taggedObjs.emplace(tag, new TObjRegistry(endpoint.GetNodeId())).first;
        }

        // Call Link under endpoint elector mutex.
        // Probably a bit more optimal way is:
        // - get TObjRegistry (as shared ptr) and release this mutex
        // - call obj->Link whithout mutex
        // - check KnownEndpoints_ stil has same TObjRegistry for given endpoint
        // - in case of false send notification
        return obj->Link(registryIt->second);
    }
}

void TEndpointElectorSafe::ForEachEndpoint(const THandleCb& cb, i32 minPriority, i32 maxPriority, const void* tag) const {
    std::shared_lock guard(Mutex_);

    auto it = std::lower_bound(Records_.begin(), Records_.end(), minPriority, [](const TEndpointRecord& l, i32 r) {
        return l.Priority < r;
    });

    while (it != Records_.end()) {
        if (it->Priority > maxPriority)
            break;

        const TTaggedObjRegistry& taggedObjs = KnownEndpointsByNodeId_.at(it->NodeId).TaggedObjs;

        auto registry = taggedObjs.find(tag);
        if (registry != taggedObjs.end()) {
            cb(it->NodeId, *registry->second);
        } else {
            cb(it->NodeId, TObjRegistry(it->NodeId));
        }

        it++;
    }
}

void TEndpointObj::Unlink() {
    if (ObjRegistry_) {
        ObjRegistry_->Remove(this);
    }
    ObjRegistry_ = nullptr;
}

bool TEndpointObj::Link(std::shared_ptr<TEndpointElectorSafe::TObjRegistry> registry) {
    if (registry->Add(this)) {
        if (ObjRegistry_) {
            ObjRegistry_->Remove(this);
        }
        ObjRegistry_ = registry;
        return true;
    }
    return false;
}

size_t TEndpointObj::ObjectCount() const {
    return ObjRegistry_->Size();
}

bool TEndpointObj::ObjectRegistred() const {
    return bool(ObjRegistry_);
}

}
