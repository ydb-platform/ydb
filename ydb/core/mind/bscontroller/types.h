#pragma once

#include "defs.h"

#include "scheme.h"
#include "diff.h"

namespace NKikimr::NBsController {
    struct TPDiskId;
    struct TVSlotId;
}

template<>
struct THash<NKikimr::NBsController::TPDiskId> {
    size_t operator ()(NKikimr::NBsController::TPDiskId) const;
};

template<>
struct THash<NKikimr::NBsController::TVSlotId> {
    size_t operator ()(NKikimr::NBsController::TVSlotId) const;
};

namespace NKikimr {
    namespace NBsController {

        using TNodeId = Schema::Node::TKey::Type;
        using TGroupId = TIdWrapper<Schema::Group::TKey::Type, TGroupIdTag>;

        using TBoxId = ui64;
        using TBoxStoragePoolId = std::tuple<ui64, ui64>;

        class TBlobStorageController;

        struct TGroupLatencyStats {
            TMaybe<TDuration> PutTabletLog;
            TMaybe<TDuration> PutUserData;
            TMaybe<TDuration> GetFast;

            double GetNormalizedLatencyValue() const {
                i64 value = 0;
                for (const auto& item : {PutTabletLog, PutUserData, GetFast}) {
                    if (item) {
                        i64 itemValue = item->MicroSeconds();
                        value = Max(value, itemValue);
                    }
                }
                return Min(1.0, value * 0.2e-6); // 5 seconds is the maximum latency
            }
        };

        struct TPDiskId {
            Schema::PDisk::NodeID::Type NodeId = 0;
            Schema::PDisk::PDiskID::Type PDiskId = 0;

            TPDiskId(const Schema::PDisk::TKey::Type &key)
                : NodeId(std::get<0>(key))
                , PDiskId(std::get<1>(key))
            {}

            TPDiskId(Schema::PDisk::NodeID::Type nodeId, Schema::PDisk::PDiskID::Type pdiskId)
                : NodeId(nodeId)
                , PDiskId(pdiskId)
            {}

            static TPDiskId MinForNode(Schema::PDisk::NodeID::Type nodeId) {
                return TPDiskId(nodeId, Min<Schema::PDisk::PDiskID::Type>());
            }

            static TPDiskId MaxForNode(Schema::PDisk::NodeID::Type nodeId) {
                return TPDiskId(nodeId, Max<Schema::PDisk::PDiskID::Type>());
            }

            TPDiskId() = default;
            TPDiskId(const TPDiskId&) = default;
            TPDiskId &operator=(const TPDiskId &other) = default;

            TString ToString() const {
                return TStringBuilder() << NodeId << ":" << PDiskId;
            }

            Schema::PDisk::TKey::Type GetKey() const {
                return std::tie(NodeId, PDiskId);
            }

            friend bool operator ==(const TPDiskId &x, const TPDiskId &y) { return x.GetKey() == y.GetKey(); }
            friend bool operator !=(const TPDiskId &x, const TPDiskId &y) { return x.GetKey() != y.GetKey(); }
            friend bool operator < (const TPDiskId &x, const TPDiskId &y) { return x.GetKey() <  y.GetKey(); }
        };

        struct TVSlotId {
            Schema::VSlot::NodeID::Type NodeId = 0;
            Schema::VSlot::PDiskID::Type PDiskId = 0;
            Schema::VSlot::VSlotID::Type VSlotId = 0;

            TVSlotId(const Schema::VSlot::TKey::Type &key)
                : NodeId(std::get<0>(key))
                , PDiskId(std::get<1>(key))
                , VSlotId(std::get<2>(key))
            {}

            TVSlotId(Schema::VSlot::NodeID::Type nodeId, Schema::VSlot::PDiskID::Type pdiskId, Schema::VSlot::VSlotID::Type vslotId)
                : NodeId(nodeId)
                , PDiskId(pdiskId)
                , VSlotId(vslotId)
            {}

            TVSlotId() = default;
            TVSlotId(const TVSlotId&) = default;

            TVSlotId(TPDiskId pdiskId, Schema::VSlot::VSlotID::Type vslotId)
                : NodeId(pdiskId.NodeId)
                , PDiskId(pdiskId.PDiskId)
                , VSlotId(vslotId)
            {}

            TVSlotId(const NKikimrBlobStorage::TVSlotId& pb)
                : NodeId(pb.GetNodeId())
                , PDiskId(pb.GetPDiskId())
                , VSlotId(pb.GetVSlotId())
            {}

            TVSlotId &operator=(const TVSlotId &other) = default;

            static TVSlotId MinForPDisk(TPDiskId pdiskId) {
                return TVSlotId(pdiskId, Min<Schema::VSlot::VSlotID::Type>());
            }

            static TVSlotId MaxForPDisk(TPDiskId pdiskId) {
                return TVSlotId(pdiskId, Max<Schema::VSlot::VSlotID::Type>());
            }

            TPDiskId ComprisingPDiskId() const {
                return TPDiskId(NodeId, PDiskId);
            }

            TString ToString() const {
                return TStringBuilder() << NodeId << ":" << PDiskId << ":" << VSlotId;
            }

            Schema::VSlot::TKey::Type GetKey() const {
                return std::tie(NodeId, PDiskId, VSlotId);
            }

            void Serialize(NKikimrBlobStorage::TVSlotId *pb) const {
                pb->SetNodeId(NodeId);
                pb->SetPDiskId(PDiskId);
                pb->SetVSlotId(VSlotId);
            }

            friend bool operator ==(const TVSlotId &x, const TVSlotId &y) { return x.GetKey() == y.GetKey(); }
            friend bool operator !=(const TVSlotId &x, const TVSlotId &y) { return x.GetKey() != y.GetKey(); }
            friend bool operator < (const TVSlotId &x, const TVSlotId &y) { return x.GetKey() <  y.GetKey(); }
        };

        template<typename TKey, typename TValue>
        class TOverlayMap {
            using TBaseMap = TMap<TKey, THolder<TValue>>;
            using TIterator = typename TBaseMap::iterator;
            using TConstIterator = typename TBaseMap::const_iterator;

            TBaseMap& Base;
            TBaseMap Overlay;

            class TDiff {
                const TOverlayMap& Map;

            public:
                class TIterator {
                    const TOverlayMap& Map;
                    TConstIterator OverlayIt;
                    TConstIterator BaseIt;

                public:
                    TIterator(const TOverlayMap& map, TConstIterator overlayIt)
                        : Map(map)
                        , OverlayIt(overlayIt)
                        , BaseIt(overlayIt != map.Overlay.end() ? map.Base.lower_bound(overlayIt->first) : map.Base.end())
                    {}

                    std::pair<const typename TBaseMap::value_type*, TConstIterator> operator *() const {
                        Y_DEBUG_ABORT_UNLESS(OverlayIt != Map.Overlay.end());
                        return std::make_pair(BaseIt != Map.Base.end() && BaseIt->first == OverlayIt->first ?
                            &*BaseIt : nullptr, OverlayIt);
                    }

                    TIterator& operator ++() {
                        if (++OverlayIt != Map.Overlay.end()) {
                            ui8 n; // optimistically try a few options ahead
                            for (n = 4; BaseIt != Map.Base.end() && BaseIt->first < OverlayIt->first && --n; ++BaseIt)
                            {}
                            if (!n) { // locate using O(log2(N)) algorithm
                                BaseIt = Map.Base.lower_bound(OverlayIt->first);
                            }
                        }
                        return *this;
                    }

                    bool operator !=(const TIterator& other) const {
                        return OverlayIt != other.OverlayIt;
                    }
                };

            public:
                TDiff(const TOverlayMap& map) : Map(map) {}
                TIterator begin() const { return TIterator(Map, Map.Overlay.begin()); }
                TIterator end() const { return TIterator(Map, Map.Overlay.end()); }
            };

        public:
            TOverlayMap(TBaseMap& base)
                : Base(base)
            {}

            // SCAN RANGE -- function scans all items with key fitting condition from <= key <= to when bounds are provided;
            // in other case minimum and maximum values are used for these bounds. Callback is invoked with parameters
            // (RO key, RO value, getMutableItem callback), where getMutableItem returns pointer to item that can be
            // modified safely (when being added to delta map).
            template<typename T>
            void ScanRange(const TMaybe<TKey>& from, const TMaybe<TKey>& to, T&& callback) {
                auto baseIt = from ? Base.lower_bound(*from) : Base.begin();
                auto overlayIt = from ? Overlay.lower_bound(*from) : Overlay.begin();
                while (baseIt != Base.end() || overlayIt != Overlay.end()) {
                    // first, check exit condition (if set)
                    if (to) {
                        const TKey& current = baseIt == Base.end() ? overlayIt->first :
                            overlayIt == Overlay.end() ? baseIt->first : std::min(baseIt->first, overlayIt->first);
                        if (*to < current) {
                            break;
                        }
                    }

                    // check if we have only the base item, but no overlay one
                    if (overlayIt == Overlay.end() || (baseIt != Base.end() && baseIt->first < overlayIt->first)) {
                        TValue *mutableItem = nullptr;
                        auto getMutableItem = [&] {
                            if (!mutableItem) {
                                overlayIt = Clone(overlayIt, baseIt);
                                mutableItem = overlayIt->second.Get();
                                ++overlayIt;
                            }
                            return mutableItem;
                        };
                        if (!callback(baseIt->first, *baseIt->second, getMutableItem)) {
                            break;
                        }
                        ++baseIt;
                    } else {
                        if (overlayIt->second) {
                            auto getMutableItem = [&] { return overlayIt->second.Get(); };
                            if (!callback(overlayIt->first, *overlayIt->second, getMutableItem)) {
                                break;
                            }
                        }
                        if (baseIt != Base.end() && !(overlayIt->first < baseIt->first)) {
                            ++baseIt;
                        }
                        ++overlayIt;
                    }
                }
            }

            template<typename T>
            void ForEachInRange(const TMaybe<TKey>& from, const TMaybe<TKey>& to, T&& callback) const {
                auto& m = const_cast<TOverlayMap&>(*this); // we remove const qualifier as it is completely safe unless getMutableItem is called
                m.ScanRange(from, to, [&](const TKey& key, const TValue& value, const auto& /*getMutableItem*/) {
                    return callback(key, value);
                });
            }

            template<typename T>
            void ForEach(T&& callback) const {
                ForEachInRange({}, {}, [&](const TKey& key, const TValue& value) {
                    callback(key, value);
                    return true;
                });
            }

            const TValue *Find(const TKey& key) const {
                TConstIterator it = Overlay.find(key);
                if (it != Overlay.end()) {
                    return it->second.Get();
                }
                it = Base.find(key);
                return it != Base.end() ? it->second.Get() : nullptr;
            }

            TValue *FindForUpdate(const TKey& key) {
                TIterator it = Overlay.lower_bound(key);
                if (it == Overlay.end() || it->first != key) {
                    const TConstIterator baseIt = Base.find(key);
                    if (baseIt != Base.end()) {
                        it = Clone(it, baseIt);
                    } else {
                        return nullptr;
                    }
                }
                return it->second.Get();
            }

            template<typename... TArgs>
            TValue *ConstructInplaceNewEntry(TKey key, TArgs&&... args) {
                TIterator it = Overlay.lower_bound(key);
                if (it != Overlay.end() && it->first == key) {
                    Y_ABORT_UNLESS(!it->second);
                    it->second = MakeHolder<TValue>(std::forward<TArgs>(args)...);
                } else {
                    Y_ABORT_UNLESS(!Base.count(key));
                    it = Overlay.emplace_hint(it, std::move(key), MakeHolder<TValue>(std::forward<TArgs>(args)...));
                }
                return it->second.Get();
            }

            void DeleteExistingEntry(const TKey& key) {
                TIterator it = Overlay.lower_bound(key);
                if (it == Overlay.end() || it->first != key) {
                    Y_ABORT_UNLESS(Base.count(key)); // ensure that this entry exists in the base map
                    Overlay.emplace_hint(it, key, nullptr);
                } else if (Base.count(key)) {
                    auto& value = it->second;
                    Y_ABORT_UNLESS(value); // this entry must not be already deleted
                    value.Reset();
                } else {
                    // just remove this entity from overlay as there is no corresponding entity in base map
                    Overlay.erase(it);
                }
            }

            void ApplyToTable(TBlobStorageController *controller, NTabletFlatExecutor::TTransactionContext& txc) const {
                TValue::Apply(controller, [&](auto *adapter) {
                    for (const auto& row : Overlay) {
                        if (row.second) {
                            adapter->IssueUpdateRow(txc, row.first, *row.second);
                        } else {
                            adapter->IssueEraseRow(txc, row.first);
                        }
                    }
                });
            }

            auto Diff() const {
                return TDiff(*this);
            }

            auto Diff() {
                return TDiff(*this);
            }

            bool Changed() const {
                return !Overlay.empty();
            }

            void Preserve(std::deque<std::pair<void**, void*>>& v) {
                auto baseIt = Base.begin();
                for (const auto& [key, overlay] : Overlay) {
                    while (baseIt != Base.end() && baseIt->first < key) {
                        ++baseIt;
                    }
                    if (baseIt == Base.end()) {
                        break;
                    }
                    if (overlay && baseIt->first == key) {
                        v.push_back(baseIt->second->Preserve());
                    }
                }
            }

            void Commit() {
                auto baseIt = Base.begin();
                for (auto it = Overlay.begin(), next = it; it != Overlay.end() && (++next, true); it = next) {
                    auto& [key, overlay] = *it;

                    while (baseIt != Base.end() && baseIt->first < key) {
                        ++baseIt;
                    }
                    const bool hasBase = baseIt != Base.end() && !(key < baseIt->first);

                    if (!overlay) {
                        Y_DEBUG_ABORT_UNLESS(hasBase);
                        baseIt = Base.erase(baseIt);
                    } else if (hasBase) {
                        overlay->OnCommit();
                        baseIt->second = std::move(overlay);
                    } else {
                        baseIt = Base.insert(baseIt, Overlay.extract(it));
                    }
                }
                Overlay.clear();
            }

            void Rollback() {
                auto baseIt = Base.begin();
                for (const auto& [key, overlay] : Overlay) {
                    while (baseIt != Base.end() && baseIt->first < key) {
                        ++baseIt;
                    }
                    if (baseIt == Base.end()) {
                        break;
                    }
                    if (overlay && baseIt->first == key) {
                        baseIt->second->OnRollback();
                    }
                }
            }

        private:
            TIterator Clone(TIterator it, TConstIterator baseIt) {
                TIterator res = Overlay.emplace_hint(it, baseIt->first, MakeHolder<TValue>(*baseIt->second));
                baseIt->second->OnClone(res->second);
                return res;
            }
        };

    } // NBsController
} // NKikimr

inline size_t THash<NKikimr::NBsController::TPDiskId>::operator ()(NKikimr::NBsController::TPDiskId x) const {
    auto key = x.GetKey();
    using T = decltype(key);
    return THash<T>()(key);
}

template<>
struct std::hash<NKikimr::NBsController::TPDiskId> : THash<NKikimr::NBsController::TPDiskId> {};

inline size_t THash<NKikimr::NBsController::TVSlotId>::operator ()(NKikimr::NBsController::TVSlotId x) const {
    auto key = x.GetKey();
    using T = decltype(key);
    return THash<T>()(key);
}

template<>
inline NKikimr::NBsController::TPDiskId Min<NKikimr::NBsController::TPDiskId>() noexcept {
    using T = NKikimr::NBsController::Schema::PDisk;
    return {
        Min<T::NodeID::Type>(),
        Min<T::PDiskID::Type>()
    };
}

template<>
inline NKikimr::NBsController::TPDiskId Max<NKikimr::NBsController::TPDiskId>() noexcept {
    using T = NKikimr::NBsController::Schema::PDisk;
    return {
        Max<T::NodeID::Type>(),
        Max<T::PDiskID::Type>()
    };
}
