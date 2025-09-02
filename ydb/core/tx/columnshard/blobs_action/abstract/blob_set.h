#pragma once
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/util/gen_step.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/string.h>
#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/system/types.h>

namespace NKikimrColumnShardBlobOperationsProto {
class TTabletByBlob;
class TTabletsByBlob;
}

namespace NKikimr::NOlap {

class TTabletByBlob {
private:
    THashMap<TUnifiedBlobId, TTabletId> Data;
public:
    NKikimrColumnShardBlobOperationsProto::TTabletByBlob SerializeToProto() const;

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardBlobOperationsProto::TTabletByBlob& proto);

    THashMap<TUnifiedBlobId, TTabletId>::const_iterator begin() const {
        return Data.begin();
    }

    THashMap<TUnifiedBlobId, TTabletId>::const_iterator end() const {
        return Data.end();
    }

    const THashMap<TUnifiedBlobId, TTabletId>* operator->() const {
        return &Data;
    }

    THashMap<TUnifiedBlobId, TTabletId>* operator->() {
        return &Data;
    }

};

class TBlobsByGenStep {
private:
    struct TGenStepFromLogoBlobIdComparator {
        bool operator()(const TLogoBlobID& l, const TLogoBlobID& r) const {
            TGenStep gsl(l);
            TGenStep gsr(r);
            if (gsl == gsr) {
                return l < r;
            } else {
                return gsl < gsr;
            }
        }
    };
    std::set<TLogoBlobID, TGenStepFromLogoBlobIdComparator> Blobs;
public:
    [[nodiscard]] bool Add(const TLogoBlobID& blobId) {
        return Blobs.emplace(blobId).second;
    }
    [[nodiscard]] bool Remove(const TLogoBlobID& blobId) {
        return Blobs.erase(blobId);
    }
    bool IsEmpty() const {
        return Blobs.empty();
    }
    size_t GetSize() const {
        return Blobs.size();
    }

    TGenStep GetMinGenStepVerified() const {
        AFL_VERIFY(Blobs.size());
        return TGenStep(*Blobs.begin());
    }

    template <class TActor>
    requires std::invocable<TActor&, const TGenStep&, const TLogoBlobID&>
    bool ExtractTo(const TGenStep& lessOrEqualThan, const ui32 countLimit, const TActor& actor) {
        ui32 idx = 0;
        for (auto it = Blobs.begin(); it != Blobs.end(); ++it) {
            TGenStep gs(*it);
            if (lessOrEqualThan < gs) {
                Blobs.erase(Blobs.begin(), it);
                return true;
            }
            if (++idx > countLimit) {
                Blobs.erase(Blobs.begin(), it);
                return false;
            }
            actor(gs, *it);
        }
        Blobs.clear();
        return true;
    }
};

class TTabletsByBlob {
private:
    THashMap<TUnifiedBlobId, THashSet<TTabletId>> Data;
    i32 Size = 0;
public:
    ui32 GetSize() const {
        return Size;
    }

    void Clear() {
        Data.clear();
        Size = 0;
    }

    NKikimrColumnShardBlobOperationsProto::TTabletsByBlob SerializeToProto() const;

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardBlobOperationsProto::TTabletsByBlob& proto);

    THashMap<TUnifiedBlobId, THashSet<TTabletId>>::const_iterator begin() const {
        return Data.begin();
    }

    THashMap<TUnifiedBlobId, THashSet<TTabletId>>::const_iterator end() const {
        return Data.end();
    }

    bool Contains(const TTabletId tabletId, const TUnifiedBlobId& blobId) const {
        auto it = Data.find(blobId);
        if (it == Data.end()) {
            return false;
        }
        return it->second.contains(tabletId);
    }

    const THashSet<TTabletId>* Find(const TUnifiedBlobId& blobId) const {
        auto it = Data.find(blobId);
        if (it == Data.end()) {
            return nullptr;
        }
        return &it->second;
    }

    bool IsEmpty() const {
        return Data.empty();
    }

    template <class TFilter>
    TTabletsByBlob ExtractBlobs(const TFilter& filter, const std::optional<ui32> countLimit = {}) {
        TTabletsByBlob result;
        THashSet<TUnifiedBlobId> idsRemove;
        ui32 count = 0;
        for (auto&& i : Data) {
            if (filter(i.first, i.second)) {
                idsRemove.emplace(i.first);
                Size -= i.second.size();
                result.Data.emplace(i.first, i.second);
                result.Size += i.second.size();
                count += i.second.size();
                if (countLimit && count >= *countLimit) {
                    break;
                }
            }
        }
        for (auto&& i : idsRemove) {
            Data.erase(i);
        }
        return result;
    }

    class TIterator {
    private:
        const TTabletsByBlob& Owner;
        THashMap<TUnifiedBlobId, THashSet<TTabletId>>::const_iterator BlobsIterator;
        THashSet<TTabletId>::const_iterator TabletsIterator;
    public:
        TIterator(const TTabletsByBlob& owner)
            : Owner(owner) {
            BlobsIterator = Owner.Data.begin();
            if (BlobsIterator != Owner.Data.end()) {
                TabletsIterator = BlobsIterator->second.begin();
            }
        }

        const TUnifiedBlobId& GetBlobId() const {
            AFL_VERIFY(IsValid());
            return BlobsIterator->first;
        }

        TTabletId GetTabletId() const {
            AFL_VERIFY(IsValid());
            return *TabletsIterator;
        }

        bool IsValid() const {
            return BlobsIterator != Owner.Data.end() && TabletsIterator != BlobsIterator->second.end();
        }

        void operator++() {
            AFL_VERIFY(IsValid());
            ++TabletsIterator;
            if (TabletsIterator == BlobsIterator->second.end()) {
                ++BlobsIterator;
                if (BlobsIterator != Owner.Data.end()) {
                    TabletsIterator = BlobsIterator->second.begin();
                }
            }
        }
    };

    TIterator GetIterator() const {
        return TIterator(*this);
    }

    bool ExtractFront(TTabletId& tabletId, TUnifiedBlobId& blobId) {
        if (Data.empty()) {
            return false;
        }
        auto& b = Data.begin()->second;
        AFL_VERIFY(b.size());
        tabletId = *b.begin();
        blobId = Data.begin()->first;
        b.erase(b.begin());
        if (b.empty()) {
            Data.erase(Data.begin());
        }
        AFL_VERIFY(--Size >= 0);
        return true;
    }

    bool ExtractFrontTo(TTabletsByBlob& dest) {
        TTabletId tabletId;
        TUnifiedBlobId blobId;
        if (!ExtractFront(tabletId, blobId)) {
            return false;
        }
        AFL_VERIFY(dest.Add(tabletId, blobId));
        return true;
    }

    bool ExtractBlobTo(const TUnifiedBlobId& blobId, TTabletsByBlob& dest) {
        auto it = Data.find(blobId);
        if (it == Data.end()) {
            return false;
        }
        AFL_VERIFY(dest.Add(blobId, it->second));
        Size -= it->second.size();
        AFL_VERIFY(Size >= 0);
        Data.erase(it);
        return true;
    }

    bool Add(const TTabletId tabletId, const TUnifiedBlobId& blobId) {
        THashSet<TUnifiedBlobId> hashSet = {blobId};
        return Add(tabletId, hashSet);
    }

    bool Add(const TTabletsByBlob& blobs) {
        bool uniqueOnly = true;
        for (auto&& i : blobs.Data) {
            if (!Add(i.first, i.second)) {
                uniqueOnly = false;
            }
        }
        return uniqueOnly;
    }

    bool Add(const TTabletId tabletId, const THashSet<TUnifiedBlobId>& blobIds) {
        bool hasSkipped = false;
        for (auto&& i : blobIds) {
            auto it = Data.find(i);
            if (it == Data.end()) {
                THashSet<TTabletId> tabletsLocal = {tabletId};
                it = Data.emplace(i, tabletsLocal).first;
                Size += 1;
            } else {
                if (!it->second.emplace(tabletId).second) {
                    hasSkipped = true;
                } else {
                    Size += 1;
                }
            }
        }
        return !hasSkipped;
    }

    bool Add(const TUnifiedBlobId& blobId, const THashSet<TTabletId>& tabletIds) {
        bool hasSkipped = false;
        if (tabletIds.empty()) {
            return true;
        }
        auto& hashSet = Data[blobId];
        for (auto&& i : tabletIds) {
            if (!hashSet.emplace(i).second) {
                hasSkipped = true;
            } else {
                Size += 1;
            }
        }
        return !hasSkipped;
    }

    bool Remove(const TTabletId tabletId, const TUnifiedBlobId& blobId) {
        auto it = Data.find(blobId);
        if (it == Data.end()) {
            return false;
        }
        auto itTablet = it->second.find(tabletId);
        if (itTablet == it->second.end()) {
            return false;
        }
        it->second.erase(itTablet);
        AFL_VERIFY(--Size >= 0);
        if (it->second.empty()) {
            Data.erase(it);
        }
        return true;
    }

    TString DebugString() const;
};

class TBlobsByTablet {
private:
    THashMap<TTabletId, THashSet<TUnifiedBlobId>> Data;
public:
    class TIterator {
    private:
        const TBlobsByTablet* Owner;
        THashMap<TTabletId, THashSet<TUnifiedBlobId>>::const_iterator TabletsIterator;
        THashSet<TUnifiedBlobId>::const_iterator BlobsIterator;
    public:
        TIterator(const TBlobsByTablet& owner)
            : Owner(&owner)
        {
            TabletsIterator = Owner->Data.begin();
            if (TabletsIterator != Owner->Data.end()) {
                BlobsIterator = TabletsIterator->second.begin();
            }
        }

        const TUnifiedBlobId& GetBlobId() const {
            AFL_VERIFY(IsValid());
            return *BlobsIterator;
        }

        TTabletId GetTabletId() const {
            AFL_VERIFY(IsValid());
            return TabletsIterator->first;
        }

        bool IsValid() const {
            return TabletsIterator != Owner->Data.end() && BlobsIterator != TabletsIterator->second.end();
        }

        void operator++() {
            AFL_VERIFY(IsValid());
            ++BlobsIterator;
            if (BlobsIterator == TabletsIterator->second.end()) {
                ++TabletsIterator;
                if (TabletsIterator != Owner->Data.end()) {
                    BlobsIterator = TabletsIterator->second.begin();
                }
            }
        }
    };

    TIterator GetIterator() const {
        return TIterator(*this);
    }

    THashMap<TTabletId, THashSet<TUnifiedBlobId>>::const_iterator begin() const {
        return Data.begin();
    }

    THashMap<TTabletId, THashSet<TUnifiedBlobId>>::const_iterator end() const {
        return Data.end();
    }

    void GetFront(const ui32 count, TBlobsByTablet& result) const {
        TBlobsByTablet resultLocal;
        ui32 countReady = 0;
        for (auto&& i : Data) {
            for (auto&& b : i.second) {
                if (countReady >= count) {
                    std::swap(result, resultLocal);
                    return;
                }
                resultLocal.Add(i.first, b);
                ++countReady;
            }
        }
        std::swap(result, resultLocal);
    }

    bool Contains(const TTabletId tabletId, const TUnifiedBlobId& blobId) const {
        auto it = Data.find(tabletId);
        if (it == Data.end()) {
            return false;
        }
        return it->second.contains(blobId);
    }

    const THashSet<TUnifiedBlobId>* Find(const TTabletId tabletId) const {
        auto it = Data.find(tabletId);
        if (it == Data.end()) {
            return nullptr;
        }
        return &it->second;
    }

    void ExtractFront(const ui32 count, TBlobsByTablet* result) {
        TBlobsByTablet resultLocal;
        ui32 countLocal = 0;
        while (Data.size()) {
            auto& t = *Data.begin();
            while (t.second.size()) {
                auto& b = *t.second.begin();
                if (result && countLocal >= count) {
                    std::swap(*result, resultLocal);
                    return;
                }
                if (countLocal >= count) {
                    return;
                }
                ++countLocal;
                if (result) {
                    resultLocal.Add(t.first, b);
                }
                t.second.erase(t.second.begin());
            }
            if (t.second.empty()) {
                Data.erase(Data.begin());
            }
        }
        if (result) {
            std::swap(*result, resultLocal);
        }
    }

    bool Add(const TTabletId tabletId, const TUnifiedBlobId& blobId) {
        auto it = Data.find(tabletId);
        if (it == Data.end()) {
            THashSet<TUnifiedBlobId> hashSet = {blobId};
            Data.emplace(tabletId, std::move(hashSet));
            return true;
        } else {
            return it->second.emplace(blobId).second;
        }
    }

    ui32 GetSize() const {
        ui32 result = 0;
        for (auto&& i : Data) {
            result += i.second.size();
        }
        return result;
    }

    bool IsEmpty() const {
        return Data.empty();
    }

    bool Remove(const TTabletId tabletId, const TUnifiedBlobId& blobId) {
        auto it = Data.find(tabletId);
        if (it == Data.end()) {
            return false;
        } else {
            const bool result = it->second.erase(blobId);
            if (result && it->second.empty()) {
                Data.erase(it);
            }
            return result;
        }
    }

    bool Remove(const TTabletId tabletId) {
        auto it = Data.find(tabletId);
        if (it == Data.end()) {
            return false;
        } else {
            Data.erase(it);
            return true;
        }
    }
};

class TBlobsCategories {
private:
    TTabletId SelfTabletId;
    YDB_READONLY_DEF(TBlobsByTablet, Sharing);
    YDB_ACCESSOR_DEF(TBlobsByTablet, Direct);
    YDB_READONLY_DEF(TBlobsByTablet, Borrowed);
public:
    bool IsEmpty() const {
        return Sharing.IsEmpty() && Direct.IsEmpty() && Borrowed.IsEmpty();
    }

    bool HasSharingOnly() const {
        return !Sharing.IsEmpty() && Direct.IsEmpty() && Borrowed.IsEmpty();
    }

    class TIterator {
    private:
        const TBlobsCategories* Owner;
        std::optional<TBlobsByTablet::TIterator> Sharing;
        std::optional<TBlobsByTablet::TIterator> Direct;
        std::optional<TBlobsByTablet::TIterator> Borrowed;
        TBlobsByTablet::TIterator* CurrentIterator = nullptr;

        void SwitchIterator() {
            CurrentIterator = nullptr;
            if (Sharing && Sharing->IsValid()) {
                CurrentIterator = &*Sharing;
            } else {
                Sharing.reset();
            }
            if (Direct && Direct->IsValid()) {
                CurrentIterator = &*Direct;
            } else {
                Direct.reset();
            }
            if (Borrowed && Borrowed->IsValid()) {
                CurrentIterator = &*Borrowed;
            } else {
                Borrowed.reset();
            }
        }

    public:
        TIterator(const TBlobsCategories& owner)
            : Owner(&owner)
        {
            Sharing = Owner->Sharing.GetIterator();
            Direct = Owner->Direct.GetIterator();
            Borrowed = Owner->Borrowed.GetIterator();
            SwitchIterator();
        }

        const TUnifiedBlobId& GetBlobId() const {
            AFL_VERIFY(IsValid());
            return CurrentIterator->GetBlobId();
        }

        TTabletId GetTabletId() const {
            AFL_VERIFY(IsValid());
            return CurrentIterator->GetTabletId();
        }

        bool IsValid() const {
            return CurrentIterator && CurrentIterator->IsValid();
        }

        void operator++() {
            AFL_VERIFY(IsValid());
            ++*CurrentIterator;
            if (!CurrentIterator->IsValid()) {
                SwitchIterator();
            }
        }
    };

    TIterator GetIterator() const {
        return TIterator(*this);
    }

    void AddDirect(const TTabletId tabletId, const TUnifiedBlobId& id) {
        AFL_VERIFY(Direct.Add(tabletId, id));
    }
    void AddBorrowed(const TTabletId tabletId, const TUnifiedBlobId& id) {
        AFL_VERIFY(Borrowed.Add(tabletId, id));
    }
    void AddSharing(const TTabletId tabletId, const TUnifiedBlobId& id) {
        AFL_VERIFY(Sharing.Add(tabletId, id));
    }
    [[nodiscard]] bool RemoveSharing(const TTabletId tabletId, const TUnifiedBlobId& id) {
        return Sharing.Remove(tabletId, id);
    }
    [[nodiscard]] bool RemoveBorrowed(const TTabletId tabletId, const TUnifiedBlobId& id) {
        return Borrowed.Remove(tabletId, id);
    }
    TBlobsCategories(const TTabletId selfTabletId)
        : SelfTabletId(selfTabletId)
    {
        Y_UNUSED(SelfTabletId);
    }
};

}
