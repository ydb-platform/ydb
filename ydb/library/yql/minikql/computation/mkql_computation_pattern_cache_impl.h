#pragma once

#include "mkql_computation_pattern_cache.h"

using namespace std::placeholders;

namespace NKikimr::NMiniKQL {

class TComputationPatternLRUCache::TLRUPatternCacheImpl {
private:
    template <class T, class THash>
    class TLRUList {
        using OnExpiredCallbackFn = std::function<void(const T&)>;
        using SizeInBytesFn = std::function<size_t(const T&)>;

    public:
        explicit TLRUList(size_t maxSize, size_t maxSizeInBytes, SizeInBytesFn sizeInBytes, OnExpiredCallbackFn onExpiredCallback)
        : MaxSize_(maxSize)
        , MaxSizeInBytes_(maxSizeInBytes)
        , GetSizeInBytes_(sizeInBytes)
        , OnExpiredCallback_(onExpiredCallback)
        {}

        size_t Size() const {
            return List_.size();
        }

        size_t SizeInBytes() const {
            return SizeInBytes_;
        }

        bool Has(const T& item) const {
            return Iterators_.contains(item);
        }

        void PushBack(const T& item) {
            auto it = Iterators_.find(item);
            Y_ENSURE(it == Iterators_.end());
            List_.push_back(item);
            Iterators_.emplace(item, std::prev(List_.end()));
            SizeInBytes_ += GetSizeInBytes_(item);

            while (Size() > MaxSize_ || SizeInBytes() > MaxSizeInBytes_) {
                auto&& expired = std::move(List_.front());
                Remove(expired);
                List_.pop_front();

                OnExpiredCallback_(expired);
            }
        }

        void Touch(const T& item) {
            auto it = Iterators_.find(item);
            Y_ENSURE(it != Iterators_.end());
            List_.erase(it->second);
            List_.push_back(it->first);
            it->second = std::prev(List_.end());
        }

        void Remove(const T& item) {
            auto it = Iterators_.find(item);
            Y_ENSURE(it != Iterators_.end());

            auto size = GetSizeInBytes_(item);
            Y_ENSURE(size <= SizeInBytes_);

            List_.erase(it->second);
            Iterators_.erase(it);
            SizeInBytes_ -= size;
        }

        void Clear() {
            List_.clear();
            Iterators_.clear();
            SizeInBytes_ = 0;
        }

    private:
        const size_t MaxSize_, MaxSizeInBytes_;

        SizeInBytesFn GetSizeInBytes_;
        OnExpiredCallbackFn OnExpiredCallback_;

        TList<T> List_;
        THashMap<T, typename TList<T>::iterator, THash> Iterators_;
        size_t SizeInBytes_ = 0;
    };

public:
    TLRUPatternCacheImpl(
        size_t maxPatternsSize,
        size_t maxPatternsSizeInBytes,
        size_t maxCompiledPatternsSize,
        size_t maxCompiledPatternsSizeInBytes)
        : LRUPatternList_(maxPatternsSize, maxPatternsSizeInBytes,
            [](const TCacheEntry& entry) {
                return entry.second->SizeForCache;
            },
            std::bind(&TLRUPatternCacheImpl::OnPatternExpiredCallback, this, _1))
        , LRUCompiledPatternList_(maxCompiledPatternsSize, maxCompiledPatternsSizeInBytes,
            [](const TCacheEntry& entry) {
                return entry.second->Pattern->CompiledCodeSize();
            },
            std::bind(&TLRUPatternCacheImpl::OnCompiledPatternExpiredCallback, this, _1))
    {}

    size_t PatternsSize() const {
        return ProgramToEntryMap_.size();
    }

    size_t PatternsSizeInBytes() const {
        return LRUPatternList_.SizeInBytes();
    }

    size_t CompiledPatternsSize() const {
        return LRUCompiledPatternList_.Size();
    }

    size_t CompiledPatternsSizeInBytes() const {
        return LRUCompiledPatternList_.SizeInBytes();
    }

    std::shared_ptr<TPatternCacheEntry> Find(const TString& serializedProgram) {
        auto it = ProgramToEntryMap_.find(serializedProgram);
        if (it == ProgramToEntryMap_.end()) {
            return {};
        }

        LRUPatternList_.Touch(it->second);
        if (LRUCompiledPatternList_.Has(it->second)) {
            LRUCompiledPatternList_.Touch(it->second);
        }

        return it->second.second;
    }

    void InsertOrUpdate(const TString& serializedProgram, const std::shared_ptr<TPatternCacheEntry>& newEntry) {
        auto [it, isNew] = ProgramToEntryMap_.emplace(std::piecewise_construct,
            std::forward_as_tuple(serializedProgram),
            std::forward_as_tuple(std::make_shared<TString>(serializedProgram), newEntry));

        // TODO: what to do if new and old entries are different?
        auto entry = it->second.second;

        if (!isNew) {
            // Remove entry from lists to update sizes in bytes later.
            LRUPatternList_.Remove(it->second);
            if (LRUCompiledPatternList_.Has(it->second)) {
                LRUCompiledPatternList_.Remove(it->second);
            }
        }

        entry->UpdateSizeForCache();
        LRUPatternList_.PushBack(it->second);

        if (entry->Pattern->IsCompiled()) {
            LRUCompiledPatternList_.PushBack(it->second);
        }

        entry->IsInCache = true;
    }

    void NotifyPatternCompiled(const TString& serializedProgram) {
        auto it = ProgramToEntryMap_.find(serializedProgram);
        if (it == ProgramToEntryMap_.end()) {
            return;
        }

        auto entry = it->second.second;

        Y_ENSURE(entry->Pattern->IsCompiled());
        Y_ENSURE(!LRUCompiledPatternList_.Has(it->second));

        LRUPatternList_.Touch(it->second);
        LRUCompiledPatternList_.PushBack(it->second);
    }

    void Clear() {
        for (auto& [_, entry] : ProgramToEntryMap_) {
            entry.second->IsInCache = false;
        }
        ProgramToEntryMap_.clear();

        LRUPatternList_.Clear();
        LRUCompiledPatternList_.Clear();
    }

private:
    using TCacheEntry = std::pair<std::shared_ptr<TString>, std::shared_ptr<TPatternCacheEntry>>;

    struct TCacheEntryHash {
        size_t operator()(const TCacheEntry& entry) const {
            return THash<TString>()(*entry.first);
        }
    };

    void OnPatternExpiredCallback(const TCacheEntry& entry) {
        Y_ENSURE(ProgramToEntryMap_.erase(*entry.first));
        entry.second->IsInCache = false;
        if (LRUCompiledPatternList_.Has(entry)) {
            LRUCompiledPatternList_.Remove(entry);
            OnCompiledPatternExpiredCallback(entry);
        }
    }

    void OnCompiledPatternExpiredCallback(const TCacheEntry& entry) {
        entry.second->Pattern->RemoveCompiledCode();
        entry.second->AccessTimes = 0;
    }

    THashMap<TString, TCacheEntry> ProgramToEntryMap_;
    TLRUList<TCacheEntry, TCacheEntryHash> LRUPatternList_, LRUCompiledPatternList_;
};

} // namespace NKikimr::NMiniKQL
