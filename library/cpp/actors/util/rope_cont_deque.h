#pragma once

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <deque>

namespace NRopeDetails {

template<typename TChunk>
class TChunkList {
    std::deque<TChunk> Chunks;
 
    static constexpr size_t MaxInplaceItems = 4;
    using TInplace = TStackVec<TChunk, MaxInplaceItems>;
    TInplace Inplace;

private:
    template<typename TChunksIt, typename TInplaceIt, typename TValue>
    struct TIterator {
        TChunksIt ChunksIt;
        TInplaceIt InplaceIt;

        TIterator() = default;

        TIterator(TChunksIt chunksIt, TInplaceIt inplaceIt)
            : ChunksIt(std::move(chunksIt))
            , InplaceIt(inplaceIt)
        {}

        template<typename A, typename B, typename C>
        TIterator(const TIterator<A, B, C>& other)
            : ChunksIt(other.ChunksIt)
            , InplaceIt(other.InplaceIt)
        {}

        TIterator(const TIterator&) = default;
        TIterator(TIterator&&) = default;
        TIterator& operator =(const TIterator&) = default;
        TIterator& operator =(TIterator&&) = default;

        TValue& operator *() const { return InplaceIt != TInplaceIt() ? *InplaceIt : *ChunksIt; }
        TValue* operator ->() const { return InplaceIt != TInplaceIt() ? &*InplaceIt : &*ChunksIt; }

        TIterator& operator ++() {
            if (InplaceIt != TInplaceIt()) {
                ++InplaceIt;
            } else {
                ++ChunksIt;
            }
            return *this;
        }

        TIterator& operator --() {
            if (InplaceIt != TInplaceIt()) {
                --InplaceIt;
            } else {
                --ChunksIt;
            }
            return *this;
        }

        template<typename A, typename B, typename C>
        bool operator ==(const TIterator<A, B, C>& other) const {
            return ChunksIt == other.ChunksIt && InplaceIt == other.InplaceIt;
        }

        template<typename A, typename B, typename C>
        bool operator !=(const TIterator<A, B, C>& other) const {
            return ChunksIt != other.ChunksIt || InplaceIt != other.InplaceIt;
        }
    };

public:
    using iterator = TIterator<typename std::deque<TChunk>::iterator, typename TInplace::iterator, TChunk>;
    using const_iterator = TIterator<typename std::deque<TChunk>::const_iterator, typename TInplace::const_iterator, const TChunk>;

public:
    TChunkList() = default;
    TChunkList(const TChunkList& other) = default;
    TChunkList(TChunkList&& other) = default;
    TChunkList& operator=(const TChunkList& other) = default;
    TChunkList& operator=(TChunkList&& other) = default;

    template<typename... TArgs>
    void PutToEnd(TArgs&&... args) {
        InsertBefore(end(), std::forward<TArgs>(args)...);
    }

    template<typename... TArgs>
    iterator InsertBefore(iterator pos, TArgs&&... args) {
        if (!Inplace) {
            pos.InplaceIt = Inplace.end();
        }
        if (Chunks.empty() && Inplace.size() < MaxInplaceItems) {
            return {{}, Inplace.emplace(pos.InplaceIt, std::forward<TArgs>(args)...)};
        } else {
            if (Inplace) {
                Y_VERIFY_DEBUG(Chunks.empty());
                for (auto& item : Inplace) {
                    Chunks.push_back(std::move(item));
                }
                pos.ChunksIt = pos.InplaceIt - Inplace.begin() + Chunks.begin();
                Inplace.clear();
            }
            return {Chunks.emplace(pos.ChunksIt, std::forward<TArgs>(args)...), {}};
        }
    }

    iterator Erase(iterator pos) {
        if (Inplace) {
            return {{}, Inplace.erase(pos.InplaceIt)};
        } else {
            return {Chunks.erase(pos.ChunksIt), {}};
        }
    }

    iterator Erase(iterator first, iterator last) {
        if (Inplace) {
            return {{}, Inplace.erase(first.InplaceIt, last.InplaceIt)};
        } else {
            return {Chunks.erase(first.ChunksIt, last.ChunksIt), {}};
        }
    }

    void EraseFront() {
        if (Inplace) {
            Inplace.erase(Inplace.begin());
        } else {
            Chunks.pop_front();
        }
    }

    void EraseBack() {
        if (Inplace) {
            Inplace.pop_back();
        } else {
            Chunks.pop_back();
        }
    }

    iterator Splice(iterator pos, TChunkList& from, iterator first, iterator last) {
        if (!Inplace) {
            pos.InplaceIt = Inplace.end();
        }
        size_t n = 0;
        for (auto it = first; it != last; ++it, ++n)
        {}
        if (Chunks.empty() && Inplace.size() + n <= MaxInplaceItems) {
            if (first.InplaceIt != typename TInplace::iterator()) {
                Inplace.insert(pos.InplaceIt, first.InplaceIt, last.InplaceIt);
            } else {
                Inplace.insert(pos.InplaceIt, first.ChunksIt, last.ChunksIt);
            }
        } else {
            if (Inplace) {
                Y_VERIFY_DEBUG(Chunks.empty());
                for (auto& item : Inplace) {
                    Chunks.push_back(std::move(item));
                }
                pos.ChunksIt = pos.InplaceIt - Inplace.begin() + Chunks.begin();
                Inplace.clear();
            }
            if (first.InplaceIt != typename TInplace::iterator()) {
                Chunks.insert(pos.ChunksIt, first.InplaceIt, last.InplaceIt);
            } else {
                Chunks.insert(pos.ChunksIt, first.ChunksIt, last.ChunksIt);
            }
        }
        return from.Erase(first, last);
    }

    operator bool() const               { return !Inplace.empty() || !Chunks.empty(); }
    TChunk& GetFirstChunk()             { return Inplace ? Inplace.front() : Chunks.front(); }
    const TChunk& GetFirstChunk() const { return Inplace ? Inplace.front() : Chunks.front(); }
    TChunk& GetLastChunk()              { return Inplace ? Inplace.back() : Chunks.back(); }
    iterator begin()                    { return {Chunks.begin(), Inplace ? Inplace.begin() : typename TInplace::iterator()}; }
    const_iterator begin() const        { return {Chunks.begin(), Inplace ? Inplace.begin() : typename TInplace::const_iterator()}; }
    iterator end()                      { return {Chunks.end(), Inplace ? Inplace.end() : typename TInplace::iterator()}; }
    const_iterator end() const          { return {Chunks.end(), Inplace ? Inplace.end() : typename TInplace::const_iterator()}; }
};

} // NRopeDetails
