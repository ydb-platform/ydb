#pragma once

#include <ydb/core/mind/defs.h>
#include <ydb/core/util/tuples.h>
#include <map>
#include <list>
#include <utility>

namespace NKikimr {
namespace NHive {

class TSequencer {
public:
    using TElementType = ui64;
    using TOwnerType = std::pair<ui64, ui64>;

    struct TSequence {
        TElementType Begin = 0;
        TElementType Next = 0;
        TElementType End = 0;

        constexpr TSequence(TElementType begin, TElementType next, TElementType end)
            : Begin(begin)
            , Next(next)
            , End(end)
        {
        }

        constexpr TSequence(TElementType begin, TElementType end)
            : Begin(begin)
            , Next(begin)
            , End(end)
        {
        }

        constexpr TSequence(TElementType point)
            : Begin(point)
            , Next(point + 1)
            , End(point + 1)
        {
        }

        constexpr TSequence()
            : Begin(0)
            , Next(0)
            , End(0)
        {
        }

        bool operator <(TSequence sequence) const {
            return Begin < sequence.Begin;
        }

        bool operator ==(TSequence sequence) const {
            return Begin == sequence.Begin && End == sequence.End;
        }

        bool operator !=(TSequence sequence) const {
            return !(operator ==(sequence));
        }

        TSequence BiteOff(size_t size) {
            size = Min(size, Size());
            TSequence result(Next, Next, Next + size);
            Next += size;
            return result;
        }

        size_t Size() const {
            return End - Next;
        }

        bool Empty() const {
            return Next == End;
        }

        bool Contains(TElementType element) const {
            return element >= Begin && element < End;
        }

        void Clear() {
            Begin = End = Next = 0;
        }

        TElementType GetNext() const {
            return Next;
        }
    };

    static constexpr TElementType NO_ELEMENT = {};
    static constexpr TElementType NO_OWNER = {};
    static TSequence NO_SEQUENCE;
};

// to generate sequences/elements from another
class TSequenceGenerator : public TSequencer {
public:
    bool AddFreeSequence(TOwnerType owner, TSequence sequence); // free elements, which we could use to allocate from
    void AddAllocatedSequence(TOwnerType owner, TSequence sequence); // allocated elements by other owners
    bool AddSequence(TOwnerType owner, TSequence sequence);
    TElementType AllocateElement(std::vector<TOwnerType>& modified);
    TSequence AllocateSequence(TOwnerType owner, size_t size, std::vector<TOwnerType>& modified); // size = max possible size, but not exact size
    TSequence GetSequence(TOwnerType owner);
    TOwnerType GetOwner(TElementType element); // for unit tests only
    size_t FreeSize() const; // for unit tests only
    size_t AllocatedSequencesSize() const;
    size_t AllocatedSequencesCount() const;
    size_t NextFreeSequenceIndex() const;
    TElementType GetNextElement() const;
    void Clear();

protected:
    std::unordered_map<TOwnerType, TSequence> SequenceByOwner;
    std::map<TSequence, TOwnerType> AllocatedSequences;
    std::list<TOwnerType> FreeSequences;
    size_t FreeSequencesIndex = 0;
    size_t FreeSize_ = 0;
    size_t AllocatedSize_ = 0;
};

// to keep ownership of sequences
class TOwnershipKeeper : public TSequencer {
public:
    using TOwnerType = TElementType;

    bool AddOwnedSequence(TOwnerType owner, TSequence sequence); // all sequence range we owns, free and used, including allocated by the others
    TOwnerType GetOwner(TElementType element);
    void Clear();
    void GetOwnedSequences(TOwnerType owner, std::vector<TSequence>& sequences) const;

protected:
    std::map<TSequence, TOwnerType> OwnedSequences;
};

}
}
