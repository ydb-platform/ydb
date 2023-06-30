#pragma once

#include "public.h"

#include <library/cpp/yt/memory/public.h>

#include <atomic>
#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! An insert-only concurrent skip-list.
/*!
 *  All mutating methods (including ctor and dtor) must be called from a single (writer) thread.
 *  All const methods can be called from arbitrary (reader) threads.
 *
 *  Kudos to Yandex RTMR Team :)
 */
template <class TKey, class TComparer>
class TSkipList
{
private:
    class TNode;

public:
    TSkipList(
        TChunkedMemoryPool* pool,
        const TComparer& comparer);

    ~TSkipList();

    //! Returns the number of distinct keys in the list.
    int GetSize() const;

    //! Tries to insert a new key.
    //! If a key equivalent to |pivot| is already present then invokes |existingKeyConsumer| passing that key.
    //! Otherwise invokes |newKeyProvider| to obtain the actual key and inserts that key.
    template <class TPivot, class TNewKeyProvider, class TExistingKeyConsumer>
    void Insert(
        const TPivot& pivot,
        const TNewKeyProvider& newKeyProvider,
        const TExistingKeyConsumer& existingKeyConsumer);

    //! Tries to insert a key.
    //! Returns |false| if a key equivalent to |key| is already present.
    //! Otherwise returns |true| and inserts |key|.
    bool Insert(const TKey& key);


    class TIterator
    {
    public:
        TIterator();
        TIterator(const TSkipList* owner, const TNode* current);
        TIterator(const TIterator& other);

        TIterator& operator = (const TIterator& other);

        //! Advances the iterator to the previous item.
        void MovePrev();

        //! Advances the iterator to the next item.
        void MoveNext();

        //! Returns |true| if the iterator points to a valid item.
        bool IsValid() const;

        //! Returns the key the iterator points to.
        const TKey& GetCurrent() const;

    private:
        const TNode* Head_;
        const TNode* Current_;

    };

    //! Tries to find a key equivalent to |pivot|.
    //! If succeeds then returns an iterator pointing to that key.
    //! Otherwise returns an invalid iterator.
    template <class TPivot>
    TIterator FindEqualTo(const TPivot& pivot) const;

    //! Returns an iterator pointing to the smallest key that compares greater than or
    //! equal to |pivot|. If no such key is found then returns an invalid iterator.
    template <class TPivot>
    TIterator FindGreaterThanOrEqualTo(const TPivot& pivot) const;

    //! Returns an iterator pointing to the largest key that compares less than or
    //! equal to |pivot|. If no such key is found then returns an invalid iterator.
    template <class TPivot>
    TIterator FindLessThanOrEqualTo(const TPivot& pivot) const;

private:
    static const int MaxHeight = 12;
    static const int InverseProbability = 4;

    class TNode
    {
    public:
        explicit TNode(const TKey& key);

        const TKey& GetKey() const;

        TNode* GetPrev(int height) const;
        TNode* GetNext(int height) const;
        void SetPrev(int height, TNode* next);
        void SetNext(int height, TNode* next);

        void InsertAfter(int height, TNode** prevs);

        static size_t GetByteSize(int height);

    private:
        const TKey Key_;
        using TLink = std::pair<std::atomic<TNode*>, std::atomic<TNode*>>;
        TLink Link_[1]; // variable-size array with actual size up to MaxHeight
    };

    static_assert(sizeof(std::atomic<TNode*>) == sizeof(intptr_t), "std::atomic<TNode*> does not seem to be lock-free.");

private:
    TChunkedMemoryPool* const Pool_;
    const TComparer Comparer_;
    TNode* const Head_;
    std::array<TNode*, MaxHeight> Prevs_;

    std::atomic<int> Size_ = {0};
    std::atomic<int> Height_ = {1};

private:
    static int GenerateHeight();

    TNode* AllocateNode(const TKey& key, int height);
    TNode* AllocateHeadNode();

    template <class TPivot>
    TNode* DoFindGreaterThanOrEqualTo(const TPivot& pivot, TNode** prevs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SKIP_LIST_INL_H_
#include "skip_list-inl.h"
#undef SKIP_LIST_INL_H_
