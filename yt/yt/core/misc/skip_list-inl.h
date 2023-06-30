#ifndef SKIP_LIST_INL_H_
#error "Direct inclusion of this file is not allowed, include skip_list.h"
// For the sake of sane code completion.
#include "skip_list.h"
#endif

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <util/random/random.h>

#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::TIterator::TIterator()
    : Head_(nullptr)
    , Current_(nullptr)
{ }

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::TIterator::TIterator(
    const TSkipList* owner,
    const TNode* current)
    : Head_(owner->Head_)
    , Current_(current)
{ }

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::TIterator::TIterator(const TIterator& other)
    : Head_(other.Head_)
    , Current_(other.Current_)
{ }

template <class TKey, class TComparer>
const TKey& TSkipList<TKey, TComparer>::TIterator::GetCurrent() const
{
    YT_ASSERT(IsValid());
    return Current_->GetKey();
}

template <class TKey, class TComparer>
bool TSkipList<TKey, TComparer>::TIterator::IsValid() const
{
    return Current_ != Head_;
}

template <class TKey, class TComparer>
void TSkipList<TKey, TComparer>::TIterator::MovePrev()
{
    YT_ASSERT(IsValid());
    Current_ = Current_->GetPrev(0);
}

template <class TKey, class TComparer>
void TSkipList<TKey, TComparer>::TIterator::MoveNext()
{
    YT_ASSERT(IsValid());
    Current_ = Current_->GetNext(0);
}

template <class TKey, class TComparer>
typename TSkipList<TKey, TComparer>::TIterator& TSkipList<TKey, TComparer>::TIterator::operator=(const TIterator& other)
{
    Head_ = other.Head_;
    Current_ = other.Current_;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::TNode::TNode(const TKey& key)
    : Key_(key)
{ }

template <class TKey, class TComparer>
const TKey& TSkipList<TKey, TComparer>::TNode::GetKey() const
{
    return Key_;
}

template <class TKey, class TComparer>
typename TSkipList<TKey, TComparer>::TNode* TSkipList<TKey, TComparer>::TNode::GetPrev(int height) const
{
    return Link_[height].first;
}

template <class TKey, class TComparer>
typename TSkipList<TKey, TComparer>::TNode* TSkipList<TKey, TComparer>::TNode::GetNext(int height) const
{
    return Link_[height].second;
}

template <class TKey, class TComparer>
void TSkipList<TKey, TComparer>::TNode::SetPrev(int height, TNode* next)
{
    Link_[height].first = next;
}

template <class TKey, class TComparer>
void TSkipList<TKey, TComparer>::TNode::SetNext(int height, TNode* next)
{
    Link_[height].second = next;
}

template <class TKey, class TComparer>
void TSkipList<TKey, TComparer>::TNode::InsertAfter(int height, TNode** prevs)
{
    for (int index = 0; index < height; ++index) {
        // NB: GetPrev->GetPrev->GetNext may return this, not Prev.
        auto next = prevs[index]->GetNext(index);
        SetNext(index, next);
        SetPrev(index, prevs[index]);
        next->SetPrev(index, this);
        prevs[index]->SetNext(index, this);
    }
}

template <class TKey, class TComparer>
size_t TSkipList<TKey, TComparer>::TNode::GetByteSize(int height)
{
    // -1 since Link_ is of size 1.
    return sizeof(TNode) + sizeof(TLink) * (height - 1);
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::TSkipList(
    TChunkedMemoryPool* pool,
    const TComparer& comparer)
    : Pool_(pool)
    , Comparer_(comparer)
    , Head_(AllocateHeadNode())
{
    for (int index = 0; index < MaxHeight; ++index) {
        Prevs_[index] = Head_;
    }
}

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::~TSkipList()
{
    if (!std::is_trivially_destructible<TKey>::value) {
        auto* current = Head_;
        while (current) {
            auto* next = current->GetNext(0);
            current->~TNode();
            current = next;
        }
    }
}

template <class TKey, class TComparer>
int TSkipList<TKey, TComparer>::GetSize() const
{
    return Size_;
}

template <class TKey, class TComparer>
template <class TPivot, class TNewKeyProvider, class TExistingKeyConsumer>
void TSkipList<TKey, TComparer>::Insert(
    const TPivot& pivot,
    const TNewKeyProvider& newKeyProvider,
    const TExistingKeyConsumer& existingKeyConsumer)
{
    auto* lastInserted = Prevs_[0];
    auto* next = lastInserted->GetNext(0);

    if ((lastInserted != Head_ && Comparer_(lastInserted->GetKey(), pivot) >= 0) ||
        (next != Head_ && Comparer_(next->GetKey(), pivot) < 0))
    {
        next = DoFindGreaterThanOrEqualTo(pivot, Prevs_.data());
    }

    if (next != Head_ && Comparer_(next->GetKey(), pivot) == 0) {
        existingKeyConsumer(next->GetKey());
        return;
    }

    int currentHeight = Height_;
    int randomHeight = GenerateHeight();

    // Upgrade current height if needed.
    if (randomHeight > currentHeight) {
        for (int index = currentHeight; index < randomHeight; ++index) {
            Prevs_[index] = Head_;
        }
        Height_ = randomHeight;
    }

    // Insert a new node.
    auto* node = AllocateNode(newKeyProvider(), randomHeight);
    node->InsertAfter(randomHeight, Prevs_.data());
    ++Size_;

    for (int index = 0; index < randomHeight; ++index) {
        Prevs_[index] = node;
    }
}

template <class TKey, class TComparer>
bool TSkipList<TKey, TComparer>::Insert(const TKey& key)
{
    bool result = true;
    Insert(
        key,
        [&] () { return key; },
        [&] (const TKey& /*key*/) { result = false; });
    return result;
}

template <class TKey, class TComparer>
template <class TPivot>
typename TSkipList<TKey, TComparer>::TIterator TSkipList<TKey, TComparer>::FindLessThanOrEqualTo(const TPivot& pivot) const
{
    auto* lowerBound = DoFindGreaterThanOrEqualTo(pivot, nullptr);
    if (lowerBound != Head_ && Comparer_(lowerBound->GetKey(), pivot) == 0) {
        return TIterator(this, lowerBound);
    }
    lowerBound = lowerBound->GetPrev(0);
    return lowerBound != Head_
        ? TIterator(this, lowerBound)
        : TIterator();
}

template <class TKey, class TComparer>
template <class TPivot>
typename TSkipList<TKey, TComparer>::TIterator TSkipList<TKey, TComparer>::FindGreaterThanOrEqualTo(const TPivot& pivot) const
{
    auto* lowerBound = DoFindGreaterThanOrEqualTo(pivot, nullptr);
    return lowerBound != Head_
        ? TIterator(this, lowerBound)
        : TIterator();
}

template <class TKey, class TComparer>
template <class TPivot>
typename TSkipList<TKey, TComparer>::TIterator TSkipList<TKey, TComparer>::FindEqualTo(const TPivot& pivot) const
{
    auto* lowerBound = DoFindGreaterThanOrEqualTo(pivot, nullptr);
    return lowerBound != Head_ && Comparer_(lowerBound->GetKey(), pivot) == 0
        ? TIterator(this, lowerBound)
        : TIterator();
}

template <class TKey, class TComparer>
int TSkipList<TKey, TComparer>::GenerateHeight()
{
    int height = 1;
    while (height < MaxHeight && (RandomNumber<unsigned int>() % InverseProbability) == 0) {
        ++height;
    }
    return height;
}

template <class TKey, class TComparer>
typename TSkipList<TKey, TComparer>::TNode* TSkipList<TKey, TComparer>::AllocateNode(const TKey& key, int height)
{
    auto* buffer = Pool_->AllocateAligned(TNode::GetByteSize(height));
    new (buffer) TNode(key);
    return reinterpret_cast<TNode*>(buffer);
}

template <class TKey, class TComparer>
typename TSkipList<TKey, TComparer>::TNode* TSkipList<TKey, TComparer>::AllocateHeadNode()
{
    auto head = AllocateNode(TKey(), MaxHeight);
    for (int index = 0; index < MaxHeight; ++index) {
        head->SetNext(index, head);
        head->SetPrev(index, head);
    }
    return head;
}

template <class TKey, class TComparer>
template <class TPivot>
typename TSkipList<TKey, TComparer>::TNode* TSkipList<TKey, TComparer>::DoFindGreaterThanOrEqualTo(const TPivot& pivot, TNode** prevs) const
{
    auto* current = Head_;
    auto* lastChecked = Head_;
    int height = Height_ - 1;
    while (true) {
        auto* next = current->GetNext(height);
        if (next != Head_ && next != lastChecked && Comparer_(next->GetKey(), pivot) < 0) {
            current = next;
        } else {
            if (prevs) {
                prevs[height] = current;
            }
            if (height > 0) {
                lastChecked = next;
                --height;
            } else {
                return next;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
