#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Forward declarations.

template <class T, size_t ChunkSize>
struct TPersistentQueueChunk;

template <class T, size_t ChunkSize>
class TPersistentQueueBase;

template <class T, size_t ChunkSize>
class TPersistentQueueIterator;

template <class T, size_t ChunkSize>
class TPersistentQueue;

template <class T, size_t ChunkSize>
class TIndexedPersistentQueue;

////////////////////////////////////////////////////////////////////////////////
// Implementation.

template <class T, size_t ChunkSize>
struct TPersistentQueueChunk
    : public TRefCounted
{
    TIntrusivePtr<TPersistentQueueChunk<T, ChunkSize>> Next;
    T Elements[ChunkSize];
};

template <class T, size_t ChunkSize>
class TPersistentQueueIterator
{
public:
    TPersistentQueueIterator();

    TPersistentQueueIterator& operator++();    // prefix
    TPersistentQueueIterator  operator++(int); // postfix

    const T& operator*() const;
    const T* operator->() const;

    bool operator==(const TPersistentQueueIterator& other) const = default;

private:
    using TChunk = TPersistentQueueChunk<T, ChunkSize>;
    using TChunkPtr = TIntrusivePtr<TChunk>;

    friend class TPersistentQueueBase<T, ChunkSize>;
    friend class TPersistentQueue<T, ChunkSize>;
    friend class TIndexedPersistentQueue<T, ChunkSize>;

    TPersistentQueueIterator(TChunkPtr chunk, size_t index);

    TChunkPtr CurrentChunk_;
    size_t CurrentIndex_ = 0;
};

template <class T, size_t ChunkSize>
class TPersistentQueueBase
{
public:
    using TIterator = TPersistentQueueIterator<T, ChunkSize>;

    size_t Size() const;
    bool Empty() const;

    TIterator Begin() const;
    TIterator End() const;

    // STL interop.
    TIterator begin() const;
    TIterator end() const;

protected:
    using TChunk = TPersistentQueueChunk<T, ChunkSize>;
    using TChunkPtr = TIntrusivePtr<TChunk>;

    friend class TPersistentQueue<T, ChunkSize>;

    size_t Size_ = 0;
    TIterator Head_;
    TIterator Tail_;
};

template <class T, size_t ChunkSize>
class TPersistentQueueSnapshot
    : public TPersistentQueueBase<T, ChunkSize>
{
public:
    template <class C>
    void Save(C& context) const;
};

////////////////////////////////////////////////////////////////////////////////
// Interface.

//! A partially persistent queue.
/*!
 *  Implemented as a linked-list of chunks each carrying #ChunkSize elements.
 *
 *  Can be modified from a single thread.
 *  Snapshots can be read from arbitrary threads.
 */
template <class T, size_t ChunkSize>
class TPersistentQueue
    : public TPersistentQueueBase<T, ChunkSize>
{
public:
    virtual void Enqueue(T value);
    virtual T Dequeue();
    virtual void Clear();

    using TSnapshot = TPersistentQueueSnapshot<T, ChunkSize>;
    TSnapshot MakeSnapshot() const;

    template <class C>
    void Load(C& context);

private:
    using TChunk = TPersistentQueueChunk<T, ChunkSize>;
    using TChunkPtr = TIntrusivePtr<TChunk>;
};

// Almost zero-cost until used extension over TPersistentQueue.
// Provides random-access to data.
// TPersistentQueue is implemented via linked list.
// Random access is implemented via random-access index (std::vector).
// To avoid rebuilding index on every Enqueue/Deque index is built only on Freeze.
// Random access until Freeze is forbidden.
template <class T, size_t ChunkSize>
class TIndexedPersistentQueue
    : public TPersistentQueue<T, ChunkSize>
{
    using TBase = TPersistentQueue<T, ChunkSize>;

public:
    void Enqueue(T value) override;
    T Dequeue() override;
    void Clear() override;

    void Freeze();
    const T& operator[](int index) const;

private:
    using TChunk = TPersistentQueueChunk<T, ChunkSize>;

    bool Frozen_ = false;
    int Shift_ = 0;
    std::vector<TChunk*> Chunks_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PERSISTENT_QUEUE_INL_H_
#include "persistent_queue-inl.h"
#undef PERSISTENT_QUEUE_INL_H_
