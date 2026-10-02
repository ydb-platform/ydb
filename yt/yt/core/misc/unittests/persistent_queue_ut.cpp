#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/persistent_queue.h>

#include <thread>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TQueueType = TPersistentQueue<int, 10>;
using TSnapshot = TPersistentQueueSnapshot<int, 10>;

TEST(TPersistentQueueTest, Empty)
{
    TQueueType queue;
    EXPECT_EQ(0u, queue.Size());
    EXPECT_TRUE(queue.Empty());
    EXPECT_EQ(queue.Begin(), queue.End());

    auto snapshot = queue.MakeSnapshot();
    EXPECT_EQ(0u, snapshot.Size());
    EXPECT_TRUE(snapshot.Empty());
    EXPECT_EQ(snapshot.Begin(), snapshot.End());
}

#ifndef NDEBUG

TEST(TPersistentQueueChunkDeathTest, RejectsAccessToUnconstructedElements)
{
    auto chunk = New<TPersistentQueueChunk<int, 2>>();
    EXPECT_DEATH({ (void)chunk->GetElement(0); }, "index < ConstructedSize_");

    chunk->Append(42);
    EXPECT_EQ(42, chunk->GetElement(0));

    const TPersistentQueueChunk<int, 2>& constChunk = *chunk;
    EXPECT_DEATH({ (void)constChunk.GetElement(1); }, "index < ConstructedSize_");
}

#endif

TEST(TPersistentQueueTest, EnqueueDequeue)
{
    TQueueType queue;

    const int N = 100;

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(i, static_cast<ssize_t>(queue.Size()));
        queue.Enqueue(i);
    }

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(N - i, static_cast<ssize_t>(queue.Size()));
        EXPECT_EQ(i, queue.Dequeue());
    }
}

TEST(TPersistentQueueTest, Iterate)
{
    TQueueType queue;

    const int N = 100;

    for (int i = 0; i < 2 * N; ++i) {
        queue.Enqueue(i);
    }

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(i, queue.Dequeue());
    }

    int expected = N;
    for (int x : queue) {
        EXPECT_EQ(expected, x);
        ++expected;
    }
}

TEST(TPersistentQueueTest, Snapshot1)
{
    TQueueType queue;

    const int N = 100;
    std::vector<TSnapshot> snapshots;

    for (int i = 0; i < N; ++i) {
        snapshots.push_back(queue.MakeSnapshot());
        queue.Enqueue(i);
    }

    for (int i = 0; i < N; ++i) {
        const auto& snapshot = snapshots[i];
        EXPECT_EQ(i, static_cast<ssize_t>(snapshot.Size()));
        int expected = 0;
        for (int x : snapshot) {
            EXPECT_EQ(expected, x);
            ++expected;
        }
    }
}

TEST(TPersistentQueueTest, Snapshot2)
{
    TQueueType queue;

    const int N = 100;
    std::vector<TSnapshot> snapshots;

    for (int i = 0; i < N; ++i) {
        queue.Enqueue(i);
    }

    for (int i = 0; i < N; ++i) {
        snapshots.push_back(queue.MakeSnapshot());
        EXPECT_EQ(i, queue.Dequeue());
    }

    for (int i = 0; i < N; ++i) {
        const auto& snapshot = snapshots[i];
        EXPECT_EQ(i, static_cast<ssize_t>(N - snapshot.Size()));
        int expected = i;
        for (int x : snapshot) {
            EXPECT_EQ(expected, x);
            ++expected;
        }
    }
}

TEST(TPersistentQueueTest, Clear)
{
    TQueueType queue;

    queue.Enqueue(1);

    EXPECT_EQ(1u, queue.Size());

    queue.Clear();

    EXPECT_EQ(0u, queue.Size());

    auto snapshot = queue.MakeSnapshot();
    EXPECT_EQ(snapshot.Begin(), snapshot.End());
}

////////////////////////////////////////////////////////////////////////////////

struct TTrackedQueueValue
{
    static inline int LiveCount = 0;
    static inline int DefaultConstructionCount = 0;

    int Value = 0;

    TTrackedQueueValue()
    {
        ++LiveCount;
        ++DefaultConstructionCount;
    }

    explicit TTrackedQueueValue(int value)
        : Value(value)
    {
        ++LiveCount;
    }

    TTrackedQueueValue(TTrackedQueueValue&& other) noexcept
        : Value(std::exchange(other.Value, -1))
    {
        ++LiveCount;
    }

    TTrackedQueueValue& operator=(TTrackedQueueValue&& other) noexcept
    {
        Value = std::exchange(other.Value, -1);
        return *this;
    }

    ~TTrackedQueueValue()
    {
        --LiveCount;
    }
};

TEST(TPersistentQueueTest, ConstructsOnlyEnqueuedValues)
{
    ASSERT_EQ(0, TTrackedQueueValue::LiveCount);
    TTrackedQueueValue::DefaultConstructionCount = 0;

    TPersistentQueue<TTrackedQueueValue, 3> queue;
    EXPECT_EQ(0, TTrackedQueueValue::LiveCount);
    EXPECT_EQ(0, queue.GetByteSize());
    for (int index = 0; index < 7; ++index) {
        queue.Enqueue(TTrackedQueueValue(index));
        EXPECT_EQ(index + 1, TTrackedQueueValue::LiveCount);
        EXPECT_EQ(0, TTrackedQueueValue::DefaultConstructionCount);
        EXPECT_EQ(
            static_cast<i64>(((index + 1) / 3 + 1) * sizeof(TPersistentQueueChunk<TTrackedQueueValue, 3>)),
            queue.GetByteSize());
    }
    queue.Clear();
    EXPECT_EQ(0, TTrackedQueueValue::LiveCount);
    EXPECT_EQ(0, queue.GetByteSize());
}

TEST(TPersistentQueueTest, SnapshotAndIteratorKeepValuesAlive)
{
    ASSERT_EQ(0, TTrackedQueueValue::LiveCount);
    using TQueue = TPersistentQueue<TTrackedQueueValue, 3>;
    TQueue::TSnapshot firstSnapshot;
    TQueue::TSnapshot secondSnapshot;
    TQueue::TIterator iterator;
    {
        TQueue queue;
        queue.Enqueue(TTrackedQueueValue(1));
        firstSnapshot = queue.MakeSnapshot();
        iterator = queue.Begin();
        queue.Enqueue(TTrackedQueueValue(2));
        secondSnapshot = queue.MakeSnapshot();
        queue.Clear();
        queue.Enqueue(TTrackedQueueValue(3));
        EXPECT_EQ(3, TTrackedQueueValue::LiveCount);
        EXPECT_EQ(1, firstSnapshot.Begin()->Value);
        EXPECT_EQ(1u, firstSnapshot.Size());
        EXPECT_EQ(2u, secondSnapshot.Size());
    }
    EXPECT_EQ(2, TTrackedQueueValue::LiveCount);
    firstSnapshot = {};
    secondSnapshot = {};
    EXPECT_EQ(2, TTrackedQueueValue::LiveCount);
    EXPECT_EQ(1, iterator->Value);
    EXPECT_EQ(2, (++iterator)->Value);
    iterator = {};
    EXPECT_EQ(0, TTrackedQueueValue::LiveCount);
}

TEST(TPersistentQueueTest, DequeuedValuesLiveUntilChunkRelease)
{
    ASSERT_EQ(0, TTrackedQueueValue::LiveCount);
    TPersistentQueue<TTrackedQueueValue, 2> queue;
    queue.Enqueue(TTrackedQueueValue(7));
    auto snapshot = queue.MakeSnapshot();
    {
        auto value = queue.Dequeue();
        EXPECT_EQ(7, value.Value);
        EXPECT_EQ(2, TTrackedQueueValue::LiveCount);
        EXPECT_EQ(-1, snapshot.Begin()->Value);
    }
    EXPECT_EQ(1, TTrackedQueueValue::LiveCount);
    queue.Clear();
    EXPECT_EQ(1, TTrackedQueueValue::LiveCount);
    snapshot = {};
    EXPECT_EQ(0, TTrackedQueueValue::LiveCount);
}

////////////////////////////////////////////////////////////////////////////////

struct TThrowingQueueValue
{
    static inline int LiveCount = 0;
    static inline bool ThrowOnMove = false;

    int Value;

    explicit TThrowingQueueValue(int value)
        : Value(value)
    {
        ++LiveCount;
    }

    TThrowingQueueValue(TThrowingQueueValue&& other) noexcept(false)
        : Value(other.Value)
    {
        if (ThrowOnMove) {
            throw std::runtime_error("Injected move failure");
        }
        other.Value = -1;
        ++LiveCount;
    }

    TThrowingQueueValue& operator=(TThrowingQueueValue&&) = delete;

    ~TThrowingQueueValue()
    {
        --LiveCount;
    }
};

template <size_t ChunkSize>
void TestThrowingQueueValue()
{
    using TQueue = TPersistentQueue<TThrowingQueueValue, ChunkSize>;
    ASSERT_EQ(0, TThrowingQueueValue::LiveCount);
    TQueue queue;

    TThrowingQueueValue::ThrowOnMove = true;
    EXPECT_THROW(queue.Enqueue(TThrowingQueueValue(0)), std::runtime_error);
    EXPECT_TRUE(queue.Empty());
    EXPECT_EQ(queue.Begin(), queue.End());
    EXPECT_EQ(0, TThrowingQueueValue::LiveCount);
    TThrowingQueueValue::ThrowOnMove = false;

    for (int index = 0; index + 1 < static_cast<int>(ChunkSize); ++index) {
        queue.Enqueue(TThrowingQueueValue(index));
    }
    auto snapshot = queue.MakeSnapshot();
    TThrowingQueueValue::ThrowOnMove = true;
    EXPECT_THROW(queue.Enqueue(TThrowingQueueValue(42)), std::runtime_error);
    EXPECT_EQ(ChunkSize - 1, queue.Size());
    EXPECT_EQ(static_cast<int>(ChunkSize) - 1, TThrowingQueueValue::LiveCount);
    EXPECT_EQ(queue.End(), snapshot.End());
    TThrowingQueueValue::ThrowOnMove = false;

    queue.Enqueue(TThrowingQueueValue(42));
    queue.Enqueue(TThrowingQueueValue(43));
    EXPECT_EQ(ChunkSize + 1, queue.Size());
    int expected = 0;
    for (const auto& value : snapshot) {
        EXPECT_EQ(expected++, value.Value);
    }
    EXPECT_EQ(static_cast<int>(ChunkSize) - 1, expected);
    {
        auto begin = queue.Begin();
        TThrowingQueueValue::ThrowOnMove = true;
        EXPECT_THROW(queue.Dequeue(), std::runtime_error);
        TThrowingQueueValue::ThrowOnMove = false;
        EXPECT_EQ(ChunkSize + 1, queue.Size());
        EXPECT_EQ(static_cast<int>(ChunkSize) + 1, TThrowingQueueValue::LiveCount);
        ASSERT_EQ(begin, queue.Begin());
    }
    for (int index = 0; index + 1 < static_cast<int>(ChunkSize); ++index) {
        EXPECT_EQ(index, queue.Dequeue().Value);
    }
    {
        auto begin = queue.Begin();
        TThrowingQueueValue::ThrowOnMove = true;
        EXPECT_THROW(queue.Dequeue(), std::runtime_error);
        TThrowingQueueValue::ThrowOnMove = false;
        EXPECT_EQ(2u, queue.Size());
        EXPECT_EQ(static_cast<int>(ChunkSize) + 1, TThrowingQueueValue::LiveCount);
        ASSERT_EQ(begin, queue.Begin());
    }
    EXPECT_EQ(42, queue.Dequeue().Value);
    EXPECT_EQ(43, queue.Dequeue().Value);
    queue.Clear();
    snapshot = {};
    EXPECT_EQ(0, TThrowingQueueValue::LiveCount);
}

TEST(TPersistentQueueTest, ConstructionFailureAndNonAssignableValues)
{
    TestThrowingQueueValue<3>();
    TestThrowingQueueValue<1>();
}

struct alignas(128) TAlignedQueueValue
{
    int Value;

    explicit TAlignedQueueValue(int value)
        : Value(value)
    { }
};

TEST(TPersistentQueueTest, OverAlignedValues)
{
    TPersistentQueue<TAlignedQueueValue, 2> queue;
    for (int index = 0; index < 5; ++index) {
        queue.Enqueue(TAlignedQueueValue(index));
    }
    int expected = 0;
    for (const auto& value : queue) {
        EXPECT_EQ(expected++, value.Value);
        EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(&value) % alignof(TAlignedQueueValue));
    }
    EXPECT_EQ(5, expected);
}

TEST(TPersistentQueueTest, IndexedQueueAfterDequeueAndClear)
{
    TIndexedPersistentQueue<std::string, 3> queue;
    for (int iteration = 0; iteration < 2; ++iteration) {
        for (int index = 0; index < 9; ++index) {
            queue.Enqueue(ToString(index));
        }
        EXPECT_EQ("0", queue.Dequeue());
        EXPECT_EQ("1", queue.Dequeue());
        auto snapshot = queue.MakeSnapshot();
        queue.Freeze();
        for (int index = 0; index < 7; ++index) {
            EXPECT_EQ(ToString(index + 2), queue[index]);
        }
        queue.Clear();
        int expected = 2;
        for (const auto& value : snapshot) {
            EXPECT_EQ(ToString(expected++), value);
        }
        EXPECT_EQ(9, expected);
    }
}

TEST(TPersistentQueueTest, SnapshotSerializationDoesNotDependOnChunkSize)
{
    for (int size : {0, 1, 3, 4, 9}) {
        TPersistentQueue<std::string, 3> queue;
        for (int index = 0; index < size; ++index) {
            queue.Enqueue(ToString(index));
        }
        auto snapshot = queue.MakeSnapshot();
        queue.Enqueue("later");
        queue.Clear();

        TStringStream actual;
        TStreamSaveContext saveContext(&actual);
        snapshot.Save(saveContext);
        saveContext.Finish();

        TStringStream expected;
        TStreamSaveContext expectedContext(&expected);
        TSizeSerializer::Save(expectedContext, size);
        for (int index = 0; index < size; ++index) {
            Save(expectedContext, ToString(index));
        }
        expectedContext.Finish();
        EXPECT_EQ(expected.Str(), actual.Str());

        TPersistentQueue<std::string, 2> loaded;
        TStreamLoadContext loadContext(&actual);
        loaded.Load(loadContext);
        EXPECT_EQ(size, static_cast<ssize_t>(loaded.Size()));
        for (int index = 0; index < size; ++index) {
            EXPECT_EQ(ToString(index), loaded.Dequeue());
        }
        EXPECT_TRUE(loaded.Empty());
    }
}

TEST(TPersistentQueueTest, SnapshotReadWhileAppendingAndClearing)
{
    TPersistentQueue<int, 64> queue;
    for (int index = 0; index < 32; ++index) {
        queue.Enqueue(index);
    }
    std::atomic<bool> started = false;
    std::atomic<bool> stopped = false;
    bool valid = true;
    std::thread reader([snapshot = queue.MakeSnapshot(), &started, &stopped, &valid] {
        started.store(true);
        do {
            int expected = 0;
            for (int value : snapshot) {
                valid &= value == expected++;
            }
            valid &= expected == 32;
        } while (!stopped.load());
    });
    while (!started.load()) {
        std::this_thread::yield();
    }
    for (int index = 32; index < 256; ++index) {
        queue.Enqueue(index);
    }
    queue.Clear();
    stopped.store(true);
    reader.join();
    EXPECT_TRUE(valid);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
