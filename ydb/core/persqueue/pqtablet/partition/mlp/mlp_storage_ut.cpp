#include "mlp_storage.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPStorageTests) {

struct MockTimeProvider : public ITimeProvider {

    MockTimeProvider() {
        Value = TInstant::Seconds(1761034384);
    }

    TInstant Now() override {
        return Value;
    }

    void Tick(TDuration duration) {
        Value += duration;
    }

    TInstant Value;
};

struct TUtils {
    TUtils()
        : TimeProvider(TIntrusivePtr<MockTimeProvider>(new MockTimeProvider()))
        , Storage(TimeProvider, 1, 8)
        , BaseWriteTimestamp(TimeProvider->Now() - TDuration::Seconds(8))
    {
    }

    TIntrusivePtr<MockTimeProvider> TimeProvider;
    TStorage Storage;

    TInstant BaseWriteTimestamp;
    ui64 Offset = 0;

    void AddMessage(size_t count) {
        for (size_t i = 0; i < count; ++i) {
            Storage.AddMessage(Offset, true, Offset, BaseWriteTimestamp + TDuration::Seconds(Offset));
            ++Offset;
        }
    }

    NKikimrPQ::TMLPStorageSnapshot CreateSnapshot() {
        NKikimrPQ::TMLPStorageSnapshot snapshot;
        Storage.SerializeTo(snapshot);
        return snapshot;
    }

    NKikimrPQ::TMLPStorageWAL CreateWAL() {
        NKikimrPQ::TMLPStorageWAL wal;
        Storage.GetBatch().SerializeTo(wal);
        Cerr << "STORAGE DUMP: " << Storage.DebugString() << Endl;
        Cerr << "WAL: " << wal.ShortDebugString() << Endl;
        return wal;
    }

    void LoadSnapshot(const NKikimrPQ::TMLPStorageSnapshot& snapshot) {
        Storage.Initialize(snapshot);
        Cerr << "SNAPSHOT: " << snapshot.ShortDebugString() << Endl;
        Cerr << "STORAGE DUMP: " << Storage.DebugString() << Endl;
    }

    void LoadWAL(const NKikimrPQ::TMLPStorageWAL& wal) {
        Storage.ApplyWAL(wal);
        Cerr << "WAL: " << wal.ShortDebugString() << Endl;
        Cerr << "STORAGE DUMP: " << Storage.DebugString() << Endl;
    }

    void AssertSlowZone(std::vector<ui64> expectedOffsets) {
        auto i = expectedOffsets.begin();
        auto m = Storage.begin();

        while (i != expectedOffsets.end() && m != Storage.end()) {
            UNIT_ASSERT_VALUES_EQUAL(*i, (*m).Offset);
            UNIT_ASSERT((*m).SlowZone);
            ++i;
            ++m;
        }

        UNIT_ASSERT(i == expectedOffsets.end());
        if (m != Storage.end()) {
            UNIT_ASSERT(!(*m).SlowZone);
        }
    }

    std::optional<TStorage::TMessageWrapper> GetMessage(ui64 offset) {
        for (auto it = Storage.begin(); it != Storage.end(); ++it) {
            if ((*it).Offset == offset) {
                return *it;
            }
        }

        return std::nullopt;
    }

    ui64 Next(TDuration timeout = TDuration::Seconds(8)) {
        TStorage::TPosition position;
        return Storage.Next(TimeProvider->Now() + timeout, position).value();
    }

    void AssertEquals(TUtils& other) {
        auto i = other.Storage.begin();
        auto m = Storage.begin();

        while (i != other.Storage.end() && m != Storage.end()) {
            UNIT_ASSERT_VALUES_EQUAL((*i).Offset, (*m).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).SlowZone, (*m).SlowZone, (*i).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).Status, (*m).Status, (*i).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).ProcessingCount, (*m).ProcessingCount, (*i).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).ProcessingDeadline, (*m).ProcessingDeadline, (*i).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).WriteTimestamp, (*m).WriteTimestamp, (*i).Offset);

            ++i;
            ++m;
        }

        UNIT_ASSERT(i == other.Storage.end());
        UNIT_ASSERT(m == Storage.end());
    }
};

Y_UNIT_TEST(NextFromEmptyStorage) {
    TStorage storage(CreateDefaultTimeProvider());

    TStorage::TPosition position;
    auto result = storage.Next(TInstant::Now(), position);
    UNIT_ASSERT(!result.has_value());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(CommitToEmptyStorage) {
    TStorage storage(CreateDefaultTimeProvider());

    auto result = storage.Commit(123);
    UNIT_ASSERT(!result);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(UnlockToEmptyStorage) {
    TStorage storage(CreateDefaultTimeProvider());

    auto result = storage.Unlock(123);
    UNIT_ASSERT(!result);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(ChangeDeadlineEmptyStorage) {
    TStorage storage(CreateDefaultTimeProvider());

    auto result = storage.ChangeMessageDeadline(123, TInstant::Now());
    UNIT_ASSERT(!result);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(AddMessageToEmptyStorage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider);

    storage.AddMessage(0, true, 5, writeTimestamp);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 0);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 1);

    timeProvider->Tick(TDuration::Seconds(7));

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(AddNotFirstMessageToEmptyStorage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider);

    storage.AddMessage(3, true, 5, writeTimestamp);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 4);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(AddMessageWithSkippedMessage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider);

    storage.AddMessage(3, true, 5, timeProvider->Now() - TDuration::Seconds(137));
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 4);

    Cerr << "DUMP 1: " << storage.DebugString() << Endl;

    storage.AddMessage(7, true, 5, writeTimestamp);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 7);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 8);

    Cerr << "DUMP 2: " << storage.DebugString() << Endl;

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 7);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(NextWithoutKeepMessageOrderStorage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);
    auto processingDeadline = timeProvider->Now() + TDuration::Seconds(13);

    TStorage storage(timeProvider);

    storage.AddMessage(3, true, 5, writeTimestamp);

    TStorage::TPosition position;
    auto result = storage.Next(processingDeadline + TDuration::MilliSeconds(31), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*result, 3);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, processingDeadline + TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(NextWithKeepMessageOrderStorage) {
    TStorage storage(CreateDefaultTimeProvider());
    storage.SetKeepMessageOrder(true);
    storage.AddMessage(3, true, 5, TInstant::Now());

    TStorage::TPosition position;
    auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*result, 3);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(NextWithWriteRetentionPeriod) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    TStorage storage(timeProvider);
    storage.SetRetentionPeriod(TDuration::Seconds(5));

    storage.AddMessage(3, true, 5, timeProvider->Now());
    storage.AddMessage(4, true, 5, timeProvider->Now() + TDuration::Seconds(7));

    timeProvider->Tick(TDuration::Seconds(6));

    // skip message by retention
    TStorage::TPosition position;
    auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*result, 4);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(SkipLockedMessage) {
    TStorage storage(CreateDefaultTimeProvider());
    {
        TStorage::TPosition position;
        storage.AddMessage(3, true, 5, TInstant::Now());
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
    }

    TStorage::TPosition position;
    auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT_C(!result.has_value(), "The message already locked");

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(SkipLockedMessageGroups) {
    TStorage storage(CreateDefaultTimeProvider());
    storage.SetKeepMessageOrder(true);
    storage.AddMessage(3, true, 5, TInstant::Now());
    storage.AddMessage(4, true, 5, TInstant::Now());
    storage.AddMessage(5, true, 7, TInstant::Now());

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*result, 3);
    }

    TStorage::TPosition position;
    auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*result, 5);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 3);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(CommitLockedMessage_WithoutKeepMessageOrder) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider);
    storage.AddMessage(3, true, 5, writeTimestamp);

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*result, 3);
    }
    {
        auto result = storage.Commit(3);
        UNIT_ASSERT(result);
    }

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(CommitLockedMessage_WithKeepMessageOrder) {
    TStorage storage(CreateDefaultTimeProvider());
    storage.SetKeepMessageOrder(true);
    storage.AddMessage(3, true, 5, TInstant::Now());

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
    }

    auto result = storage.Commit(3);
    UNIT_ASSERT(result);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(CommitUnlockedMessage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider);
    storage.AddMessage(3, true, 5, writeTimestamp);

    auto result = storage.Commit(3);
    UNIT_ASSERT(result);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(CommitCommittedMessage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider);
    storage.AddMessage(3, true, 5, writeTimestamp);

    {
        auto result = storage.Commit(3);
        UNIT_ASSERT(result);
    }
    {
        auto result = storage.Commit(3);
        UNIT_ASSERT(!result);
    }

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(UnlockLockedMessage_WithoutKeepMessageOrder) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider);
    storage.AddMessage(3, true, 5, writeTimestamp);

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
    }

    auto result = storage.Unlock(3);
    UNIT_ASSERT(result);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(UnlockLockedMessage_WithKeepMessageOrder) {
    TStorage storage(CreateDefaultTimeProvider());
    {
        storage.SetKeepMessageOrder(true);
        storage.AddMessage(3, true, 5, TInstant::Now());
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
    }

    auto result = storage.Unlock(3);
    UNIT_ASSERT(result);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(UnlockUnlockedMessage) {
    TStorage storage(CreateDefaultTimeProvider());
    storage.AddMessage(3, true, 5, TInstant::Now());

    auto result = storage.Unlock(3);
    UNIT_ASSERT(!result);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(UnlockCommittedMessage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider);
    storage.AddMessage(3, true, 5, writeTimestamp);

    {
        auto result = storage.Commit(3);
        UNIT_ASSERT(result);
    }

    auto result = storage.Unlock(3);
    UNIT_ASSERT(!result);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(ChangeDeadlineLockedMessage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider);
    storage.AddMessage(3, true, 5, writeTimestamp);

    {
        TStorage::TPosition position;
        auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(5), position);
        UNIT_ASSERT(result.has_value());
    }

    timeProvider->Tick(TDuration::Seconds(1));

    auto result = storage.ChangeMessageDeadline(3, timeProvider->Now() + TDuration::Seconds(7));
    UNIT_ASSERT(result);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, timeProvider->Now() + TDuration::Seconds(7));
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());
}

Y_UNIT_TEST(ChangeDeadlineUnlockedMessage) {
    auto now = TInstant::Now();

    TStorage storage(CreateDefaultTimeProvider());
    storage.AddMessage(3, true, 5, TInstant::Now());

    auto result = storage.ChangeMessageDeadline(3, now + TDuration::Seconds(5));
    UNIT_ASSERT(!result);

    auto deadline = storage.GetMessageDeadline(3);
    UNIT_ASSERT_VALUES_EQUAL(deadline, TInstant::Zero());
}

Y_UNIT_TEST(EmptyStorageSerialization) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    NKikimrPQ::TMLPStorageSnapshot snapshot;

    {
        TStorage storage(timeProvider);

        storage.SerializeTo(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetFormatVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstUncommittedOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseDeadlineSeconds(), storage.GetBaseDeadline().Seconds());
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseWriteTimestampSeconds(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMessages().size(), 0);
    }
    {
        TStorage storage(timeProvider);

        storage.Initialize(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseDeadline().Seconds(), snapshot.GetMeta().GetBaseDeadlineSeconds());
        UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseWriteTimestamp().Seconds(), 0);

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(StorageSerialization) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    NKikimrPQ::TMLPStorageSnapshot snapshot;

    auto writeTimestamp3 = timeProvider->Now() - TDuration::Seconds(10);
    auto writeTimestamp4 = timeProvider->Now() - TDuration::Seconds(9);
    auto writeTimestamp5 = timeProvider->Now() - TDuration::Seconds(9);
    auto writeTimestamp6 = timeProvider->Now() - TDuration::Seconds(8);

    auto deadline4 = timeProvider->Now() + TDuration::Seconds(113);

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);

        storage.AddMessage(3, true, 5, writeTimestamp3);
        storage.AddMessage(4, true, 7, writeTimestamp4);
        storage.AddMessage(5, true, 11, writeTimestamp5);
        storage.AddMessage(6, true, 13, writeTimestamp6);

        storage.Commit(3);
        TStorage::TPosition position;
        storage.Next(deadline4, position);
        storage.Commit(5);

        storage.SerializeTo(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetFormatVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstOffset(), 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstUncommittedOffset(), 4);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseDeadlineSeconds(), storage.GetBaseDeadline().Seconds());
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseWriteTimestampSeconds(), timeProvider->Now().Seconds() - 10);
        UNIT_ASSERT(snapshot.GetMessages().size() > 0);

        Cerr << "DUMP 1: " << storage.DebugString() << Endl;
    }

    timeProvider->Tick(TDuration::Seconds(13));

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        Cerr << "DUMP 2: " << storage.DebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 7);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 6);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 4);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseDeadline().Seconds(), snapshot.GetMeta().GetBaseDeadlineSeconds());

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp3);
        }
        ++it;
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 4);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, deadline4);
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp4);
        }
        ++it;
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 5);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp5);
        }
        ++it;
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 6);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp6);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 4);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(StorageSerialization_WAL_Unlocked) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(13);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, writeTimestamp);

        auto batch = storage.GetBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AddedMessageCount(), 1); // new message
        batch.SerializeTo(wal);

        Cerr << "DUMP 1: " << storage.DebugString() << Endl;
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);

        Cerr << "DUMP 2: " << storage.DebugString() << Endl;

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());


        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(StorageSerialization_WAL_Locked) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(13);
    auto deadline = timeProvider->Now() + TDuration::Seconds(4);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, writeTimestamp);

        TStorage::TPosition position;
        auto r = storage.Next(deadline, position);
        UNIT_ASSERT(r);
        UNIT_ASSERT_VALUES_EQUAL(r.value(), 3);

        auto batch = storage.GetBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AddedMessageCount(), 1); // new message and changed message
        UNIT_ASSERT_VALUES_EQUAL(batch.ChangedMessageCount(), 1); // new message and changed message
        batch.SerializeTo(wal);

        Cerr << "DUMP 1: " << storage.DebugString() << Endl;
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        Cerr << "DUMP: " << storage.DebugString() << Endl;
        storage.ApplyWAL(wal);
        Cerr << "DUMP AFTER WAL: " << storage.DebugString() << Endl;

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, deadline);
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(StorageSerialization_WAL_Committed) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(13);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, writeTimestamp);

        auto r = storage.Commit(3);
        UNIT_ASSERT(r);

        auto batch = storage.GetBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AddedMessageCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(batch.ChangedMessageCount(), 1);
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(StorageSerialization_WAL_DLQ) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(13);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);
        storage.SetMaxMessageProcessingCount(1);
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, writeTimestamp);

        TStorage::TPosition position;
        auto r = storage.Next(timeProvider->Now() + TDuration::Seconds(7), position);
        UNIT_ASSERT(r);

        storage.Unlock(3);

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::DLQ);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        const auto& dlq = storage.GetDLQMessages();
        UNIT_ASSERT_VALUES_EQUAL(dlq.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(dlq.front(), 3);

        auto batch = storage.GetBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AddedMessageCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(batch.ChangedMessageCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(batch.DLQMessageCount(), 1);
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::DLQ);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 1);

        const auto& dlq = storage.GetDLQMessages();
        UNIT_ASSERT_VALUES_EQUAL(dlq.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(dlq.front(), 3);
    }
}

Y_UNIT_TEST(StorageSerialization_WAL_WithHole) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    auto writeTimestamp3 = timeProvider->Now() - TDuration::Seconds(17);
    auto writeTimestamp7 = timeProvider->Now() - TDuration::Seconds(13);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, writeTimestamp3);
        storage.AddMessage(7, true, 5, writeTimestamp7);

        auto batch = storage.GetBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AddedMessageCount(), 2);
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);

        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 7);

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 7);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp7);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(StorageSerialization_WAL_WithMoveBaseTime_Deadline) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    auto writeTimestamp3 = timeProvider->Now() - TDuration::Seconds(17);
    auto writeTimestamp4 = timeProvider->Now() - TDuration::Seconds(13);

    auto deadline3 = timeProvider->Now() + TDuration::Seconds(5);
    auto deadline4 = timeProvider->Now() + TDuration::Seconds(13);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, writeTimestamp3);
        storage.AddMessage(4, true, 6, writeTimestamp4);
        {
            TStorage::TPosition position;
            auto r = storage.Next(deadline3, position);
            UNIT_ASSERT(r);
            UNIT_ASSERT_VALUES_EQUAL(*r, 3);
        }

        {
            auto [message, _] = storage.GetMessage(3);
            UNIT_ASSERT(message);
            UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 5);
        }
        
        timeProvider->Tick(TDuration::Seconds(3));
        storage.MoveBaseDeadline();
        {
            TStorage::TPosition position;
            auto r = storage.Next(deadline4, position);
            UNIT_ASSERT(r);
            UNIT_ASSERT_VALUES_EQUAL(*r, 4);
        }

        {
            auto [message, _] = storage.GetMessage(3);
            UNIT_ASSERT(message);
            UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 2); // 5 - 3
        }
        {
            auto [message, _] = storage.GetMessage(4);
            UNIT_ASSERT(message);
            UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 10);
        }

        auto batch = storage.GetBatch();
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(7));

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);

        UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseDeadline(), timeProvider->Now() - TDuration::Seconds(7));

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, deadline3);
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp3);
        }
        ++it;
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 4);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, deadline4);
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp4);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(CompactStorage_ByCommittedOffset) {
    TStorage storage(CreateDefaultTimeProvider());
    storage.AddMessage(3, true, 5, TInstant::Now());
    storage.AddMessage(4, true, 7, TInstant::Now());
    storage.AddMessage(5, true, 11, TInstant::Now());
    storage.AddMessage(6, true, 13, TInstant::Now());

    storage.Commit(3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 4);
    storage.Commit(5);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 4);

    auto result = storage.Compact();
    UNIT_ASSERT_VALUES_EQUAL_C(result, 1, "must remove only message with offset 3 because it is committed");

    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 4);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 7);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 4);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 4);

    auto it = storage.begin();
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 4);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    }
    ++it;
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 5);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    }
    ++it;
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset,6);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    }
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 3);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 2); // offsets 4 and 6
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1); // offset 5
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(CompactStorage_ByRetention) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() + TDuration::Seconds(12);

    TStorage storage(timeProvider);
    storage.SetRetentionPeriod(TDuration::Seconds(1));

    storage.AddMessage(3, true, 5, timeProvider->Now());
    storage.AddMessage(4, true, 7, timeProvider->Now() + TDuration::Seconds(11));
    storage.AddMessage(5, true, 11, writeTimestamp);

    timeProvider->Tick(TDuration::Seconds(13));

    auto result = storage.Compact();
    Cerr << storage.DebugString() << Endl;
    UNIT_ASSERT_VALUES_EQUAL_C(result, 2, "must remove message with offset 3 and 4");

    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 5);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 6);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 5);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 5);

    auto it = storage.begin();
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 5);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    }
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1); // offset 5
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(CompactStorage_WithDLQ) {
    TStorage storage(CreateDefaultTimeProvider());
    storage.SetMaxMessageProcessingCount(1);
    storage.AddMessage(3, true, 5, TInstant::Now());
    storage.AddMessage(4, true, 7, TInstant::Now());

    TStorage::TPosition position;
    storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    storage.Unlock(3);
    storage.Commit(4);

    {
        auto [message, _] = storage.GetMessage(3);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::DLQ);
    }
    {
        auto [message, _] = storage.GetMessage(4);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Committed);
    }

    auto result = storage.Compact();
    UNIT_ASSERT_VALUES_EQUAL_C(result, 0, "Keep DLQ messages");

    auto it = storage.begin();
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::DLQ);
    }
    ++it;
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 4);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    }
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1); // offset 4
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 1); // offset 3
}

Y_UNIT_TEST(ProccessDeadlines) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    TStorage storage(timeProvider);
    storage.SetKeepMessageOrder(true);
    storage.AddMessage(3, true, 5, TInstant::Now());
    storage.AddMessage(4, true, 7, TInstant::Now());
    storage.AddMessage(5, true, 11, TInstant::Now());
    storage.AddMessage(6, true, 13, TInstant::Now());

    TStorage::TPosition position;
    storage.Next(timeProvider->Now() + TDuration::Seconds(10), position);
    timeProvider->Tick(TDuration::Seconds(5));
    storage.Next(timeProvider->Now() + TDuration::Seconds(10), position);
    timeProvider->Tick(TDuration::Seconds(7));

    auto result = storage.ProccessDeadlines();
    UNIT_ASSERT_VALUES_EQUAL(result, 1);

    {
        auto [message, _] = storage.GetMessage(3);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Unprocessed);
        UNIT_ASSERT_VALUES_EQUAL(message->ReceiveCount, 1);
    }
    {
        auto [message, _] = storage.GetMessage(4);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->ReceiveCount, 1);
    }

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 4);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 3); // offsets 4 and 5 and 6
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1); // offset 3
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(MoveBaseDeadline) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    TStorage storage(timeProvider);
    storage.SetKeepMessageOrder(true);
    storage.AddMessage(3, true, 5, TInstant::Now());
    storage.AddMessage(4, true, 7, TInstant::Now());
    storage.AddMessage(5, true, 11, TInstant::Now());

    TStorage::TPosition position;
    storage.Next(timeProvider->Now() + TDuration::Seconds(3), position);
    storage.Next(timeProvider->Now() + TDuration::Seconds(5), position);
    storage.Next(timeProvider->Now() + TDuration::Seconds(7), position);

    timeProvider->Tick(TDuration::Seconds(5));

    storage.MoveBaseDeadline();

    {
        auto [message, _] = storage.GetMessage(3);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 0);
    }
    {
        auto [message, _] = storage.GetMessage(4);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 0);
    }
    {
        auto [message, _] = storage.GetMessage(5);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 2);
    }
}

auto assertMetrics = [](auto restoredMetrics, auto metrics) {
    UNIT_ASSERT_VALUES_EQUAL(restoredMetrics.InflyMessageCount, metrics.InflyMessageCount);
    UNIT_ASSERT_VALUES_EQUAL(restoredMetrics.UnprocessedMessageCount, metrics.UnprocessedMessageCount);
    UNIT_ASSERT_VALUES_EQUAL(restoredMetrics.LockedMessageCount, metrics.LockedMessageCount);
    UNIT_ASSERT_VALUES_EQUAL(restoredMetrics.LockedMessageGroupCount, metrics.LockedMessageGroupCount);
    UNIT_ASSERT_VALUES_EQUAL(restoredMetrics.CommittedMessageCount, metrics.CommittedMessageCount);
    UNIT_ASSERT_VALUES_EQUAL(restoredMetrics.DeadlineExpiredMessageCount, metrics.DeadlineExpiredMessageCount);
    UNIT_ASSERT_VALUES_EQUAL(restoredMetrics.DLQMessageCount, metrics.DLQMessageCount);
};

Y_UNIT_TEST(SlowZone_MoveUnprocessedToSlowZone) {
    TUtils utils;
    utils.AddMessage(6);
    auto snapshot = utils.CreateSnapshot();
    utils.AddMessage(1);
    auto wal = utils.CreateWAL();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    TUtils utilsD;
    utilsD.LoadSnapshot(snapshot);
    utilsD.LoadWAL(wal);

    assertMetrics(utilsD.Storage.GetMetrics(), utils.Storage.GetMetrics());
    utilsD.AssertEquals(utils);
}

Y_UNIT_TEST(SlowZone_MoveLockedToSlowZone) {
    TUtils utils;
    utils.AddMessage(6);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    auto snapshot = utils.CreateSnapshot();
    utils.AddMessage(1);
    auto wal = utils.CreateWAL();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, utils.TimeProvider->Now() + TDuration::Seconds(13));
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    TUtils utilsD;
    utilsD.LoadSnapshot(snapshot);
    utilsD.LoadWAL(wal);

    assertMetrics(utilsD.Storage.GetMetrics(), utils.Storage.GetMetrics());
    utilsD.AssertEquals(utils);
}

Y_UNIT_TEST(SlowZone_LongScenario) {
    const size_t maxMessages = 8;

    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto now = timeProvider->Now();

    TStorage storage(timeProvider, 1, maxMessages); // fast zone = 6, slow zone = 2
    storage.SetKeepMessageOrder(true);
    storage.SetMaxMessageProcessingCount(1);
    storage.SetRetentionPeriod(TDuration::Seconds(7 * 13));

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    storage.SerializeTo(snapshot);

    {
        UNIT_ASSERT(storage.AddMessage(0, true, 100, now - TDuration::Seconds(7 * 12)));
        TStorage::TPosition position;
        auto r = storage.Next(now + TDuration::Seconds(50), position);
        UNIT_ASSERT_VALUES_EQUAL(r.value(), 0);
    }
    {
        UNIT_ASSERT(storage.AddMessage(1, true, 101, now - TDuration::Seconds(7 * 11)));
        storage.Commit(1);
    }
    {
        UNIT_ASSERT(storage.AddMessage(2, true, 102, now - TDuration::Seconds(7 * 10)));
    }
    {
        UNIT_ASSERT(storage.AddMessage(3, true, 103, now - TDuration::Seconds(7 * 9)));
    }
    {
        UNIT_ASSERT(storage.AddMessage(4, true, 104, now - TDuration::Seconds(7 * 8)));
    }

    Cerr << "DUMP 1: " << storage.DebugString() << Endl;

    NKikimrPQ::TMLPStorageWAL wal1;
    auto batch1 = storage.GetBatch();
    batch1.SerializeTo(wal1);
    auto metrics1 = storage.GetMetrics();

    {
        UNIT_ASSERT(storage.AddMessage(5, true, 105, now - TDuration::Seconds(7 * 7)));
    }
    {
        // Fast zone is end Move message with offset 0 to the slow zone
        UNIT_ASSERT(storage.AddMessage(6, true, 106, now - TDuration::Seconds(7 * 6)));
    }

    Cerr << "DUMP 2: " << storage.DebugString() << Endl;

    NKikimrPQ::TMLPStorageWAL wal2;
    auto batch2 = storage.GetBatch();
    batch2.SerializeTo(wal2);
    auto metrics2 = storage.GetMetrics();

    {
        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, true);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
        }
        {
            UNIT_ASSERT(++it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, false);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
        }
    }

    {
        // Fast zone is end Move message with 1 offset to the slow zone but it is committed. Skip it.
        UNIT_ASSERT(storage.AddMessage(7, true, 107, now - TDuration::Seconds(7 * 6)));
    }

    {
        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, true);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
        }
        {
            // offset 1 is commited and didn`t moved to slow zone
            UNIT_ASSERT(++it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, false);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
        }
    }

    {
        // Fast zone is end Move message with offset 2 to the slow zone
        UNIT_ASSERT(storage.AddMessage(8, true, 108, now - TDuration::Seconds(7 * 5)));
    }

    {
        // Now 2 messages in the slow zone
        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, true);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
        }
        {
            UNIT_ASSERT(++it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, true);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
        }
        {
            UNIT_ASSERT(++it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, false);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        }
    }

    {
        UNIT_ASSERT(storage.Commit(2));
    }

    {
        // Committed message removed from the slow zone
        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, true);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
        }
        {
            UNIT_ASSERT(++it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, false);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        }
    }

    Cerr << "DUMP 3: " << storage.DebugString() << Endl;
    NKikimrPQ::TMLPStorageWAL wal3;
    auto batch3 = storage.GetBatch();
    batch3.SerializeTo(wal3);
    auto metrics3 = storage.GetMetrics();

    {
        UNIT_ASSERT(storage.Commit(0));
    }

    {
        // Committed message removed from the slow zone
        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, false);
        }
    }

    Cerr << "DUMP 4: " << storage.DebugString() << Endl;
    NKikimrPQ::TMLPStorageWAL wal4;
    auto batch4 = storage.GetBatch();
    batch4.SerializeTo(wal4);
    auto metrics4 = storage.GetMetrics();

    {
        // Fast zone is end Move message with offset 3 to the slow zone
        UNIT_ASSERT(storage.AddMessage(9, true, 108, now - TDuration::Seconds(7 * 4)));
        // Fast zone is end Move message with offset 4 to the slow zone
        UNIT_ASSERT(storage.AddMessage(10, true, 108, now - TDuration::Seconds(7 * 3)));
    }
    {
        TStorage::TPosition position;
        auto r = storage.Next(now + TDuration::Seconds(50), position);
        UNIT_ASSERT_VALUES_EQUAL(r.value(), 3);
    }
    {
        TStorage::TPosition position;
        auto r = storage.Next(now + TDuration::Seconds(50), position);
        UNIT_ASSERT_VALUES_EQUAL(r.value(), 4);
    }

    Cerr << "DUMP 5: " << storage.DebugString() << Endl;
    NKikimrPQ::TMLPStorageWAL wal5;
    auto batch5 = storage.GetBatch();
    batch5.SerializeTo(wal5);
    auto metrics5 = storage.GetMetrics();

    NKikimrPQ::TMLPStorageSnapshot snapshot5;
    storage.SerializeTo(snapshot5);
    



    TStorage restoredStorage(timeProvider, 1, maxMessages); // fast zone = 6, slow zone = 2
    restoredStorage.SetKeepMessageOrder(true);
    restoredStorage.SetMaxMessageProcessingCount(1);
    restoredStorage.SetRetentionPeriod(TDuration::Seconds(7 * 13));

    Cerr << "SNAPSHOT: " << snapshot.ShortDebugString() << Endl;
    restoredStorage.Initialize(snapshot);
    Cerr << "RESTORED SNAPSHOT: " << restoredStorage.DebugString() << Endl;

    Cerr << "WAL 1: " << wal1.ShortDebugString() << Endl;
    restoredStorage.ApplyWAL(wal1);
    Cerr << "RESTORED WAL 1: " << restoredStorage.DebugString() << Endl;
    assertMetrics(metrics1, restoredStorage.GetMetrics());

    {
        // Committed message removed from the slow zone
        auto it = restoredStorage.begin();
        {
            UNIT_ASSERT(it != restoredStorage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, false);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 0);
        }
    }

    Cerr << "WAL 2: " << wal2.ShortDebugString() << Endl;
    restoredStorage.ApplyWAL(wal2);
    Cerr << "RESTORED WAL 2: " << restoredStorage.DebugString() << Endl;
    assertMetrics(metrics2, restoredStorage.GetMetrics());

    {
        // Committed message removed from the slow zone
        auto it = restoredStorage.begin();
        {
            UNIT_ASSERT(it != restoredStorage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, true);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 0);
        }
        {
            UNIT_ASSERT(++it != restoredStorage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, false);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
        }
    }

    Cerr << "WAL 3: " << wal3.ShortDebugString() << Endl;
    restoredStorage.ApplyWAL(wal3);
    Cerr << "RESTORED WAL 3: " << restoredStorage.DebugString() << Endl;
    assertMetrics(metrics3, restoredStorage.GetMetrics());

    {
        // Committed message removed from the slow zone
        auto it = restoredStorage.begin();
        {
            UNIT_ASSERT(it != restoredStorage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, true);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
        }
        {
            UNIT_ASSERT(++it != restoredStorage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, false);
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        }
    }

    Cerr << "WAL 4: " << wal4.ShortDebugString() << Endl;
    restoredStorage.ApplyWAL(wal4);
    Cerr << "RESTORED WAL 4: " << restoredStorage.DebugString() << Endl;
    assertMetrics(metrics4, restoredStorage.GetMetrics());

    {
        // Committed message removed from the slow zone
        auto it = restoredStorage.begin();
        {
            UNIT_ASSERT(it != restoredStorage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, false);
        }
    }

    Cerr << "WAL 5: " << wal5.ShortDebugString() << Endl;
    restoredStorage.ApplyWAL(wal5);
    Cerr << "RESTORED WAL 5: " << restoredStorage.DebugString() << Endl;
    assertMetrics(metrics5, restoredStorage.GetMetrics());

    {
        // Committed message removed from the slow zone
        auto it = restoredStorage.begin();
        {
            UNIT_ASSERT(it != restoredStorage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, true);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
        }
        {
            UNIT_ASSERT(++it != restoredStorage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 4);
            UNIT_ASSERT_VALUES_EQUAL(message.SlowZone, true);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
        }
    }

    TStorage restoredStorage5(timeProvider, 1, maxMessages); // fast zone = 6, slow zone = 2
    restoredStorage5.SetKeepMessageOrder(true);
    restoredStorage5.SetMaxMessageProcessingCount(1);
    restoredStorage5.SetRetentionPeriod(TDuration::Seconds(7 * 13));

    Cerr << "RESTORED SNAPSHOT 5: " << snapshot5.ShortDebugString() << Endl;

    restoredStorage5.Initialize(snapshot5);
    assertMetrics(metrics5, restoredStorage5.GetMetrics());
}

}

} // namespace NKikimr::NPQ::NMLP
