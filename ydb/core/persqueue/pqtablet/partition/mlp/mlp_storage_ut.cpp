#include "mlp_storage.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/join.h>
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
        Storage.SetKeepMessageOrder(true);
        Storage.SetMaxMessageProcessingCount(1);
        Storage.SetRetentionPeriod(TDuration::Seconds(10));
        Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
    }

    TIntrusivePtr<MockTimeProvider> TimeProvider;
    TStorage Storage;

    TInstant BaseWriteTimestamp;
    ui64 Offset = 0;

    NKikimrPQ::TMLPStorageSnapshot BeginSnapshot;
    NKikimrPQ::TMLPStorageSnapshot EndSnapshot;
    NKikimrPQ::TMLPStorageWAL WAL;

    void AddMessage(size_t count) {
        for (size_t i = 0; i < count; ++i) {
            Storage.AddMessage(Offset, true, Offset, BaseWriteTimestamp + TDuration::Seconds(Offset));
            ++Offset;
        }
    }

    void Begin() {
        BeginSnapshot = CreateSnapshot();
    }

    void End() {
        WAL = CreateWAL();
        EndSnapshot = CreateSnapshot();
    }

    void AssertLoad() {
        {
            TUtils utils;
            utils.LoadSnapshot(BeginSnapshot);
            utils.LoadWAL(WAL);

            utils.AssertEquals(*this);
        }
        {
            TUtils utils;
            utils.LoadSnapshot(EndSnapshot);

            utils.AssertEquals(*this);
        }
    } 

    NKikimrPQ::TMLPStorageSnapshot CreateSnapshot() {
        // Clear batch
        auto batch = Storage.GetBatch();
        Y_UNUSED(batch);

        NKikimrPQ::TMLPStorageSnapshot snapshot;
        Storage.SerializeTo(snapshot);
        Cerr << "CREATE" << Endl;
        Cerr << "> STORAGE DUMP: " << Storage.DebugString() << Endl;
        Cerr << "> SNAPSHOT: " << snapshot.ShortDebugString() << Endl;
        return snapshot;
    }

    NKikimrPQ::TMLPStorageWAL CreateWAL() {
        NKikimrPQ::TMLPStorageWAL wal;
        Storage.GetBatch().SerializeTo(wal);
        Cerr << "CREATE" << Endl;
        Cerr << "> STORAGE DUMP: " << Storage.DebugString() << Endl;
        Cerr << "> WAL: " << wal.ShortDebugString() << Endl;
        return wal;
    }

    void LoadSnapshot(const NKikimrPQ::TMLPStorageSnapshot& snapshot) {
        Cerr << "LOAD" << Endl;
        Cerr << "< SNAPSHOT: " << snapshot.ShortDebugString() << Endl;
        Storage.Initialize(snapshot);
        Cerr << "< STORAGE DUMP: " << Storage.DebugString() << Endl;
    }

    void LoadWAL(const NKikimrPQ::TMLPStorageWAL& wal) {
        Cerr << "LOAD" << Endl;
        Cerr << "< WAL: " << wal.ShortDebugString() << Endl;
        Storage.ApplyWAL(wal);
        Cerr << "< STORAGE DUMP: " << Storage.DebugString() << Endl;
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
        auto result = Storage.Next(TimeProvider->Now() + timeout, position);
        UNIT_ASSERT(result);
        return result.value();
    }

    bool Commit(ui64 offset) {
        return Storage.Commit(offset);
    }

    bool Unlock(ui64 offset) {
        return Storage.Unlock(offset);
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

        auto join = [](const auto& vs) {
            return JoinRange(",", vs.begin(), vs.end());
        };

        UNIT_ASSERT_VALUES_EQUAL(join(other.Storage.GetDLQMessages()), join(Storage.GetDLQMessages()));
        UNIT_ASSERT_VALUES_EQUAL(join(other.Storage.GetLockedMessageGroupsId()), join(Storage.GetLockedMessageGroupsId()));

        auto ometrics = other.Storage.GetMetrics();
        auto metrics = Storage.GetMetrics();

        UNIT_ASSERT_VALUES_EQUAL(ometrics.InflyMessageCount, metrics.InflyMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.UnprocessedMessageCount, metrics.UnprocessedMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.LockedMessageCount, metrics.LockedMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.LockedMessageGroupCount, metrics.LockedMessageGroupCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.CommittedMessageCount, metrics.CommittedMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.DeadlineExpiredMessageCount, metrics.DeadlineExpiredMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.DLQMessageCount, metrics.DLQMessageCount);
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

Y_UNIT_TEST(NextWithInfinityRetentionPeriod) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    TStorage storage(timeProvider);
    storage.SetRetentionPeriod(std::nullopt);

    storage.AddMessage(3, true, 5, timeProvider->Now());

    // skip message by retention
    TStorage::TPosition position;
    auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*result, 3);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
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
    TUtils utils;
    auto writeTimestamp = utils.BaseWriteTimestamp + TDuration::Seconds(7);

    utils.Begin();
    utils.Storage.AddMessage(3, true, 5, writeTimestamp);

    auto r = utils.Next();
    UNIT_ASSERT(r);
    utils.Storage.Unlock(3);

    utils.End();

    auto it = utils.Storage.begin();
    {
        UNIT_ASSERT(it != utils.Storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::DLQ);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    }
    ++it;
    UNIT_ASSERT(it == utils.Storage.end());

    const auto& dlq = utils.Storage.GetDLQMessages();
    UNIT_ASSERT_VALUES_EQUAL(dlq.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(dlq.front(), 3);

    utils.TimeProvider->Tick(TDuration::Seconds(5));

    utils.AssertLoad();
}

Y_UNIT_TEST(StorageSerialization_WAL_DeadLetterPolicy_Delete) {
    TUtils utils;
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
    auto writeTimestamp = utils.BaseWriteTimestamp + TDuration::Seconds(7);

    utils.Begin();
    utils.Storage.AddMessage(3, true, 5, writeTimestamp);

    auto r = utils.Next();
    UNIT_ASSERT(r);
    utils.Storage.Unlock(3);

    utils.End();

    auto it = utils.Storage.begin();
    {
        UNIT_ASSERT(it != utils.Storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    }
    ++it;
    UNIT_ASSERT(it == utils.Storage.end());

    const auto& dlq = utils.Storage.GetDLQMessages();
    UNIT_ASSERT_VALUES_EQUAL(dlq.size(), 0);

    utils.TimeProvider->Tick(TDuration::Seconds(5));

    utils.AssertLoad();
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

    timeProvider->Tick(TDuration::Seconds(12));

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
    storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);

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
        UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    }
    {
        auto [message, _] = storage.GetMessage(4);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
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

Y_UNIT_TEST(SlowZone_MoveUnprocessedToSlowZone) {
    TUtils utils;
    utils.AddMessage(6);
    utils.Begin();
    utils.AddMessage(1);
    utils.End();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveLockedToSlowZone) {
    TUtils utils;
    utils.AddMessage(6);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    utils.Begin();
    utils.AddMessage(1);
    utils.End();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, utils.TimeProvider->Now() + TDuration::Seconds(13));
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveCommittedToSlowZone) {
    TUtils utils;
    utils.AddMessage(6);
    UNIT_ASSERT(utils.Commit(0));
    utils.Begin();
    utils.AddMessage(1);
    utils.End();

    // Committed message isn't moved to SlowZone
    utils.AssertSlowZone({ });

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveDLQToSlowZone) {
    TUtils utils;
    utils.AddMessage(6);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    UNIT_ASSERT(utils.Unlock(0));
    utils.Begin();
    utils.AddMessage(1);
    utils.End();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::DLQ);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveToSlowZoneAndLock) {
    TUtils utils;
    utils.AddMessage(6);
    utils.Begin();
    utils.AddMessage(1);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    utils.End();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, utils.TimeProvider->Now() + TDuration::Seconds(13));
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveToSlowZoneAndCommit) {
    TUtils utils;
    utils.AddMessage(6);
    utils.Begin();
    utils.AddMessage(1);
    UNIT_ASSERT(utils.Commit(0));
    utils.End();

    utils.AssertSlowZone({ });

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveToSlowZoneAndDLQ) {
    TUtils utils;
    utils.AddMessage(6);
    utils.Begin();
    utils.AddMessage(1);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    UNIT_ASSERT(utils.Unlock(0));
    utils.End();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::DLQ);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Lock) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    utils.End();

    utils.AssertSlowZone({ 0, 1 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, utils.TimeProvider->Now() + TDuration::Seconds(13));
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Commit_First) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT(utils.Commit(0));
    utils.End();

    utils.AssertSlowZone({1});

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Commit) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT(utils.Commit(1));
    utils.End();

    utils.AssertSlowZone({0});

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_DLQ) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    UNIT_ASSERT(utils.Unlock(0));
    utils.End();

    utils.AssertSlowZone({ 0, 1 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::DLQ);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_CommitToFast) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT(utils.Commit(2));
    utils.Storage.Compact();
    utils.End();

    utils.AssertSlowZone({0, 1});
    // Compaction removed the message with offset 2
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflyMessageCount, 7);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_CommitAndAdd) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT(utils.Commit(1));
    utils.AddMessage(1);
    utils.End();

    utils.AssertSlowZone({0, 2});

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Retention_1message) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();

    utils.TimeProvider->Tick(TDuration::Seconds(2));
    utils.Storage.Compact();

    utils.End();

    utils.AssertSlowZone({1});
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 2);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflyMessageCount, 7);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Retention_2message) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();

    utils.TimeProvider->Tick(TDuration::Seconds(3));
    utils.Storage.Compact();

    utils.End();

    utils.AssertSlowZone({});
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 2);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflyMessageCount, 6);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Retention_3message) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();

    utils.TimeProvider->Tick(TDuration::Seconds(4));
    utils.Storage.Compact();

    utils.End();

    utils.AssertSlowZone({});
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflyMessageCount, 5);

    utils.AssertLoad();
}

Y_UNIT_TEST(ChangeDeadLettePolicy_Delete) {
    TUtils utils;
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
    utils.Storage.SetMaxMessageProcessingCount(1);

    utils.AddMessage(1);
    utils.Next();
    utils.Storage.Unlock(0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 1);

    utils.Begin();
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().CommittedMessageCount, 1);
    utils.End();

    utils.AssertLoad();
}

Y_UNIT_TEST(ChangeDeadLettePolicy_Unspecified) {
    TUtils utils;
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
    utils.Storage.SetMaxMessageProcessingCount(1);

    utils.AddMessage(1);
    utils.Next();
    utils.Storage.Unlock(0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 1);

    utils.Begin();
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().UnprocessedMessageCount, 1);
    utils.End();

    utils.AssertLoad();
}

}

} // namespace NKikimr::NPQ::NMLP
