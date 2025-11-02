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

Y_UNIT_TEST(NextFromEmptyStorage) {
    TStorage storage(CreateDefaultTimeProvider());

    auto result = storage.Next(TInstant::Now());
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
    TStorage storage(CreateDefaultTimeProvider());

    storage.AddMessage(0, true, 5, TInstant::Now());
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 0);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 1);

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
    TStorage storage(CreateDefaultTimeProvider());

    storage.AddMessage(3, true, 5, TInstant::Now());
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 4);

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
    TStorage storage(CreateDefaultTimeProvider());

    storage.AddMessage(3, true, 5, TInstant::Now());
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 4);

    storage.AddMessage(7, true, 5, TInstant::Now());
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 7);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 8);

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
    TStorage storage(CreateDefaultTimeProvider());
    storage.AddMessage(3, true, 5, TInstant::Now());

    auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1));
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Message, 3);
    UNIT_ASSERT_VALUES_EQUAL(result->FromOffset, 4);

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

    auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1));
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Message, 3);
    UNIT_ASSERT_VALUES_EQUAL(result->FromOffset, 4);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(NextWithWriteReteintion) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    TStorage storage(timeProvider);
    storage.SetReteintion(TDuration::Seconds(5));

    storage.AddMessage(3, true, 5, timeProvider->Now());
    storage.AddMessage(4, true, 5, timeProvider->Now() + TDuration::Seconds(7));

    timeProvider->Tick(TDuration::Seconds(6));

    // skip message by reteintion
    auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(1));
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Message, 4);
    UNIT_ASSERT_VALUES_EQUAL(result->FromOffset, 5);

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
        storage.AddMessage(3, true, 5, TInstant::Now());
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1));
        UNIT_ASSERT(result.has_value());
    }

    auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1));
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
    {
        storage.SetKeepMessageOrder(true);
        storage.AddMessage(3, true, 5, TInstant::Now());
        storage.AddMessage(4, true, 5, TInstant::Now());
        storage.AddMessage(5, true, 7, TInstant::Now());
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1));
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Message, 3);
    }

    auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1));
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Message, 5);

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
    TStorage storage(CreateDefaultTimeProvider());
    {
        storage.AddMessage(3, true, 5, TInstant::Now());
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1));
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

Y_UNIT_TEST(CommitLockedMessage_WithKeepMessageOrder) {
    TStorage storage(CreateDefaultTimeProvider());
    {
        storage.SetKeepMessageOrder(true);
        storage.AddMessage(3, true, 5, TInstant::Now());
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1));
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
    TStorage storage(CreateDefaultTimeProvider());
    storage.AddMessage(3, true, 5, TInstant::Now());

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

Y_UNIT_TEST(CommitCommittedMessage) {
    TStorage storage(CreateDefaultTimeProvider());
    {
        storage.AddMessage(3, true, 5, TInstant::Now());
        auto result = storage.Commit(3);
        UNIT_ASSERT(result);
    }

    auto result = storage.Commit(3);
    UNIT_ASSERT(!result);

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
    TStorage storage(CreateDefaultTimeProvider());
    {
        storage.AddMessage(3, true, 5, TInstant::Now());
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1));
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

Y_UNIT_TEST(UnlockLockedMessage_WithKeepMessageOrder) {
    TStorage storage(CreateDefaultTimeProvider());
    {
        storage.SetKeepMessageOrder(true);
        storage.AddMessage(3, true, 5, TInstant::Now());
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1));
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
    TStorage storage(CreateDefaultTimeProvider());
    {
        storage.AddMessage(3, true, 5, TInstant::Now());
        auto result = storage.Commit(3);
        UNIT_ASSERT(result);
    }

    auto result = storage.Unlock(3);
    UNIT_ASSERT(!result);

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
    auto now = TInstant::Now();

    TStorage storage(CreateDefaultTimeProvider());
    {
        storage.AddMessage(3, true, 5, TInstant::Now());
        auto result = storage.Next(now + TDuration::Seconds(1));
        UNIT_ASSERT(result.has_value());
    }

    auto result = storage.ChangeMessageDeadline(3, now + TDuration::Seconds(5));
    UNIT_ASSERT(result);

    auto deadline = storage.GetMessageDeadline(3);
    UNIT_ASSERT_VALUES_EQUAL(deadline.Seconds(), now.Seconds() + 5);
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
    NKikimrPQ::TMLPStorageSnapshot snapshot;

    {
        TStorage storage(CreateDefaultTimeProvider());

        storage.SerializeTo(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetFormatVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstUncommittedOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseDeadlineMilliseconds(), storage.GetBaseDeadline().MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMessages().size(), 0);
    }
    {
        TStorage storage(CreateDefaultTimeProvider());

        storage.Initialize(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseDeadline().MilliSeconds(), snapshot.GetMeta().GetBaseDeadlineMilliseconds());

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
    NKikimrPQ::TMLPStorageSnapshot snapshot;

    {
        TStorage storage(CreateDefaultTimeProvider());
        storage.SetKeepMessageOrder(true);

        storage.AddMessage(3, true, 5, TInstant::Now());
        storage.AddMessage(4, true, 7, TInstant::Now());
        storage.AddMessage(5, true, 11, TInstant::Now());
        storage.AddMessage(6, true, 13, TInstant::Now());

        storage.Commit(3);
        storage.Next(TInstant::Now() + TDuration::Seconds(1));
        storage.Commit(5);

        storage.SerializeTo(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetFormatVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstOffset(), 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstUncommittedOffset(), 4);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseDeadlineMilliseconds(), storage.GetBaseDeadline().MilliSeconds());
        UNIT_ASSERT(snapshot.GetMessages().size() > 0);
    }
    {
        TStorage storage(CreateDefaultTimeProvider());
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 7);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 6);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 4);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseDeadline().MilliSeconds(), snapshot.GetMeta().GetBaseDeadlineMilliseconds());

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

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, timeProvider->Now());

        auto batch = storage.GetBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AffectedMessageCount(), 1); // new message
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(CreateDefaultTimeProvider());
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);

        const auto* message = storage.GetMessage(3);
        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Unprocessed);

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

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, timeProvider->Now());

        auto r = storage.Next(timeProvider->Now() + TDuration::Seconds(7));
        UNIT_ASSERT(r);
        UNIT_ASSERT_VALUES_EQUAL(r.value().Message, 3);

        auto batch = storage.GetBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AffectedMessageCount(), 2); // new message and changed message
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(CreateDefaultTimeProvider());
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);

        const auto* message = storage.GetMessage(3);
        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 7);

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

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, timeProvider->Now());

        auto r = storage.Commit(3);
        UNIT_ASSERT(r);

        auto batch = storage.GetBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AffectedMessageCount(), 2); // new message and changed message
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(CreateDefaultTimeProvider());
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);

        const auto* message = storage.GetMessage(3);
        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Committed);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 0);

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

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider);
        storage.SetKeepMessageOrder(true);
        storage.SetMaxMessageReceiveCount(1);
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, timeProvider->Now());

        auto r = storage.Next(timeProvider->Now() + TDuration::Seconds(7));
        UNIT_ASSERT(r);

        storage.Unlock(3);

        auto message = storage.GetMessage(3);
        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::DLQ);

        const auto& dlq = storage.GetDLQMessages();
        UNIT_ASSERT_VALUES_EQUAL(dlq.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(dlq.front(), 3);

        auto batch = storage.GetBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AffectedMessageCount(), 3); // new message and changed message and DLQ
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(CreateDefaultTimeProvider());
        storage.SetKeepMessageOrder(true);

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);

        const auto* message = storage.GetMessage(3);
        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::DLQ);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 0);

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

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflyMessageCount, 3);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 2); // offsets 4 and 6
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1); // offset 5
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
}

Y_UNIT_TEST(CompactStorage_ByReteintion) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    TStorage storage(timeProvider);
    storage.SetReteintion(TDuration::Seconds(1));

    storage.AddMessage(3, true, 5, timeProvider->Now());
    storage.AddMessage(4, true, 7, timeProvider->Now() + TDuration::Seconds(11));
    storage.AddMessage(5, true, 11, timeProvider->Now() + TDuration::Seconds(12));

    timeProvider->Tick(TDuration::Seconds(13));

    auto result = storage.Compact();
    Cerr << storage.DebugString() << Endl;
    UNIT_ASSERT_VALUES_EQUAL_C(result, 2, "must remove message with offset 3 and 4");

    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 5);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 6);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 5);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 5);

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
    storage.SetMaxMessageReceiveCount(1);
    storage.AddMessage(3, true, 5, TInstant::Now());
    storage.AddMessage(4, true, 7, TInstant::Now());

    storage.Next(TInstant::Now() + TDuration::Seconds(1));
    storage.Unlock(3);
    storage.Commit(4);

    {
        auto* message = storage.GetMessage(3);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::DLQ);
    }
    {
        auto* message = storage.GetMessage(4);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Committed);
    }

    auto result = storage.Compact();
    UNIT_ASSERT_VALUES_EQUAL_C(result, 0, "Keep DLQ messages");

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

    storage.Next(timeProvider->Now() + TDuration::Seconds(10));
    timeProvider->Tick(TDuration::Seconds(5));
    storage.Next(timeProvider->Now() + TDuration::Seconds(10));
    timeProvider->Tick(TDuration::Seconds(7));

    auto result = storage.ProccessDeadlines();
    UNIT_ASSERT_VALUES_EQUAL(result, 1);

    {
        auto* message = storage.GetMessage(3);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Unprocessed);
        UNIT_ASSERT_VALUES_EQUAL(message->ReceiveCount, 1);
    }
    {
        auto* message = storage.GetMessage(4);
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

    storage.Next(timeProvider->Now() + TDuration::Seconds(3));
    storage.Next(timeProvider->Now() + TDuration::Seconds(5));
    storage.Next(timeProvider->Now() + TDuration::Seconds(7));

    timeProvider->Tick(TDuration::Seconds(5));

    storage.MoveBaseDeadline();

    {
        auto* message = storage.GetMessage(3);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 0);
    }
    {
        auto* message = storage.GetMessage(4);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 0);
    }
    {
        auto* message = storage.GetMessage(5);
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 2);
    }
}

}

} // namespace NKikimr::NPQ::NMLP
