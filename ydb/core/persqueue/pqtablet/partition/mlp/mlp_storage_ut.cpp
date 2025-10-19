#include "mlp_storage.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPStorageTests) {

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

    storage.AddMessage(0, true, 5);
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

    storage.AddMessage(3, true, 5);
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

    storage.AddMessage(3, true, 5);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 4);

    storage.AddMessage(7, true, 5);
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
    storage.AddMessage(3, true, 5);

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
    storage.AddMessage(3, true, 5);

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

Y_UNIT_TEST(SkipLockedMessage) {
    TStorage storage(CreateDefaultTimeProvider());
    {
        storage.AddMessage(3, true, 5);
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

Y_UNIT_TEST(CommitLockedMessage_WithoutKeepMessageOrder) {
    TStorage storage(CreateDefaultTimeProvider());
    {
        storage.AddMessage(3, true, 5);
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
        storage.AddMessage(3, true, 5);
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
    storage.AddMessage(3, true, 5);

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
        storage.AddMessage(3, true, 5);
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
        storage.AddMessage(3, true, 5);
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
        storage.AddMessage(3, true, 5);
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
    storage.AddMessage(3, true, 5);

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
        storage.AddMessage(3, true, 5);
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
        storage.AddMessage(3, true, 5);
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
    storage.AddMessage(3, true, 5);

    auto result = storage.ChangeMessageDeadline(3, now + TDuration::Seconds(5));
    UNIT_ASSERT(!result);

    auto deadline = storage.GetMessageDeadline(3);
    UNIT_ASSERT_VALUES_EQUAL(deadline, TInstant::Zero());
}

Y_UNIT_TEST(EmptyStorageSerialization) {
    NKikimrPQ::TMLPStorageSnapshot snapshot;

    {
        TStorage storage(CreateDefaultTimeProvider());

        storage.CreateSnapshot(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetFormatVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstUncommittedOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseDeadlineMilliseconds(), storage.GetBaseDeadline().MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMessages().size(), 0);
    }
    {
        TStorage storage(CreateDefaultTimeProvider());

        storage.InitializeFromSnapshot(snapshot);

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

        storage.AddMessage(3, true, 5);
        storage.AddMessage(4, true, 7);
        storage.AddMessage(5, true, 11);
        storage.AddMessage(6, true, 13);

        storage.Commit(3);
        storage.Next(TInstant::Now() + TDuration::Seconds(1));
        storage.Commit(5);

        storage.CreateSnapshot(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetFormatVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstOffset(), 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstUncommittedOffset(), 4);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseDeadlineMilliseconds(), storage.GetBaseDeadline().MilliSeconds());
        UNIT_ASSERT(snapshot.GetMessages().size() > 0);
    }
    {
        TStorage storage(CreateDefaultTimeProvider());
        storage.SetKeepMessageOrder(true);

        storage.InitializeFromSnapshot(snapshot);

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


}

} // namespace NKikimr::NPQ::NMLP
