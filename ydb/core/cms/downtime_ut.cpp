#include "cluster_info.h"
#include "cms_state.h"
#include "cms_ut_common.h"
#include "downtime.h"
#include "ut_helpers.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NCmsTest {

using namespace NCms;
using namespace NKikimrBlobStorage;
using namespace NKikimrCms;

static void CheckDowntime(TDowntime::TSegments::const_iterator i,
                          TInstant start , TInstant end, const TString &reason)
{
    UNIT_ASSERT_VALUES_EQUAL(i->Start, start);
    UNIT_ASSERT_VALUES_EQUAL(i->End, end);
    if (reason != "any")
        UNIT_ASSERT_VALUES_EQUAL(i->Reason, reason);
}

static void CheckDowntime(TDowntime::TSegments::const_iterator i,
                          TDowntime::TSegments::const_iterator iend,
                          TInstant start , TInstant end, const TString &reason)
{
    UNIT_ASSERT(i != iend);
    CheckDowntime(i, start, end, reason);
    UNIT_ASSERT(++i == iend);
}

template<typename... Ts>
void CheckDowntime(TDowntime::TSegments::const_iterator i,
                   TDowntime::TSegments::const_iterator iend,
                   TInstant start , TInstant end, const TString &reason,
                   Ts... intervals)
{
    UNIT_ASSERT(i != iend);
    CheckDowntime(i, start, end, reason);
    UNIT_ASSERT(++i != iend);
    CheckDowntime(i, iend, intervals...);
}

template<typename... Ts>
void CheckDowntime(const TDowntime &downtime, Ts... intervals)
{
    return CheckDowntime(downtime.GetDowntimeSegments().begin(),
                         downtime.GetDowntimeSegments().end(),
                         intervals...);
}

Y_UNIT_TEST_SUITE(TDowntimeTest) {
    Y_UNIT_TEST(AddDowntime)
    {
        TDowntime downtime1(TDuration::Seconds(10));
        TInstant t1 = Now();
        TInstant t2 = t1 + TDuration::Seconds(3);
        TInstant t3 = t1 + TDuration::Seconds(6);
        TInstant t4 = t1 + TDuration::Seconds(9);
        TInstant t5 = t1 + TDuration::Seconds(12);
        TInstant t6 = t1 + TDuration::Seconds(15);
        TInstant t7 = t1 + TDuration::Seconds(18);
        TInstant t8 = t1 + TDuration::Seconds(21);
        TInstant t9 = t1 + TDuration::Seconds(30);
        TInstant t10 = t1 + TDuration::Seconds(33);

        UNIT_ASSERT(downtime1.Empty());
        downtime1.AddDowntime(t1, t2, "s1");
        downtime1.AddDowntime(t6, t7, "s2");
        CheckDowntime(downtime1, t1, t2, "s1", t6, t7, "s2");
        downtime1.AddDowntime(t4, t5, "s3");
        CheckDowntime(downtime1, t1, t7, "s1, s3, s2");
        downtime1.AddDowntime(t3, t4, "s4");
        CheckDowntime(downtime1, t1, t7, "s1, s3, s2, s4");
        downtime1.AddDowntime(t1, t4, "s5");
        CheckDowntime(downtime1, t1, t7, "s5, s1, s3, s2, s4");

        downtime1.Clear();
        downtime1.AddDowntime(t2, t3, "");
        downtime1.AddDowntime(t3, t4, "s1");
        CheckDowntime(downtime1, t2, t4, "s1");

        downtime1.Clear();
        downtime1.AddDowntime(t2, TDuration::Seconds(3), "s1");
        downtime1.AddDowntime({t3, t4, ""});
        CheckDowntime(downtime1, t2, t4, "s1");

        TDowntime downtime2;
        downtime1.Clear();
        downtime1.AddDowntime(t1, t2, "s1");
        downtime1.AddDowntime(t6, t7, "s2");
        downtime2.AddDowntime(t4, t5, "s3");
        downtime2.AddDowntime(t9, t10, "s4");
        downtime2.AddDowntime(downtime1);
        CheckDowntime(downtime2, t1, t2, "s1", t4, t5, "s3",
                      t6, t7, "s2", t9, t10, "s4");
        downtime1.AddDowntime(downtime2);
        CheckDowntime(downtime1, t1, t7, "any", t9, t10, "s4");

        NCms::TPDiskInfo pdisk;
        TAction action1 = MakeAction(TAction::SHUTDOWN_HOST, 1, TDuration::Seconds(6).GetValue());
        TPermissionInfo permission1;
        permission1.Action = action1;
        permission1.PermissionId = "permission-1";
        permission1.Deadline = t9;
        pdisk.AddLock(permission1);
        TNotificationInfo notification1;
        notification1.NotificationId = "notification-1";
        notification1.Notification.SetTime(t6.GetValue());
        pdisk.AddExternalLock(notification1, action1);
        pdisk.Downtime.AddDowntime(t1, t2, "known downtime");
        pdisk.State = UP;
        pdisk.Timestamp = t2;
        downtime2.Clear();
        downtime2.AddDowntime(pdisk, t3);
        CheckDowntime(downtime2, t1, t2, "known downtime", t3, t5, "permission-1",
                      t6, t8, "notification-1");

        pdisk.State = DOWN;
        pdisk.Lock.Clear();
        pdisk.ExternalLocks.clear();
        pdisk.Downtime.Clear();
        downtime2.Clear();
        downtime2.AddDowntime(pdisk, t3);
        CheckDowntime(downtime2, t2, t3, "known downtime");

        pdisk.State = UP;
        TPermissionInfo permission2;
        permission2.Action = action1;
        permission2.PermissionId = "permission-2";
        permission2.Deadline = t2;
        pdisk.AddLock(permission2);
        downtime2.Clear();
        downtime2.AddDowntime(pdisk, t3);
        CheckDowntime(downtime2, t3, t4, "permission-2");

        pdisk.Lock.Clear();
        TNotificationInfo notification2;
        notification2.NotificationId = "notification-2";
        notification2.Notification.SetTime(t2.GetValue());
        pdisk.AddExternalLock(notification2, action1);
        downtime2.Clear();
        downtime2.AddDowntime(pdisk, t3);
        CheckDowntime(downtime2, t3, t4, "notification-2");
    }

    Y_UNIT_TEST(HasUpcomingDowntime)
    {
        TDowntime downtime1(TDuration::Seconds(5));
        TInstant t1 = Now();
        TInstant t2 = t1 + TDuration::Seconds(3);
        TInstant t3 = t1 + TDuration::Seconds(6);
        TInstant t4 = t1 + TDuration::Seconds(9);
        TInstant t5 = t1 + TDuration::Seconds(23);
        TInstant t6 = t1 + TDuration::Seconds(26);
        TInstant t7 = t1 + TDuration::Seconds(33);

        downtime1.AddDowntime(t2, t4);
        UNIT_ASSERT(!downtime1.HasUpcomingDowntime(t1, TDuration::Seconds(0), TDuration::Seconds(5)));
        UNIT_ASSERT(downtime1.HasUpcomingDowntime(t1, TDuration::Seconds(3), TDuration::Seconds(5)));
        UNIT_ASSERT(!downtime1.HasUpcomingDowntime(t2, TDuration::Seconds(3), TDuration::Seconds(15)));
        UNIT_ASSERT(downtime1.HasUpcomingDowntime(t2, TDuration::Seconds(3), TDuration::Seconds(5)));
        UNIT_ASSERT(!downtime1.HasUpcomingDowntime(t3, TDuration::Seconds(3), TDuration::Seconds(15)));
        UNIT_ASSERT(downtime1.HasUpcomingDowntime(t3, TDuration::Seconds(3), TDuration::Seconds(5)));
        UNIT_ASSERT(downtime1.HasUpcomingDowntime(t4, TDuration::Seconds(3), TDuration::Seconds(5)));
        UNIT_ASSERT(!downtime1.HasUpcomingDowntime(t5, TDuration::Seconds(3), TDuration::Seconds(5)));

        downtime1.AddDowntime(t5, t7);
        UNIT_ASSERT(!downtime1.HasUpcomingDowntime(t1, TDuration::Seconds(0), TDuration::Seconds(10)));
        UNIT_ASSERT(downtime1.HasUpcomingDowntime(t1, TDuration::Seconds(25), TDuration::Seconds(10)));
        UNIT_ASSERT(!downtime1.HasUpcomingDowntime(t1, TDuration::Seconds(30), TDuration::Seconds(11)));
        UNIT_ASSERT(!downtime1.HasUpcomingDowntime(t6, TDuration::Seconds(0), TDuration::Seconds(11)));
    }

    Y_UNIT_TEST(SetIgnoredDowntimeGap)
    {
        TDowntime downtime1(TDuration::Seconds(5));
        TInstant t1 = Now();
        TInstant t2 = t1 + TDuration::Seconds(3);
        TInstant t3 = t1 + TDuration::Seconds(13);
        TInstant t4 = t1 + TDuration::Seconds(16);
        TInstant t5 = t1 + TDuration::Seconds(23);
        TInstant t6 = t1 + TDuration::Seconds(26);
        TInstant t7 = t1 + TDuration::Seconds(40);
        TInstant t8 = t1 + TDuration::Seconds(43);

        downtime1.AddDowntime(t1, t2, "s1");
        downtime1.AddDowntime(t3, t4, "s2");
        downtime1.AddDowntime(t5, t6, "s3");
        downtime1.AddDowntime(t7, t8, "s4");
        CheckDowntime(downtime1, t1, t2, "s1", t3, t4, "s2",
                      t5, t6, "s3", t7, t8, "s4");

        downtime1.SetIgnoredDowntimeGap(TDuration::Seconds(10));
        CheckDowntime(downtime1, t1, t6, "s1, s2, s3", t7, t8, "s4");

        downtime1.SetIgnoredDowntimeGap(TDuration::Seconds(5));
        CheckDowntime(downtime1, t1, t6, "s1, s2, s3", t7, t8, "s4");

        downtime1.SetIgnoredDowntimeGap(TDuration::Seconds(15));
        CheckDowntime(downtime1, t1, t8, "s1, s2, s3, s4");
    }

    Y_UNIT_TEST(CleanupOldSegments)
    {
        TDowntime downtime1(TDuration::Seconds(5));
        TInstant t1 = Now();
        TInstant t2 = t1 + TDuration::Seconds(3);
        TInstant t3 = t1 + TDuration::Seconds(9);
        TInstant t4 = t1 + TDuration::Seconds(12);
        TInstant t5 = t1 + TDuration::Seconds(15);
        TInstant t6 = t1 + TDuration::Seconds(18);
        TInstant t7 = t1 + TDuration::Seconds(21);

        downtime1.AddDowntime(t1, t2, "s1");
        downtime1.AddDowntime(t3, t4, "s2");
        downtime1.AddDowntime(t6, t7, "s3");

        downtime1.CleanupOldSegments(t5);
        CheckDowntime(downtime1, t3, t4, "s2", t6, t7, "s3");
    }
}

} // NCmsTest
} // NKikimr
