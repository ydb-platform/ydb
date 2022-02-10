#include <ydb/core/ymq/base/action.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSQS {

Y_UNIT_TEST_SUITE(ActionParsingTest) {
    Y_UNIT_TEST(ToAndFromStringAreConsistent) {
        EAction first = static_cast<EAction>(EAction::Unknown + 1);
        for (EAction i = first; i < EAction::ActionsArraySize; i = static_cast<EAction>(i + 1)) {
            UNIT_ASSERT_STRINGS_EQUAL(ActionToString(i), ActionToString(ActionFromString(ActionToString(i))));
        }
    }

    Y_UNIT_TEST(ActionsForQueueTest) {
        EAction first = static_cast<EAction>(EAction::Unknown + 1);
        for (EAction i = first; i < EAction::ActionsArraySize; i = static_cast<EAction>(i + 1)) {
            UNIT_ASSERT_C(IsActionForQueue(i) || IsActionForUser(i), i);
            UNIT_ASSERT_C(!(IsActionForQueue(i) && IsActionForUser(i)), i);
        }
    }

    Y_UNIT_TEST(BatchActionTest) {
        EAction first = static_cast<EAction>(EAction::Unknown + 1);
        for (EAction i = first; i < EAction::ActionsArraySize; i = static_cast<EAction>(i + 1)) {
            const TString& name = ActionToString(i);
            const EAction nonBatch = GetNonBatchAction(i);
            if (TStringBuf(name).EndsWith("Batch")) {
                UNIT_ASSERT_C(IsBatchAction(i), i);
                UNIT_ASSERT_UNEQUAL_C(i, nonBatch, i);
                UNIT_ASSERT_C(TStringBuf(name).StartsWith(ActionToString(nonBatch)), i);
            } else {
                UNIT_ASSERT_C(!IsBatchAction(i), i);
                UNIT_ASSERT_EQUAL_C(i, nonBatch, i);
            }
        }
    }

    Y_UNIT_TEST(ActionsForMessageTest) {
        UNIT_ASSERT(IsActionForMessage(EAction::SendMessage));
        UNIT_ASSERT(IsActionForMessage(EAction::SendMessageBatch));
        UNIT_ASSERT(IsActionForMessage(EAction::DeleteMessage));
        UNIT_ASSERT(IsActionForMessage(EAction::DeleteMessageBatch));
        UNIT_ASSERT(IsActionForMessage(EAction::ReceiveMessage));
        UNIT_ASSERT(IsActionForMessage(EAction::ChangeMessageVisibility));
        UNIT_ASSERT(IsActionForMessage(EAction::ChangeMessageVisibilityBatch));

        UNIT_ASSERT(!IsActionForMessage(EAction::CreateQueue));
        UNIT_ASSERT(!IsActionForMessage(EAction::DeleteQueue));
        UNIT_ASSERT(!IsActionForMessage(EAction::PurgeQueue));
        UNIT_ASSERT(!IsActionForMessage(EAction::PurgeQueueBatch));
    }

    Y_UNIT_TEST(FastActionsTest) {
        UNIT_ASSERT(IsFastAction(EAction::SendMessage));
        UNIT_ASSERT(IsFastAction(EAction::SendMessageBatch));
        UNIT_ASSERT(IsFastAction(EAction::DeleteMessage));
        UNIT_ASSERT(IsFastAction(EAction::DeleteMessageBatch));
        UNIT_ASSERT(IsFastAction(EAction::ReceiveMessage));
        UNIT_ASSERT(IsFastAction(EAction::ChangeMessageVisibility));
        UNIT_ASSERT(IsFastAction(EAction::ChangeMessageVisibilityBatch));
        UNIT_ASSERT(IsFastAction(EAction::GetQueueAttributes));
        UNIT_ASSERT(IsFastAction(EAction::GetQueueAttributesBatch));
        UNIT_ASSERT(IsFastAction(EAction::GetQueueUrl));
        UNIT_ASSERT(IsFastAction(EAction::SetQueueAttributes));

        UNIT_ASSERT(!IsFastAction(EAction::CreateQueue));
        UNIT_ASSERT(!IsFastAction(EAction::DeleteQueue));
        UNIT_ASSERT(!IsFastAction(EAction::CreateUser));
        UNIT_ASSERT(!IsFastAction(EAction::DeleteUser));
    }
}

} // namespace NKikimr::NSQS
