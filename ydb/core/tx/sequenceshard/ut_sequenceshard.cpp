#include "ut_helpers.h"

namespace NKikimr {
namespace NSequenceShard {

    Y_UNIT_TEST_SUITE(SequenceShardTests) {

        Y_UNIT_TEST(Basics) {
            TTestContext ctx;
            ctx.Setup();

            // first time creation must succeed
            {
                auto createResult = ctx.CreateSequence(
                    MakeHolder<TEvSequenceShard::TEvCreateSequence>(TPathId(123, 42)));
                UNIT_ASSERT_VALUES_EQUAL(createResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvCreateSequenceResult::SUCCESS);
            }

            // second time we must get an expected error
            {
                auto createResult = ctx.CreateSequence(
                    MakeHolder<TEvSequenceShard::TEvCreateSequence>(TPathId(123, 42)));
                UNIT_ASSERT_VALUES_EQUAL(createResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvCreateSequenceResult::SEQUENCE_ALREADY_EXISTS);
            }

            // creating a different path id should succeed
            {
                auto createResult = ctx.CreateSequence(
                    TEvSequenceShard::TEvCreateSequence::Build(TPathId(123, 51))
                        .SetStartValue(100001)
                        .SetCache(10)
                        .Done());
                UNIT_ASSERT_VALUES_EQUAL(createResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvCreateSequenceResult::SUCCESS);
            }

            // allocate from the first path, default granularity
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 1);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 1u);
            }

            // allocate from the first path, specific granularity
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42), 10);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 2);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 10u);
            }

            // allocate from the second path, default granularity
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 51));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 100001);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 10u);
            }

            // allocate from the second path, specific granularity
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 51), 50);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 100011);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 50u);
            }

            // allocate from non-existing path
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 99));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_NOT_FOUND);
            }

            // try to allocate all possible values from the first path
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42), Max<ui64>());
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 12);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), Max<i64>() - 11);
            }

            // try to allocate one more value from the first path
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42), 1);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_OVERFLOW);
            }

            // drop the first path
            {
                auto dropResult = ctx.DropSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(dropResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvDropSequenceResult::SUCCESS);
            }

            // dropping an already dropped path should fail
            {
                auto dropResult = ctx.DropSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(dropResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvDropSequenceResult::SEQUENCE_NOT_FOUND);
            }

            ctx.RebootTablet();

            // allocate from the first (dropped) path after reboot
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_NOT_FOUND);
            }

            // allocate from the second path after reboot
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 51));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 100061);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 10u);
            }

            // update sequence to a new initial value
            {
                auto updateResult = ctx.UpdateSequence(
                    TEvSequenceShard::TEvUpdateSequence::Build(TPathId(123, 51))
                        .SetNextValue(200000)
                        .SetNextUsed(true)
                        .Done());
                UNIT_ASSERT_VALUES_EQUAL(updateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvUpdateSequenceResult::SUCCESS);
            }

            // allocate from sequence after update
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 51));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 200001);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 10u);
            }

            // update sequence to a new default granularity
            {
                auto updateResult = ctx.UpdateSequence(
                    TEvSequenceShard::TEvUpdateSequence::Build(TPathId(123, 51))
                        .SetCache(5u)
                        .Done());
                UNIT_ASSERT_VALUES_EQUAL(updateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvUpdateSequenceResult::SUCCESS);
            }

            // allocate from sequence after update
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 51));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 200011);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 5u);
            }

            // get sequence
            {
                auto getResult = ctx.GetSequence(TPathId(123, 51));
                UNIT_ASSERT_VALUES_EQUAL(getResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvGetSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(getResult->Record.GetNextValue(), 200016);
                UNIT_ASSERT_VALUES_EQUAL(getResult->Record.GetCache(), 5u);
            }
        }

        Y_UNIT_TEST(MarkedPipeRetries) {
            TTestContext ctx;
            ctx.Setup();

            ctx.SwitchToMarked(123, 1, 1); // gen 1:1

            // create a sequence
            {
                auto createResult = ctx.CreateSequence(
                    MakeHolder<TEvSequenceShard::TEvCreateSequence>(TPathId(123, 42)));
                UNIT_ASSERT_VALUES_EQUAL(createResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvCreateSequenceResult::SUCCESS);
            }

            ctx.SwitchToMarked(123, 2, 1); // gen 2:1

            // drop a sequence
            {
                auto dropResult = ctx.DropSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(dropResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvDropSequenceResult::SUCCESS);
            }

            ctx.SwitchToMarked(123, 1, 1); // back to gen 1:1

            // retry creating a sequence
            {
                auto createResult = ctx.CreateSequence(
                    MakeHolder<TEvSequenceShard::TEvCreateSequence>(TPathId(123, 42)));
                UNIT_ASSERT_VALUES_EQUAL(createResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvCreateSequenceResult::PIPE_OUTDATED);
            }

            ctx.SwitchToMarked(123, 3, 1); // gen 3:1

            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_NOT_FOUND);
            }

            ctx.SwitchToMarked(123, 2, 1); // back to gen 2:1

            // retry dropping a sequence
            {
                auto dropResult = ctx.DropSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(dropResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvDropSequenceResult::PIPE_OUTDATED);
            }

            ctx.RebootTablet();

            ctx.SwitchToMarked(123, 1, 2); // gen 1:2 (old gen 1 recreated pipe to new tablet)

            // retry of create sequence must fail even after reboot
            {
                auto createResult = ctx.CreateSequence(
                    MakeHolder<TEvSequenceShard::TEvCreateSequence>(TPathId(123, 42)));
                UNIT_ASSERT_VALUES_EQUAL(createResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvCreateSequenceResult::PIPE_OUTDATED);
            }
        }

        Y_UNIT_TEST(FreezeRestoreRedirect) {
            TTestContext ctx;
            ctx.Setup();

            // create a sequence
            {
                auto createResult = ctx.CreateSequence(
                    TEvSequenceShard::TEvCreateSequence::Build(TPathId(123, 42))
                        .SetMinValue(1)
                        .SetStartValue(1)
                        .SetCache(100)
                        .Done());
                UNIT_ASSERT_VALUES_EQUAL(createResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvCreateSequenceResult::SUCCESS);
            }

            // allocate some values
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42), 10);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 1);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 10u);
            }

            // freeze record for use in restore
            NKikimrTxSequenceShard::TEvFreezeSequenceResult freezeRecord;

            // freeze sequence, it's idempotent so repeat several times
            for (int i = 0; i < 3; ++i) {
                auto freezeResult = ctx.FreezeSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(freezeResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvFreezeSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(freezeResult->Record.GetNextValue(), 11);
                UNIT_ASSERT_VALUES_EQUAL(freezeResult->Record.GetCache(), 100u);
                freezeRecord = freezeResult->Record;
            }

            // allocate some values
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42), 10);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_FROZEN);
            }

            // restore sequence using frozen values
            // note: we use different path id and the same tablet, normally
            // we would use the same path id and different tablets
            {
                auto restoreResult = ctx.RestoreSequence(
                    MakeHolder<TEvSequenceShard::TEvRestoreSequence>(TPathId(123, 43), freezeRecord));
                UNIT_ASSERT_VALUES_EQUAL(restoreResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvRestoreSequenceResult::SUCCESS);
            }

            // allocate some values
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 43));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 11);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 100u);
            }

            // restoring again must fail, since sequence was active and may have changed its values
            {
                auto restoreResult = ctx.RestoreSequence(
                    MakeHolder<TEvSequenceShard::TEvRestoreSequence>(TPathId(123, 43), freezeRecord));
                UNIT_ASSERT_VALUES_EQUAL(restoreResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvRestoreSequenceResult::SEQUENCE_ALREADY_ACTIVE);
            }

            // redirect sequence, it's idempotent so repeat several times
            for (int i = 0; i < 3; ++i) {
                auto redirectResult = ctx.RedirectSequence(TPathId(123, 42), 12345);
                UNIT_ASSERT_VALUES_EQUAL(redirectResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvRedirectSequenceResult::SUCCESS);
            }

            // allocate some values from redirected sequence
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_MOVED);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetMovedTo(), 12345u);
            }

            // freeze the previously restored sequence
            {
                auto freezeResult = ctx.FreezeSequence(TPathId(123, 43));
                UNIT_ASSERT_VALUES_EQUAL(freezeResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvFreezeSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(freezeResult->Record.GetNextValue(), 111);
                UNIT_ASSERT_VALUES_EQUAL(freezeResult->Record.GetCache(), 100u);
                freezeRecord = freezeResult->Record;
            }

            // restore over redirected sequence
            {
                auto restoreResult = ctx.RestoreSequence(
                    MakeHolder<TEvSequenceShard::TEvRestoreSequence>(TPathId(123, 42), freezeRecord));
                UNIT_ASSERT_VALUES_EQUAL(restoreResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvRestoreSequenceResult::SUCCESS);
            }

            // redirect the second sequence
            {
                auto redirectResult = ctx.RedirectSequence(TPathId(123, 43), 54321);
                UNIT_ASSERT_VALUES_EQUAL(redirectResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvRedirectSequenceResult::SUCCESS);
            }

            // check we cannot freeze a redirected sequence
            {
                auto freezeResult = ctx.FreezeSequence(TPathId(123, 43));
                UNIT_ASSERT_VALUES_EQUAL(freezeResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvFreezeSequenceResult::SEQUENCE_MOVED);
                UNIT_ASSERT_VALUES_EQUAL(freezeResult->Record.GetMovedTo(), 54321u);
            }

            // allocate some values from the first sequence
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), 111);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 100u);
            }
        }

        Y_UNIT_TEST(NegativeIncrement) {
            TTestContext ctx;
            ctx.Setup();

            // creating a sequence with negative increment
            {
                auto createResult = ctx.CreateSequence(
                    TEvSequenceShard::TEvCreateSequence::Build(TPathId(123, 42))
                        .SetIncrement(-1)
                        .SetCache(10)
                        .Done());
                UNIT_ASSERT_VALUES_EQUAL(createResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvCreateSequenceResult::SUCCESS);
            }

            // allocate some values
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), -1);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 10u);
            }

            // allocate some values
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), -11);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 10u);
            }

            // allocate all remaining values (there are 2^63 negative values in total, 20 we have consumed already)
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42), Max<ui64>());
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), -21);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 9223372036854775808ull - 20);
            }

            // must not be able to allocate even one more value
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42), 1);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_OVERFLOW);
            }

            // update sequence to enable cycling
            {
                auto updateResult = ctx.UpdateSequence(
                    TEvSequenceShard::TEvUpdateSequence::Build(TPathId(123, 42))
                        .SetCycle(true)
                        .Done());
                UNIT_ASSERT_VALUES_EQUAL(updateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvUpdateSequenceResult::SUCCESS);
            }

            // allocate some values
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), -1);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 10u);
            }

            // allocate some values
            {
                auto allocateResult = ctx.AllocateSequence(TPathId(123, 42));
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetStatus(),
                    NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationStart(), -11);
                UNIT_ASSERT_VALUES_EQUAL(allocateResult->Record.GetAllocationCount(), 10u);
            }
        }

    }

} // namespace NSequenceShard
} // namespace NKikimr
