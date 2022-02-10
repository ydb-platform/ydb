#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include "sequencer.h"

using namespace NKikimr;
using namespace NHive;

Y_UNIT_TEST_SUITE(Sequencer) {
    Y_UNIT_TEST(Basic1) {
        TSequenceGenerator sequencer;
        std::vector<TSequencer::TOwnerType> modified;

        UNIT_ASSERT_VALUES_EQUAL(sequencer.FreeSize(), 0);
        sequencer.AddFreeSequence({TSequencer::NO_OWNER, 1}, {1000, 2400});
        UNIT_ASSERT_VALUES_EQUAL(sequencer.FreeSize(), 1400);
        sequencer.AddFreeSequence({TSequencer::NO_OWNER, 2}, {2450, 2500});
        UNIT_ASSERT_VALUES_EQUAL(sequencer.FreeSize(), 1450);
        sequencer.AddFreeSequence({TSequencer::NO_OWNER, 3}, {2400, 2450});
        UNIT_ASSERT_VALUES_EQUAL(sequencer.FreeSize(), 1500);
        auto owner1 = sequencer.GetOwner(1003);
        UNIT_ASSERT_EQUAL(owner1.first, TSequencer::NO_OWNER);
        auto result1 = sequencer.AllocateSequence({1, 1}, 10, modified); // 1500 - 10 = 1490
        UNIT_ASSERT_VALUES_EQUAL(result1.Begin, 1000);
        UNIT_ASSERT_VALUES_EQUAL(result1.Next, 1000);
        UNIT_ASSERT_VALUES_EQUAL(result1.End, 1010);
        UNIT_ASSERT_VALUES_EQUAL(result1.Size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.FreeSize(), 1490);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.AllocatedSequencesSize(), 10);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.AllocatedSequencesCount(), 1);
        auto owner2 = sequencer.GetOwner(1003);
        UNIT_ASSERT_EQUAL(owner2, TSequencer::TOwnerType(1, 1));
        auto result2 = sequencer.AllocateSequence({2, 2}, 1390, modified);
        UNIT_ASSERT_VALUES_EQUAL(result2.Begin, 1010);
        UNIT_ASSERT_VALUES_EQUAL(result2.Next, 1010);
        UNIT_ASSERT_VALUES_EQUAL(result2.End, 2400);
        UNIT_ASSERT_VALUES_EQUAL(result2.Size(), 1390);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.FreeSize(), 100);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.AllocatedSequencesSize(), 1400);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.AllocatedSequencesCount(), 2);
        auto owner3 = sequencer.GetOwner(1013);
        UNIT_ASSERT_EQUAL(owner3, TSequencer::TOwnerType(2, 2));
        for (int i = 0; i < 50; ++i) {
            auto element = sequencer.AllocateElement(modified);
            UNIT_ASSERT(element != TSequencer::NO_ELEMENT);
        }
        UNIT_ASSERT_VALUES_EQUAL(sequencer.FreeSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.AllocatedSequencesSize(), 1400);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.AllocatedSequencesCount(), 2);
        for (int i = 0; i < 50; ++i) {
            auto element = sequencer.AllocateElement(modified);
            UNIT_ASSERT(element != TSequencer::NO_ELEMENT);
        }
        UNIT_ASSERT_VALUES_EQUAL(sequencer.FreeSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.AllocatedSequencesSize(), 1400);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.AllocatedSequencesCount(), 2);
        auto result3 = sequencer.AllocateSequence({4, 4}, 10, modified);
        UNIT_ASSERT_EQUAL(result3, TSequencer::NO_SEQUENCE);
        sequencer.AddFreeSequence({TSequencer::NO_OWNER, 4}, {2500, 2600});
        UNIT_ASSERT_VALUES_EQUAL(sequencer.FreeSize(), 100);
        auto result4 = sequencer.AllocateSequence({4, 4}, 10, modified);
        UNIT_ASSERT_VALUES_EQUAL(result4.Begin, 2500);
        UNIT_ASSERT_VALUES_EQUAL(result4.Size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.FreeSize(), 90);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.AllocatedSequencesSize(), 1410);
        UNIT_ASSERT_VALUES_EQUAL(sequencer.AllocatedSequencesCount(), 3);
    }
}
