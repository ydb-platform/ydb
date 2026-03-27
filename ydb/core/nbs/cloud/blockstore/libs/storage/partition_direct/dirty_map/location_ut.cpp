#include "location.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocationTest)
{
    Y_UNIT_TEST(TestIsDDisk)
    {
        // Test DDisk locations
        UNIT_ASSERT(IsDDisk(ELocation::DDisk0));
        UNIT_ASSERT(IsDDisk(ELocation::DDisk1));
        UNIT_ASSERT(IsDDisk(ELocation::DDisk2));
        UNIT_ASSERT(IsDDisk(ELocation::HODDisk0));
        UNIT_ASSERT(IsDDisk(ELocation::HODDisk1));
        // Test PBuffer locations (should return false)
        UNIT_ASSERT(!IsDDisk(ELocation::PBuffer0));
        UNIT_ASSERT(!IsDDisk(ELocation::PBuffer1));
        UNIT_ASSERT(!IsDDisk(ELocation::PBuffer2));
        UNIT_ASSERT(!IsDDisk(ELocation::HOPBuffer0));
        UNIT_ASSERT(!IsDDisk(ELocation::HOPBuffer1));
        // Test Unknown location
        UNIT_ASSERT(!IsDDisk(ELocation::Unknown));
    }

    Y_UNIT_TEST(TestIsPBuffer)
    {
        // Test PBuffer locations
        UNIT_ASSERT(IsPBuffer(ELocation::PBuffer0));
        UNIT_ASSERT(IsPBuffer(ELocation::PBuffer1));
        UNIT_ASSERT(IsPBuffer(ELocation::PBuffer2));
        UNIT_ASSERT(IsPBuffer(ELocation::HOPBuffer0));
        UNIT_ASSERT(IsPBuffer(ELocation::HOPBuffer1));
        // Test DDisk locations (should return false)
        UNIT_ASSERT(!IsPBuffer(ELocation::DDisk0));
        UNIT_ASSERT(!IsPBuffer(ELocation::DDisk1));
        UNIT_ASSERT(!IsPBuffer(ELocation::DDisk2));
        UNIT_ASSERT(!IsPBuffer(ELocation::HODDisk0));
        UNIT_ASSERT(!IsPBuffer(ELocation::HODDisk1));
        // Test Unknown location
        UNIT_ASSERT(!IsPBuffer(ELocation::Unknown));
    }

    Y_UNIT_TEST(TestLocationMaskMakeEmpty)
    {
        auto mask = TLocationMask::MakeEmpty();
        UNIT_ASSERT(mask.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0, mask.Count());
        UNIT_ASSERT(!mask.HasDDisk());
        UNIT_ASSERT(!mask.HasPBuffer());
        UNIT_ASSERT(!mask.OnlyDDisk());
        UNIT_ASSERT(!mask.OnlyPBuffer());
    }

    Y_UNIT_TEST(TestLocationMaskMakePBuffer)
    {
        // Test with all PBuffers enabled
        auto mask = TLocationMask::MakePBuffer(true, true, true, true, true);
        UNIT_ASSERT(!mask.Empty());
        UNIT_ASSERT_VALUES_EQUAL(5, mask.Count());
        UNIT_ASSERT(mask.HasPBuffer());
        UNIT_ASSERT(mask.OnlyPBuffer());
        UNIT_ASSERT(!mask.HasDDisk());
        UNIT_ASSERT(!mask.OnlyDDisk());
        // Test individual PBuffers
        UNIT_ASSERT(mask.Get(ELocation::PBuffer0));
        UNIT_ASSERT(mask.Get(ELocation::PBuffer1));
        UNIT_ASSERT(mask.Get(ELocation::PBuffer2));
        UNIT_ASSERT(mask.Get(ELocation::HOPBuffer0));
        UNIT_ASSERT(mask.Get(ELocation::HOPBuffer1));
        // Test with mixed PBuffers
        auto partialMask =
            TLocationMask::MakePBuffer(true, false, true, false, true);
        UNIT_ASSERT_VALUES_EQUAL(3, partialMask.Count());
        UNIT_ASSERT(partialMask.Get(ELocation::PBuffer0));
        UNIT_ASSERT(!partialMask.Get(ELocation::PBuffer1));
        UNIT_ASSERT(partialMask.Get(ELocation::PBuffer2));
        UNIT_ASSERT(!partialMask.Get(ELocation::HOPBuffer0));
        UNIT_ASSERT(partialMask.Get(ELocation::HOPBuffer1));
    }

    Y_UNIT_TEST(TestLocationMaskMakeDDisk)
    {
        // Test with all DDisks enabled
        auto mask = TLocationMask::MakeDDisk(true, true, true, true, true);
        UNIT_ASSERT(!mask.Empty());
        UNIT_ASSERT_VALUES_EQUAL(5, mask.Count());
        UNIT_ASSERT(mask.HasDDisk());
        UNIT_ASSERT(mask.OnlyDDisk());
        UNIT_ASSERT(!mask.HasPBuffer());
        UNIT_ASSERT(!mask.OnlyPBuffer());
        // Test individual DDisks
        UNIT_ASSERT(mask.Get(ELocation::DDisk0));
        UNIT_ASSERT(mask.Get(ELocation::DDisk1));
        UNIT_ASSERT(mask.Get(ELocation::DDisk2));
        UNIT_ASSERT(mask.Get(ELocation::HODDisk0));
        UNIT_ASSERT(mask.Get(ELocation::HODDisk1));
        // Test with mixed DDisks
        auto partialMask =
            TLocationMask::MakeDDisk(true, false, true, false, true);
        UNIT_ASSERT_VALUES_EQUAL(3, partialMask.Count());
        UNIT_ASSERT(partialMask.Get(ELocation::DDisk0));
        UNIT_ASSERT(!partialMask.Get(ELocation::DDisk1));
        UNIT_ASSERT(partialMask.Get(ELocation::DDisk2));
        UNIT_ASSERT(!partialMask.Get(ELocation::HODDisk0));
        UNIT_ASSERT(partialMask.Get(ELocation::HODDisk1));
    }

    Y_UNIT_TEST(TestLocationMaskMakePrimaryDDisk)
    {
        auto mask = TLocationMask::MakePrimaryDDisks();
        UNIT_ASSERT(!mask.Empty());
        UNIT_ASSERT_VALUES_EQUAL(3, mask.Count());
        UNIT_ASSERT(mask.HasDDisk());
        UNIT_ASSERT(mask.OnlyDDisk());
        UNIT_ASSERT(!mask.HasPBuffer());
        UNIT_ASSERT(mask.Get(ELocation::DDisk0));
        UNIT_ASSERT(mask.Get(ELocation::DDisk1));
        UNIT_ASSERT(mask.Get(ELocation::DDisk2));
        UNIT_ASSERT(!mask.Get(ELocation::HODDisk0));
        UNIT_ASSERT(!mask.Get(ELocation::HODDisk1));
    }

    Y_UNIT_TEST(TestLocationMaskMakePrimaryPBuffers)
    {
        auto mask = TLocationMask::MakePrimaryPBuffers();
        UNIT_ASSERT(!mask.Empty());
        UNIT_ASSERT_VALUES_EQUAL(3, mask.Count());
        UNIT_ASSERT(mask.HasPBuffer());
        UNIT_ASSERT(mask.OnlyPBuffer());
        UNIT_ASSERT(!mask.HasDDisk());
        UNIT_ASSERT(mask.Get(ELocation::PBuffer0));
        UNIT_ASSERT(mask.Get(ELocation::PBuffer1));
        UNIT_ASSERT(mask.Get(ELocation::PBuffer2));
        UNIT_ASSERT(!mask.Get(ELocation::HOPBuffer0));
        UNIT_ASSERT(!mask.Get(ELocation::HOPBuffer1));
    }

    Y_UNIT_TEST(TestLocationMaskSetReset)
    {
        TLocationMask mask;
        UNIT_ASSERT(mask.Empty());
        // Set a location
        mask.Set(ELocation::PBuffer0);
        UNIT_ASSERT(!mask.Empty());
        UNIT_ASSERT(mask.Get(ELocation::PBuffer0));
        UNIT_ASSERT_VALUES_EQUAL(1, mask.Count());
        // Set another location
        mask.Set(ELocation::DDisk1);
        UNIT_ASSERT_VALUES_EQUAL(2, mask.Count());
        UNIT_ASSERT(mask.Get(ELocation::PBuffer0));
        UNIT_ASSERT(mask.Get(ELocation::DDisk1));
        // Reset a location
        mask.Reset(ELocation::PBuffer0);
        UNIT_ASSERT_VALUES_EQUAL(1, mask.Count());
        UNIT_ASSERT(!mask.Get(ELocation::PBuffer0));
        UNIT_ASSERT(mask.Get(ELocation::DDisk1));
        // Reset the remaining location
        mask.Reset(ELocation::DDisk1);
        UNIT_ASSERT(mask.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0, mask.Count());
    }

    Y_UNIT_TEST(TestLocationMaskHasAndOnlyMethods)
    {
        // Test with mixed locations
        auto mixedMask =
            TLocationMask::MakePBuffer(true, false, false, false, false);
        mixedMask.Set(ELocation::DDisk0);
        UNIT_ASSERT(mixedMask.HasDDisk());
        UNIT_ASSERT(mixedMask.HasPBuffer());
        UNIT_ASSERT(!mixedMask.OnlyDDisk());
        UNIT_ASSERT(!mixedMask.OnlyPBuffer());
        // Test with only DDisk
        auto ddiskMask = TLocationMask::MakePrimaryDDisks();
        UNIT_ASSERT(ddiskMask.HasDDisk());
        UNIT_ASSERT(!ddiskMask.HasPBuffer());
        UNIT_ASSERT(ddiskMask.OnlyDDisk());
        UNIT_ASSERT(!ddiskMask.OnlyPBuffer());
        // Test with only PBuffer
        auto pbufferMask = TLocationMask::MakePrimaryPBuffers();
        UNIT_ASSERT(!pbufferMask.HasDDisk());
        UNIT_ASSERT(pbufferMask.HasPBuffer());
        UNIT_ASSERT(!pbufferMask.OnlyDDisk());
        UNIT_ASSERT(pbufferMask.OnlyPBuffer());
    }

    Y_UNIT_TEST(TestLocationMaskGetLocation)
    {
        auto mask = TLocationMask::MakePBuffer(true, false, true, false, true);
        mask.Set(ELocation::DDisk1);

        // Test getting locations by try
        {
            auto loc0 = mask.GetLocation(0);
            UNIT_ASSERT(loc0.has_value());
            UNIT_ASSERT_EQUAL(ELocation::DDisk1, *loc0);
        }

        {
            auto loc1 = mask.GetLocation(1);
            UNIT_ASSERT(loc1.has_value());
            UNIT_ASSERT_EQUAL(ELocation::PBuffer0, *loc1);
        }
    }

    Y_UNIT_TEST(TestLocationMaskEquality)
    {
        auto mask1 =
            TLocationMask::MakePBuffer(true, true, false, false, false);
        auto mask2 =
            TLocationMask::MakePBuffer(true, true, false, false, false);
        auto mask3 =
            TLocationMask::MakePBuffer(true, false, false, false, false);
        UNIT_ASSERT(mask1 == mask2);
        UNIT_ASSERT(!(mask1 == mask3));
        // Test with empty masks
        auto empty1 = TLocationMask::MakeEmpty();
        auto empty2 = TLocationMask::MakeEmpty();
        UNIT_ASSERT(empty1 == empty2);
    }

    Y_UNIT_TEST(TestLocationMaskPrint)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            "[D+....P.....]",
            TLocationMask::MakeDDisk(true, false, false, false, false).Print());
        UNIT_ASSERT_VALUES_EQUAL(
            "[D.+...P.....]",
            TLocationMask::MakeDDisk(false, true, false, false, false).Print());
        UNIT_ASSERT_VALUES_EQUAL(
            "[D..+..P.....]",
            TLocationMask::MakeDDisk(false, false, true, false, false).Print());
        UNIT_ASSERT_VALUES_EQUAL(
            "[D...*.P.....]",
            TLocationMask::MakeDDisk(false, false, false, true, false).Print());
        UNIT_ASSERT_VALUES_EQUAL(
            "[D....*P.....]",
            TLocationMask::MakeDDisk(false, false, false, false, true).Print());

        UNIT_ASSERT_VALUES_EQUAL(
            "[D.....P+....]",
            TLocationMask::MakePBuffer(true, false, false, false, false)
                .Print());
        UNIT_ASSERT_VALUES_EQUAL(
            "[D.....P.+...]",
            TLocationMask::MakePBuffer(false, true, false, false, false)
                .Print());
        UNIT_ASSERT_VALUES_EQUAL(
            "[D.....P..+..]",
            TLocationMask::MakePBuffer(false, false, true, false, false)
                .Print());
        UNIT_ASSERT_VALUES_EQUAL(
            "[D.....P...*.]",
            TLocationMask::MakePBuffer(false, false, false, true, false)
                .Print());
        UNIT_ASSERT_VALUES_EQUAL(
            "[D.....P....*]",
            TLocationMask::MakePBuffer(false, false, false, false, true)
                .Print());
    }

    Y_UNIT_TEST(TestAllLocationsArray)
    {
        UNIT_ASSERT_VALUES_EQUAL(10, AllLocations.size());
        // Verify all expected locations are present
        bool foundDDisk0 = false;
        bool foundDDisk1 = false;
        bool foundDDisk2 = false;
        bool foundHODDisk0 = false;
        bool foundHODDisk1 = false;
        bool foundPBuffer0 = false;
        bool foundPBuffer1 = false;
        bool foundPBuffer2 = false;
        bool foundHOPBuffer0 = false;
        bool foundHOPBuffer1 = false;
        for (auto location: AllLocations) {
            switch (location) {
                case ELocation::DDisk0:
                    foundDDisk0 = true;
                    break;
                case ELocation::DDisk1:
                    foundDDisk1 = true;
                    break;
                case ELocation::DDisk2:
                    foundDDisk2 = true;
                    break;
                case ELocation::HODDisk0:
                    foundHODDisk0 = true;
                    break;
                case ELocation::HODDisk1:
                    foundHODDisk1 = true;
                    break;
                case ELocation::PBuffer0:
                    foundPBuffer0 = true;
                    break;
                case ELocation::PBuffer1:
                    foundPBuffer1 = true;
                    break;
                case ELocation::PBuffer2:
                    foundPBuffer2 = true;
                    break;
                case ELocation::HOPBuffer0:
                    foundHOPBuffer0 = true;
                    break;
                case ELocation::HOPBuffer1:
                    foundHOPBuffer1 = true;
                    break;
                case ELocation::Unknown:
                    UNIT_FAIL("Unknown location should not be in AllLocations");
                    break;
            }
        }
        UNIT_ASSERT(foundDDisk0);
        UNIT_ASSERT(foundDDisk1);
        UNIT_ASSERT(foundDDisk2);
        UNIT_ASSERT(foundHODDisk0);
        UNIT_ASSERT(foundHODDisk1);
        UNIT_ASSERT(foundPBuffer0);
        UNIT_ASSERT(foundPBuffer1);
        UNIT_ASSERT(foundPBuffer2);
        UNIT_ASSERT(foundHOPBuffer0);
        UNIT_ASSERT(foundHOPBuffer1);
    }

    Y_UNIT_TEST(TestTranslateDDiskToPBuffer)
    {
        // Test translation from DDisk to PBuffer
        UNIT_ASSERT_EQUAL(
            ELocation::PBuffer0,
            TranslateDDiskToPBuffer(ELocation::DDisk0));
        UNIT_ASSERT_EQUAL(
            ELocation::PBuffer1,
            TranslateDDiskToPBuffer(ELocation::DDisk1));
        UNIT_ASSERT_EQUAL(
            ELocation::PBuffer2,
            TranslateDDiskToPBuffer(ELocation::DDisk2));
        UNIT_ASSERT_EQUAL(
            ELocation::HOPBuffer0,
            TranslateDDiskToPBuffer(ELocation::HODDisk0));
        UNIT_ASSERT_EQUAL(
            ELocation::HOPBuffer1,
            TranslateDDiskToPBuffer(ELocation::HODDisk1));
    }

    Y_UNIT_TEST(TestTranslatePBufferToDDisk)
    {
        // Test translation from PBuffer to DDisk
        UNIT_ASSERT_EQUAL(
            ELocation::DDisk0,
            TranslatePBufferToDDisk(ELocation::PBuffer0));
        UNIT_ASSERT_EQUAL(
            ELocation::DDisk1,
            TranslatePBufferToDDisk(ELocation::PBuffer1));
        UNIT_ASSERT_EQUAL(
            ELocation::DDisk2,
            TranslatePBufferToDDisk(ELocation::PBuffer2));
        UNIT_ASSERT_EQUAL(
            ELocation::HODDisk0,
            TranslatePBufferToDDisk(ELocation::HOPBuffer0));
        UNIT_ASSERT_EQUAL(
            ELocation::HODDisk1,
            TranslatePBufferToDDisk(ELocation::HOPBuffer1));
    }

    Y_UNIT_TEST(TestIteratorOverEmptyMask)
    {
        TLocationMask mask;
        TSet<ELocation> locations;
        for (ELocation location: mask) {
            locations.insert(location);
        }
        UNIT_ASSERT_EQUAL(0, locations.size());
    }

    Y_UNIT_TEST(TestIteratorWithLast)
    {
        TLocationMask mask;
        mask.Set(ELocation::HODDisk1);
        TSet<ELocation> locations;
        for (ELocation location: mask) {
            locations.insert(location);
        }
        UNIT_ASSERT_EQUAL(1, locations.size());
        UNIT_ASSERT(locations.contains(ELocation::HODDisk1));
    }

    Y_UNIT_TEST(TestIterator)
    {
        TLocationMask mask =
            TLocationMask::MakeDDisk(true, false, true, false, true);
        TSet<ELocation> locations;
        for (ELocation location: mask) {
            locations.insert(location);
        }
        UNIT_ASSERT_EQUAL(3, locations.size());
        UNIT_ASSERT(locations.contains(ELocation::DDisk0));
        UNIT_ASSERT(locations.contains(ELocation::DDisk2));
        UNIT_ASSERT(locations.contains(ELocation::HODDisk1));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
