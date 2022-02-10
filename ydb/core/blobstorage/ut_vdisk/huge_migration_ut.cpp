#include "defaults.h"
#include "gen_restarts.h"
#include <library/cpp/testing/unittest/registar.h>

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Test Suite for checking migration of HugeBlobMap, i.e. we change diapason of huge blobs and
// see how we can handle forward/backward migration
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(THugeMigration) {
    // Old Huge Map (before migragion)
    static void OldMap(NKikimr::TVDiskConfig *cfg) {
        cfg->MaxLogoBlobDataSize = 128u << 10u;
        cfg->MinHugeBlobInBytes = 64u << 10u;
        cfg->MilestoneHugeBlobInBytes = 64u << 10u;
    }

    // New Huge Map (after migragion)
    static void NewMap(NKikimr::TVDiskConfig *cfg) {
        cfg->MaxLogoBlobDataSize = 128u << 10u;
        cfg->MinHugeBlobInBytes = 32u << 10u;
        cfg->MilestoneHugeBlobInBytes = 64u << 10u;
    }

    void ExtendMap(ui32 msgSize) {
        auto vdiskWriteSetup = std::make_shared<TFastVDiskSetup>();
        vdiskWriteSetup->AddConfigModifier(OldMap);
        auto vdiskReadSetup = std::make_shared <TFastVDiskSetup>();
        vdiskReadSetup->AddConfigModifier(NewMap);
        TWriteRestartReadSettings settings(100, msgSize, HUGEB, vdiskWriteSetup, vdiskReadSetup);
        WriteRestartRead(settings, TIMEOUT);
    }

    Y_UNIT_TEST(ExtendMap_HugeBlobs) {
        // 1. Old Map: we write huge blobs
        // 2. Extend map and restart
        // 3. We successfully read these blobs
        ExtendMap(66u << 10u);
    }

    Y_UNIT_TEST(ExtendMap_SmallBlobsBecameHuge) {
        // 1. Old Map: we write small blobs
        // 2. Extend map and restart
        // 3. We successfully read these blobs (which became huge)
        ExtendMap(46u << 10u);
    }

    Y_UNIT_TEST(RollbackMap_HugeBlobs) {
        // 1. New Map: we write huge blobs (both for new and old maps)
        // 2. Rollback map and restart
        // 3. We successfully read these blobs
        auto vdiskWriteSetup = std::make_shared<TFastVDiskSetup>();
        vdiskWriteSetup->AddConfigModifier(NewMap);
        auto vdiskReadSetup = std::make_shared <TFastVDiskSetup>();
        vdiskReadSetup->AddConfigModifier(OldMap);
        TWriteRestartReadSettings settings(100, 66u << 10u, HUGEB, vdiskWriteSetup, vdiskReadSetup);
        WriteRestartRead(settings, TIMEOUT);
    }

    // We can't write RollbackMap_HugeBlobsBecameSmall => we got error and this is correct
}

