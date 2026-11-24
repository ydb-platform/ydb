#include "layout_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NBsController;

namespace {

    struct TLayoutTest {
        const TGroupGeometryInfo Geometry;
        TGroupMapper::TGroupDefinition Group;
        std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition> PDisks;

        explicit TLayoutTest(ui32 numVDisksPerDomain = 1)
            : Geometry(CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, numVDisksPerDomain))
        {
            Geometry.ResizeGroup(Group);
            ui32 diskIndex = 0;
            TGroupMapper::Traverse(Group, [&](TVDiskIdShort vdiskId, TPDiskId) {
                const TPDiskId pdiskId(++diskIndex, 1);
                Group[vdiskId.FailRealm][vdiskId.FailDomain][vdiskId.VDisk] = pdiskId;
                PDisks.emplace(pdiskId, NLayoutChecker::TPDiskLayoutPosition(
                    {0}, {1u + vdiskId.FailRealm}, {10u + diskIndex}, {100u + diskIndex}));
            });
        }

        NLayoutChecker::TPDiskLayoutPosition& Position(ui32 realm, ui32 domain, ui32 vdisk = 0) {
            return PDisks.at(Group[realm][domain][vdisk]);
        }

        bool IsValid(bool allowMultipleRealmsOccupation) {
            TString error;
            return CheckLayoutByGroupDefinition(Group, PDisks, Geometry, allowMultipleRealmsOccupation, error);
        }
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(GroupLayoutValidation) {
    Y_UNIT_TEST(MultipleRealmsOccupationEnabledByDefault) {
        UNIT_ASSERT(Schema::State::AllowMultipleRealmsOccupation::Default);
    }

    Y_UNIT_TEST(ValidLayout) {
        TLayoutTest layout;
        UNIT_ASSERT(layout.IsValid(true));
        UNIT_ASSERT(layout.IsValid(false));
    }

    Y_UNIT_TEST(MultipleRealmsOccupation) {
        TLayoutTest layout;
        layout.Position(2, 2).Realm = {50};
        UNIT_ASSERT(layout.IsValid(true));
        UNIT_ASSERT(!layout.IsValid(false));
    }

    Y_UNIT_TEST(DifferentFailRealmsInOnePhysicalRealm) {
        TLayoutTest layout;
        layout.Position(2, 2).Realm = layout.Position(1, 0).Realm;
        UNIT_ASSERT(!layout.IsValid(true));
        UNIT_ASSERT(!layout.IsValid(false));
    }

    Y_UNIT_TEST(DifferentRealmGroups) {
        TLayoutTest layout;
        layout.Position(2, 2).RealmGroup = {50};
        UNIT_ASSERT(!layout.IsValid(true));
        UNIT_ASSERT(!layout.IsValid(false));
    }

    Y_UNIT_TEST(DifferentFailDomainsInOnePhysicalDomain) {
        TLayoutTest layout;
        layout.Position(0, 1).Domain = layout.Position(0, 0).Domain;
        UNIT_ASSERT(!layout.IsValid(true));
        UNIT_ASSERT(!layout.IsValid(false));
    }

    Y_UNIT_TEST(MultipleVDisksInFailDomainRequireDistinctPhysicalDomains) {
        TLayoutTest layout(2);
        UNIT_ASSERT(layout.IsValid(true));
        UNIT_ASSERT(layout.IsValid(false));

        layout.Position(0, 0, 1).Domain = layout.Position(0, 0, 0).Domain;
        UNIT_ASSERT(!layout.IsValid(true));
        UNIT_ASSERT(!layout.IsValid(false));
    }

    Y_UNIT_TEST(DuplicatePDisk) {
        TLayoutTest layout;
        layout.Group[0][1][0] = layout.Group[0][0][0];
        UNIT_ASSERT(!layout.IsValid(true));
    }

    Y_UNIT_TEST(EmptySlot) {
        TLayoutTest layout;
        layout.Group[0][0][0] = TPDiskId();
        UNIT_ASSERT(!layout.IsValid(true));
    }
}
