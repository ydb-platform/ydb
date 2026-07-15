#include <ydb/library/yaml_config/public/storage_defaults.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NYamlConfig {
    namespace {

        Y_UNIT_TEST_SUITE(StorageDefaults) {
            Y_UNIT_TEST(ParsesNamesCaseInsensitively) {
                UNIT_ASSERT(TryParseDefaultDiskType("rot") == EDefaultDiskType::Rot);
                UNIT_ASSERT(TryParseDefaultDiskType("SSD") == EDefaultDiskType::Ssd);
                UNIT_ASSERT(TryParseDefaultDiskType("NvMe") == EDefaultDiskType::Nvme);
                UNIT_ASSERT(!TryParseDefaultDiskType("HDD"));

                UNIT_ASSERT(TryParseFailDomainType("rack") == EFailDomainType::Rack);
                UNIT_ASSERT(TryParseFailDomainType("Body") == EFailDomainType::Body);
                UNIT_ASSERT(TryParseFailDomainType("DISK") == EFailDomainType::Disk);
                UNIT_ASSERT(!TryParseFailDomainType("host"));
            }

            Y_UNIT_TEST(ProvidesDiskDefaults) {
                UNIT_ASSERT_VALUES_EQUAL(DefaultDiskTypeName(EDefaultDiskType::Rot), "ROT");
                UNIT_ASSERT_VALUES_EQUAL(DefaultStoragePoolKind(EDefaultDiskType::Ssd), "ssd");
                UNIT_ASSERT_VALUES_EQUAL(DefaultPDiskCategory(EDefaultDiskType::Rot), 0);
                UNIT_ASSERT_VALUES_EQUAL(DefaultPDiskCategory(EDefaultDiskType::Ssd), 1);
                UNIT_ASSERT_VALUES_EQUAL(
                    DefaultPDiskCategory(EDefaultDiskType::Nvme),
                    144115188075855873ULL);
            }

            Y_UNIT_TEST(ProvidesFailDomainDefaults) {
                UNIT_ASSERT_VALUES_EQUAL(FailDomainTypeName(EFailDomainType::Rack), "rack");
                UNIT_ASSERT_VALUES_EQUAL(FailDomainTypeName(EFailDomainType::Body), "body");
                UNIT_ASSERT_VALUES_EQUAL(FailDomainTypeName(EFailDomainType::Disk), "disk");

                UNIT_ASSERT((GetFailDomainGeometryRange(EFailDomainType::Rack) ==
                             TFailDomainGeometryRange{10, 20, 10, 40}));
                UNIT_ASSERT((GetFailDomainGeometryRange(EFailDomainType::Body) ==
                             TFailDomainGeometryRange{10, 20, 10, 50}));
                UNIT_ASSERT((GetFailDomainGeometryRange(EFailDomainType::Disk) ==
                             TFailDomainGeometryRange{10, 20, 10, 256}));

                UNIT_ASSERT(TryInferFailDomainType({10, 20, 10, 50}) == EFailDomainType::Body);
                UNIT_ASSERT(!TryInferFailDomainType({10, 20, 10, 60}));
            }
        } // Y_UNIT_TEST_SUITE(StorageDefaults)

    } // anonymous namespace
} // namespace NKikimr::NYamlConfig
