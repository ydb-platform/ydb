#include "object_storage.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <ydb/core/protos/external_sources.pb.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(ObjectStorageTest) {
    Y_UNIT_TEST(SuccessValidation) {
        auto source = NExternalSource::CreateObjectStorageExternalSource({}, nullptr, 1000, nullptr, false, false);
        NKikimrExternalSources::TSchema schema;
        NKikimrExternalSources::TGeneral general;
        UNIT_ASSERT_NO_EXCEPTION(source->Pack(schema, general));
    }

    Y_UNIT_TEST(FailedCreate) {
        auto source = NExternalSource::CreateObjectStorageExternalSource({}, nullptr, 1000, nullptr, false, false);
        NKikimrExternalSources::TSchema schema;
        NKikimrExternalSources::TGeneral general;
        general.mutable_attributes()->insert({"a", "b"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "Unknown attribute a");
    }

    Y_UNIT_TEST(FailedValidation) {
        auto source = NExternalSource::CreateObjectStorageExternalSource({}, nullptr, 1000, nullptr, false, false);
        NKikimrExternalSources::TSchema schema;
        NKikimrExternalSources::TGeneral general;
        general.mutable_attributes()->insert({"projection.h", "b"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "Partition by must always be specified");
    }

    Y_UNIT_TEST(WildcardsValidation) {
        auto source = NExternalSource::CreateObjectStorageExternalSource({}, nullptr, 1000, nullptr, false);
        NKikimrExternalSources::TSchema schema;

        {  // location
            NKikimrExternalSources::TGeneral general;
            general.set_location("{");
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "Location '{' contains invalid wildcard:");
        }

        {  // file pattern
            NKikimrExternalSources::TGeneral general;
            general.mutable_attributes()->insert({"file_pattern", "{"});
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "File pattern '{' contains invalid wildcard:");
            general.set_location("/test_file");
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "Path pattern cannot be used with file_pattern");
        }

        {  // partitioned by
            NKikimrExternalSources::TGeneral general;
            general.set_location("*");
            general.mutable_attributes()->insert({"partitioned_by", "[year]"});
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "Location '*' contains wildcards");
        }
    }
}

} // NKikimr
