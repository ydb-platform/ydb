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

    Y_UNIT_TEST(FailedJsonListValidation) {
        static auto invalidTypes = {
            Ydb::Type::DATE,
            Ydb::Type::DATETIME,
            Ydb::Type::TIMESTAMP,
            Ydb::Type::INTERVAL,
            Ydb::Type::DATE32,
            Ydb::Type::DATETIME64,
            Ydb::Type::TIMESTAMP64,
            Ydb::Type::INTERVAL64,
            Ydb::Type::TZ_DATE,
            Ydb::Type::TZ_DATETIME,
            Ydb::Type::TZ_TIMESTAMP,
        };
        auto source = NExternalSource::CreateObjectStorageExternalSource({}, nullptr, 1000, nullptr, false, false);
        NKikimrExternalSources::TSchema schema;
        for (const auto typeId : invalidTypes) {
            auto newColumn = schema.add_column();
            newColumn->mutable_type()->set_type_id(typeId);
        }
        NKikimrExternalSources::TGeneral general;
        general.mutable_attributes()->insert({"format", "json_list"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "Date, Timestamp and Interval types are not allowed in json_list format");
    }

    Y_UNIT_TEST(FailedOptionalTypeValidation) {
        auto source = NExternalSource::CreateObjectStorageExternalSource({}, nullptr, 1000, nullptr, false, false);
        NKikimrExternalSources::TSchema schema;
        NKikimrExternalSources::TGeneral general;
        auto newColumn = schema.add_column();
        newColumn->mutable_type()->mutable_optional_type()->mutable_item()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::INT32);
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "Double optional types are not supported");
    }

    Y_UNIT_TEST(WildcardsValidation) {
        auto source = NExternalSource::CreateObjectStorageExternalSource({}, nullptr, 1000, nullptr, false, false);
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

    Y_UNIT_TEST(FailedPartitionedByValidation) {
        const auto source = NExternalSource::CreateObjectStorageExternalSource({}, nullptr, 1000, nullptr, false, false);
        NKikimrExternalSources::TSchema schema;
        {
            NKikimrExternalSources::TGeneral general;
            general.mutable_attributes()->emplace("partitioned_by", "{\"year\": \"2025\"}");
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "partitioned_by must be an array of column names");
        }
        {
            NKikimrExternalSources::TGeneral general;
            general.mutable_attributes()->emplace("partitioned_by", "[{\"year\": \"2025\"}]");
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "partitioned_by must be an array of strings");
        }
        {
            NKikimrExternalSources::TGeneral general;
            general.mutable_attributes()->emplace("partitioned_by", "[{");
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "Failed to parse partitioned_by:");
        }
    }
}

} // NKikimr
