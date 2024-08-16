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
}

} // NKikimr
