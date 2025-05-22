#include "constructor.h"

namespace NKikimr::NOlap::NIndexes {

NKikimr::TConclusionStatus IIndexMetaConstructor::DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    if (jsonInfo.Has("storage_id")) {
        if (!jsonInfo["storage_id"].IsString()) {
            return TConclusionStatus::Fail("incorrect storage_id field in json index description (have to be string)");
        }
        StorageId = jsonInfo["storage_id"].GetStringSafe();
        if (!*StorageId) {
            return TConclusionStatus::Fail("storage_id cannot be empty string");
        } else if (*StorageId != "__LOCAL_METADATA" && *StorageId != "__DEFAULT") {
            return TConclusionStatus::Fail("storage_id have to been one of variant ['__LOCAL_METADATA', '__DEFAULT']");
        }
    }
    return DoDeserializeFromJson(jsonInfo);
}

}   // namespace NKikimr::NOlap::NIndexes