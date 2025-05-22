#include "constructor.h"

namespace NKikimr::NOlap::NIndexes {

TConclusionStatus TSkipBitmapIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    if (!jsonInfo.Has("bits_storage_type")) {
        BitsStorageConstructor = IBitsStorageConstructor::GetDefault();
    } else if (!jsonInfo["bits_storage_type"].IsString()) {
        return TConclusionStatus::Fail("bits_storage_type have to been string");
    } else {
        const TString name = jsonInfo["bits_storage_type"].GetString();
        BitsStorageConstructor = std::shared_ptr<IBitsStorageConstructor>(IBitsStorageConstructor::TFactory::Construct(name));
        if (!BitsStorageConstructor) {
            return TConclusionStatus::Fail("bits_storage_type '" + name + "' is unknown for bitset storage factory: " +
                                           JoinSeq(",", IBitsStorageConstructor::TFactory::GetRegisteredKeys()));
        }
    }
    return TBase::DoDeserializeFromJson(jsonInfo);
}

}   // namespace NKikimr::NOlap::NIndexes
