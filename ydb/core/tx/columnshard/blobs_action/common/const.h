#pragma once
#include <util/generic/string.h>

namespace NKikimr::NOlap::NBlobOperations {

class TGlobal {
public:
    static const inline TString DefaultStorageId = "__DEFAULT";
    static const inline TString MemoryStorageId = "__MEMORY";
    static const inline TString LocalMetadataStorageId = "__LOCAL_METADATA";
};

inline bool IsValidColumnStorageId(const TString& storageId) {
    return !storageId || storageId == TGlobal::DefaultStorageId || storageId == TGlobal::MemoryStorageId ||
           storageId == TGlobal::LocalMetadataStorageId;
}

}   // namespace NKikimr::NOlap::NBlobOperations
