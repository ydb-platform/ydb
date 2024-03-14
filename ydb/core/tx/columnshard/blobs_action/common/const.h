#pragma once

namespace NKikimr::NOlap::NBlobOperations {

class TGlobal {
public:
    static const inline TString DefaultStorageId = "__DEFAULT";
    static const inline TString MemoryStorageId = "__MEMORY";
};

}