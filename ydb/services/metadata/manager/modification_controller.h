#pragma once
#include "common.h"
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadataManager {

class IModificationObjectsController {
public:
    using TPtr = std::shared_ptr<IModificationObjectsController>;
    virtual ~IModificationObjectsController() = default;
    virtual void ModificationProblem(const TString& errorMessage) = 0;
    virtual void ModificationFinished() = 0;
};

class TEvModificationFinished: public TEventLocal<TEvModificationFinished, EvModificationFinished> {
public:
};

class TEvModificationProblem: public TEventLocal<TEvModificationProblem, EvModificationProblem> {
private:
    YDB_ACCESSOR_DEF(TString, ErrorMessage);
public:
    TEvModificationProblem(const TString& errorMessage)
        : ErrorMessage(errorMessage) {

    }
};

}
