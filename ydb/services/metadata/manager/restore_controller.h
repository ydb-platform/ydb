#pragma once
#include "common.h"

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NModifications {

template <class TObject>
class IRestoreObjectsController {
public:
    using TPtr = std::shared_ptr<IRestoreObjectsController>;
    virtual ~IRestoreObjectsController() = default;

    virtual void OnRestoringFinished(std::vector<TObject>&& objects, const TString& transactionId) = 0;
    virtual void OnRestoringProblem(const TString& errorMessage) = 0;
};

template <class TObject>
class TEvRestoreFinished: public TEventLocal<TEvRestoreFinished<TObject>, EvRestoreFinished> {
private:
    YDB_ACCESSOR_DEF(std::vector<TObject>, Objects);
    YDB_READONLY_DEF(TString, TransactionId);
public:
    TEvRestoreFinished(std::vector<TObject>&& objects, const TString& transactionId)
        : Objects(std::move(objects))
        , TransactionId(transactionId)
    {

    }
};

class TEvRestoreProblem: public TEventLocal<TEvRestoreProblem, EvRestoreProblem> {
private:
    YDB_ACCESSOR_DEF(TString, ErrorMessage);
public:
    TEvRestoreProblem(const TString& errorMessage)
        : ErrorMessage(errorMessage) {

    }
};

}
