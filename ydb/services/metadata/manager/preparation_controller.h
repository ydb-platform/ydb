#pragma once
#include "common.h"

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NModifications {

template <class TObject>
class TEvAlterPreparationFinished: public TEventLocal<TEvAlterPreparationFinished<TObject>, EvAlterPreparationFinished> {
private:
    YDB_READONLY_DEF(std::vector<TObject>, Objects);
public:
    TEvAlterPreparationFinished(std::vector<TObject>&& objects)
        : Objects(std::move(objects))
    {

    }
};

class TEvAlterPreparationProblem: public TEventLocal<TEvAlterPreparationProblem, EvAlterPreparationProblem> {
private:
    YDB_ACCESSOR_DEF(TString, ErrorMessage);
public:
    TEvAlterPreparationProblem(const TString& errorMessage)
        : ErrorMessage(errorMessage) {

    }
};

}
