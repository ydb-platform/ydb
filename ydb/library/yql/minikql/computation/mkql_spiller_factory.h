#pragma once

#include "mkql_spiller.h"

namespace NKikimr::NMiniKQL {

class ISpillerFactory : private TNonCopyable
{
public:
    virtual ISpiller::TPtr CreateSpiller(ISpiller::TErrorCallback errorCallback) = 0;

    virtual ~ISpillerFactory(){}
};

}//namespace NKikimr::NMiniKQL
