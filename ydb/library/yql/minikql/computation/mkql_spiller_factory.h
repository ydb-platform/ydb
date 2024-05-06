#pragma once

#include "mkql_spiller.h"

namespace NKikimr::NMiniKQL {

class ISpillerFactory : private TNonCopyable
{
public:
    virtual ISpiller::TPtr CreateSpiller() const = 0;

    virtual ~ISpillerFactory(){}
};

}//namespace NKikimr::NMiniKQL
