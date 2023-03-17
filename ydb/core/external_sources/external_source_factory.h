#pragma once

#include "external_source.h"

namespace NKikimr::NExternalSource {

struct IExternalSourceFactory : public TThrRefBase {
    using TPtr = TIntrusivePtr<IExternalSourceFactory>;

    virtual IExternalSource::TPtr GetOrCreate(const TString& type) const = 0;
};

IExternalSourceFactory::TPtr CreateExternalSourceFactory();

}
