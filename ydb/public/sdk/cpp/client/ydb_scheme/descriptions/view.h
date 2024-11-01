#pragma once

#include <util/generic/string.h>

namespace Ydb::Scheme {
    class ViewDescription;
}

namespace NYdb::NScheme {

class TViewDescription {
public:
    explicit TViewDescription(const Ydb::Scheme::ViewDescription& desc);

    const TString& GetQueryText() const;

private:
    TString QueryText;
};

}
