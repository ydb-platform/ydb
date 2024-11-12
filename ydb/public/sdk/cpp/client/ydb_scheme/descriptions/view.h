#pragma once

#include <util/generic/string.h>

namespace Ydb::View {
    class ViewDescription;
}

namespace NYdb::NScheme {

class TViewDescription {
public:
    explicit TViewDescription(const Ydb::View::ViewDescription& desc);

    const TString& GetQueryText() const;

private:
    TString QueryText;
};

}
