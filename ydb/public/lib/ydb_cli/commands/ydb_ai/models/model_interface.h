#pragma once

#include <util/generic/fwd.h>

#include <memory>

namespace NYdb::NConsoleClient::NAi {

class IModel {
public:
    using TPtr = std::shared_ptr<IModel>;

    virtual TString Chat(const TString& input) = 0;
};

} // namespace NYdb::NConsoleClient::NAi
