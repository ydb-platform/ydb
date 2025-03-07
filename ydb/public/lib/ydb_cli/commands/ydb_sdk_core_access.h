#pragma once

#include <ydb/public/sdk/cpp/src/client/types/core_facility/core_facility.h>
#include <memory>

namespace NYdb::inline Dev {
    class TDriver;
}

namespace NYdb {
namespace NConsoleClient {

class TDummyClient {
    class TImpl;
public:
    TDummyClient(NYdb::TDriver& driver);
    std::shared_ptr<NYdb::ICoreFacility> GetCoreFacility();
private:
    std::shared_ptr<TImpl> Impl_;
};

}
}
