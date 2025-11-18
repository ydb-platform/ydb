#include <ydb/library/actors/interconnect/ut/lib/port_manager.h>
#include <cstdlib>

namespace NInterconnectTest {

class TSimplePortManager : public IPortManager {
public:
    TSimplePortManager() = default;
    ui16 GetPort() noexcept override {
        static ui16 port = 1024 + rand() % 1000;
        return port++;
    }
};

IPortManager::TPtr CreatePortmanager() {
    return MakeIntrusive<TSimplePortManager>();
}

}
