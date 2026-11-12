#include <ydb/library/actors/interconnect/ut/lib/port_manager.h>
#include <library/cpp/testing/unittest/tests_data.h>

namespace NInterconnectTest {

class TUtilsPortManager : public IPortManager {
public:
    TUtilsPortManager() = default;
    ui16 GetPort() override {
        return PortManager.GetPort();
    }
private:
    TPortManager PortManager;
};

IPortManager::TPtr CreatePortmanager() {
    return MakeIntrusive<TUtilsPortManager>();
}

}