#pragma once

#include <util/generic/ptr.h>

namespace NInterconnectTest {

class IPortManager : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IPortManager>;
    virtual ~IPortManager() = default;
    virtual ui16 GetPort() = 0;
};

IPortManager::TPtr CreatePortmanager();
    
}
