#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <yql/essentials/utils/runnable.h>

namespace NYql::NFmr {

struct TTableDataServiceServerConnection {
    TString Host;
    ui16 Port;
};

class ITableDataServiceDiscovery: public IRunnable {
public:
    using TPtr = TIntrusivePtr<ITableDataServiceDiscovery>;

    virtual ~ITableDataServiceDiscovery() = default;

    virtual ui64 GetHostCount() const = 0;

    virtual const std::vector<TTableDataServiceServerConnection>& GetHosts() const = 0;
};

} // namespace NYql::NFmr
