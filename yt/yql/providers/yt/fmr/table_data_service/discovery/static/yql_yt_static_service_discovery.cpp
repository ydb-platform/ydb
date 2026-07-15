#include "yql_yt_static_service_discovery.h"

#include <util/generic/yexception.h>

namespace NYql::NFmr {

namespace {

class TStaticTableDataServiceDiscovery: public ITableDataServiceDiscovery {
public:
    TStaticTableDataServiceDiscovery(TStaticTableDataServiceDiscoverySettings settings)
        : Settings_(std::move(settings))
    {
    }

    void Start() override {
    }

    void Stop() override {
    }

    ui64 GetHostCount() const override {
        return Settings_.Hosts.size();
    }

    TTableDataServiceServerConnection GetHost(ui64 index) const override {
        Y_ENSURE(index < Settings_.Hosts.size(),
            "TDS host index " << index << " is out of range [0, " << Settings_.Hosts.size() << ")");
        return Settings_.Hosts[index];
    }


private:
    const TStaticTableDataServiceDiscoverySettings Settings_;
};

}

ITableDataServiceDiscovery::TPtr MakeStaticTableDataServiceDiscovery(TStaticTableDataServiceDiscoverySettings settings) {
    return MakeIntrusive<TStaticTableDataServiceDiscovery>(std::move(settings));
}

} // namespace NYql::NFmr
