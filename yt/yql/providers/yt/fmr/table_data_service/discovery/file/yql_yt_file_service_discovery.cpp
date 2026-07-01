#include "yql_yt_file_service_discovery.h"
#include <util/stream/file.h>
#include <util/string/split.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

class TFileTableDataServiceDiscovery: public ITableDataServiceDiscovery {
public:
    TFileTableDataServiceDiscovery(TFileTableDataServiceDiscoverySettings&& settings)
        : WorkersPath_(settings.Path)
    {
        TFileInput readHosts(WorkersPath_);
        TString currentRow;
        std::vector<TString> connection;
        std::vector<TTableDataServiceServerConnection> workerConnections;
        while (readHosts.ReadLine(currentRow)) {
            StringSplitter(currentRow).Split(':').Collect(&connection);
            YQL_ENSURE(connection.size() == 2);
            TString host = connection[0];
            ui16 port;
            if (!TryFromString<ui16>(connection[1], port)) {
                ythrow yexception() << " Failed to convert port " << connection[1] << " to ui16\n";
            }
            workerConnections.emplace_back(host, port);
        }
        WorkerConnections_ = workerConnections;
    }

    void Start() override {
    }

    void Stop() override {
    }

    ui64 GetHostCount() const override {
        return WorkerConnections_.size();
    }

    TTableDataServiceServerConnection GetHost(ui64 index) const override {
        Y_ENSURE(index < WorkerConnections_.size(),
            "TDS host index " << index << " is out of range [0, " << WorkerConnections_.size() << ")");
        return WorkerConnections_[index];
    }


private:
    const TString WorkersPath_;
    std::vector<TTableDataServiceServerConnection> WorkerConnections_;
};

}

ITableDataServiceDiscovery::TPtr MakeFileTableDataServiceDiscovery(TFileTableDataServiceDiscoverySettings settings) {
    return MakeIntrusive<TFileTableDataServiceDiscovery>(std::move(settings));
}

} // namespace NYql::NFmr
