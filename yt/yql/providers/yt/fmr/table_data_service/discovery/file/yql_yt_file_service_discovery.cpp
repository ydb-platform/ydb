#include "yql_yt_file_service_discovery.h"
#include <util/stream/file.h>
#include <util/string/split.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

class TFileTableDataServiceDiscovery: public ITableDataServiceDiscovery {
public:
    TFileTableDataServiceDiscovery(const TFileTableDataServiceDiscoverySettings& settings): WorkersPath_(settings.Path) {
        Start();
    }

    void Start() override {
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
        HasStarted_ = true;
    }

    void Stop() override {}

    ui64 GetHostCount() const override {
        CheckHasStarted();
        return WorkerConnections_.size();
    }

    const std::vector<TTableDataServiceServerConnection>& GetHosts() const override {
        CheckHasStarted();
        return WorkerConnections_;
    }

private:
    const TString WorkersPath_;
    std::vector<TTableDataServiceServerConnection> WorkerConnections_;
    bool HasStarted_ = false;

    void CheckHasStarted() const {
        if (!HasStarted_) {
            ythrow yexception() << "File service discovery has not started yet";
        }
    }
};

}

ITableDataServiceDiscovery::TPtr MakeFileTableDataServiceDiscovery(const TFileTableDataServiceDiscoverySettings& settings) {
    return MakeIntrusive<TFileTableDataServiceDiscovery>(settings);
}

} // namespace NYql::NFmr
