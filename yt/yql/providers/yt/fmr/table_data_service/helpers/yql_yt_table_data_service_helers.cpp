#include "yql_yt_table_data_service_helpers.h"

namespace NYql::NFmr {

TString WriteHostsToFile(TTempFileHandle& file, ui64 WorkersNum, const std::vector<TTableDataServiceServerConnection>& connections) {
    TString tempFileName = file.Name();
    TFileOutput writeHosts(file.Name());
    for (size_t i = 0; i < WorkersNum; ++i) {
        writeHosts.Write(TStringBuilder() << connections[i].Host << ":" << connections[i].Port << "\n");
    }
    return tempFileName;
}

IFmrServer::TPtr MakeTableDataServiceServer(ui16 port) {
    TTableDataServiceServerSettings tableDataServiceWorkerSettings{.WorkerId = 0, .WorkersNum = 1, .Port = port};
    auto tableDataServiceServer = MakeTableDataServiceServer(MakeLocalTableDataService(), tableDataServiceWorkerSettings);
    tableDataServiceServer->Start();
    return tableDataServiceServer;
}

ITableDataService::TPtr MakeTableDataServiceClient(ui16 port) {
    TTempFileHandle hostsFile{};
    std::vector<TTableDataServiceServerConnection> connections{{.Host = "localhost", .Port = port}};
    ui64 workersNum = 1;
    auto path = WriteHostsToFile(hostsFile, workersNum, connections);

    auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path=path});
    return MakeTableDataServiceClient(tableDataServiceDiscovery);
}

} // namespace NYql::NFmr
