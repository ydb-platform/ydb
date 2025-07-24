#include <yt/yql/providers/yt/fmr/table_data_service/client/impl/yql_yt_table_data_service_client_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/table_data_service/server/yql_yt_table_data_service_server.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>

namespace NYql::NFmr {

// Helper functions, mostly used for testing purposes.

TString WriteHostsToFile(TTempFileHandle& file, ui64 WorkersNum, const std::vector<TTableDataServiceServerConnection>& connections);

IFmrServer::TPtr MakeTableDataServiceServer(ui16 port);

ITableDataService::TPtr MakeTableDataServiceClient(ui16 port);

void SetupTableDataServiceDiscovery(TTempFileHandle& hostsFile, ui16 port);

} // namespace NYql::NFmr
