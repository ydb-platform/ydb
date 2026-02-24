#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/tvm/interface/yql_yt_fmr_tvm_interface.h>
#include <yql/essentials/utils/runnable.h>

namespace NYql::NFmr {

using IFmrServer = IRunnable;

struct TTableDataServiceServerSettings {
    TString Host = "localhost";
    ui16 Port = 7000;
    std::vector<TTvmId> AllowedSourceTvmIds;
};

IFmrServer::TPtr MakeTableDataServiceServer(
    ILocalTableDataService::TPtr tableDataSerivce,
    const TTableDataServiceServerSettings& settings,
    IFmrTvmClient::TPtr tvmClient = nullptr
);

} // namespace NYql::NFmr
