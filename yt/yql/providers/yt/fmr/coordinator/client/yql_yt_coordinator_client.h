#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/fmr/tvm/interface/yql_yt_fmr_tvm_interface.h>

namespace NYql::NFmr {

struct TFmrCoordinatorClientSettings {
    ui16 Port;
    TString Host = "localhost";
    TTvmId DestinationTvmId = 0;
};

IFmrCoordinator::TPtr MakeFmrCoordinatorClient(
    const TFmrCoordinatorClientSettings& settings,
    IFmrTvmClient::TPtr tvmClient = nullptr
);

} // namespace NYql::NFmr
