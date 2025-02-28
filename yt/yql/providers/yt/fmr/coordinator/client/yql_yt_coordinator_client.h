#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>

namespace NYql::NFmr {

struct TFmrCoordinatorClientSettings {
    ui16 Port;
    TString Host = "localhost";
};

IFmrCoordinator::TPtr MakeFmrCoordinatorClient(const TFmrCoordinatorClientSettings& settings);

} // namespace NYql::NFmr
