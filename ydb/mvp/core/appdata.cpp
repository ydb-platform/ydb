#include "appdata.h"
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

TMVPAppData::~TMVPAppData() {
    if (GRpcClientLow) {
        GRpcClientLow->Stop(true);
    }
}
