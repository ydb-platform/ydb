#include "yql_yt_fmr_tvm_impl.h"

namespace NYql::NFmr {

IFmrTvmClient::TPtr MakeFmrTvmClient(const TFmrTvmToolSettings&) {
    return nullptr;
}

IFmrTvmClient::TPtr MakeFmrTvmClient(const TFmrTvmApiSettings&) {
    return nullptr;
}

} // namespace NYql::NFmr
