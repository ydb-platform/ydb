#include <yt/yql/providers/yt/fmr/yt_service/interface/yql_yt_yt_service.h>

namespace NYql::NFmr {

struct TFmrYtSerivceSettings {
};

IYtService::TPtr MakeFmrYtSerivce();

} // namespace NYql::NFmr
