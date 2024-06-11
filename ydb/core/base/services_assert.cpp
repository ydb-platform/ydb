#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/services/services.pb.h>

static_assert(static_cast<ui32>(NKikimrServices::EServiceKikimr_MIN) > static_cast<ui32>(NActorsServices::EServiceCommon_MAX), "KIKIMR SERVICES IDs SHOULD BE GREATER THAN COMMON ONES");
static_assert(NKikimrServices::TActivity::EType_ARRAYSIZE < 768, "ACTOR ACTIVITY TYPES MUST BE NOT VERY BIG TO BE ARRAY INDICES"); // If we would have many different actor activities, it is OK to increase this value.
