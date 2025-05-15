#include <ydb/public/api/client/yc_public/events/common.pb.h>

#include <util/generic/string.h>
#include <util/generic/map.h>

namespace NAuditHelpers {

TString MaybeRemoveSuffix(const TString& token);
::yandex::cloud::events::Authentication::SubjectType GetCloudSubjectType(const TString& subjectType);


} // namespace NAuditHelpers
