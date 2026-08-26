#include "resource_pool_classifier_settings.h"

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <ydb/library/aclib/aclib.h>


namespace NKikimr::NResourcePool {

//// TClassifierSettings

std::optional<TString> TClassifierSettings::Validate() const {
    if (!ResourcePool && !Action) {
        return TStringBuilder() << "Invalid resource pool classifier configuration, either resource pool or action must be specified";
    }
    if (ResourcePool && Action) {
        return TStringBuilder() << "Invalid resource pool classifier configuration, resource pool must not be used for Reject action";
    }
    if (!MemberName) {
        return std::nullopt;
    }
    NACLib::TUserToken token(*MemberName, TVector<NACLib::TSID>{});
    if (token.IsSystemUser()) {
        return TStringBuilder() << "Invalid resource pool classifier configuration, cannot create classifier for system user " << *MemberName;
    }
    return std::nullopt;
}

}  // namespace NKikimr::NResourcePool
