#include "audit_log_helpers.h"

namespace NAuditHelpers {
    TString MaybeRemoveSuffix(const TString& token) {
        const TString suffix = "@as";
        return token.EndsWith(suffix)
            ? token.substr(0, token.length() - suffix.length())
            : token;
    }

    ::yandex::cloud::events::Authentication::SubjectType GetCloudSubjectType(const TString& subjectType) {
        static const TMap<TString, ::yandex::cloud::events::Authentication::SubjectType> Types {
            {"service_account", ::yandex::cloud::events::Authentication::SERVICE_ACCOUNT},
            {"federated_account", ::yandex::cloud::events::Authentication::FEDERATED_USER_ACCOUNT},
            {"user_account", ::yandex::cloud::events::Authentication::YANDEX_PASSPORT_USER_ACCOUNT},
        };
        return Types.Value(subjectType, ::yandex::cloud::events::Authentication::SUBJECT_TYPE_UNSPECIFIED);
    }    
} // namespace NAuditHelpers
