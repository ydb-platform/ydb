#pragma once

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NSecrets {

struct TSchemaSecretEvents {
    enum EEvents {
        EvDescribeSecretsResponse = EventSpaceBegin(TKikimrEvents::ES_SCHEMA_SECRETS),
        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SCHEMA_SECRETS), "expect EvEnd < EventSpaceEnd");
};

struct TEvDescribeSecretsResponse : public NActors::TEventLocal<TEvDescribeSecretsResponse, TSchemaSecretEvents::EvDescribeSecretsResponse> {
    struct TDescription {
        TDescription(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
            : Status(status)
            , Issues(std::move(issues))
        {}

        TDescription(const std::vector<TString>& secretValues)
            : SecretValues(secretValues)
            , Status(Ydb::StatusIds::SUCCESS)
        {}

        std::vector<TString> SecretValues;
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
    };

    TEvDescribeSecretsResponse(const TDescription& description)
        : Description(description)
    {}

    TDescription Description;
};

}  // namespace NKikimr::NSecrets
