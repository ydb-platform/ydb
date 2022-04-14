#include "roles_fetcher.h"

#include <library/cpp/tvmauth/client/misc/roles/parser.h>

#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/string/builder.h>
#include <util/string/join.h>

namespace NTvmAuth::NTvmTool {
    TRolesFetcher::TRolesFetcher(const TRolesFetcherSettings& settings, TLoggerPtr logger)
        : Settings_(settings)
        , Logger_(std::move(logger))
    {
    }

    bool TRolesFetcher::IsTimeToUpdate(TDuration sinceUpdate) const {
        return Settings_.UpdatePeriod < sinceUpdate;
    }

    bool TRolesFetcher::ShouldWarn(TDuration sinceUpdate) const {
        return Settings_.WarnPeriod < sinceUpdate;
    }

    bool TRolesFetcher::AreRolesOk() const {
        return bool(GetCurrentRoles());
    }

    NUtils::TFetchResult TRolesFetcher::FetchActualRoles(const TKeepAliveHttpClient::THeaders& authHeader,
                                                         TKeepAliveHttpClient& client) const {
        const TRequest req = CreateRequest(authHeader);

        TStringStream out;
        THttpHeaders outHeaders;

        TKeepAliveHttpClient::THttpCode code = client.DoGet(
            req.Url,
            &out,
            req.Headers,
            &outHeaders);

        return {code, std::move(outHeaders), "/v2/roles", out.Str(), {}};
    }

    void TRolesFetcher::Update(NUtils::TFetchResult&& fetchResult) {
        if (fetchResult.Code == HTTP_NOT_MODIFIED) {
            Y_ENSURE(CurrentRoles_.Get(),
                     "tvmtool did not return any roles because current roles are actual,"
                     " but there are no roles in memory - this should never happen");
            return;
        }

        Y_ENSURE(fetchResult.Code == HTTP_OK,
                 "Unexpected code from tvmtool: " << fetchResult.Code << ". " << fetchResult.Response);

        CurrentRoles_.Set(NRoles::TParser::Parse(std::make_shared<TString>(std::move(fetchResult.Response))));

        Logger_->Debug(
            TStringBuilder() << "Succeed to update roles with revision "
                             << CurrentRoles_.Get()->GetMeta().Revision);
    }

    NTvmAuth::NRoles::TRolesPtr TRolesFetcher::GetCurrentRoles() const {
        return CurrentRoles_.Get();
    }

    TRolesFetcher::TRequest TRolesFetcher::CreateRequest(const TKeepAliveHttpClient::THeaders& authHeader) const {
        TRequest request{
            .Url = "/v2/roles?self=" + CGIEscapeRet(Settings_.SelfAlias),
            .Headers = authHeader,
        };

        NRoles::TRolesPtr roles = CurrentRoles_.Get();
        if (roles) {
            request.Headers.emplace(IfNoneMatch_, Join("", "\"", roles->GetMeta().Revision, "\""));
        }

        return request;
    }
}
