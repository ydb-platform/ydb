#include "roles_fetcher.h"

#include <library/cpp/tvmauth/client/misc/disk_cache.h>
#include <library/cpp/tvmauth/client/misc/roles/decoder.h>
#include <library/cpp/tvmauth/client/misc/roles/parser.h>

#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/string/builder.h>
#include <util/string/join.h>

namespace NTvmAuth::NTvmApi {
    static TString CreatePath(const TString& dir, const TString& file) {
        return dir.EndsWith("/")
                   ? dir + file
                   : dir + "/" + file;
    }

    TRolesFetcher::TRolesFetcher(const TRolesFetcherSettings& settings, TLoggerPtr logger)
        : Settings_(settings)
        , Logger_(logger)
        , CacheFilePath_(CreatePath(Settings_.CacheDir, "roles"))
    {
        Client_ = std::make_unique<TKeepAliveHttpClient>(
            Settings_.TiroleHost,
            Settings_.TirolePort,
            Settings_.Timeout,
            Settings_.Timeout);
    }

    TInstant TRolesFetcher::ReadFromDisk() {
        TDiskReader dr(CacheFilePath_, Logger_.Get());
        if (!dr.Read()) {
            return {};
        }

        std::pair<TString, TString> data = ParseDiskFormat(dr.Data());
        if (data.second != Settings_.IdmSystemSlug) {
            Logger_->Warning(
                TStringBuilder() << "Roles in disk cache are for another slug (" << data.second
                                 << "). Self=" << Settings_.IdmSystemSlug);
            return {};
        }

        CurrentRoles_.Set(NRoles::TParser::Parse(std::make_shared<TString>(std::move(data.first))));
        Logger_->Debug(
            TStringBuilder() << "Succeed to read roles with revision "
                             << CurrentRoles_.Get()->GetMeta().Revision
                             << " from " << CacheFilePath_);

        return dr.Time();
    }

    bool TRolesFetcher::AreRolesOk() const {
        return bool(GetCurrentRoles());
    }

    bool TRolesFetcher::IsTimeToUpdate(const TRetrySettings& settings, TDuration sinceUpdate) {
        return settings.RolesUpdatePeriod < sinceUpdate;
    }

    bool TRolesFetcher::ShouldWarn(const TRetrySettings& settings, TDuration sinceUpdate) {
        return settings.RolesWarnPeriod < sinceUpdate;
    }

    NUtils::TFetchResult TRolesFetcher::FetchActualRoles(const TString& serviceTicket) {
        TStringStream out;
        THttpHeaders outHeaders;

        TRequest req = CreateTiroleRequest(serviceTicket);
        TKeepAliveHttpClient::THttpCode code = Client_->DoGet(
            req.Url,
            &out,
            req.Headers,
            &outHeaders);

        const THttpInputHeader* reqId = outHeaders.FindHeader("X-Request-Id");

        Logger_->Debug(
            TStringBuilder() << "Succeed to perform request for roles to " << Settings_.TiroleHost
                             << " (request_id=" << (reqId ? reqId->Value() : "")
                             << "). code=" << code);

        return {code, std::move(outHeaders), "/v1/get_actual_roles", out.Str(), {}};
    }

    void TRolesFetcher::Update(NUtils::TFetchResult&& fetchResult, TInstant now) {
        if (fetchResult.Code == HTTP_NOT_MODIFIED) {
            Y_ENSURE(CurrentRoles_.Get(),
                     "tirole did not return any roles because current roles are actual,"
                     " but there are no roles in memory - this should never happen");
            return;
        }

        Y_ENSURE(fetchResult.Code == HTTP_OK,
                 "Unexpected code from tirole: " << fetchResult.Code << ". " << fetchResult.Response);

        const THttpInputHeader* codec = fetchResult.Headers.FindHeader("X-Tirole-Compression");
        const TStringBuf codecBuf = codec ? codec->Value() : "";

        NRoles::TRawPtr blob;
        try {
            blob = std::make_shared<TString>(NRoles::TDecoder::Decode(
                codecBuf,
                std::move(fetchResult.Response)));
        } catch (const std::exception& e) {
            throw yexception() << "Failed to decode blob with codec '" << codecBuf
                               << "': " << e.what();
        }

        CurrentRoles_.Set(NRoles::TParser::Parse(blob));

        Logger_->Debug(
            TStringBuilder() << "Succeed to update roles with revision "
                             << CurrentRoles_.Get()->GetMeta().Revision);

        TDiskWriter dw(CacheFilePath_, Logger_.Get());
        dw.Write(PrepareDiskFormat(*blob, Settings_.IdmSystemSlug), now);
    }

    NTvmAuth::NRoles::TRolesPtr TRolesFetcher::GetCurrentRoles() const {
        return CurrentRoles_.Get();
    }

    void TRolesFetcher::ResetConnection() {
        Client_->ResetConnection();
    }

    static const char DELIMETER = '\t';

    std::pair<TString, TString> TRolesFetcher::ParseDiskFormat(TStringBuf filebody) {
        TStringBuf slug = filebody.RNextTok(DELIMETER);
        return {TString(filebody), CGIUnescapeRet(slug)};
    }

    TString TRolesFetcher::PrepareDiskFormat(TStringBuf roles, TStringBuf slug) {
        TStringStream res;
        res.Reserve(roles.size() + 1 + slug.size());
        res << roles << DELIMETER << CGIEscapeRet(slug);
        return res.Str();
    }

    TRolesFetcher::TRequest TRolesFetcher::CreateTiroleRequest(const TString& serviceTicket) const {
        TRolesFetcher::TRequest res;

        TStringStream url;
        url.Reserve(512);
        url << "/v1/get_actual_roles?";
        url << "system_slug=" << CGIEscapeRet(Settings_.IdmSystemSlug) << "&";
        Settings_.ProcInfo.AddToRequest(url);
        res.Url = std::move(url.Str());

        res.Headers.reserve(2);
        res.Headers.emplace(XYaServiceTicket_, serviceTicket);

        NRoles::TRolesPtr roles = CurrentRoles_.Get();
        if (roles) {
            res.Headers.emplace(IfNoneMatch_, Join("", "\"", roles->GetMeta().Revision, "\""));
        }

        return res;
    }
}
