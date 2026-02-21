#include <ydb/core/protos/s3_settings.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>

#include <library/cpp/uri/uri.h>
#include <util/string/builder.h>

namespace NKikimr::NColumnShard::NTiers {

class TS3Uri {
private:
    YDB_READONLY_DEF(std::optional<NKikimrSchemeOp::TS3Settings_EScheme>, Scheme);
    YDB_READONLY_DEF(TString, Bucket);
    YDB_READONLY_DEF(TString, Host);
    YDB_READONLY_DEF(std::optional<ui16>, Port);
    YDB_READONLY_DEF(std::optional<TString>, Folder);

    enum TUriStyle {
        PATH_STYLE = 1,
        VIRTUAL_HOSTED_STYLE = 2,
    };

    TUriStyle UriStyle = PATH_STYLE;

    inline static const std::vector<TString> BucketHostSeparators = { ".s3.", ".s3-" };

private:
    static TStringBuf StripPath(const TStringBuf& path) {
        TStringBuf stripped = path;
        while (stripped.SkipPrefix("/")) {
        }
        while (stripped.ChopSuffix("/")) {
        }
        return stripped;
    }

    static std::optional<TUriStyle> DeduceUriStyle(const NUri::TUri& uri) {
        const bool hasSubdomain = std::count(uri.GetHost().begin(), uri.GetHost().end(), '.') >= 2;
        const bool hasPath = !StripPath(uri.GetField(NUri::TField::FieldPath)).Empty();
        if (hasSubdomain && !hasPath) {
            return VIRTUAL_HOSTED_STYLE;
        }
        if (!hasSubdomain && hasPath) {
            return PATH_STYLE;
        }

        // URI style deduction copied from AWS SDK for Java
        for (const TString& sep : BucketHostSeparators) {
            if (uri.GetHost().StartsWith(sep.substr(1))) {
                return PATH_STYLE;
            }
            if (uri.GetHost().Contains(sep)) {
                return VIRTUAL_HOSTED_STYLE;
            }
        }

        return std::nullopt;
    }

    static TConclusion<TS3Uri> ParsePathStyleUri(const NUri::TUri& input) {
        TS3Uri result;
        result.UriStyle = PATH_STYLE;

        TStringBuf path = StripPath(input.GetField(NUri::TField::FieldPath));

        if (path.Empty()) {
            return TConclusionStatus::Fail(TStringBuilder() << "Missing bucket in path-style S3 uri: " << input.Serialize());
        }

        TStringBuf folder;
        TStringBuf bucket;
        if (path.TryRSplit('/', folder, bucket)) {
            result.Folder = folder;
            result.Bucket = bucket;
        } else {
            result.Bucket = path;
        }

        result.Host = input.GetHost();

        if (auto status = result.FillStyleAgnosticFields(input); status.IsFail()) {
            return status;
        }
        return result;
    }

    static TConclusion<TS3Uri> ParseVirtualHostedStyleUri(const NUri::TUri& input) {
        TS3Uri result;
        result.UriStyle = VIRTUAL_HOSTED_STYLE;

        for (auto&& sep : BucketHostSeparators) {
            if (const ui64 findSep = input.GetHost().find(sep); findSep != TStringBuf::npos) {
                result.Bucket = input.GetHost().SubStr(0, findSep);
                result.Host = input.GetHost().SubStr(findSep + 1);
                break;
            }
        }

        if (result.Host.empty()) {
            TStringBuf host;
            TStringBuf bucket;
            if (input.GetHost().TrySplit('.', bucket, host)) {
                result.Host = host;
                result.Bucket = bucket;
            } else {
                return TConclusionStatus::Fail(TStringBuilder() << "Missing bucket in virtual-hosted style S3 uri: " << input.Serialize());
            }
        }

        if (TStringBuf path = StripPath(input.GetField(NUri::TField::FieldPath))) {
            result.Folder = path;
        }

        if (auto status = result.FillStyleAgnosticFields(input); status.IsFail()) {
            return status;
        }
        return result;
    }

    TConclusionStatus FillStyleAgnosticFields(const NUri::TUri& from) {
        if (from.GetField(NUri::TField::FieldPort)) {
            Port = from.GetPort();
        }

        switch (from.GetScheme()) {
            case NUri::TScheme::SchemeEmpty:
                break;
            case NUri::TScheme::SchemeHTTP:
                Scheme = NKikimrSchemeOp::TS3Settings_EScheme_HTTP;
                break;
            case NUri::TScheme::SchemeHTTPS:
                Scheme = NKikimrSchemeOp::TS3Settings_EScheme_HTTPS;
                break;
            default:
                return TConclusionStatus::Fail(TStringBuilder() << "Unexpected scheme in url: " << from.Serialize());
        }

        return TConclusionStatus::Success();
    }

public:
    static TConclusion<TS3Uri> ParseUri(const TString& input) {
        NUri::TUri uri;
        if (uri.Parse(input, NUri::TFeature::NewFeaturesRecommended) != NUri::TState::EParsed::ParsedOK) {
            return TConclusionStatus::Fail("Cannot parse URI: " + input);
        }

        TUriStyle uriStyle;
        if (const auto deducedStyle = DeduceUriStyle(uri)) {
            uriStyle = *deducedStyle;
        } else {
            uriStyle = PATH_STYLE;
        }

        switch (uriStyle) {
            case PATH_STYLE:
                return ParsePathStyleUri(uri);
            case VIRTUAL_HOSTED_STYLE:
                return ParseVirtualHostedStyleUri(uri);
        }
    }

    TString GetEndpoint() const {
        TString endpoint = Host;
        if (Port) {
            endpoint += TStringBuilder() << ':' << *Port;
        }
        if (Folder) {
            endpoint += TStringBuilder() << '/' << *Folder;
        }
        return endpoint;
    }

    void FillSettings(NKikimrSchemeOp::TS3Settings& settings) const {
        settings.SetEndpoint(GetEndpoint());
        settings.SetBucket(Bucket);
        if (Scheme) {
            settings.SetScheme(*Scheme);
        }

        settings.SetUseVirtualAddressing(UriStyle == VIRTUAL_HOSTED_STYLE);
    }
};

}   // namespace NKikimr::NColumnShard::NTiers
