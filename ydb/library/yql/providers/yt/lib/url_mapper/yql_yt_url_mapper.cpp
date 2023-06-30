#include "yql_yt_url_mapper.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <library/cpp/uri/http_url.h>
#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

namespace NYql {

TYtUrlMapper::TYtUrlMapper(const TYtGatewayConfig& config)
    : RemoteFilePatterns_(BuildRemoteFilePatterns(config))
{
}

bool TYtUrlMapper::MapYtUrl(const TString& url, TString* cluster, TString* path) const {
    THttpURL parsedUrl;
    if (THttpURL::ParsedOK != parsedUrl.Parse(url, THttpURL::FeaturesAll | NUri::TFeature::FeatureConvertHostIDN | NUri::TFeature::FeatureNoRelPath, {}, 65536)) {
        return false;
    }

    const auto rawScheme = parsedUrl.GetField(NUri::TField::FieldScheme);
    auto host = parsedUrl.GetHost();
    if (NUri::EqualNoCase(rawScheme, "yt") || host.StartsWith("yt.yandex") || host.EndsWith(".yt.yandex-team.ru") || host.EndsWith(".yt.yandex.net")) {
        if (cluster) {
            if (host.StartsWith("yt.")) {
                host = parsedUrl.GetField(NUri::TField::FieldPath);
                host = host.Skip(1).Before('/');
            } else {
                host.ChopSuffix(".yt.yandex-team.ru");
                host.ChopSuffix(".yt.yandex.net");
            }
            *cluster = host;
        }

        if (path) {
            if (!FindMatchingRemoteFilePattern(url)) {
                return false;
            }
            TCgiParameters params(parsedUrl.GetField(NUri::TField::FieldQuery));
            *path = params.Has("path") ? params.Get("path") : TString{TStringBuf(parsedUrl.GetField(NUri::TField::FieldPath)).Skip(1)};
        }

        return true;
    }
    return false;
}

TVector<TRegExMatch> TYtUrlMapper::BuildRemoteFilePatterns(const TYtGatewayConfig& config) {
    TVector<TRegExMatch> res;
    for (auto& fp : config.GetRemoteFilePatterns()) {
        res.emplace_back(fp.GetPattern());
    }
    return res;
}

bool TYtUrlMapper::FindMatchingRemoteFilePattern(const TString& url) const {
    return AnyOf(RemoteFilePatterns_, [&](auto& p) {
        return p.Match(url.data());
    });
}

}
