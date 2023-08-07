#include "url_preprocessing.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/yexception.h>

namespace NYql {

void TUrlPreprocessing::Configure(bool restrictedUser, const TGatewaysConfig& cfg) {
    RestrictedUser_ = restrictedUser;

    try {
        if (cfg.HasFs()) {
            const auto fsCfg = cfg.GetFs();
            for (auto& s: fsCfg.GetCustomSchemes()) {
                Mapper_.AddMapping(s.GetPattern(), s.GetTargetUrl());
            }
            if (restrictedUser) {
                for (auto& a: fsCfg.GetExternalAllowedUrls()) {
                    AllowedUrls_.Add(a.GetPattern(), a.GetAlias());
                }
            } else {
                for (auto& a: fsCfg.GetAllowedUrls()) {
                    AllowedUrls_.Add(a.GetPattern(), a.GetAlias());
                }
            }
        }
    } catch (const yexception& e) {
        ythrow yexception() << "UrlPreprocessing: " << e.what();
    }
}

std::pair<TString, TString> TUrlPreprocessing::Preprocess(const TString& url) {
    TString convertedUrl;

    if (!Mapper_.MapUrl(url, convertedUrl)) {
        convertedUrl = url;
    } else {
        YQL_LOG(INFO) << "Remap url from " << url << " to " << convertedUrl;
    }

    TString alias;
    if (RestrictedUser_ || !AllowedUrls_.IsEmpty()) {
        if (auto a = AllowedUrls_.Match(convertedUrl)) {
            alias = *a;
        } else {
            YQL_LOG(WARN) << "Url " << convertedUrl << " is not in allowed list, reject accessing";
            ythrow yexception() << "It is not allowed to access url " << url;
        }
    }
    YQL_LOG(INFO) << "UrlPreprocessing: " << convertedUrl << ", alias=" << alias;
    return {convertedUrl, alias};
}

} // NYql
