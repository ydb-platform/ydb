#pragma once

#include <library/cpp/regex/pcre/regexp.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NYql {

class TYtGatewayConfig;

class TYtUrlMapper {
public:
    explicit TYtUrlMapper(const TYtGatewayConfig& config);
    bool MapYtUrl(const TString& url, TString* cluster, TString* path) const;

private:
    static TVector<TRegExMatch> BuildRemoteFilePatterns(const TYtGatewayConfig& config);
    bool FindMatchingRemoteFilePattern(const TString& url) const;

private:
    const TVector<TRegExMatch> RemoteFilePatterns_;
};
}
