#pragma once

#include <ydb/public/api/protos/draft/fq.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <vector>

namespace NFqRun {

struct TConnectionDdl {
    FederatedQuery::ConnectionContent Content;
    bool IfNotExists = false;
};

// Local fqrun secrets are expanded into the existing host-owned connection credentials.
THashMap<TString, TString> LoadConnectionSecrets(const TString& path);
std::vector<TConnectionDdl> ParseConnectionDdl(const TString& sql, const THashMap<TString, TString>& secrets);

}   // namespace NFqRun
