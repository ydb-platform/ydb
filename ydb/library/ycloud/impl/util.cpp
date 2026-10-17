#include <ydb/library/ycloud/impl/util.h>

#include <library/cpp/svnversion/svnversion.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <functional>

namespace NCloud {

namespace {

TString GetRevision() {
    TString version = std::invoke([]() {
        TString res = GetTag();
        return res.empty() ? GetBranch() : res;
    });

    if (const auto pos = version.rfind('/'); pos != TString::npos) {
        version = version.substr(pos + 1);
    }

    if (TString commitId = GetProgramCommitId(); !commitId.empty()) {
        if (commitId.size() > 7) {
            commitId = commitId.substr(0, 7);
        }

        if (version.empty()) {
            version = commitId;
        } else {
            version += '.' + commitId;
        }
    }

    return version.empty() ? "unknown" : version;
}

} // namespace

TString BuildUserAgentPrefix(const TStringBuf userAgentHint) {
    static const TString revision = GetRevision();
    return userAgentHint.empty()
        ? TStringBuilder() << "ydb" << '/' << revision
        : TStringBuilder() << userAgentHint << '/' << revision;
}

} // namespace NCloud
