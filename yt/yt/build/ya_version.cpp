#include "ya_version.h"

#include "build.h"

#include <build/scripts/c_templates/svnversion.h>

#include <util/stream/format.h>
#include <util/stream/str.h>

#include <util/system/compiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString GetCommitHash()
{
    TString commit = GetProgramHash();
    if (commit.empty()) {
        commit = GetProgramCommitId();
    }

    return commit;
}

TString TruncateCommitHash(TString commit)
{
    constexpr int CommitHashPrefixLength = 16;

    // When we use `ya make --dist` distbuild makes mess instead of svn revision:
    //   BUILD_USER == "Unknown user"
    //   ARCADIA_SOURCE_REVISION = "-1"
    // When Hermes looks at such horrors it goes crazy.
    // Here are some hacks to help Hermes keep its sanity.
    if (commit == "-1") {
        commit = TString(CommitHashPrefixLength, '0');
    } else {
        commit = commit.substr(0, CommitHashPrefixLength);
    }
    return commit;
}

void OutputCreateBranchCommitVersion(TStringBuf branch, TStringStream& out)
{
    out << branch << "-" << GetVersionType();

#if !defined(NDEBUG)
    out << "debug";
#endif

#if defined(TSTRING_IS_STD_STRING)
    out << "-std-string";
#endif

#if defined(_asan_enabled_)
    out << "-asan";
#endif

    TString commit;
    int svnRevision = GetProgramSvnRevision();
    if (svnRevision <= 0) {
        commit = GetCommitHash();
        commit = TruncateCommitHash(commit);
    } else {
        commit = "r" + ToString(svnRevision);
    }

    TString buildUser = GetProgramBuildUser();
    if (buildUser == "Unknown user") {
        buildUser = "distbuild";
    }

    out << "~" << commit;
    if (buildUser != "teamcity") {
        out << "+" << buildUser;
    }
}

TString CreateBranchCommitVersion(TStringBuf branch)
{
    TStringStream out;
    OutputCreateBranchCommitVersion(branch, out);
    return out.Str();
}

TString CreateYTVersion(int major, int minor, int patch, TStringBuf branch)
{
    TStringStream out;
    out << major << "." << minor << "." << patch << "-";
    OutputCreateBranchCommitVersion(branch, out);
    return out.Str();
}

TString GetYaHostName()
{
    return GetProgramBuildHost();
}

TString GetYaBuildDate()
{
    return GetProgramBuildDate();
}

namespace {

////////////////////////////////////////////////////////////////////////////////

TString BuildRpcUserAgent() noexcept
{
    // Fill user agent from build information.
    // For trunk:
    // - yt-cpp/r<revision>
    // For YT release branch.
    // - yt-cpp/<YT version string>
    // For arbitrary branch different from trunk:
    // - yt-cpp/<branch>~<commit>
    // For local build from detached head or arc sync'ed state:
    // - yt-cpp/local~<commit>

    TString branch(GetBranch());
    TStringStream out;

    out << "yt-cpp/";

    if (branch == "trunk") {
        int svnRevision = GetProgramSvnRevision();
        out << "trunk~r" << svnRevision;
    } else if (branch.StartsWith("releases/yt")) {
        // Simply re-use YT version string. It looks like the following:
        // 20.3.7547269-stable-ya~bb57c034bfb47caa.
        TString ytVersion(GetVersion());
        out << ytVersion;
    } else {
        auto commit = GetCommitHash();
        auto truncatedCommit = TruncateCommitHash(commit);

        // In detached head arc state branch seems to coincide with commit hash.
        // Let's use that in order to distinguish detached head from regular branch state.
        if (branch == commit) {
            branch = "local";
        }

        out << branch << "~" << truncatedCommit;
    }

    return out.Str();
}

const TString CachedUserAgent = BuildRpcUserAgent();

////////////////////////////////////////////////////////////////////////////////

} // namespace

const TString& GetRpcUserAgent()
{
    return CachedUserAgent;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
