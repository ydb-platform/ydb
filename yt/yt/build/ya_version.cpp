#include "ya_version.h"

#include "build.h"

#include <build/scripts/c_templates/svnversion.h>

#include <util/stream/format.h>
#include <util/stream/str.h>

#include <util/system/compiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

std::string GetCommitHash()
{
    std::string commit = GetProgramHash();
    if (commit.empty()) {
        commit = GetProgramCommitId();
    }

    return commit;
}

std::string TruncateCommitHash(std::string commit)
{
    constexpr int CommitHashPrefixLength = 16;

    // When we use `ya make --dist` distbuild makes mess instead of svn revision:
    //   BUILD_USER == "Unknown user"
    //   ARCADIA_SOURCE_REVISION = "-1"
    // When Hermes looks at such horrors it goes crazy.
    // Here are some hacks to help Hermes keep its sanity.
    if (commit == "-1") {
        commit = std::string(CommitHashPrefixLength, '0');
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

#if defined(_lsan_enabled_)
    out << "-lsan";
#endif

#if defined(_msan_enabled_)
    out << "-msan";
#endif

#if defined(_tsan_enabled_)
    out << "-tsan";
#endif

#if defined(_ubsan_enabled_)
    out << "-ubsan";
#endif

// Future-proofing. The check for YT_ROPSAN_ENABLE_PTR_TAGGING is technically
// redundant as it follows from access or serialization checks.
#if defined(YT_ROPSAN_ENABLE_ACCESS_CHECK) || \
    defined(YT_ROPSAN_ENABLE_SERIALIZATION_CHECK) || \
    defined(YT_ROPSAN_ENABLE_LEAK_DETECTION) || \
    defined(YT_ROPSAN_ENABLE_PTR_TAGGING)
    out << "-ropsan";
#endif

#if defined(YT_ROPSAN_ENABLE_ACCESS_CHECK)
    out << "A";
#endif

#if defined(YT_ROPSAN_ENABLE_SERIALIZATION_CHECK)
    out << "S";
#endif

#if defined(YT_ROPSAN_ENABLE_LEAK_DETECTION)
    out << "L";
#endif

    std::string commit;
    int svnRevision = GetProgramSvnRevision();
    if (svnRevision <= 0) {
        commit = GetCommitHash();
        commit = TruncateCommitHash(commit);
    } else {
        commit = "r" + ToString(svnRevision);
    }

    std::string buildUser = GetProgramBuildUser();
    if (buildUser == "Unknown user") {
        buildUser = "distbuild";
    }

    out << "~" << commit;
    if (buildUser != "teamcity" && buildUser != "sandbox") {
        out << "+" << buildUser;
    }
}

std::string CreateBranchCommitVersion(TStringBuf branch)
{
    TStringStream out;
    OutputCreateBranchCommitVersion(branch, out);
    return out.Str();
}

std::string CreateYTVersion(int major, int minor, int patch, TStringBuf branch)
{
    TStringStream out;
    out << major << "." << minor << "." << patch << "-";
    OutputCreateBranchCommitVersion(branch, out);
    return out.Str();
}

std::string GetYaHostName()
{
    return GetProgramBuildHost();
}

std::string GetYaBuildDate()
{
    return GetProgramBuildDate();
}

const std::string& GetRpcUserAgent()
{
    static const auto result = [&] {
        // Fill user agent from build information.
        // For trunk:
        // - yt-cpp/r<revision>
        // For YT release branch.
        // - yt-cpp/<YT version string>
        // For arbitrary branch different from trunk:
        // - yt-cpp/<branch>~<commit>
        // For local build from detached head or arc sync'ed state:
        // - yt-cpp/local~<commit>

        std::string branch(GetBranch());
        TStringStream out;

        out << "yt-cpp/";

        if (branch == "trunk") {
            int svnRevision = GetProgramSvnRevision();
            out << "trunk~r" << svnRevision;
        } else if (branch.starts_with("releases/yt")) {
            // Simply re-use YT version string. It looks like the following:
            // 20.3.7547269-stable-ya~bb57c034bfb47caa.
            std::string ytVersion(GetVersion());
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
    }();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
