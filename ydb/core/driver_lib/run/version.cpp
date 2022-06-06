#include <library/cpp/svnversion/svnversion.h>
#include "version.h"

TMaybe<NActors::TInterconnectProxyCommon::TVersionInfo> VERSION = NActors::TInterconnectProxyCommon::TVersionInfo{
    // version of this binary
    "22-2-border-2",

    // compatible versions; must include all compatible old ones, including this one; version verification occurs on both
    // peers and connection is accepted if at least one of peers accepts the version of the other peer
    {
        "22-2-border-2"
    }
};

TString GetBranchName(TString url) {
    bool found = false;
    for (const char *prefix : {"arcadia.yandex.ru/arc/", "arcadia/arc/", "arcadia.arc.yandex.ru/arc/"}) {
        const char *base = url.data();
        const char *p = strstr(base, prefix);
        if (p) {
            url = url.substr(p + strlen(prefix) - base);
            found = true;
            break;
        }
    }
    if (!found) {
        return TString();
    }

    static TString suffix("/arcadia");
    if (url.EndsWith(suffix)) {
        url = url.substr(0, url.length() - suffix.length());
    } else if (url.EndsWith("/arc/trunk")) {
        url = "trunk";
    } else {
        return TString();
    }

    return url;
}

void CheckVersionTag() {
    if (VERSION) {
        const char *version = GetProgramSvnVersion();
        const char *p = strstr(version, "URL: ");
        if (p) {
            p += 5; // shift "URL: "
            const char *end = strchr(p, '\n');
            if (end) {
                while (end > p && std::isspace(end[-1])) {
                    --end;
                }
                TString url(p, end);
                TString branch = GetBranchName(url);
                if (branch != "trunk" && VERSION->Tag == "trunk") {
                    Y_FAIL("non-trunk branch %s from URL# %s contains VersionTag# trunk", branch.data(), url.data());
                }
            }
        }
    }
}
