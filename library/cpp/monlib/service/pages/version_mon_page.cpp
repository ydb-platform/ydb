#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/build_info/build_info.h>
#include <library/cpp/malloc/api/malloc.h>

#include "version_mon_page.h"

using namespace NMonitoring;

void TVersionMonPage::OutputText(IOutputStream& out, NMonitoring::IMonHttpRequest&) {
    const char* version = GetProgramSvnVersion();
    out << version;
    if (!TString(version).EndsWith("\n"))
        out << "\n";
    out << GetBuildInfo() << "\n\n";
    out << "linked with malloc: " << NMalloc::MallocInfo().Name << "\n";
}
