#include "mon_page.h"

using namespace NMonitoring;

IMonPage::IMonPage(const TString& path, const TString& title)
    : Path(path)
    , Title(title)
{
    Y_ABORT_UNLESS(!Path.StartsWith('/'));
    Y_ABORT_UNLESS(!Path.EndsWith('/'));
}

void IMonPage::OutputNavBar(IOutputStream& out) {
    TVector<const IMonPage*> parents;
    for (const IMonPage* p = this; p; p = p->Parent) {
        parents.push_back(p);
    }
    std::reverse(parents.begin(), parents.end());

    out << "<ol class='breadcrumb'>\n";

    TString absolutePath;
    for (size_t i = 0; i < parents.size(); ++i) {
        const TString& title = parents[i]->GetTitle();
        if (i == parents.size() - 1) {
            out << "<li>" << title << "</li>\n";
        } else {
            if (!absolutePath.EndsWith('/')) {
                absolutePath += '/';
            }
            absolutePath += parents[i]->GetPath();
            out << "<li class='active'><a href='" << absolutePath << "'>" << title << "</a></li>\n";
        }
    }
    out << "</ol>\n";
}
