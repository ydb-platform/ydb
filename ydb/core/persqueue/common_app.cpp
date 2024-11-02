#include "common_app.h"


namespace NKikimr::NPQ::NApp {

THtmlAppPage::THtmlAppPage(IOutputStream& str, const TString& title)
    : Str(str) {
    Str << R"(
<HTML>
    <TITLE>)" << title << R"(</TITLE>
    <STYLE>
.properties {
    border-bottom-style: solid;
    border-top-style: solid;
    border-width: 1px;
    border-color: darkgrey;
    padding-bottom: 10px;
}

.tgrid {
    width: 100%;
    border: 0;
}
    </STYLE>
    <BODY>
        <H3>)" << title << R"(
    )";
}

THtmlAppPage::~THtmlAppPage() {
    Str << R"(
    <BODY>
</HTML>
    )";
}

TNavigationBar::TNavigationBar(IOutputStream& str)
    : Str(str) {
    Str << R"(<UL CLASS="nav nav-tabs">)";
}

TNavigationBar::~TNavigationBar() {
    if (FirstContent) {
        Str << R"(</UL>)";
    } else {
        Str << R"(</DIV>)";
    }
}

void TNavigationBar::Add(const TString& code, const TString& caption) {
    auto& __stream = Str;

    auto link = [&]() {
        __stream << "<a href=\"#" << code << "\" data-toggle=\"tab\">" << caption << "</a>";
    };

    if (FirstTab) {
        FirstTab = false;
        LI_CLASS("active") {
            link();
        }
    } else {
        LI() {
            link();
        }
    }
}

TNavigationBarContent::TNavigationBarContent(TNavigationBar& navigationBar, const TString& id)
    : NavigationBar(navigationBar) {

    auto& __stream = NavigationBar.Str;
    if (NavigationBar.FirstContent) {
        __stream << R"(
</UL>
<DIV CLASS="tab-content">
    <DIV CLASS="tab-pane fade in active">
        )";
    } else {
        __stream << R"(    <DIV CLASS="tab-pane fade">)";
    }
}

TNavigationBarContent::~TNavigationBarContent() {
    auto& __stream = NavigationBar.Str;
    __stream << R"(</DIV">)";
    NavigationBar.FirstContent = false;
}

TProperties::TProperties(IOutputStream& str, const TString& caption)
    : Str(str) {

    Str << R"(
<TABLE CLASS="properties">
    <CAPTION>)" << caption << R"(</CAPTION>
    <TBODY>
    )";
}

TProperties::~TProperties() {
    Str << R"(
    </TBODY>
</TABLE>
    )";
}

template<typename T>
void TProperties::Add(const TString& name, const T& value) {
    auto& __stream = Str;

    TABLER() {
        TABLED() { __stream << name;}
        TABLED() { __stream << value; }
    }
}

template void TProperties::Add(const TString& name, const TString& value);
template void TProperties::Add(const TString& name, const ui64& value);

}
