#include "common_app.h"


namespace NKikimr::NPQ::NApp {

THtmlAppPage::THtmlAppPage(IOutputStream& str, const TString& title)
    : Str(str) {
    Str << R"(
<HTML>
    <TITLE>)" << title << R"(</TITLE>
    <STYLE>
.row {
  --bs-gutter-x: 1.5rem;
  --bs-gutter-y: 0;
  display: flex;
  flex-wrap: wrap;
  margin-top: calc(-1 * var(--bs-gutter-y));
  margin-right: calc(-0.5 * var(--bs-gutter-x));
  margin-left: calc(-0.5 * var(--bs-gutter-x));
}

.row > * {
  box-sizing: border-box;
  flex-shrink: 0;
  width: 100%;
  max-width: 100%;
  padding-right: calc(var(--bs-gutter-x) * 0.5);
  padding-left: calc(var(--bs-gutter-x) * 0.5);
  margin-top: var(--bs-gutter-y);
}

.col {
  flex: 1 0 0%;
}

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
    <DIV CLASS="tab-pane fade in active container" id=")" << id << R"(">)";
    } else {
        __stream << R"(    <DIV CLASS="tab-pane fade" id=")" << id << R"(">)";
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

void TProperties::Add(const TString& name, const TString& value) {
    auto& __stream = Str;

    TABLER() {
        TABLED() { __stream << name;}
        TABLED() { __stream << value; }
    }
}

}
