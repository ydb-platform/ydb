#pragma once

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/string/builder.h>


#define HTML_APP_PAGE(str, title) WITH_SCOPED(__stream, ::NKikimr::NPQ::NApp::THtmlAppPage(str, TStringBuilder() << title))

#define NAVIGATION_BAR() WITH_SCOPED(__navigationBar, ::NKikimr::NPQ::NApp::TNavigationBar(__stream))
#define NAVIGATION_TAB(id, caption) __navigationBar.Add(TStringBuilder() << id, caption)
#define NAVIGATION_TAB_CONTENT(id) WITH_SCOPED(__navigationContent, ::NKikimr::NPQ::NApp::TNavigationBarContent(__navigationBar, id))

#define PROPERTIES(caption) WITH_SCOPED(__properties, ::NKikimr::NPQ::NApp::TProperties(__stream, caption))
#define PROPERTY(caption, value) __properties.Add(caption, value)

#define LAYOUT_ROW(columns)
#define LAYOUT_COLUMN()


namespace NKikimr::NPQ::NApp {

struct THtmlAppPage {
    THtmlAppPage(IOutputStream&, const TString& title);
    ~THtmlAppPage();

    inline operator IOutputStream&() noexcept {
        return Str;
    }

private:
    IOutputStream& Str;
};

struct TNavigationBar {
    friend struct TNavigationBarContent;

    TNavigationBar(IOutputStream&);
    ~TNavigationBar();

    void Add(const TString& id, const TString& caption);

    inline operator IOutputStream&() noexcept {
        return Str;
    }

private:
    IOutputStream& Str;
    bool FirstTab = true;
    bool FirstContent = true;
};

struct TNavigationBarContent {
    TNavigationBarContent(TNavigationBar&, const TString& id);
    ~TNavigationBarContent();
private:
    TNavigationBar& NavigationBar;
};

struct TProperties {
    TProperties(IOutputStream&, const TString& caption);
    ~TProperties();

    template<typename T>
    void Add(const TString& name, const T& value);

private:
    IOutputStream& Str;
};

}
