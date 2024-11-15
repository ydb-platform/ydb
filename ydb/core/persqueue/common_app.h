#pragma once

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/string/builder.h>


#define HTML_APP_PAGE(str, title) WITH_SCOPED(__stream, ::NKikimr::NPQ::NApp::THtmlAppPage(str, TStringBuilder() << title))
#define HTML_PART(str, title) WITH_SCOPED(__stream, ::NKikimr::NPQ::NApp::THtmlPart(str))

#define NAVIGATION_BAR() WITH_SCOPED(__navigationBar, ::NKikimr::NPQ::NApp::TNavigationBar(__stream))
#define NAVIGATION_TAB(id, caption) __navigationBar.Add(TStringBuilder() << id, caption)
#define NAVIGATION_TAB_CONTENT(id) WITH_SCOPED(__navigationContent, ::NKikimr::NPQ::NApp::TNavigationBarContent(__navigationBar, id))

#define PROPERTIES(caption) WITH_SCOPED(__properties, ::NKikimr::NPQ::NApp::TProperties(__stream, caption))
#define PROPERTY(caption, value) __properties.Add(caption, TStringBuilder() << value)

#define LAYOUT_ROW() DIV_CLASS("raw")
#define LAYOUT_COLUMN() DIV_CLASS("col")


namespace NKikimr::NPQ::NApp {

struct THtmlPart {
    THtmlPart(IOutputStream&);
    ~THtmlPart();

    inline operator IOutputStream&() noexcept {
        return Str;
    }

protected:
    IOutputStream& Str;
};

struct THtmlAppPage : public THtmlPart {
    THtmlAppPage(IOutputStream&, const TString& title);
    ~THtmlAppPage();
};

struct TNavigationBar : public THtmlPart {
    friend struct TNavigationBarContent;

    TNavigationBar(IOutputStream&);
    ~TNavigationBar();

    void Add(const TString& id, const TString& caption);

private:
    bool FirstTab = true;
    bool FirstContent = true;
};

struct TNavigationBarContent {
    TNavigationBarContent(TNavigationBar&, const TString& id);
    ~TNavigationBarContent();
private:
    TNavigationBar& NavigationBar;
};

struct TProperties : public THtmlPart {
    TProperties(IOutputStream&, const TString& caption);
    ~TProperties();

    void Add(const TString& name, const TString& value);
};

}
