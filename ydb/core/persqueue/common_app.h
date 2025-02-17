#pragma once

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/string/builder.h>


#define HTML_APP_PAGE(str, title) WITH_SCOPED(__stream, ::NKikimr::NPQ::NApp::THtmlAppPage(str, TStringBuilder() << title))
#define HTML_PART(str) WITH_SCOPED(__stream, ::NKikimr::NPQ::NApp::THtmlPart(str))

#define NAVIGATION_BAR() WITH_SCOPED(__navigationBar, ::NKikimr::NPQ::NApp::TNavigationBar(__stream))
#define NAVIGATION_TAB(id, caption) __navigationBar.Add(TStringBuilder() << id, TStringBuilder() << caption)
#define NAVIGATION_TAB_CONTENT(id) WITH_SCOPED(__navigationContent, ::NKikimr::NPQ::NApp::TNavigationBarContent(__navigationBar, id))
#define NAVIGATION_TAB_CONTENT_PART(id) WITH_SCOPED(__navigationContent, ::NKikimr::NPQ::NApp::TNavigationBarContentPart(__stream, TStringBuilder() << id))

#define PROPERTIES(caption) WITH_SCOPED(__properties, ::NKikimr::NPQ::NApp::TProperties(__stream, caption))
#define PROPERTY(caption, value) __properties.Add(caption, TStringBuilder() << value)

#define LAYOUT() DIV()
#define LAYOUT_ROW() DIV_CLASS("row")
#define LAYOUT_COLUMN() DIV_CLASS("col")

#define CONFIGURATION(val) WITH_SCOPED(__configuration, ::NKikimr::NPQ::NApp::TConfiguration(__stream, val)) {}


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

struct TNavigationBarContentPart : public THtmlPart  {
    TNavigationBarContentPart(IOutputStream&, const TString& id);
    ~TNavigationBarContentPart();
};

struct TProperties : public THtmlPart {
    TProperties(IOutputStream&, const TString& caption);
    ~TProperties();

    void Add(const TString& name, const TString& value);
};

struct TConfiguration : public THtmlPart {
    TConfiguration(IOutputStream&, const TString& value);
    ~TConfiguration() = default;
};

}
