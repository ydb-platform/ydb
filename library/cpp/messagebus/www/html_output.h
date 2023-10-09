#pragma once

#include "concat_strings.h"

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <util/system/tls.h>

extern Y_POD_THREAD(IOutputStream*) HtmlOutputStreamPtr;

static IOutputStream& HtmlOutputStream() {
    Y_ABORT_UNLESS(!!HtmlOutputStreamPtr);
    return *HtmlOutputStreamPtr;
}

struct THtmlOutputStreamPushPop {
    IOutputStream* const Prev;

    THtmlOutputStreamPushPop(IOutputStream* outputStream)
        : Prev(HtmlOutputStreamPtr)
    {
        HtmlOutputStreamPtr = outputStream;
    }

    ~THtmlOutputStreamPushPop() {
        HtmlOutputStreamPtr = Prev;
    }
};

struct TChars {
    TString Text;
    bool NeedEscape;

    TChars(TStringBuf text)
        : Text(text)
        , NeedEscape(true)
    {
    }
    TChars(TStringBuf text, bool escape)
        : Text(text)
        , NeedEscape(escape)
    {
    }
    TChars(const char* text)
        : Text(text)
        , NeedEscape(true)
    {
    }
    TChars(const char* text, bool escape)
        : Text(text)
        , NeedEscape(escape)
    {
    }

    TString Escape() {
        if (NeedEscape) {
            return EncodeHtmlPcdata(Text);
        } else {
            return Text;
        }
    }
};

struct TAttr {
    TString Name;
    TString Value;

    TAttr(TStringBuf name, TStringBuf value)
        : Name(name)
        , Value(value)
    {
    }

    TAttr() {
    }

    bool operator!() const {
        return !Name;
    }
};

static inline void Doctype() {
    HtmlOutputStream() << "<!doctype html>\n";
}

static inline void Nl() {
    HtmlOutputStream() << "\n";
}

static inline void Sp() {
    HtmlOutputStream() << " ";
}

static inline void Text(TStringBuf text) {
    HtmlOutputStream() << EncodeHtmlPcdata(text);
}

static inline void Line(TStringBuf text) {
    Text(text);
    Nl();
}

static inline void WriteAttr(TAttr a) {
    if (!!a) {
        HtmlOutputStream() << " " << a.Name << "='" << EncodeHtmlPcdata(a.Value) << "'";
    }
}

static inline void Open(TStringBuf tag, TAttr a1 = TAttr(), TAttr a2 = TAttr(), TAttr a3 = TAttr(), TAttr a4 = TAttr()) {
    HtmlOutputStream() << "<" << tag;
    WriteAttr(a1);
    WriteAttr(a2);
    WriteAttr(a3);
    WriteAttr(a4);
    HtmlOutputStream() << ">";
}

static inline void Open(TStringBuf tag, TStringBuf cssClass, TStringBuf id = "") {
    Open(tag, TAttr("class", cssClass), !!id ? TAttr("id", id) : TAttr());
}

static inline void OpenBlock(TStringBuf tag, TStringBuf cssClass = "") {
    Open(tag, cssClass);
    Nl();
}

static inline void Close(TStringBuf tag) {
    HtmlOutputStream() << "</" << tag << ">\n";
}

static inline void CloseBlock(TStringBuf tag) {
    Close(tag);
    Nl();
}

static inline void TagWithContent(TStringBuf tag, TChars content) {
    HtmlOutputStream() << "<" << tag << ">" << content.Escape() << "</" << tag << ">";
}

static inline void BlockTagWithContent(TStringBuf tag, TStringBuf content) {
    TagWithContent(tag, content);
    Nl();
}

static inline void TagWithClass(TStringBuf tag, TStringBuf cssClass) {
    Open(tag, cssClass);
    Close(tag);
}

static inline void Hn(unsigned n, TStringBuf title) {
    BlockTagWithContent(ConcatStrings("h", n), title);
}

static inline void Small(TStringBuf text) {
    TagWithContent("small", text);
}

static inline void HnWithSmall(unsigned n, TStringBuf title, TStringBuf small) {
    TString tagName = ConcatStrings("h", n);
    Open(tagName);
    HtmlOutputStream() << title;
    Sp();
    Small(small);
    Close(tagName);
}

static inline void H1(TStringBuf title) {
    Hn(1, title);
}

static inline void H2(TStringBuf title) {
    Hn(2, title);
}

static inline void H3(TStringBuf title) {
    Hn(3, title);
}

static inline void H4(TStringBuf title) {
    Hn(4, title);
}

static inline void H5(TStringBuf title) {
    Hn(5, title);
}

static inline void H6(TStringBuf title) {
    Hn(6, title);
}

static inline void Pre(TStringBuf content) {
    HtmlOutputStream() << "<pre>" << EncodeHtmlPcdata(content) << "</pre>\n";
}

static inline void Li(TStringBuf content) {
    BlockTagWithContent("li", content);
}

static inline void LiWithClass(TStringBuf cssClass, TStringBuf content) {
    Open("li", cssClass);
    Text(content);
    Close("li");
}

static inline void OpenA(TStringBuf href) {
    Open("a", TAttr("href", href));
}

static inline void A(TStringBuf href, TStringBuf text) {
    OpenA(href);
    Text(text);
    Close("a");
}

static inline void Td(TStringBuf content) {
    TagWithContent("td", content);
}

static inline void Th(TStringBuf content, TStringBuf cssClass = "") {
    OpenBlock("th", cssClass);
    Text(content);
    CloseBlock("th");
}

static inline void DivWithClassAndContent(TStringBuf cssClass, TStringBuf content) {
    Open("div", cssClass);
    Text(content);
    Close("div");
}

static inline void BootstrapError(TStringBuf text) {
    DivWithClassAndContent("alert alert-danger", text);
}

static inline void BootstrapInfo(TStringBuf text) {
    DivWithClassAndContent("alert alert-info", text);
}

static inline void ScriptHref(TStringBuf href) {
    Open("script",
         TAttr("language", "javascript"),
         TAttr("type", "text/javascript"),
         TAttr("src", href));
    Close("script");
    Nl();
}

static inline void LinkStylesheet(TStringBuf href) {
    Open("link", TAttr("rel", "stylesheet"), TAttr("href", href));
    Close("link");
    Nl();
}

static inline void LinkFavicon(TStringBuf href) {
    Open("link", TAttr("rel", "shortcut icon"), TAttr("href", href));
    Close("link");
    Nl();
}

static inline void Title(TChars title) {
    TagWithContent("title", title);
    Nl();
}

static inline void Code(TStringBuf content) {
    TagWithContent("code", content);
}

struct TTagGuard {
    const TString TagName;

    TTagGuard(TStringBuf tagName, TStringBuf cssClass, TStringBuf id = "")
        : TagName(tagName)
    {
        Open(TagName, cssClass, id);
    }

    TTagGuard(TStringBuf tagName, TAttr a1 = TAttr(), TAttr a2 = TAttr(), TAttr a3 = TAttr(), TAttr a4 = TAttr())
        : TagName(tagName)
    {
        Open(tagName, a1, a2, a3, a4);
    }

    ~TTagGuard() {
        Close(TagName);
    }
};

struct TDivGuard: public TTagGuard {
    TDivGuard(TStringBuf cssClass, TStringBuf id = "")
        : TTagGuard("div", cssClass, id)
    {
    }

    TDivGuard(TAttr a1 = TAttr(), TAttr a2 = TAttr(), TAttr a3 = TAttr())
        : TTagGuard("div", a1, a2, a3)
    {
    }
};

struct TAGuard {
    TAGuard(TStringBuf href) {
        OpenA(href);
    }

    ~TAGuard() {
        Close("a");
    }
};

struct TScriptFunctionGuard {
    TTagGuard Script;

    TScriptFunctionGuard()
        : Script("script")
    {
        Line("$(function() {");
    }

    ~TScriptFunctionGuard() {
        Line("});");
    }
};
