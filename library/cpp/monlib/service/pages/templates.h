#pragma once

#include <util/stream/output.h>
#include <util/system/defaults.h>

#define WITH_SCOPED(var, value) if (auto var = (value); Y_UNUSED(var), true)

#define TAG(name) WITH_SCOPED(tmp, ::NMonitoring::name(__stream))
#define TAG_CLASS(name, cls) WITH_SCOPED(tmp, ::NMonitoring::name(__stream, cls))
#define TAG_CLASS_STYLE(name, cls, style) WITH_SCOPED(tmp, ::NMonitoring::name(__stream, {{"class", cls}, {"style", style}}))
#define TAG_CLASS_ID(name, cls, id) WITH_SCOPED(tmp, ::NMonitoring::name(__stream, cls, "", id))
#define TAG_CLASS_FOR(name, cls, for0) WITH_SCOPED(tmp, ::NMonitoring::name(__stream, cls, for0))
#define TAG_ATTRS(name, ...) WITH_SCOPED(tmp, ::NMonitoring::name(__stream, ##__VA_ARGS__))

#define HTML(str) WITH_SCOPED(__stream, ::NMonitoring::TOutputStreamRef(str))

#define HEAD() TAG(THead)
#define BODY() TAG(TBody)
#define HTML_TAG() TAG(THtml)
#define DIV() TAG(TDiv)
#define DIV_CLASS(cls) TAG_CLASS(TDiv, cls)
#define DIV_CLASS_ID(cls, id) TAG_CLASS_ID(TDiv, cls, id)
#define PRE() TAG(TPre)
#define TABLE() TAG(TTable)
#define TABLE_CLASS(cls) TAG_CLASS(TTable, cls)
#define TABLE_SORTABLE() TABLE_CLASS("table-sortable")
#define TABLE_SORTABLE_CLASS(cls) TABLE_CLASS(cls " table-sortable")
#define TABLEHEAD() TAG(TTableHead)
#define TABLEHEAD_CLASS(cls) TAG_CLASS(TTableHead, cls)
#define TABLEBODY() TAG(TTableBody)
#define TABLEBODY_CLASS(cls) TAG_CLASS(TTableBody, cls)
#define TABLER() TAG(TTableR)
#define TABLER_CLASS(cls) TAG_CLASS(TTableR, cls)
#define TABLED() TAG(TTableD)
#define TABLED_CLASS(cls) TAG_CLASS(TTableD, cls)
#define TABLED_ATTRS(...) TAG_ATTRS(TTableD, ##__VA_ARGS__)
#define TABLEH() TAG(TTableH)
#define TABLEH_CLASS(cls) TAG_CLASS(TTableH, cls)
#define FORM() TAG(TFormC)
#define FORM_CLASS(cls) TAG_CLASS(TFormC, cls)
#define LABEL() TAG(TLabelC)
#define LABEL_CLASS(cls) TAG_CLASS(TLabelC, cls)
#define LABEL_CLASS_FOR(cls, for0) TAG_CLASS_FOR(TLabelC, cls, for0)
#define SPAN_CLASS(cls) TAG_CLASS(TSpanC, cls)
#define SPAN_CLASS_STYLE(cls, style) TAG_CLASS_STYLE(TSpanC, cls, style)

#define PARA() TAG(TPara)
#define PARA_CLASS(cls) TAG_CLASS(TPara, cls)

#define H1_CLASS(cls) TAG_CLASS(TH1, cls)
#define H2_CLASS(cls) TAG_CLASS(TH2, cls)
#define H3_CLASS(cls) TAG_CLASS(TH3, cls)
#define H4_CLASS(cls) TAG_CLASS(TH4, cls)
#define H5_CLASS(cls) TAG_CLASS(TH5, cls)
#define H6_CLASS(cls) TAG_CLASS(TH6, cls)

#define SMALL() TAG(TSMALL)
#define STRONG() TAG(TSTRONG)

#define LI() TAG(TLIST)
#define LI_CLASS(cls) TAG_CLASS(TLIST, cls)
#define UL() TAG(TULIST)
#define UL_CLASS(cls) TAG_CLASS(TULIST, cls)
#define OL() TAG(TOLIST)
#define OL_CLASS(cls) TAG_CLASS(TOLIST, cls)

#define DL() TAG(DLIST)
#define DL_CLASS(cls) TAG_CLASS(DLIST, cls)
#define DT() TAG(DTERM)
#define DT_CLASS(cls) TAG_CLASS(DTERM, cls)
#define DD() TAG(DDESC)
#define DD_CLASS(cls) TAG_CLASS(DDESC, cls)

#define CAPTION() TAG(TCaption)
#define CAPTION_CLASS(cls) CAPTION_CLASS(TCaption, cls)

#define HTML_OUTPUT_PARAM(str, param) str << #param << ": " << param << "<br/>"
#define HTML_OUTPUT_TIME_PARAM(str, param) str << #param << ": " << ToStringLocalTimeUpToSeconds(param) << "<br/>"

#define COLLAPSED_BUTTON_CONTENT(targetId, buttonText) \
    WITH_SCOPED(tmp, ::NMonitoring::TCollapsedButton(__stream, targetId, buttonText))

#define HREF(path) \
    WITH_SCOPED(tmp, ::NMonitoring::THref(__stream, path))

namespace NMonitoring {
    struct THref {
        THref(IOutputStream& str, TStringBuf path)
            : Str(str)
        {
            Str << "<a href="<< path << '>';
        }

        ~THref() {
            Str <<  "</a>";
        }

        explicit inline operator bool() const noexcept {
            return true; // just to work with WITH_SCOPED
        }

        IOutputStream& Str;
    };

    template <const char* tag>
    struct TTag {
        TTag(IOutputStream& str, TStringBuf cls = "", TStringBuf for0 = "", TStringBuf id = "")
            : Str(str)
        {
            Str << "<" << tag;

            if (!cls.empty()) {
                Str << " class=\"" << cls << "\"";
            }

            if (!for0.empty()) {
                Str << " for=\"" << for0 << "\"";
            }

            if (!id.empty()) {
                Str << "id=\"" << id << "\"";
            }
            Str << ">";
        }

        TTag(IOutputStream& str, std::initializer_list<std::pair<TStringBuf, TStringBuf>> attributes)
            : Str(str)
        {
            Str << "<" << tag;
            for (const std::pair<TStringBuf, TStringBuf>& attr : attributes) {
                if (!attr.second.empty()) {
                    Str << ' ' << attr.first << "=\"" << attr.second << "\"";
                }
            }
            Str << ">";
        }

        ~TTag() {
            try {
                Str << "</" << tag << ">";
            } catch (...) {
            }
        }

        explicit inline operator bool() const noexcept {
            return true; // just to work with WITH_SCOPED
        }

        IOutputStream& Str;
    };

    // a nice class for creating collapsable regions of html output
    struct TCollapsedButton {
        TCollapsedButton(IOutputStream& str, const TString& targetId, const TString& buttonText)
            : Str(str)
        {
            Str << "<button type='button' class='btn' data-toggle='collapse' data-target='#" << targetId << "'>"
                << buttonText << "</button>";
            Str << "<div id='" << targetId << "' class='collapse'>";
        }

        ~TCollapsedButton() {
            try {
                Str << "</div>";
            } catch (...) {
            }
        }

        explicit inline operator bool() const noexcept {
            return true; // just to work with WITH_SCOPED
        }

        IOutputStream& Str;
    };

    struct TOutputStreamRef {
        TOutputStreamRef(IOutputStream& str)
            : Str(str)
        {
        }

        inline operator IOutputStream&() noexcept {
            return Str;
        }

        explicit inline operator bool() const noexcept {
            return true; // just to work with WITH_SCOPED
        }

        IOutputStream& Str;
    };

    extern const char HtmlTag[5];
    extern const char HeadTag[5];
    extern const char BodyTag[5];
    extern const char DivTag[4];
    extern const char TableTag[6];
    extern const char TableHeadTag[6];
    extern const char TableBodyTag[6];
    extern const char TableRTag[3];
    extern const char TableDTag[3];
    extern const char TableHTag[3];
    extern const char FormTag[5];
    extern const char LabelTag[6];
    extern const char SpanTag[5];
    extern const char CaptionTag[8];
    extern const char PreTag[4];
    extern const char ParaTag[2];
    extern const char H1Tag[3];
    extern const char H2Tag[3];
    extern const char H3Tag[3];
    extern const char H4Tag[3];
    extern const char H5Tag[3];
    extern const char H6Tag[3];
    extern const char SmallTag[6];
    extern const char StrongTag[7];
    extern const char ListTag[3];
    extern const char UListTag[3];
    extern const char OListTag[3];
    extern const char DListTag[3];
    extern const char DTermTag[3];
    extern const char DDescTag[3];
    extern const char InputTag[6];

    typedef TTag<HtmlTag> THtml;
    typedef TTag<HeadTag> THead;
    typedef TTag<BodyTag> TBody;
    typedef TTag<DivTag> TDiv;
    typedef TTag<TableTag> TTable;
    typedef TTag<TableHeadTag> TTableHead;
    typedef TTag<TableBodyTag> TTableBody;
    typedef TTag<TableRTag> TTableR;
    typedef TTag<TableDTag> TTableD;
    typedef TTag<TableHTag> TTableH;
    typedef TTag<FormTag> TFormC;
    typedef TTag<LabelTag> TLabelC;
    typedef TTag<SpanTag> TSpanC;
    typedef TTag<CaptionTag> TCaption;
    typedef TTag<PreTag> TPre;
    typedef TTag<ParaTag> TPara;
    typedef TTag<H1Tag> TH1;
    typedef TTag<H2Tag> TH2;
    typedef TTag<H3Tag> TH3;
    typedef TTag<H4Tag> TH4;
    typedef TTag<H5Tag> TH5;
    typedef TTag<H6Tag> TH6;
    typedef TTag<SmallTag> TSMALL;
    typedef TTag<StrongTag> TSTRONG;
    typedef TTag<ListTag> TLIST;
    typedef TTag<UListTag> TULIST;
    typedef TTag<OListTag> TOLIST;
    typedef TTag<DListTag> DLIST;
    typedef TTag<DTermTag> DTERM;
    typedef TTag<DDescTag> DDESC;
    typedef TTag<InputTag> TInput;
}
