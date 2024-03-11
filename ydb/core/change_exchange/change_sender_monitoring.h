#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/maybe.h>

namespace NKikimr::NChangeExchange {

template <typename T>
static void Link(IOutputStream& str, const TStringBuf path, const T& title) {
    HTML(str) {
        HREF(path) {
            str << title;
        }
    }
}

TString TabletPath(ui64 tabletId);

template <typename T>
void Header(IOutputStream& str, const T& title, ui64 tabletId) {
    HTML(str) {
        DIV_CLASS("page-header") {
            TAG(TH3) {
                str << title;
                SMALL() {
                    str << "&nbsp;";
                    HREF(TabletPath(tabletId)) {
                        str << tabletId;
                    }
                }
            }
        }
    }
}

template <typename T>
static void TermDesc(IOutputStream& str, const TStringBuf term, const T& desc) {
    HTML(str) {
        DT() { str << term; }
        DD() { str << desc; }
    }
}

template <typename T>
static void TermDescLink(IOutputStream& str, const TStringBuf term, const T& desc, const TStringBuf path) {
    HTML(str) {
        DT() { str << term; }
        DD() { Link(str, path, desc); }
    }
}

void Panel(IOutputStream& str, std::function<void(IOutputStream&)> title, std::function<void(IOutputStream&)> body);
void SimplePanel(IOutputStream& str, const TStringBuf title, std::function<void(IOutputStream&)> body);
void CollapsedPanel(IOutputStream& str, const TStringBuf title, const TStringBuf targetId,
    std::function<void(IOutputStream&)> body);

TPathId ParsePathId(TStringBuf str);
void PathLink(IOutputStream& str, const TPathId& pathId);
void ActorLink(IOutputStream& str, ui64 tabletId, const TPathId& pathId, const TMaybe<ui64>& partitionId = {});

}
