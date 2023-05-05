#pragma once

#include <ydb/core/base/pathid.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/string/printf.h>

namespace NKikimr::NDataShard {

template <typename T>
void Header(IOutputStream& str, const T& title, ui64 tabletId) {
    HTML(str) {
        DIV_CLASS("page-header") {
            TAG(TH3) {
                str << title;
                SMALL() {
                    str << "&nbsp;";
                    HREF(Sprintf("app?TabletID=%" PRIu64, tabletId)) {
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

void Panel(IOutputStream& str, std::function<void(IOutputStream&)> title, std::function<void(IOutputStream&)> body);
void SimplePanel(IOutputStream& str, const TStringBuf title, std::function<void(IOutputStream&)> body);
void CollapsedPanel(IOutputStream& str, const TStringBuf title, const TStringBuf targetId,
    std::function<void(IOutputStream&)> body);

TPathId ParsePathId(TStringBuf str);
void PathLink(IOutputStream& str, const TPathId& pathId);
void ActorLink(IOutputStream& str, ui64 tabletId, const TPathId& pathId, const TMaybe<ui64>& partitionId = {});

}
