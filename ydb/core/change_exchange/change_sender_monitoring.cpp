#include "change_sender_monitoring.h"

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/split.h>

namespace NKikimr::NChangeExchange {

void Panel(IOutputStream& str, std::function<void(IOutputStream&)> title, std::function<void(IOutputStream&)> body) {
    HTML(str) {
        DIV_CLASS("panel panel-default") {
            DIV_CLASS("panel-heading") {
                H4_CLASS("panel-title") {
                    title(str);
                }
            }
            body(str);
        }
    }
}

void SimplePanel(IOutputStream& str, const TStringBuf title, std::function<void(IOutputStream&)> body) {
    auto titleRenderer = [&title](IOutputStream& str) {
        HTML(str) {
            str << title;
        }
    };

    auto bodyRenderer = [body = std::move(body)](IOutputStream& str) {
        HTML(str) {
            DIV_CLASS("panel-body") {
                body(str);
            }
        }
    };

    Panel(str, std::move(titleRenderer), std::move(bodyRenderer));
}

void CollapsedPanel(IOutputStream& str, const TStringBuf title, const TStringBuf targetId,
        std::function<void(IOutputStream&)> body)
{
    auto titleRenderer = [&title, &targetId](IOutputStream& str) {
        HTML(str) {
            str << "<a data-toggle='collapse' href='#" << targetId << "'>"
                << title
            << "</a>";
        }
    };

    auto bodyRenderer = [&targetId, body = std::move(body)](IOutputStream& str) {
        HTML(str) {
            str << "<div id='" << targetId << "' class='collapse'>";
            DIV_CLASS("panel-body") {
                body(str);
            }
            str << "</div>";
        }
    };

    Panel(str, std::move(titleRenderer), std::move(bodyRenderer));
}

template <typename P, typename D>
static bool TryGetNext(TStringBuf& s, D delim, P& param) {
    TMaybe<TStringBuf> buf;
    GetNext(s, delim, buf);
    if (!buf) {
        return false;
    }

    return TryFromString(*buf, param);
}

TPathId ParsePathId(TStringBuf str) {
    ui64 ownerId;
    ui64 localPathId;

    if (!TryGetNext(str, ':', ownerId) || !TryGetNext(str, ':', localPathId)) {
        return {};
    }

    return TPathId(ownerId, localPathId);
}

TString TabletPath(ui64 tabletId) {
    return Sprintf("app?TabletID=%" PRIu64, tabletId);
}

void PathLink(IOutputStream& str, const TPathId& pathId) {
    const TString path = TStringBuilder() << "app"
        << "?TabletID=" << pathId.OwnerId
        << "&Page=" << "PathInfo"
        << "&OwnerPathId=" << pathId.OwnerId
        << "&LocalPathId=" << pathId.LocalPathId;
    Link(str, path, pathId);
}

void ActorLink(IOutputStream& str, ui64 tabletId, const TPathId& pathId, const TMaybe<ui64>& partitionId) {
    auto path = TStringBuilder() << "app"
        << "?TabletID=" << tabletId
        << "&page=" << "change-sender"
        << "&pathId=" << pathId.OwnerId << ":" << pathId.LocalPathId;

    if (partitionId) {
        path << "&partitionId=" << partitionId;
    }

    Link(str, path, "Viewer");
}

}
