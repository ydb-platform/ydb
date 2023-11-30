#include "tablet_list_renderer.h"

#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/tablet/tablet_sys.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <util/generic/algorithm.h>


namespace NKikimr {
namespace NNodeTabletMonitor {

/////////////////////////// TTabletStateClassifier ////////////////////////////

bool TTabletStateClassifier::IsActiveTablet(const NKikimrWhiteboard::TTabletStateInfo& state)
{
    return state.GetState() != NKikimrWhiteboard::TTabletStateInfo::Dead;
}

bool TTabletStateClassifier::IsDeadTablet(const NKikimrWhiteboard::TTabletStateInfo& state)
{
    return !IsActiveTablet(state);
}

ui32 TTabletStateClassifier::GetMaxTabletStateClass() const
{
    return TABLET_STATE_CLASS_MAX;
}

std::function<bool(const NKikimrWhiteboard::TTabletStateInfo&)> TTabletStateClassifier::GetTabletStateClassFilter(ui32 cls) const
{
    switch(cls) {
    case ACTIVE_TABLETS:
        return &IsActiveTablet;
    case DEAD_TABLETS:
        return &IsDeadTablet;
    default:
        Y_ABORT("Bad tablet state class");
        return nullptr;
    }
}

TString TTabletStateClassifier::GetTabletStateClassName(ui32 cls)
{
    switch(cls) {
    case ACTIVE_TABLETS:
        return "Active tablets";
    case DEAD_TABLETS:
        return "Dead tablets";
    default:
        Y_ABORT("Bad tablet state class");
        return "";
    }
}

///////////////////////////// TTabletListRenderer /////////////////////////////

void TTabletListRenderer::RenderHeader(TStringStream& str,
                  const TString& listName,
                  const TVector<TTabletListElement>& tabletsToRender,
                  const TTabletFilterInfo& filterInfo)
{
    Y_UNUSED(tabletsToRender);
    IOutputStream &__stream(str);
    TAG(TH3) {
        str << listName;
        if (filterInfo.FilterNodeId != 0) {
            str << " of Node " << filterInfo.FilterNodeId;
            if (!filterInfo.FilterNodeHost.empty())
                str << " (" << filterInfo.FilterNodeHost << ")";
        }
    }
}

void TTabletListRenderer::RenderTableHeader(TStringStream& str,
                       const TString& listName,
                       const TVector<TTabletListElement>& tabletsToRender,
                       const TTabletFilterInfo& filterInfo)
{
    Y_UNUSED(listName);
    Y_UNUSED(tabletsToRender);
    IOutputStream &__stream(str);
    TABLEHEAD() {
        TABLER() {
            if (filterInfo.FilterNodeId == 0) {
                TABLEH() {str << "<span data-toggle='tooltip' title='NodeId'>#</span>";}
                TABLEH() {str << "NodeName";}
            }
            TABLEH() {str << "TabletType";}
            TABLEH() {str << "TabletID";}
            TABLEH_CLASS("sorter-text") {str << "CreateTime";}
            TABLEH_CLASS("sorter-text") {str << "ChangeTime";}
            TABLEH() {str << "Core state";}
            TABLEH() {str << "User state";}
            TABLEH() {str << "Gen";}
            TABLEH_CLASS("sorter-false") {str << "Kill";}
        }
    }
}

TString TTabletListRenderer::MakeTabletMonURL(const TTabletListElement& elem,
                                             const TTabletFilterInfo& filterInfo)
{
    Y_UNUSED(filterInfo);
    TStringStream str;
    str << "tablets?TabletID="<< elem.TabletStateInfo->GetTabletId();
    return str.Str();
}

TString  TTabletListRenderer::GetUserStateName(const TTabletListElement& elem)
{
    if (elem.TabletStateInfo->HasUserState()) {
        return ToString(elem.TabletStateInfo->GetUserState());
    } else {
        //  user part has not not reported its state
        return "unknown";
    }
}

void TTabletListRenderer::RenderTableBody(TStringStream& str,
                     const TString& listName,
                     const TVector<TTabletListElement>& tabletsToRender,
                     const TTabletFilterInfo& filterInfo)
{
    Y_UNUSED(listName);
    IOutputStream &__stream(str);
    TABLEBODY() {
    for (const auto& elem : tabletsToRender) {
        const auto& ti = *elem.TabletStateInfo;
        const auto& nodeInfo = *elem.NodeInfo;
        auto index = elem.TabletIndex;
        TTabletTypes::EType tabletType = (TTabletTypes::EType)ti.GetType();
        TString tabletTypeStr = TTabletTypes::TypeToStr(tabletType);
        TString nodeName = ToString(nodeInfo.NodeId);

        TABLER() {
            if (filterInfo.FilterNodeId == 0) {
                TString nodeName = nodeInfo.Host.empty() ? nodeInfo.Address : nodeInfo.Host;
                TString tooltip = nodeInfo.Address;
                TABLED() {str << ToString(nodeInfo.NodeId);}
                TABLED() {str << "<span data-html='true' data-toggle='tooltip' title='" + tooltip + "'>" << nodeName << "</span>";}
            }
            TABLED() {str << tabletTypeStr;}
            str << "<td data-text='" << index << "'>"
                << "<a href=\"" << MakeTabletMonURL(elem, filterInfo) << "\">" << ti.tabletid() << "</a>"
                << "</td>";
            TABLED() {str << TInstant::MilliSeconds(ti.GetCreateTime()).ToStringUpToSeconds();}
            TABLED() {str << TInstant::MilliSeconds(ti.GetChangeTime()).ToStringUpToSeconds();}
            TABLED() {str << GetStateName((ETabletState)ti.GetState());}
            TABLED() {str << GetUserStateName(elem);}
            TABLED() {str << ti.generation();}
            TABLED() {
                str << "<a href=\"nodetabmon?action=kill_tablet&tablet_id=" << ti.tabletid() << "&node_id=" << nodeInfo.NodeId;
                if (filterInfo.FilterNodeId != 0)
                    str << "&filter_node_id=" << filterInfo.FilterNodeId;
                str << "\">"
                    << "<span class=\"glyphicon glyphicon-remove\" title=\"Restart Tablet\"/>"
                    << "</span></a>";
            }
        }
    }
    }
}

TString TTabletListRenderer::GetStateName(ETabletState state) {
    return NKikimrWhiteboard::TTabletStateInfo::ETabletState_Name(state);
}

void TTabletListRenderer::RenderPageHeader(TStringStream& str)
{
    Y_UNUSED(str);
}

void TTabletListRenderer::RenderPageFooter(TStringStream& str)
{
    Y_UNUSED(str);
}

void TTabletListRenderer::RenderTabletList(TStringStream& str,
                                           const TString& listName,
                                           const TVector<TTabletListElement>& tabletsToRender,
                                           const TTabletFilterInfo& filterInfo)
{
    IOutputStream &__stream(str);
    RenderHeader(str, listName, tabletsToRender, filterInfo);

    TABLE_SORTABLE_CLASS("table") {
        RenderTableHeader(str, listName, tabletsToRender, filterInfo);
        RenderTableBody(str, listName, tabletsToRender, filterInfo);
    }
}

} // namespace NNodeTabletMonitor
} // namespace NKikimr
