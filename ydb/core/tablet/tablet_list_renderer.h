#pragma once

#include "node_tablet_monitor.h"
#include <ydb/core/tablet/tablet_sys.h>

namespace NKikimr {
namespace NNodeTabletMonitor {

class TTabletStateClassifier : public ITabletStateClassifier {
private:
    enum ETabletStateClass : ui32 {
        ACTIVE_TABLETS = 0,
        DEAD_TABLETS = 1,
        TABLET_STATE_CLASS_MAX
    };

    static bool IsActiveTablet(const NKikimrWhiteboard::TTabletStateInfo& state);
    static bool IsDeadTablet(const NKikimrWhiteboard::TTabletStateInfo& state);

public:
    virtual ui32 GetMaxTabletStateClass() const override;
    virtual std::function<bool(const NKikimrWhiteboard::TTabletStateInfo&)> GetTabletStateClassFilter(ui32 cls) const override;
    virtual TString GetTabletStateClassName(ui32 cls) override;

};


class TTabletListRenderer : public ITabletListRenderer {
protected:
    using ETabletState = NKikimrWhiteboard::TTabletStateInfo::ETabletState;
    static TString GetStateName(ETabletState state);

    virtual TString MakeTabletMonURL(const TTabletListElement& elem,
                                    const TTabletFilterInfo& filterInfo);

    virtual TString GetUserStateName(const TTabletListElement& elem);

    virtual void RenderHeader(TStringStream& str,
                      const TString& listName,
                      const TVector<TTabletListElement>& tabletsToRender,
                      const TTabletFilterInfo& filterInfo);

    virtual void RenderTableHeader(TStringStream& str,
                           const TString& listName,
                           const TVector<TTabletListElement>& tabletsToRender,
                           const TTabletFilterInfo& filterInfo);

    virtual void RenderTableBody(TStringStream& str,
                         const TString& listName,
                         const TVector<TTabletListElement>& tabletsToRender,
                         const TTabletFilterInfo& filterInfo);

public:
    void RenderPageHeader(TStringStream& str) override;

    void RenderPageFooter(TStringStream& str) override;

    void RenderTabletList(TStringStream& str,
                          const TString& listName,
                          const TVector<TTabletListElement>& tabletsToRender,
                          const TTabletFilterInfo& filterInfo) override;
};

} // namespace NNodeTabletMonitor
} // namespace NKikimr
