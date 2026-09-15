#include "mon_render_local_db.h"

#include "mon_util.h"

#include <library/cpp/monlib/service/pages/templates.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

void RenderProtoDump(
    IOutputStream& str,
    const char* name,
    const std::optional<TString>& dump)
{
    if (!dump) {
        str << "<div class='pd-block'>" << name << " (none)</div>";
        return;
    }
    // display:list-item brings back the fold triangle that the page CSS
    // hides; the pointer marks the line as clickable.
    str << "<details class='pd-details'>"
           "<summary class='pd-summary'>"
        << name << "</summary><pre>" << HtmlEscape(*dump) << "</pre></details>";
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RenderLocalDb(IOutputStream& str, const TLocalDbContents& db)
{
    HTML (str) {
        TAG (TH3) {
            str << "Local DB";
        }
        RenderProtoDump(str, "VolumeConfig", db.VolumeConfig);
        RenderProtoDump(
            str,
            "DirectBlockGroupsConnections",
            db.DirectBlockGroupsConnections);
        RenderProtoDump(str, "AddHostInProgress", db.AddHostInProgress);
        TAG (TH4) {
            str << "VChunkConfigs (persisted overrides)";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "VChunkIndex";
                    }
                    TABLEH () {
                        str << "Config";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& [vChunkIndex, config]: db.VChunkConfigs) {
                    TABLER () {
                        TABLED () {
                            str << config.GetVChunkIndex();
                        }
                        TABLED () {
                            str << "<pre>" << HtmlEscape(config.DebugPrint())
                                << "</pre>";
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
