#include "data.h"
#include "data_uncertain.h"
#include "mon_main.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    void TData::RenderMainPage(IOutputStream& s) {
        HTML(s) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                   s << "Main";
                }
                DIV_CLASS("panel-body") {
                    KEYVALUE_TABLE({
                        KEYVALUE_P("Loaded", Loaded ? "true" : "false");
                        KEYVALUE_P("Data size, number of keys", Data.size());
                        KEYVALUE_P("RefCount size, number of blobs", RefCount.size());
                        KEYVALUE_P("Total stored data size, bytes", FormatByteSize(TotalStoredDataSize));
                        KEYVALUE_P("Keys made certain, number of keys", KeysMadeCertain.size());
                    })
                }
            }
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                   s << "Debugging";
                }
                DIV_CLASS("panel-body") {
                    KEYVALUE_TABLE({
                        KEYVALUE_P("LoadRestartTx", LoadRestartTx);
                        KEYVALUE_P("LoadRunSuccessorTx", LoadRunSuccessorTx);
                        KEYVALUE_P("LoadProcessingCycles", CyclesToDuration(LoadProcessingCycles));
                        KEYVALUE_P("LoadFinishTxCycles", CyclesToDuration(LoadFinishTxCycles));
                        KEYVALUE_P("LoadRestartTxCycles", CyclesToDuration(LoadRestartTxCycles));
                        KEYVALUE_P("LoadRunSuccessorTxCycles", CyclesToDuration(LoadRunSuccessorTxCycles));
                        KEYVALUE_P("LoadTotalCycles", CyclesToDuration(LoadTotalCycles));
                    })
                }
            }
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    s << "Uncertainty resolver";
                }
                DIV_CLASS("panel-body") {
                    UncertaintyResolver->RenderMainPage(s);
                }
            }
        }
    }

} // NKikimr::NBlobDepot
