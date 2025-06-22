#include "control.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/control/immediate_control_board_impl.h>

namespace {

struct TControls {
    std::shared_ptr<NKikimr::TControlWrapper> MergeReads;

    TControls() {
        if (auto *appData = NKikimr::AppData()) {
            if (appData->Icb) {
                MergeReads = std::make_shared<NKikimr::TControlWrapper>(0, 0, 1);
                appData->Icb->RegisterSharedControl(*MergeReads,
                    "TableServiceControls.EnableMergeDatashardReads");
            }
        }

    }
};

}

namespace NKikimr::NKqp {

std::shared_ptr<TControlWrapper> MergeDatashardReadsControl() {
    return Singleton<TControls>()->MergeReads;
}

} // namespace NKikimr::NKqp
