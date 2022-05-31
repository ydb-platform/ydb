#pragma once

#include "defs.h"

#include <ydb/core/protos/blob_depot.pb.h>

namespace NKikimr {

    struct TEvBlobDepot {
        enum {
            EvApplyConfig = EventSpaceBegin(TKikimrEvents::ES_BLOB_DEPOT),
            EvApplyConfigResult,
        };

        struct TEvApplyConfig : TEventPB<TEvApplyConfig, NKikimrBlobDepot::TEvApplyConfig, TEvBlobDepot::EvApplyConfig> {
            TEvApplyConfig() = default;

            TEvApplyConfig(ui64 txId) {
                Record.SetTxId(txId);
            }
        };

        struct TEvApplyConfigResult : TEventPB<TEvApplyConfigResult, NKikimrBlobDepot::TEvApplyConfigResult, TEvBlobDepot::EvApplyConfigResult> {
            TEvApplyConfigResult() = default;

            TEvApplyConfigResult(ui64 tabletId, ui64 txId) {
                Record.SetTabletId(tabletId);
                Record.SetTxId(txId);
            }
        };

    };

} // NKikimr
