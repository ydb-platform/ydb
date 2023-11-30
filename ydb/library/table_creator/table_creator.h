#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {

struct TEvTableCreator {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_TABLE_CREATOR),
        EvCreateTableResponse,
    };

    struct TEvCreateTableResponse : public TEventLocal<TEvCreateTableResponse, EvCreateTableResponse> {
    };
};

NActors::IActor* CreateTableCreator(
    TVector<TString> pathComponents,
    TVector<NKikimrSchemeOp::TColumnDescription> columns,
    TVector<TString> keyColumns,
    NKikimrServices::EServiceKikimr logService,
    TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings = Nothing());

} // namespace NKikimr
