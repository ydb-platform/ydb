#pragma once

#include <ydb/library/actors/core/event.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <util/generic/ptr.h>

namespace NKikimr::NDataShard::EvWrite {

using namespace NActors;

class Convertor {
public:
    static ui64 GetTxId(const TAutoPtr<IEventHandle>& ev);
    static ui64 GetProposeFlags(NKikimrDataEvents::TEvWrite::ETxMode txMode);
    static NKikimrDataEvents::TEvWrite::ETxMode GetTxMode(ui64 flags);
    static NKikimrTxDataShard::TEvProposeTransactionResult::EStatus GetStatus(NKikimrDataEvents::TEvWriteResult::EStatus status);
    static NKikimrDataEvents::TEvWriteResult::EStatus ConvertErrCode(NKikimrTxDataShard::TError::EKind code);
};
}