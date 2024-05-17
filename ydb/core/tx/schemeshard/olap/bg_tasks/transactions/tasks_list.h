#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/olap/bg_tasks/protos/data.pb.h>
#include <ydb/core/tx/schemeshard/olap/bg_tasks/events/global.h>

namespace NKikimr::NSchemeShard::NBackground {

struct TTxTasksList: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TSchemeShard>;
    ui32 PageSize = 10;
    ui32 PageIdx = 1;
    const NActors::TActorId SenderId;
    const ui64 RequestCookie;
    const TString DatabaseName;
    NKikimrSchemeShardTxBackgroundProto::TEvListResponse ProtoResponse;
public:
    explicit TTxTasksList(TSelf* self, TEvListRequest::TPtr& ev);

    virtual bool Execute(NTabletFlatExecutor::TTransactionContext&, const TActorContext&) override;

    virtual void Complete(const TActorContext&) override;
};

}