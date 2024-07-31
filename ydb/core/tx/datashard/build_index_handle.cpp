#include "build_index.h"
// #include "check_constraints.h"

namespace NKikimr::NDataShard {
class TDataShard::TTxHandleSafeBuildIndexScan: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeBuildIndexScan(TDataShard* self, TEvDataShard::TEvBuildIndexCreateRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) {
        // const auto& record = Ev->Get()->Record;

        // if (record.GetCheckingNotNullSettings().size() > 0) {

        // } else {
            Self->HandleSafe(Ev, ctx);
        // }

        return true;
    }

    void Complete(const TActorContext&) {
        // nothing
    }

private:
    TEvDataShard::TEvBuildIndexCreateRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvBuildIndexCreateRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeBuildIndexScan(this, std::move(ev)));
}
} // namespace NKikimr::NDataShard 