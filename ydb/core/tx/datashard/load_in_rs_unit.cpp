#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TLoadInRSUnit : public TExecutionUnit {
public:
    TLoadInRSUnit(TDataShard &dataShard,
                         TPipeline &pipeline);
    ~TLoadInRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TLoadInRSUnit::TLoadInRSUnit(TDataShard &dataShard,
                                           TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::LoadInRS, true, dataShard, pipeline)
{
}

TLoadInRSUnit::~TLoadInRSUnit()
{
}

bool TLoadInRSUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TLoadInRSUnit::Execute(TOperation::TPtr op,
                                               TTransactionContext &txc,
                                               const TActorContext &ctx)
{
    if (op->InReadSets().empty())
        return EExecutionStatus::Executed;

    // Load read sets from local database.
    if (!op->HasLoadedInRSFlag()) {
        if (!Pipeline.LoadInReadSets(op, txc, ctx))
            return EExecutionStatus::Restart;

        Y_ENSURE(op->HasLoadedInRSFlag());
    }

    return EExecutionStatus::Executed;
}

void TLoadInRSUnit::Complete(TOperation::TPtr,
                                    const TActorContext &)
{
}

THolder<TExecutionUnit> CreateLoadInRSUnit(TDataShard &dataShard,
                                                  TPipeline &pipeline)
{
    return THolder(new TLoadInRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
