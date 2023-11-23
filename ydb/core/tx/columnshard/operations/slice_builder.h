#pragma once
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NOlap {

class TBuildSlicesTask: public NConveyor::ITask {
private:
    std::shared_ptr<IBlobsWritingAction> Action;
    NEvWrite::TWriteData WriteData;
    const ui64 TabletId;
    const NActors::TActorId ParentActorId;
    std::optional<std::vector<NArrow::TSerializedBatch>> BuildSlices();
    std::shared_ptr<arrow::Schema> PrimaryKeySchema;
protected:
    virtual bool DoExecute() override;
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::ConstructBlobs::Slices";
    }

    TBuildSlicesTask(const ui64 tabletId, const NActors::TActorId parentActorId, const std::shared_ptr<IBlobsWritingAction>& action,
        const NEvWrite::TWriteData& writeData, const std::shared_ptr<arrow::Schema>& primaryKeySchema)
        : Action(action)
        , WriteData(writeData)
        , TabletId(tabletId)
        , ParentActorId(parentActorId)
        , PrimaryKeySchema(primaryKeySchema)
    {
        Y_ABORT_UNLESS(Action);
    }
};
}
