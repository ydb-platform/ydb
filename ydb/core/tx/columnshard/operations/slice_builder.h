#pragma once
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NOlap {

class TBuildSlicesTask: public NConveyor::ITask {
private:
    NEvWrite::TWriteData WriteData;
    const ui64 TabletId;
    const NActors::TActorId ParentActorId;
    const NActors::TActorId BufferActorId;
    std::optional<std::vector<NArrow::TSerializedBatch>> BuildSlices();
protected:
    virtual bool DoExecute() override;
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::ConstructBlobs::Slices";
    }

    TBuildSlicesTask(const ui64 tabletId, const NActors::TActorId parentActorId,
        const NActors::TActorId bufferActorId, NEvWrite::TWriteData&& writeData)
        : WriteData(std::move(writeData))
        , TabletId(tabletId)
        , ParentActorId(parentActorId)
        , BufferActorId(bufferActorId)
    {
    }
};
}
