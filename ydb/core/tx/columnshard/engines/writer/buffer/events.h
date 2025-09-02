#pragma once

#include <ydb/core/formats/arrow/process_columns.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/operations/common/context.h>
#include <ydb/core/tx/data_events/write_data.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/testlib/common/events_scheduling.h>

namespace NKikimr::NOlap::NWritingPortions {

class TEvAddInsertedDataToBuffer
    : public NActors::TEventLocal<TEvAddInsertedDataToBuffer, NColumnShard::TEvPrivate::EEv::EvWritingPortionsAddDataToBuffer> {
private:
    YDB_READONLY_DEF(std::shared_ptr<NEvWrite::TWriteData>, WriteData);
    YDB_READONLY_DEF(NArrow::TContainerWithIndexes<arrow::RecordBatch>, RecordBatch);
    YDB_READONLY_DEF(std::shared_ptr<NOlap::TWritingContext>, Context);

public:
    explicit TEvAddInsertedDataToBuffer(const std::shared_ptr<NEvWrite::TWriteData>& writeData,
        const NArrow::TContainerWithIndexes<arrow::RecordBatch>& recordBatch, const std::shared_ptr<NOlap::TWritingContext>& context)
        : WriteData(writeData)
        , RecordBatch(recordBatch)
        , Context(context) {
        AFL_VERIFY(!!WriteData);
        AFL_VERIFY(!!RecordBatch);
        AFL_VERIFY(!!Context);
    }
};

class TEvFlushBuffer: public NActors::TEventLocal<TEvFlushBuffer, NColumnShard::TEvPrivate::EEv::EvWritingPortionsFlushBuffer> {
private:
    static inline NActors::NTests::TGlobalScheduledEvents::TRegistrator TestScheduledEventRegistrator =
        (ui32)NColumnShard::TEvPrivate::EEv::EvWritingPortionsFlushBuffer;

public:
};

}   // namespace NKikimr::NOlap::NWritingPortions
