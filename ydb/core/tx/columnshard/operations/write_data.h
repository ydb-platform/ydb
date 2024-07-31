#pragma once

#include <ydb/core/tx/data_events/write_data.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>


namespace NKikimr::NColumnShard {

class TArrowData : public NEvWrite::IDataContainer {
private:
    std::optional<ui64> OriginalDataSize;
public:
    TArrowData(const NOlap::ISnapshotSchema::TPtr& schema)
        : IndexSchema(schema)
    {}

    bool Parse(const NKikimrDataEvents::TEvWrite::TOperation& proto, const NKikimr::NEvWrite::IPayloadReader& payload);
    virtual TConclusion<std::shared_ptr<arrow::RecordBatch>> ExtractBatch() override;
    ui64 GetSchemaVersion() const override;
    ui64 GetSize() const override {
        Y_ABORT_UNLESS(OriginalDataSize);
        return *OriginalDataSize;
    }

private:
    NOlap::ISnapshotSchema::TPtr IndexSchema;
    NOlap::ISnapshotSchema::TPtr BatchSchema;
    TString IncomingData;
    NEvWrite::EModificationType ModificationType = NEvWrite::EModificationType::Upsert;
};

class TProtoArrowData : public NEvWrite::IDataContainer {
private:
    std::optional<ui64> OriginalDataSize;
    NEvWrite::EModificationType ModificationType = NEvWrite::EModificationType::Replace;
public:
    TProtoArrowData(const NOlap::ISnapshotSchema::TPtr& schema)
        : IndexSchema(schema)
    {}

    bool ParseFromProto(const NKikimrTxColumnShard::TEvWrite& proto);
    virtual TConclusion<std::shared_ptr<arrow::RecordBatch>> ExtractBatch() override;
    ui64 GetSchemaVersion() const override;
    ui64 GetSize() const override {
        Y_ABORT_UNLESS(OriginalDataSize);
        return *OriginalDataSize;
    }

private:
    NOlap::ISnapshotSchema::TPtr IndexSchema;
    std::shared_ptr<arrow::Schema> ArrowSchema;
    TString IncomingData;
};

}
