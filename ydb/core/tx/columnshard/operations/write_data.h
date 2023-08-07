#pragma once

#include <ydb/core/tx/ev_write/write_data.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/protos/ev_write.pb.h>


namespace NKikimr::NColumnShard {

class IPayloadData {
public:
    virtual TString GetDataFromPayload(const ui64 index) const = 0;
    virtual ui64 AddDataToPayload(TString&& blobData) = 0;
    virtual ~IPayloadData() {}
};


template <class TEvent>
class TPayloadHelper : public IPayloadData {
    TEvent& Event;
public:
    TPayloadHelper(TEvent& ev)
        : Event(ev) {}

    TString GetDataFromPayload(const ui64 index) const override {
        TRope rope = Event.GetPayload(index);
        TString data = TString::Uninitialized(rope.GetSize());
        rope.Begin().ExtractPlainDataAndAdvance(data.Detach(), data.size());
        return data;
    }

    ui64 AddDataToPayload(TString&& blobData) override {
        TRope rope;
        rope.Insert(rope.End(), TRope(blobData));
        return Event.AddPayload(std::move(rope));
    }
};

class TArrowData : public NEvWrite::IDataContainer {
public:
    TArrowData(const NOlap::ISnapshotSchema::TPtr& schema)
        : IndexSchema(schema)
    {}

    const TString& GetData() const override {
        return IncomingData;
    }

    bool Parse(const NKikimrDataEvents::TOperationData& proto, const IPayloadData& payload);
    std::shared_ptr<arrow::RecordBatch> GetArrowBatch() const override;
    ui64 GetSchemaVersion() const override;

private:
    NOlap::ISnapshotSchema::TPtr IndexSchema;
    NOlap::ISnapshotSchema::TPtr BatchSchema;
    TString IncomingData;
};

class TProtoArrowData : public NEvWrite::IDataContainer {
public:
    TProtoArrowData(const NOlap::ISnapshotSchema::TPtr& schema)
        : IndexSchema(schema)
    {}

    const TString& GetData() const override {
        return IncomingData;
    }

    bool ParseFromProto(const NKikimrTxColumnShard::TEvWrite& proto);
    std::shared_ptr<arrow::RecordBatch> GetArrowBatch() const override;
    ui64 GetSchemaVersion() const override;

private:
    NOlap::ISnapshotSchema::TPtr IndexSchema;
    std::shared_ptr<arrow::Schema> ArrowSchema;
    TString IncomingData;
};

}
