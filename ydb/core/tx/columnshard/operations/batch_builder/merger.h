#pragma once
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NOlap {

class IMerger {
public:
    using TYdbConclusionStatus = TConclusionSpecialStatus<Ydb::StatusIds::StatusCode, Ydb::StatusIds::SUCCESS, Ydb::StatusIds::BAD_REQUEST>;
private:
    NArrow::NMerger::TRWSortableBatchPosition IncomingPosition;

    virtual TYdbConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& incoming) = 0;
    virtual TYdbConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& incoming) = 0;
protected:
    std::shared_ptr<ISnapshotSchema> Schema;
    NArrow::TContainerWithIndexes<arrow::RecordBatch> IncomingData;
    bool IncomingFinished = false;
public:
    IMerger(const NArrow::TContainerWithIndexes<arrow::RecordBatch>& incoming, const std::shared_ptr<ISnapshotSchema>& actualSchema)
        : IncomingPosition(incoming.GetContainer(), 0, actualSchema->GetPKColumnNames(), incoming->schema()->field_names(), false)
        , Schema(actualSchema)
        , IncomingData(incoming) {
        IncomingFinished = !IncomingPosition.InitPosition(0);
    }

    virtual ~IMerger() = default;

    virtual NArrow::TContainerWithIndexes<arrow::RecordBatch> BuildResultBatch() = 0;

    TYdbConclusionStatus Finish();

    TYdbConclusionStatus AddExistsDataOrdered(const std::shared_ptr<arrow::Table>& data);
};

struct TSortingKeyMapping {
    struct TItem {
        TString Name;
        TString Type;
        TString Value;
    };

    std::vector<TItem> Items;

    static TSortingKeyMapping Build(const std::shared_ptr<ISnapshotSchema>& schema,
        const NArrow::NMerger::TSortableBatchPosition& position)
    {
        TSortingKeyMapping mapping;
        auto cursor = position.BuildSortingCursor();
        const auto pkNames = schema->GetPKColumnNames();
        const auto& pkColumns = schema->GetIndexInfo().GetPrimaryKeyColumns();
        std::vector<std::shared_ptr<arrow::Field>> pkFields;
        pkFields.reserve(pkNames.size());
        const auto& allFields = schema->GetSchema()->fields();
        for (auto&& name : pkNames) {
            std::shared_ptr<arrow::Field> found;
            for (const auto& f : allFields) {
                if (f->name() == name) {
                    found = f;
                    break;
                }
            }

            if (found) {
                pkFields.emplace_back(found);
            }
        }

        auto rb = cursor.ExtractSortingPosition(pkFields);
        AFL_VERIFY(pkColumns.size() == (size_t)rb->num_columns());
        for (int i = 0; i < rb->num_columns(); ++i) {
            TItem item;
            item.Name = pkFields[i]->name();
            const auto& arr = rb->column(i);
            if (pkColumns[i].second.GetTypeId() == NScheme::NTypeIds::Bool) {
                auto sres = arr->GetScalar(0);
                if (sres.ok()) {
                    TString s = (*sres)->ToString();
                    if (s == "0") {
                        item.Value = "false";
                    } else if (s == "1") {
                        item.Value = "true";
                    } else {
                        item.Value = s;
                    }
                } else {
                    item.Value = sres.status().ToString();
                }

                item.Type = "bool";
            } else {
                auto sres = arr->GetScalar(0);
                if (sres.ok()) {
                    item.Value = (*sres)->ToString();
                } else {
                    item.Value = sres.status().ToString();
                }

                item.Type = pkFields[i]->type()->ToString();
            }

            mapping.Items.emplace_back(std::move(item));
        }

        return mapping;
    }

    NJson::TJsonValue ToJson() const {
        NJson::TJsonValue json = NJson::JSON_ARRAY;
        for (const auto& i : Items) {
            NJson::TJsonValue itemJson = NJson::JSON_MAP;
            itemJson.InsertValue("name", i.Name);
            itemJson.InsertValue("type", i.Type);
            itemJson.InsertValue("value", i.Value);
            json.AppendValue(itemJson);
        }
        return json;
    }
};

class TInsertMerger: public IMerger {
private:
    using TBase = IMerger;
    virtual TYdbConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        const auto mapping = TSortingKeyMapping::Build(Schema, exists);
        return TYdbConclusionStatus::Fail(Ydb::StatusIds::PRECONDITION_FAILED,
            TStringBuilder() << "Conflict with existing key. " << mapping.ToJson().GetStringRobust());
    }
    virtual TYdbConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        return TYdbConclusionStatus::Success();
    }
public:
    using TBase::TBase;
    virtual NArrow::TContainerWithIndexes<arrow::RecordBatch> BuildResultBatch() override {
        return IncomingData;
    }
};

class TReplaceMerger: public IMerger {
private:
    using TBase = IMerger;
    NArrow::TColumnFilter Filter = NArrow::TColumnFilter::BuildDenyFilter();
    virtual TYdbConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& /*exists*/, const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        Filter.Add(true);
        return TYdbConclusionStatus::Success();
    }
    virtual TYdbConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        Filter.Add(false);
        return TYdbConclusionStatus::Success();
    }
public:
    using TBase::TBase;

    virtual NArrow::TContainerWithIndexes<arrow::RecordBatch> BuildResultBatch() override {
        auto result = IncomingData;
        Filter.Apply(result.MutableContainer());
        return result;
    }
};

class TUpdateMerger: public IMerger {
private:
    using TBase = IMerger;
    NArrow::NMerger::TRecordBatchBuilder Builder;
    std::vector<std::optional<ui32>> IncomingColumnRemap;
    std::vector<std::shared_ptr<arrow::BooleanArray>> HasIncomingDataFlags;
    const std::optional<NArrow::NMerger::TSortableBatchPosition> DefaultExists;
    const TString InsertDenyReason;
    virtual TYdbConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& incoming) override;
    virtual TYdbConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& incoming) override {
        if (!!InsertDenyReason) {
            return TYdbConclusionStatus::Fail("insertion is impossible: " + InsertDenyReason);
        }
        if (!DefaultExists) {
            return TYdbConclusionStatus::Success();
        } else {
            return OnEqualKeys(*DefaultExists, incoming);
        }
    }
public:
    virtual NArrow::TContainerWithIndexes<arrow::RecordBatch> BuildResultBatch() override;

    TUpdateMerger(const NArrow::TContainerWithIndexes<arrow::RecordBatch>& incoming, const std::shared_ptr<ISnapshotSchema>& actualSchema,
        const TString& insertDenyReason, const std::optional<NArrow::NMerger::TSortableBatchPosition>& defaultExists = {});
};

}
