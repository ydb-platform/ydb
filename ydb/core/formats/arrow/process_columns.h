#pragma once
#include <ydb/library/conclusion/result.h>
#include <ydb/core/formats/arrow/protos/fields.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NArrow {

class TSchemaSubset {
private:
    std::vector<ui32> FieldIdx;
    TString FieldBits;
    bool Exclude = false;
public:
    TSchemaSubset() = default;
    TSchemaSubset(const std::set<ui32>& fieldsIdx, const ui32 fieldsCount) {
        AFL_VERIFY(fieldsIdx.size() <= fieldsCount);
        AFL_VERIFY(fieldsIdx.size());
        if (fieldsCount == fieldsIdx.size()) {
            return;
        }
        Exclude = (fieldsCount - fieldsIdx.size()) < fieldsIdx.size();
        if (!Exclude) {
            FieldIdx = std::vector<ui32>(fieldsIdx.begin(), fieldsIdx.end());
        } else {
            auto it = fieldsIdx.begin();
            for (ui32 i = 0; i < fieldsCount; ++i) {
                if (it == fieldsIdx.end() || i < *it) {
                    FieldIdx.emplace_back(i);
                } else if (*it == i) {
                    ++it;
                } else {
                    AFL_VERIFY(false);
                }
            }
        }
    }

    template <class T>
    std::vector<T> Apply(const std::vector<T>& fullSchema) const {
        if (FieldIdx.empty()) {
            return fullSchema;
        }
        std::vector<T> fields;
        if (!Exclude) {
            for (auto&& i : FieldIdx) {
                AFL_VERIFY(i < fullSchema->num_fields());
                fields.emplace_back(fullSchema->field(i));
            }
        } else {
            auto it = FieldIdx.begin();
            for (ui32 i = 0; i < fullSchema->num_fields(); ++i) {
                if (it == FieldIdx.end() || i < *it) {
                    fields.emplace_back(fullSchema->field(i));
                } else if (i == *it) {
                    ++it;
                } else {
                    AFL_VERIFY(false);
                }
            }
        }
        return fields;
    }

    NKikimrArrowSchema::TSchemaSubset SerializeToProto() const {
        NKikimrArrowSchema::TSchemaSubset result;
        result.MutableList()->SetExcludeFlag(Exclude);
        for (auto&& i : FieldIdx) {
            result.MutableList()->AddFieldsIdx(i);
        }
        return result;
    }

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrArrowSchema::TSchemaSubset& proto) {
        if (!proto.HasList()) {
            return TConclusionStatus::Fail("no schema subset data");
        }
        Exclude = result.MutableList()->GetExcludeFlag();
        std::vector<ui32> fieldIdx;
        for (auto&& i : proto.GetList().GetFieldsIdx()) {
            fieldIdx.emplace_back(i);
        }
        std::swap(fieldIdx, FieldIdx);
        return TConclusionStatus::Success();
    }
};

class TColumnOperator {
public:
    enum class EExtractProblemsPolicy {
        Null,
        Verify,
        Skip
    };
private:
    EExtractProblemsPolicy AbsentColumnPolicy = EExtractProblemsPolicy::Verify;

public:
    TColumnOperator& NullIfAbsent() {
        AbsentColumnPolicy = EExtractProblemsPolicy::Null;
        return *this;
    }

    TColumnOperator& VerifyIfAbsent() {
        AbsentColumnPolicy = EExtractProblemsPolicy::Verify;
        return *this;
    }

    TColumnOperator& SkipIfAbsent() {
        AbsentColumnPolicy = EExtractProblemsPolicy::Skip;
        return *this;
    }

    std::shared_ptr<arrow::RecordBatch> Extract(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames);
    std::shared_ptr<arrow::Table> Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames);
    std::shared_ptr<arrow::RecordBatch> Extract(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames);
    std::shared_ptr<arrow::Table> Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames);

    TConclusion<std::shared_ptr<arrow::RecordBatch>> Adapt(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, const TSchemaSubset* subset);
    TConclusion<std::shared_ptr<arrow::Table>> Adapt(const std::shared_ptr<arrow::Table>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, const TSchemaSubset* subset);

    TConclusion<std::shared_ptr<arrow::RecordBatch>> Reorder(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames);
    TConclusion<std::shared_ptr<arrow::Table>> Reorder(const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames);
    TConclusion<std::shared_ptr<arrow::RecordBatch>> Reorder(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames);
    TConclusion<std::shared_ptr<arrow::Table>> Reorder(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames);
};

}