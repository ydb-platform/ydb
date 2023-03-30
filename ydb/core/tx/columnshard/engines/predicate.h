#pragma once

#include "defs.h"

#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/formats/program.h>
#include <ydb/core/formats/switch_type.h>

namespace NKikimr::NOlap {

struct TPredicate {
    using EOperation = NArrow::EOperation;

    EOperation Operation{EOperation::Unspecified};
    std::shared_ptr<arrow::RecordBatch> Batch;
    bool Inclusive;

    bool Empty() const { return Batch.get() == nullptr; }
    bool Good() const { return !Empty() && Batch->num_columns() && Batch->num_rows() == 1; }
    bool IsFrom() const { return Operation == EOperation::Greater || Operation == EOperation::GreaterEqual; }
    bool IsTo() const { return Operation == EOperation::Less || Operation == EOperation::LessEqual; }

    TVector<TString> ColumnNames() const {
        TVector<TString> out;
        out.reserve(Batch->num_columns());
        for (auto& field : Batch->schema()->fields()) {
            TString name(field->name().data(), field->name().size());
            out.emplace_back(name);
        }
        return out;
    }

    std::string ToString() const {
        return Empty() ? "()" : Batch->schema()->ToString();
    }

    TPredicate() = default;

    TPredicate(EOperation op, std::shared_ptr<arrow::RecordBatch> batch, bool inclusive = false)
        : Operation(op)
        , Batch(batch)
        , Inclusive(inclusive)
    {}

    TPredicate(EOperation op, TString serializedBatch, std::shared_ptr<arrow::Schema> schema, bool inclusive)
        : Operation(op)
        , Inclusive(inclusive)
    {
        if (!serializedBatch.empty()) {
            Batch = NArrow::DeserializeBatch(serializedBatch, schema);
            Y_VERIFY(Batch);
        }
    }

    friend IOutputStream& operator << (IOutputStream& out, const TPredicate& pred) {
        out << NSsa::GetFunctionName(pred.Operation);
        if (pred.Inclusive) {
            out << "(incl) ";
        } else {
            out << "(excl) ";
        }

        for (i32 i = 0; i < pred.Batch->num_columns(); ++i) {
            auto array = pred.Batch->column(i);
            out << pred.Batch->schema()->field(i)->name() << ": ";
            NArrow::SwitchType(array->type_id(), [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

                auto& typedArray = static_cast<const TArray&>(*array);
                if (typedArray.IsNull(0)) {
                    out << "NULL";
                } else {
                    auto value = typedArray.GetView(0);
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, arrow::util::string_view>) {
                        out << "'" << std::string(value.data(), value.size()) << "'";
                    } else {
                        out << "'" << value << "'";
                    }
                }
                return true;
            });
            out << " ";
        }

        return out;
    }
};

}
