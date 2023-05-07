#include "predicate.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/switch_type.h>

namespace NKikimr::NOlap {

TPredicate::TPredicate(EOperation op, std::shared_ptr<arrow::RecordBatch> batch) noexcept
    : Operation(op)
    , Batch(std::move(batch))
{
    Y_VERIFY(IsFrom() || IsTo());
}

TPredicate::TPredicate(EOperation op, const TString& serializedBatch, const std::shared_ptr<arrow::Schema>& schema)
    : Operation(op)
{
    Y_VERIFY(IsFrom() || IsTo());
    if (!serializedBatch.empty()) {
        Batch = NArrow::DeserializeBatch(serializedBatch, schema);
        Y_VERIFY(Batch);
    }
}

TVector<TString> TPredicate::ColumnNames() const {
    TVector<TString> out;
    out.reserve(Batch->num_columns());
    for (const auto& field : Batch->schema()->fields()) {
        out.emplace_back(field->name());
    }
    return out;
}

IOutputStream& operator << (IOutputStream& out, const TPredicate& pred) {
    out << NSsa::GetFunctionName(pred.Operation);

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
                    out << "'" << std::string_view(value.data(), value.size()) << "'";
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

} // namespace NKikimr::NOlap
