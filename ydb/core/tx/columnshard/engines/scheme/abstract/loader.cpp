#include "loader.h"
#include <ydb/core/formats/arrow/common/validation.h>

namespace NKikimr::NOlap {

TString TColumnLoader::DebugString() const {
    TStringBuilder result;
    if (ExpectedSchema) {
        result << "schema:" << ExpectedSchema->ToString() << ";";
    }
    if (Transformer) {
        result << "transformer:" << Transformer->DebugString() << ";";
    }
    if (Serializer) {
        result << "serializer:" << Serializer->DebugString() << ";";
    }
    return result;
}

TColumnLoader::TColumnLoader(NArrow::NTransformation::ITransformer::TPtr transformer, const NArrow::NSerialization::TSerializerContainer& serializer,
    const std::shared_ptr<arrow::Schema>& expectedSchema, const ui32 columnId)
    : Transformer(transformer)
    , Serializer(serializer)
    , ExpectedSchema(expectedSchema)
    , ColumnId(columnId) {
    Y_ABORT_UNLESS(ExpectedSchema);
    auto fieldsCountStr = ::ToString(ExpectedSchema->num_fields());
    Y_ABORT_UNLESS(ExpectedSchema->num_fields() == 1, "%s", fieldsCountStr.data());
    Y_ABORT_UNLESS(Serializer);
}

const std::shared_ptr<arrow::Field>& TColumnLoader::GetField() const {
    return ExpectedSchema->field(0);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> TColumnLoader::Apply(const TString& data) const {
    Y_ABORT_UNLESS(Serializer);
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> columnArray =
        Transformer ? Serializer->Deserialize(data) : Serializer->Deserialize(data, ExpectedSchema);
    if (!columnArray.ok()) {
        return columnArray;
    }
    if (Transformer) {
        return Transformer->Transform(*columnArray);
    } else {
        return columnArray;
    }
}

std::shared_ptr<arrow::RecordBatch> TColumnLoader::ApplyVerified(const TString& data) const {
    return NArrow::TStatusValidator::GetValid(Apply(data));
}

std::shared_ptr<arrow::Array> TColumnLoader::ApplyVerifiedColumn(const TString& data) const {
    auto rb = ApplyVerified(data);
    AFL_VERIFY(rb->num_columns() == 1)("schema", rb->schema()->ToString());
    return rb->column(0);
}

}