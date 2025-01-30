#include "saver.h"

namespace NKikimr::NArrow::NAccessor {

TColumnSaver::TColumnSaver(NArrow::NTransformation::ITransformer::TPtr transformer, const NArrow::NSerialization::TSerializerContainer serializer)
    : Transformer(transformer)
    , Serializer(serializer)
{
    Y_ABORT_UNLESS(Serializer);
}

bool TColumnSaver::IsHardPacker() const {
    return Serializer->IsHardPacker();
}

TString TColumnSaver::Apply(std::shared_ptr<arrow::Array> data, std::shared_ptr<arrow::Field> field) const {
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{field});
    auto batch = arrow::RecordBatch::Make(schema, data->length(), {data});
    return Apply(batch);
}

TString TColumnSaver::Apply(const std::shared_ptr<arrow::RecordBatch>& data) const {
    Y_ABORT_UNLESS(Serializer);
    NArrow::NSerialization::TSerializerContainer serializer = Serializer;
    if (SerializerBySizeUpperBorder.size()) {
        auto it = SerializerBySizeUpperBorder.lower_bound(data->num_rows());
        if (it != SerializerBySizeUpperBorder.end()) {
            serializer = it->second;
        }
    }
    if (Transformer) {
        return serializer->SerializeFull(Transformer->Transform(data));
    } else {
        return serializer->SerializePayload(data);
    }
}

}