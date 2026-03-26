#include "saver.h"

namespace NKikimr::NArrow::NAccessor {

TColumnSaver::TColumnSaver(const NArrow::NSerialization::TSerializerContainer serializer)
    : Serializer(serializer)
{
    Y_ABORT_UNLESS(Serializer);
}

bool TColumnSaver::IsHardPacker() const {
    return Serializer->IsHardPacker();
}

TString TColumnSaver::Apply(std::shared_ptr<arrow20::Array> data, std::shared_ptr<arrow20::Field> field) const {
    auto schema = std::make_shared<arrow20::Schema>(arrow20::FieldVector{field});
    auto batch = arrow20::RecordBatch::Make(schema, data->length(), {data});
    return Apply(batch);
}

TString TColumnSaver::Apply(const std::shared_ptr<arrow20::RecordBatch>& data) const {
    Y_ABORT_UNLESS(Serializer);
    NArrow::NSerialization::TSerializerContainer serializer = Serializer;
    if (SerializerBySizeUpperBorder.size()) {
        auto it = SerializerBySizeUpperBorder.lower_bound(data->num_rows());
        if (it != SerializerBySizeUpperBorder.end()) {
            serializer = it->second;
        }
    }
    return serializer->SerializePayload(data);
}

}