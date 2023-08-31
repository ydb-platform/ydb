#include "abstract.h"
namespace NKikimr::NArrow::NSerialization {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> IDeserializer::Deserialize(const TString& data) const {
    if (!data) {
        return nullptr;
    }
    return DoDeserialize(data);
}

}
