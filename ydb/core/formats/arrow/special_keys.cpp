#include "special_keys.h"
#include <ydb/core/formats/arrow/serializer/full.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NArrow {

bool TSpecialKeys::DeserializeFromString(const TString& data) {
    if (!data) {
        return false;
    }
    Data = NArrow::TStatusValidator::GetValid(NArrow::NSerialization::TFullDataDeserializer().Deserialize(data));
    return !!Data;
}

std::optional<NKikimr::NArrow::TReplaceKey> TSpecialKeys::GetKeyByIndex(const ui32 position, const std::shared_ptr<arrow::Schema>& schema) const {
    Y_VERIFY(position < Data->num_rows());
    if (schema) {
        return NArrow::TReplaceKey::FromBatch(Data, schema, position);
    } else {
        return NArrow::TReplaceKey::FromBatch(Data, position);
    }
}

TString TSpecialKeys::SerializeToString() const {
    return NArrow::NSerialization::TFullDataSerializer(arrow::ipc::IpcWriteOptions::Defaults()).Serialize(Data);
}

TFirstLastSpecialKeys::TFirstLastSpecialKeys(std::shared_ptr<arrow::RecordBatch> batch, const std::vector<TString>& columnNames /*= {}*/) {
    Y_VERIFY(batch);
    Y_VERIFY(batch->num_rows());
    std::shared_ptr<arrow::RecordBatch> keyBatch = batch;
    if (columnNames.size()) {
        keyBatch = NArrow::ExtractColumns(batch, columnNames);
    }
    std::vector<bool> bits(batch->num_rows(), false);
    bits[0] = true;
    bits[batch->num_rows() - 1] = true;

    auto filter = NArrow::TColumnFilter(std::move(bits)).BuildArrowFilter(batch->num_rows());
    Data = NArrow::TStatusValidator::GetValid(arrow::compute::Filter(keyBatch, filter)).record_batch();
    Y_VERIFY(Data->num_rows() == 1 || Data->num_rows() == 2);
}

TFirstLastSpecialKeys::TFirstLastSpecialKeys(const TString& data)
    : TBase(data)
{
    Y_VERIFY(Data);
    Y_VERIFY_DEBUG(Data->ValidateFull().ok());
    Y_VERIFY(Data->num_rows() == 1 || Data->num_rows() == 2);
}

}
