#include "gorilla.h"
#include "stream.h"
#include "parsing.h"
#include <ydb/core/formats/arrow/dictionary/conversion.h>
#include <ydb/core/formats/arrow/common/validation.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>

namespace NKikimr::NArrow::NSerialization {
    TString TGorillaSerializer::DoSerializeFull(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        // TODO: ???
        return DoSerializePayload(batch);
    }

    TString TGorillaSerializer::DoSerializePayload(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        return serializeSingleColumnBatch(batch);
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> TGorillaSerializer::DoDeserialize(const TString& data) const {
        return deserializeSingleColumnBatch(data);
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> TGorillaSerializer::DoDeserialize(const TString& data, const std::shared_ptr<arrow::Schema>& schema) const {
        // TODO: ???
        (void) schema;
        return DoDeserialize(data);
    }

    NKikimr::TConclusionStatus TGorillaSerializer::DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) {
        // TODO: ???
        return TConclusionStatus::Success();
    }

    NKikimr::TConclusionStatus TGorillaSerializer::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapColumn::TSerializer& proto) {
        // TOOD: ???
        return TConclusionStatus::Success();
    }

    void TGorillaSerializer::DoSerializeToProto(NKikimrSchemeOp::TOlapColumn::TSerializer& proto) const { }

}
