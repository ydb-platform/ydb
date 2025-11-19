#include "bulk_upsert.h"

#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>

#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NKqp {

TConclusionStatus TBulkUpsertCommand::DoExecute(TKikimrRunner& kikimr) {
    if (ArrowBatch->num_rows() < PartsCount) {
        return TConclusionStatus::Fail(
            "not enough records(" + ::ToString(ArrowBatch->num_rows()) + ") for split in " + ::ToString(PartsCount) + " chunks");
    }
    ui32 cursor = 0;
    for (ui32 i = 0; i < PartsCount; ++i) {
        const ui32 size = (i + 1 != PartsCount) ? (ArrowBatch->num_rows() / PartsCount) : (ArrowBatch->num_rows() - cursor);
        TLocalHelper lHelper(kikimr);
        lHelper.SendDataViaActorSystem(TableName, ArrowBatch->Slice(cursor, size), ExpectedCode);
        cursor += size;
    }
    AFL_VERIFY(cursor == ArrowBatch->num_rows());
    return TConclusionStatus::Success();
}

TConclusionStatus TBulkUpsertCommand::DoDeserializeProperties(const TPropertiesCollection& props) {
    if (props.GetFreeArgumentsCount() != 2) {
        return TConclusionStatus::Fail("incorrect free arguments count for BULK_UPSERTcommand");
    }
    TableName = props.GetFreeArgumentVerified(0);
    ArrowBatch = NArrow::TStatusValidator::GetValid(NArrow::NSerialization::TNativeSerializer().Deserialize(Base64Decode(props.GetFreeArgumentVerified(1))));
    if (auto value = props.GetOptional("EXPECT_STATUS")) {
        if (!Ydb::StatusIds_StatusCode_Parse(*value, &ExpectedCode)) {
            return TConclusionStatus::Fail("cannot parse EXPECT_STATUS from " + *value);
        }
    }
    if (auto value = props.GetOptional("PARTS_COUNT")) {
        if (!TryFromString<ui32>(*value, PartsCount)) {
            return TConclusionStatus::Fail("cannot parse PARTS_COUNT from " + *value);
        }
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NKqp
