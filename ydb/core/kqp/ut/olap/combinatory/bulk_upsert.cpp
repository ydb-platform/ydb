#include "bulk_upsert.h"

#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>

#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NKqp {

bool TBulkUpsertCommand::DeserializeFromString(const TString& info) {
    auto lines = StringSplitter(info).SplitBySet("\n").SkipEmpty().ToList<TString>();
    if (lines.size() < 2 || lines.size() > 3) {
        return false;
    }
    TableName = Strip(lines[0]);
    ArrowBatch = Base64Decode(Strip(lines[1]));
    AFL_VERIFY(!!ArrowBatch);
    if (lines.size() == 3) {
        if (!Ydb::StatusIds_StatusCode_Parse(Strip(lines[2]), &ExpectedCode)) {
            return false;
        }
        //                if (lines[2] == "SUCCESS") {
        //                } else if (lines[2] = "INTERNAL_ERROR") {
        //                    ExpectedCode = Ydb::StatusIds::INTERNAL_ERROR;
        //                } else if (lines[2] == "BAD_REQUEST") {
        //                    ExpectedCode = Ydb::StatusIds::BAD_REQUEST;
        //                } else {
        //                    return false;
        //                }
    }
    return true;
}

TConclusionStatus TBulkUpsertCommand::DoExecute(TKikimrRunner& kikimr) {
    TLocalHelper lHelper(kikimr);
    lHelper.SendDataViaActorSystem(
        TableName, NArrow::TStatusValidator::GetValid(NArrow::NSerialization::TNativeSerializer().Deserialize(ArrowBatch)), ExpectedCode);
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NKqp
