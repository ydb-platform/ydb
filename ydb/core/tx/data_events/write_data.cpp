#include "write_data.h"
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/library/actors/core/log.h>


namespace NKikimr::NEvWrite {

TWriteData::TWriteData(const TWriteMeta& writeMeta, IDataContainer::TPtr data, const std::shared_ptr<arrow::Schema>& primaryKeySchema, const std::shared_ptr<NOlap::IBlobsWritingAction>& blobsAction)
    : WriteMeta(writeMeta)
    , Data(data)
    , PrimaryKeySchema(primaryKeySchema)
    , BlobsAction(blobsAction)
{
    Y_ABORT_UNLESS(Data);
    Y_ABORT_UNLESS(PrimaryKeySchema);
    Y_ABORT_UNLESS(BlobsAction);
}

}
