#include "write_data.h"
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NEvWrite {

TWriteData::TWriteData(const TWriteMeta& writeMeta, IDataContainer::TPtr data)
    : WriteMeta(writeMeta)
    , Data(data)
{}

}
