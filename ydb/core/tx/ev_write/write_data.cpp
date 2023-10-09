#include "write_data.h"
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <library/cpp/actors/core/log.h>


namespace NKikimr::NEvWrite {

TWriteData::TWriteData(const TWriteMeta& writeMeta, IDataContainer::TPtr data)
    : WriteMeta(writeMeta)
    , Data(data)
{
    Y_ABORT_UNLESS(Data);
}

const NKikimr::NEvWrite::IDataContainer& TWriteData::GetData() const {
    AFL_VERIFY(Data);
    return *Data;
}

}
