#include "helper.h"

namespace  NKikimr::NOlap::NEngines::NTest {

NKikimrTxColumnShard::TLogicalMetadata TLocalHelper::GetMetaProto() {
    NKikimrTxColumnShard::TLogicalMetadata result;
    result.SetDirtyWriteTimeSeconds(TInstant::Now().Seconds());
    return result;
}

}