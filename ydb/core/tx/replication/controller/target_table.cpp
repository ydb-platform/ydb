#include "target_table.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication::NController {

TTargetTable::TTargetTable(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, ETargetKind::Table, id, config)
{
}

TString TTargetTable::BuildStreamPath() const {
    return CanonizePath(ChildPath(SplitPath(GetSrcPath()), GetStreamName()));
}

TTargetIndexTable::TTargetIndexTable(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, ETargetKind::IndexTable, id, config)
{
}

TString TTargetIndexTable::BuildStreamPath() const {
    return CanonizePath(ChildPath(SplitPath(GetSrcPath()), {"indexImplTable", GetStreamName()}));
}

}
