#include "event_util.h"
#include "logging.h"
#include "stream_consumer_remover.h"
#include "target_table.h"
#include "util.h"

#include <ydb/core/base/path.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NController {


TTargetTableBase::TTargetTableBase(TReplication* replication, ETargetKind finalKind,
        ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, finalKind, id, config)
{
}

TTargetTable::TTargetTable(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetTableBase(replication, ETargetKind::Table, id, config)
{
}

TString TTargetTable::BuildStreamPath() const {
    return CanonizePath(ChildPath(SplitPath(GetSrcPath()), GetStreamName()));
}

TString TTargetTableBase::GetStreamPath() const {
    return BuildStreamPath();
}

TTargetIndexTable::TTargetIndexTable(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetTableBase(replication, ETargetKind::IndexTable, id, config)
{
}

TString TTargetIndexTable::BuildStreamPath() const {
    return CanonizePath(ChildPath(SplitPath(GetSrcPath()), {"indexImplTable", GetStreamName()}));
}

}
