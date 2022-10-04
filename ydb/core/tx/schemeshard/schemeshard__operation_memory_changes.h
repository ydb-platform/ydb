#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_path_element.h"
#include "schemeshard_info_types.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <util/generic/ptr.h>
#include <util/generic/stack.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TMemoryChanges: public TSimpleRefCount<TMemoryChanges> {
    using TPathState = std::pair<TPathId, TPathElement::TPtr>;
    TStack<TPathState> Pathes;

    using TIndexState = std::pair<TPathId, TTableIndexInfo::TPtr>;
    TStack<TIndexState> Indexes;

    using TCdcStreamState = std::pair<TPathId, TCdcStreamInfo::TPtr>;
    TStack<TCdcStreamState> CdcStreams;

    using TTableSnapshotState = std::pair<TPathId, TTxId>;
    TStack<TTableSnapshotState> TablesWithSnaphots;

    using TTableState = std::pair<TPathId, TTableInfo::TPtr>;
    TStack<TTableState> Tables;

    using TShardState = std::pair<TShardIdx, THolder<TShardInfo>>;
    TStack<TShardState> Shards;

    using TSubDomainState = std::pair<TPathId, TSubDomainInfo::TPtr>;
    TStack<TSubDomainState> SubDomains;

    using TTxState = std::pair<TOperationId, THolder<TTxState>>;
    TStack<TTxState> TxStates;

public:
    ~TMemoryChanges() = default;

    void GrabNewTxState(TSchemeShard* ss, const TOperationId& op);

    void GrabNewPath(TSchemeShard* ss, const TPathId& pathId);
    void GrabPath(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewTable(TSchemeShard* ss, const TPathId& pathId);
    void GrabTable(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewShard(TSchemeShard* ss, const TShardIdx& shardId);
    void GrabShard(TSchemeShard* ss, const TShardIdx& shardId);

    void GrabDomain(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewIndex(TSchemeShard* ss, const TPathId& pathId);
    void GrabIndex(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewCdcStream(TSchemeShard* ss, const TPathId& pathId);

    void GrabTableSnapshot(TSchemeShard* ss, const TPathId& pathId, TTxId snapshotTxId);

    void UnDo(TSchemeShard* ss);
};

}
