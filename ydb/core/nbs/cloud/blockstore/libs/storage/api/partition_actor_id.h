#pragma once

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////
// TActorId::ToString()/Parse() lose the executor pool id, which is packed into
// the high bits of the node field. A User-pool tablet actor id therefore does
// not survive that round trip and is delivered to the wrong pool's mailbox.
// Use these helpers for the partition IO endpoint id.

TString SerializePartitionActorId(const NActors::TActorId& actorId);

bool TryDeserializePartitionActorId(TStringBuf str, NActors::TActorId& actorId);

}   // namespace NYdb::NBS::NBlockStore
