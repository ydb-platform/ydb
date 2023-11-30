#pragma once

#include "defs.h"

#include <ydb/library/actors/core/actor.h>

/**
 * Lease Holder actor is used to keep dynamic node alive by extending its
 * lease in Node Broker.
 */

namespace NKikimr {
namespace NNodeBroker {

IActor *CreateLeaseHolder(TInstant expire);

} // NNodeBroker
} // NKikimr
