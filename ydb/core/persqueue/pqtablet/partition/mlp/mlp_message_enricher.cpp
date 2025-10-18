#include "mlp_message_enricher.h"

namespace NKikimr::NPQ::NMLP {

TMessageEnricherActor::TMessageEnricherActor(const TActorId& partitionActor, std::deque<TReadResult>&& replies)
    : PartitionActorId(partitionActor)
    , Replies(std::move(replies))
{
}


} // namespace NKikimr::NPQ::NMLP
