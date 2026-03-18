#include "schemeshard_impl.h"
#include "schemeshard_set_column_constraint.h"

namespace NKikimr {
namespace NSchemeShard {

void TSchemeShard::Handle(TEvSetColumnConstraint::TEvCreateRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCreateSetColumnConstraint(ev), ctx);
}

} // NSchemeShard
} // NKikimr
