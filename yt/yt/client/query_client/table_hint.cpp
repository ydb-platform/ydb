#include "table_hint.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TTableHint::Register(TRegistrar registrar)
{
    registrar.Parameter("require_sync_replica", &TThis::RequireSyncReplica)
        .Default(true);
    registrar.Parameter("push_down_group_by", &TThis::PushDownGroupBy)
        .Default(false);
}

void FormatValue(TStringBuilderBase* builder, const TTableHint& hint, TStringBuf /*spec*/)
{
    builder->AppendString("\"{");
    if (hint.PushDownGroupBy) {
        builder->AppendString("push_down_group_by=%true;");
    }
    if (!hint.RequireSyncReplica) {
        builder->AppendString("require_sync_replica=%false;");
    }
    builder->AppendString("}\"");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
