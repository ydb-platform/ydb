#include "kqp_lwtrace_probes.h"

#include <ydb/core/protos/kqp.pb.h>


void TQueryType::ToString(TStoreType value, TString* out) {
    *out = TStringBuilder() << (NKikimrKqp::EQueryType)value;
}

void TQueryAction::ToString(TStoreType value, TString* out) {
    *out = TStringBuilder() << (NKikimrKqp::EQueryAction)value;
}

LWTRACE_DEFINE_PROVIDER(KQP_PROVIDER);
