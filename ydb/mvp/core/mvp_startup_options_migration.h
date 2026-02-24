#pragma once

#include <ydb/mvp/core/protos/mvp.pb.h>

namespace NMVP {

void MigrateJwtInfoToOAuth2ExchangeIfNeeded(NMvp::TTokensConfig* tokens, NMvp::EAccessServiceType accessServiceType);

} // namespace NMVP
