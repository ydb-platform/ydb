#pragma once

#include <ydb/core/fq/libs/ydb/session.h>

namespace NFq {

ISession::TPtr CreateSdkSession(NYdb::NTable::TSession session);

} // namespace NFq
