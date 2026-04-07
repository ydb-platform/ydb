#pragma once

#include <ydb/core/fq/libs/ydb/session.h>

namespace NFq {

ISession::TPtr CreateLocalSession();

} // namespace NFq
