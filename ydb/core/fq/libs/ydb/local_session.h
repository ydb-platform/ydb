#pragma once

#include <ydb/core/fq/libs/ydb/session.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/core/fq/libs/ydb/session.h>
#include <ydb/core/fq/libs/ydb/query_actor.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/core/fq/libs/actors/logging/log.h>

namespace NFq {

ISession::TPtr CreateLocalSession();

} // namespace NFq
