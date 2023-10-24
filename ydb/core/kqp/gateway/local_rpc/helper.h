#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

namespace NKikimr::NKqp {

NYql::IKikimrGateway::TGenericResult GenericResultFromSyncOperation(const Ydb::Operations::Operation& op);

}
