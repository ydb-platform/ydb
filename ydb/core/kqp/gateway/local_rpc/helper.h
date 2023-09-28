#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

namespace NKikimr {
namespace NKqp {

NYql::IKikimrGateway::TGenericResult GenericResultFromSyncOperation(const Ydb::Operations::Operation& op);

using TAlterTableRespHandler = std::function<void(const Ydb::Table::AlterTableResponse& r)>;
void DoAlterTableSameMailbox(Ydb::Table::AlterTableRequest&& req, TAlterTableRespHandler&& cb,
    const TString& database, const TMaybe<TString>& token, const TMaybe<TString>& type);

}
}
