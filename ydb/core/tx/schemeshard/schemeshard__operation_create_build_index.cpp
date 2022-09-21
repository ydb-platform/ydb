#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_path_element.h"
#include "schemeshard_utils.h"

#include "schemeshard_impl.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NSchemeShard {

TVector<ISubOperationBase::TPtr> CreateBuildIndex(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_VERIFY(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild);

    auto config = tx.GetInitiateIndexBuild();
    TString tablePath = config.GetTable();
    const NKikimrSchemeOp::TIndexCreationConfig& indexConfig = config.GetIndex();

    TPath table = TPath::Resolve(tablePath, context.SS);
    TPath index = table.Child(indexConfig.GetName());


    TTableInfo::TPtr tableInfo = context.SS->Tables.at(table.Base()->PathId);

    //check idempotence

    //check limits

    TVector<ISubOperationBase::TPtr> result;

    {
        auto tableIndexCreation = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
        *tableIndexCreation.MutableLockGuard() = tx.GetLockGuard();
        tableIndexCreation.MutableCreateTableIndex()->CopyFrom(indexConfig);

        if (!indexConfig.HasType()) {
            tableIndexCreation.MutableCreateTableIndex()->SetType(NKikimrSchemeOp::EIndexTypeGlobal);
        }
        tableIndexCreation.MutableCreateTableIndex()->SetState(NKikimrSchemeOp::EIndexStateWriteOnly);

        result.push_back(CreateNewTableIndex(NextPartId(nextId, result), tableIndexCreation));
    }

    {
        auto initialize = TransactionTemplate(table.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexMainTable);
        *initialize.MutableLockGuard() = tx.GetLockGuard();
        auto& shapshot = *initialize.MutableInitiateBuildIndexMainTable();
        shapshot.SetTableName(table.LeafName());

        result.push_back(CreateInitializeBuildIndexMainTable(NextPartId(nextId, result), initialize));
    }

    {
        auto indexImplTableCreation = TransactionTemplate(index.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexImplTable);
        NTableIndex::TTableColumns baseTableColumns = NTableIndex::ExtractInfo(tableInfo);
        NTableIndex::TIndexColumns indexKeys = NTableIndex::ExtractInfo(indexConfig);

        TString explain;
        if (!NTableIndex::IsCompatibleIndex(baseTableColumns, indexKeys, explain)) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, explain)};
        }

        NTableIndex::TTableColumns impTableColumns = NTableIndex::CalcTableImplDescription(baseTableColumns, indexKeys);

        auto& indexImplTableDescription = *indexImplTableCreation.MutableCreateTable();
        // This description provided by user to override partition policy
        const auto& userIndexDesc = indexConfig.GetIndexImplTableDescription();
        indexImplTableDescription = NTableIndex::CalcImplTableDesc(tableInfo, impTableColumns, userIndexDesc);

        indexImplTableDescription.MutablePartitionConfig()->MutableCompactionPolicy()->SetKeepEraseMarkers(true);
        indexImplTableDescription.MutablePartitionConfig()->SetShadowData(true);

        result.push_back(CreateInitializeBuildIndexImplTable(NextPartId(nextId, result), indexImplTableCreation));
    }

    return result;
}

}
}
