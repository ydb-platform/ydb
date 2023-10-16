#pragma once

#include "yql_ydb_provider.h"

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/providers/common/transform/yql_exec.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IGraphTransformer> CreateYdbIODiscoveryTransformer(TYdbState::TPtr state);
THolder<IGraphTransformer> CreateYdbLoadTableMetadataTransformer(TYdbState::TPtr state, NYdb::TDriver driver);

THolder<TVisitorTransformerBase> CreateYdbDataSourceTypeAnnotationTransformer(TYdbState::TPtr state);
THolder<TVisitorTransformerBase> CreateYdbDataSinkTypeAnnotationTransformer(TYdbState::TPtr state);

THolder<TExecTransformerBase> CreateYdbDataSinkExecTransformer(TYdbState::TPtr state);

THolder<IGraphTransformer> CreateYdbLogicalOptProposalTransformer(TYdbState::TPtr state);
THolder<IGraphTransformer> CreateYdbPhysicalOptProposalTransformer(TYdbState::TPtr state);
THolder<IGraphTransformer> CreateYdbSourceCallableExecutionTransformer(TYdbState::TPtr state);

void MetaToYson(const TString& cluster, const TString& table,  TYdbState::TPtr state, NYson::TYsonWriter& writer);

class TYdbKey {
public:
    enum class Type {
        Table,
        TableList,
        TableScheme,
        Role
    };

public:
    TYdbKey() = default;

    Type GetKeyType() const {
        return *KeyType;
    }

    std::string_view GetTablePath() const {
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Table || KeyType == Type::TableScheme);
        return Target;
    }

    std::string_view GetFolderPath() const {
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::TableList);
        return Target;
    }

    const std::optional<std::string_view>& GetView() const {
        return View;
    }

    bool Extract(const TExprNode& key, TExprContext& ctx);

private:
    std::optional<Type> KeyType;
    std::string_view Target;
    std::optional<std::string_view> View;
};

} // namespace NYql
