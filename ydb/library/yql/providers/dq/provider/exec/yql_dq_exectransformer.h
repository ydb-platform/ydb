#pragma once

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <util/generic/ptr.h>

namespace NYql {
    struct TDqState;
    using TDqStatePtr = TIntrusivePtr<TDqState>;

    class ISkiffConverter : public TThrRefBase {
    public:
        struct TYtType {
            TString Type;
            TString SkiffType;
            NYT::TNode RowSpec;
        };

        virtual TString ConvertNodeToSkiff(
            const TDqStatePtr state,
            const IDataProvider::TFillSettings& fillSettings,
            const NYT::TNode& rowSpec, const NYT::TNode& item,
            const TVector<TString>& columns) = 0;
        virtual TYtType ParseYTType(const TExprNode& node, TExprContext& ctx, const TMaybe<NYql::TColumnOrder>& columns) = 0;
    };
    using ISkiffConverterPtr = TIntrusivePtr<ISkiffConverter>;

    IGraphTransformer* CreateDqExecTransformer(const TDqStatePtr& state);

    using TExecTransformerFactory = std::function<IGraphTransformer*(const TDqStatePtr& state)>;
    TExecTransformerFactory CreateDqExecTransformerFactory(const ISkiffConverterPtr& skiffConverter);
} // namespace NYql
