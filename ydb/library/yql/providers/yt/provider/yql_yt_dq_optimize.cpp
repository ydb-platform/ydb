#include "yql_yt_dq_optimize.h"
#include "yql_yt_helpers.h"
#include "yql_yt_optimize.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/common/dq/yql_dq_optimization_impl.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h>


namespace NYql {

using namespace NNodes;

class TYtDqOptimizers: public TDqOptimizationBase {
public:
    TYtDqOptimizers(TYtState::TPtr state)
        : State_(state)
    {
    }

    TExprNode::TPtr RewriteRead(const TExprNode::TPtr& read, TExprContext& ctx) override {
        if (auto disabledOpts = State_->Configuration->DisableOptimizers.Get(); disabledOpts && disabledOpts->contains(TStringBuilder() << "YtDqOptimizers-" << __FUNCTION__)) {
            return read;
        }
        auto ytReadTable = TYtReadTable(read);

        TYtDSource dataSource = GetDataSource(ytReadTable, ctx);
        if (!State_->Configuration->_EnableYtPartitioning.Get(dataSource.Cluster().StringValue()).GetOrElse(false)) {
            return read;
        }

        TSyncMap syncList;
        auto ret = OptimizeReadWithSettings(read, false, false, syncList, State_, ctx);
        YQL_ENSURE(syncList.empty());
        if (ret && ret != read) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;
        }
        return ret;
    }

    TExprNode::TPtr RewriteLookupRead(const TExprNode::TPtr& read, TExprContext& ctx) override {
        const auto ytReadTable = TYtReadTable(read);
        //Presume that there is the only table for read
        const auto ytSections = ytReadTable.Input();
        YQL_ENSURE(ytSections.Size() == 1);
        const auto ytPaths =  ytSections.Item(0).Paths();
        YQL_ENSURE(ytPaths.Size() == 1);
        const auto ytTable = ytPaths.Item(0).Table().Maybe<TYtTable>();
        YQL_ENSURE(ytTable);
        //read is of type: Tuple<World, InputSeq>
        const auto inputSeqType = read->GetTypeAnn()->Cast<TTupleExprType>()->GetItems().at(1);
        return Build<TDqLookupSourceWrap>(ctx, read->Pos())
            .Input(ytTable.Cast())
            .DataSource(ytReadTable.DataSource().Ptr())
            .RowType(ExpandType(read->Pos(), *GetSeqItemType(inputSeqType), ctx))
            .Settings(ytTable.Cast().Settings())
        .Done().Ptr();
    }

    TExprNode::TPtr ApplyExtractMembers(const TExprNode::TPtr& read, const TExprNode::TPtr& members, TExprContext& ctx) override {
        if (auto disabledOpts = State_->Configuration->DisableOptimizers.Get(); disabledOpts && disabledOpts->contains(TStringBuilder() << "YtDqOptimizers-" << __FUNCTION__)) {
            return read;
        }
        auto ytReadTable = TYtReadTable(read);

        TVector<TYtSection> sections;
        for (auto section: ytReadTable.Input()) {
            sections.push_back(UpdateInputFields(section, TExprBase(members), ctx));
        }
        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;
        return Build<TYtReadTable>(ctx, ytReadTable.Pos())
            .InitFrom(ytReadTable)
            .Input()
                .Add(sections)
            .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr ApplyTakeOrSkip(const TExprNode::TPtr& read, const TExprNode::TPtr& countBase, TExprContext& ctx) override {
        if (auto disabledOpts = State_->Configuration->DisableOptimizers.Get(); disabledOpts && disabledOpts->contains(TStringBuilder() << "YtDqOptimizers-" << __FUNCTION__)) {
            return read;
        }
        auto ytReadTable = TYtReadTable(read);
        auto count = TCoCountBase(countBase);

        if (ytReadTable.Input().Size() != 1) {
            return read;
        }

        TYtDSource dataSource = GetDataSource(ytReadTable, ctx);
        TString cluster = dataSource.Cluster().StringValue();

        if (!State_->Configuration->_EnableYtPartitioning.Get(cluster).GetOrElse(false)) {
            return read;
        }

        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(count.Count().Ref(), syncList, cluster, false)) {
            return read;
        }

        TYtSection section = ytReadTable.Input().Item(0);
        if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Sample)) {
            return read;
        }
        if (AnyOf(section.Paths(), [](const auto& path) { TYtPathInfo pathInfo(path); return (pathInfo.Table->Meta && pathInfo.Table->Meta->IsDynamic) || pathInfo.Ranges; })) {
            return read;
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;
        EYtSettingType settingType = count.Maybe<TCoSkip>() ? EYtSettingType::Skip : EYtSettingType::Take;
        return Build<TYtReadTable>(ctx, ytReadTable.Pos())
            .InitFrom(ytReadTable)
            .Input()
                    .Add()
                        .InitFrom(section)
                        .Settings(NYql::AddSetting(section.Settings().Ref(), settingType, count.Count().Ptr(), ctx))
                    .Build()
            .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr ApplyUnordered(const TExprNode::TPtr& read, TExprContext& ctx) override {
        if (auto disabledOpts = State_->Configuration->DisableOptimizers.Get(); disabledOpts && disabledOpts->contains(TStringBuilder() << "YtDqOptimizers-" << __FUNCTION__)) {
            return read;
        }
        auto ytReadTable = TYtReadTable(read);

        TExprNode::TListType sections(ytReadTable.Input().Size());
        for (auto i = 0U; i < sections.size(); ++i) {
            sections[i] = MakeUnorderedSection<true>(ytReadTable.Input().Item(i), ctx).Ptr();
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;
        return Build<TYtReadTable>(ctx, ytReadTable.Pos())
            .InitFrom(ytReadTable)
            .Input()
                .Add(std::move(sections))
            .Build()
            .Done().Ptr();
    }

    TExprNode::TListType ApplyExtend(const TExprNode::TListType& listOfRead, bool /*ordered*/, TExprContext& ctx) override {
        if (auto disabledOpts = State_->Configuration->DisableOptimizers.Get(); disabledOpts && disabledOpts->contains(TStringBuilder() << "YtDqOptimizers-" << __FUNCTION__)) {
            return listOfRead;
        }

        // Group readres by cluster/settings
        THashMap<std::pair<TStringBuf, TExprNode::TPtr>, std::vector<std::pair<size_t, TYtReadTable>>> reads;
        for (size_t i = 0; i < listOfRead.size(); ++i) {
            const auto child = listOfRead[i];
            auto ytReadTable = TYtReadTable(child);
            if (ytReadTable.Input().Size() != 1) {
                continue;
            }
            if (NYql::HasAnySetting(ytReadTable.Input().Item(0).Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip)) {
                continue;
            }
            reads[std::make_pair(ytReadTable.DataSource().Cluster().Value(), ytReadTable.Input().Item(0).Settings().Ptr())].emplace_back(i, ytReadTable);
        }

        if (reads.empty() || AllOf(reads, [](const auto& item) { return item.second.size() < 2; })) {
            return listOfRead;
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;

        TExprNode::TListType newListOfRead = listOfRead;
        for (auto& item: reads) {
            if (item.second.size() > 1) {
                TSyncMap syncList;
                TVector<TYtPath> paths;
                for (auto& r: item.second) {
                    if (!r.second.World().Ref().IsWorld()) {
                        syncList.emplace(r.second.World().Ptr(), syncList.size());
                    }
                    paths.insert(paths.end(), r.second.Input().Item(0).Paths().begin(), r.second.Input().Item(0).Paths().end());
                }
                const auto ndx = item.second.front().first;
                const auto& firstYtRead = item.second.front().second;
                newListOfRead[ndx] = Build<TYtReadTable>(ctx, firstYtRead.Pos())
                    .InitFrom(firstYtRead)
                    .World(ApplySyncListToWorld(ctx.NewWorld(firstYtRead.Pos()), syncList, ctx))
                    .Input()
                        .Add()
                            .Paths()
                                .Add(paths)
                            .Build()
                            .Settings(firstYtRead.Input().Item(0).Settings())
                        .Build()
                    .Build()
                    .Done().Ptr();

                for (size_t i = 1; i < item.second.size(); ++i) {
                    newListOfRead[item.second[i].first] = nullptr;
                }
            }
        }

        newListOfRead.erase(std::remove(newListOfRead.begin(), newListOfRead.end(), TExprNode::TPtr{}), newListOfRead.end());
        YQL_ENSURE(!newListOfRead.empty());

        return newListOfRead;
    }

private:
    TYtState::TPtr State_;
};

THolder<IDqOptimization> CreateYtDqOptimizers(TYtState::TPtr state) {
    Y_ABORT_UNLESS(state);
    return MakeHolder<TYtDqOptimizers>(state);
}

}
