#include "yql_yt_dq_optimize.h"
#include "yql_yt_helpers.h"
#include "yql_yt_optimize.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/common/dq/yql_dq_optimization_impl.h>
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

    TExprNode::TPtr RewriteRead(const TExprNode::TPtr& reader, TExprContext& ctx) override {
        if (auto disabledOpts = State_->Configuration->DisableOptimizers.Get(); disabledOpts && disabledOpts->contains(TStringBuilder() << "YtDqOptimizers-" << __FUNCTION__)) {
            return reader;
        }
        auto ytReader = TYtReadTable(reader);

        TYtDSource dataSource = GetDataSource(ytReader, ctx);
        if (!State_->Configuration->_EnableYtPartitioning.Get(dataSource.Cluster().StringValue()).GetOrElse(false)) {
            return reader;
        }

        TSyncMap syncList;
        auto ret = OptimizeReadWithSettings(reader, false, false, syncList, State_, ctx);
        YQL_ENSURE(syncList.empty());
        if (ret && ret != reader) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;
        }
        return ret;
    }

    TExprNode::TPtr ApplyExtractMembers(const TExprNode::TPtr& reader, const TExprNode::TPtr& members, TExprContext& ctx) override {
        if (auto disabledOpts = State_->Configuration->DisableOptimizers.Get(); disabledOpts && disabledOpts->contains(TStringBuilder() << "YtDqOptimizers-" << __FUNCTION__)) {
            return reader;
        }
        auto ytReader = TYtReadTable(reader);

        TVector<TYtSection> sections;
        for (auto section: ytReader.Input()) {
            sections.push_back(UpdateInputFields(section, TExprBase(members), ctx));
        }
        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;
        return Build<TYtReadTable>(ctx, ytReader.Pos())
            .InitFrom(ytReader)
            .Input()
                .Add(sections)
            .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr ApplyTakeOrSkip(const TExprNode::TPtr& reader, const TExprNode::TPtr& countBase, TExprContext& ctx) override {
        if (auto disabledOpts = State_->Configuration->DisableOptimizers.Get(); disabledOpts && disabledOpts->contains(TStringBuilder() << "YtDqOptimizers-" << __FUNCTION__)) {
            return reader;
        }
        auto ytReader = TYtReadTable(reader);
        auto count = TCoCountBase(countBase);

        if (ytReader.Input().Size() != 1) {
            return reader;
        }

        TYtDSource dataSource = GetDataSource(ytReader, ctx);
        TString cluster = dataSource.Cluster().StringValue();

        if (!State_->Configuration->_EnableYtPartitioning.Get(cluster).GetOrElse(false)) {
            return reader;
        }

        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(count.Count().Ref(), syncList, cluster, true, false)) {
            return reader;
        }

        TYtSection section = ytReader.Input().Item(0);
        if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Sample)) {
            return reader;
        }
        if (AnyOf(section.Paths(), [](const auto& path) { TYtPathInfo pathInfo(path); return (pathInfo.Table->Meta && pathInfo.Table->Meta->IsDynamic) || pathInfo.Ranges; })) {
            return reader;
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;
        EYtSettingType settingType = count.Maybe<TCoSkip>() ? EYtSettingType::Skip : EYtSettingType::Take;
        return Build<TYtReadTable>(ctx, ytReader.Pos())
            .InitFrom(ytReader)
            .Input()
                    .Add()
                        .InitFrom(section)
                        .Settings(NYql::AddSetting(section.Settings().Ref(), settingType, count.Count().Ptr(), ctx))
                    .Build()
            .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr ApplyUnordered(const TExprNode::TPtr& reader, TExprContext& ctx) override {
        if (auto disabledOpts = State_->Configuration->DisableOptimizers.Get(); disabledOpts && disabledOpts->contains(TStringBuilder() << "YtDqOptimizers-" << __FUNCTION__)) {
            return reader;
        }
        auto ytReader = TYtReadTable(reader);

        TExprNode::TListType sections(ytReader.Input().Size());
        for (auto i = 0U; i < sections.size(); ++i) {
            sections[i] = MakeUnorderedSection<true>(ytReader.Input().Item(i), ctx).Ptr();
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;
        return Build<TYtReadTable>(ctx, ytReader.Pos())
            .InitFrom(ytReader)
            .Input()
                .Add(std::move(sections))
            .Build()
            .Done().Ptr();
    }

    TExprNode::TListType ApplyExtend(const TExprNode::TListType& listOfReader, bool /*ordered*/, TExprContext& ctx) override {
        if (auto disabledOpts = State_->Configuration->DisableOptimizers.Get(); disabledOpts && disabledOpts->contains(TStringBuilder() << "YtDqOptimizers-" << __FUNCTION__)) {
            return listOfReader;
        }

        // Group readres by cluster/settings
        THashMap<std::pair<TStringBuf, TExprNode::TPtr>, std::vector<std::pair<size_t, TYtReadTable>>> readers;
        for (size_t i = 0; i < listOfReader.size(); ++i) {
            const auto child = listOfReader[i];
            auto ytRead = TYtReadTable(child);
            if (ytRead.Input().Size() != 1) {
                continue;
            }
            if (NYql::HasAnySetting(ytRead.Input().Item(0).Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip)) {
                continue;
            }
            readers[std::make_pair(ytRead.DataSource().Cluster().Value(), ytRead.Input().Item(0).Settings().Ptr())].emplace_back(i, ytRead);
        }

        if (readers.empty() || AllOf(readers, [](const auto& item) { return item.second.size() < 2; })) {
            return listOfReader;
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;

        TExprNode::TListType newListOfReader = listOfReader;
        for (auto& item: readers) {
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
                const auto& firstYtReader = item.second.front().second;
                newListOfReader[ndx] = Build<TYtReadTable>(ctx, firstYtReader.Pos())
                    .InitFrom(firstYtReader)
                    .World(ApplySyncListToWorld(ctx.NewWorld(firstYtReader.Pos()), syncList, ctx))
                    .Input()
                        .Add()
                            .Paths()
                                .Add(paths)
                            .Build()
                            .Settings(firstYtReader.Input().Item(0).Settings())
                        .Build()
                    .Build()
                    .Done().Ptr();

                for (size_t i = 1; i < item.second.size(); ++i) {
                    newListOfReader[item.second[i].first] = nullptr;
                }
            }
        }

        newListOfReader.erase(std::remove(newListOfReader.begin(), newListOfReader.end(), TExprNode::TPtr{}), newListOfReader.end());
        YQL_ENSURE(!newListOfReader.empty());

        return newListOfReader;
    }

private:
    TYtState::TPtr State_;
};

THolder<IDqOptimization> CreateYtDqOptimizers(TYtState::TPtr state) {
    Y_ABORT_UNLESS(state);
    return MakeHolder<TYtDqOptimizers>(state);
}

}
