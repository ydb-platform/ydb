#include "sql_into_tables.h"
#include "sql_values.h"

#include <util/string/join.h>

using namespace NYql;

namespace NSQLTranslationV1 {

using NALPDefault::SQLv1LexerTokens;

using namespace NSQLv1Generated;

TNodePtr TSqlIntoTable::Build(const TRule_into_table_stmt& node) {
    static const TMap<TString, ESQLWriteColumnMode> str2Mode = {
        {"InsertInto", ESQLWriteColumnMode::InsertInto},
        {"InsertOrAbortInto", ESQLWriteColumnMode::InsertOrAbortInto},
        {"InsertOrIgnoreInto", ESQLWriteColumnMode::InsertOrIgnoreInto},
        {"InsertOrRevertInto", ESQLWriteColumnMode::InsertOrRevertInto},
        {"UpsertInto", ESQLWriteColumnMode::UpsertInto},
        {"ReplaceInto", ESQLWriteColumnMode::ReplaceInto},
        {"InsertIntoWithTruncate", ESQLWriteColumnMode::InsertIntoWithTruncate}
    };

    auto& modeBlock = node.GetBlock1();

    TVector<TToken> modeTokens;
    switch (modeBlock.Alt_case()) {
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt1:
            modeTokens = {modeBlock.GetAlt1().GetToken1()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt2:
            modeTokens = {
                modeBlock.GetAlt2().GetToken1(),
                modeBlock.GetAlt2().GetToken2(),
                modeBlock.GetAlt2().GetToken3()
            };
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt3:
            modeTokens = {
                modeBlock.GetAlt3().GetToken1(),
                modeBlock.GetAlt3().GetToken2(),
                modeBlock.GetAlt3().GetToken3()
            };
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt4:
            modeTokens = {
                modeBlock.GetAlt4().GetToken1(),
                modeBlock.GetAlt4().GetToken2(),
                modeBlock.GetAlt4().GetToken3()
            };
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt5:
            modeTokens = {modeBlock.GetAlt5().GetToken1()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt6:
            modeTokens = {modeBlock.GetAlt6().GetToken1()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }

    TVector<TString> modeStrings;
    modeStrings.reserve(modeTokens.size());
    TVector<TString> userModeStrings;
    userModeStrings.reserve(modeTokens.size());

    for (auto& token : modeTokens) {
        auto tokenStr = Token(token);

        auto modeStr = tokenStr;
        modeStr.to_lower();
        modeStr.to_upper(0, 1);
        modeStrings.push_back(modeStr);

        auto userModeStr = tokenStr;
        userModeStr.to_upper();
        userModeStrings.push_back(userModeStr);
    }

    modeStrings.push_back("Into");
    userModeStrings.push_back("INTO");

    SqlIntoModeStr = JoinRange("", modeStrings.begin(), modeStrings.end());
    SqlIntoUserModeStr = JoinRange(" ", userModeStrings.begin(), userModeStrings.end());

    const auto& intoTableRef = node.GetRule_into_simple_table_ref3();
    const auto& tableRef = intoTableRef.GetRule_simple_table_ref1();
    const auto& tableRefCore = tableRef.GetRule_simple_table_ref_core1();

    auto service = Ctx.Scoped->CurrService;
    auto cluster = Ctx.Scoped->CurrCluster;
    std::pair<bool, TDeferredAtom> nameOrAt;
    bool isBinding = false;
    switch (tableRefCore.Alt_case()) {
        case TRule_simple_table_ref_core::AltCase::kAltSimpleTableRefCore1: {
            if (tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().HasBlock1()) {
                const auto& clusterExpr = tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetBlock1().GetRule_cluster_expr1();
                bool hasAt = tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetRule_id_or_at2().HasBlock1();
                bool result = !hasAt ?
                    ClusterExprOrBinding(clusterExpr, service, cluster, isBinding) : ClusterExpr(clusterExpr, false, service, cluster);
                if (!result) {
                    return nullptr;
                }
            }

            if (!isBinding && cluster.Empty()) {
                Ctx.Error() << "No cluster name given and no default cluster is selected";
                return nullptr;
            }

            auto id = Id(tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetRule_id_or_at2(), *this);
            nameOrAt = std::make_pair(id.first, TDeferredAtom(Ctx.Pos(), id.second));
            break;
        }
        case TRule_simple_table_ref_core::AltCase::kAltSimpleTableRefCore2: {
            auto at = tableRefCore.GetAlt_simple_table_ref_core2().HasBlock1();
            TString name;
            if (!NamedNodeImpl(tableRefCore.GetAlt_simple_table_ref_core2().GetRule_bind_parameter2(), name, *this)) {
                return nullptr;
            }
            auto named = GetNamedNode(name);
            if (!named) {
                return nullptr;
            }

            if (cluster.Empty()) {
                Ctx.Error() << "No cluster name given and no default cluster is selected";
                return nullptr;
            }

            TDeferredAtom table;
            MakeTableFromExpression(Ctx.Pos(), Ctx, named, table);
            nameOrAt = std::make_pair(at, table);
            break;
        }
        case TRule_simple_table_ref_core::AltCase::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }

    bool withTruncate = false;
    TTableHints tableHints;
    if (tableRef.HasBlock2()) {
        auto hints = TableHintsImpl(tableRef.GetBlock2().GetRule_table_hints1(), service);
        if (!hints) {
            Ctx.Error() << "Failed to parse table hints";
            return nullptr;
        }
        for (const auto& hint : *hints) {
            if (to_upper(hint.first) == "TRUNCATE") {
                withTruncate = true;
            }
        }
        std::erase_if(*hints, [](const auto &hint) { return to_upper(hint.first) == "TRUNCATE"; });
        tableHints = std::move(*hints);
    }

    TVector<TString> eraseColumns;
    if (intoTableRef.HasBlock2()) {
        if (service != StatProviderName) {
            Ctx.Error() << "ERASE BY is unsupported for " << service;
            return nullptr;
        }

        PureColumnListStr(
            intoTableRef.GetBlock2().GetRule_pure_column_list3(), *this, eraseColumns
        );
    }

    if (withTruncate) {
        if (SqlIntoModeStr != "InsertInto") {
            Error() << "Unable " << SqlIntoUserModeStr << " with truncate mode";
            return nullptr;
        }
        SqlIntoModeStr += "WithTruncate";
        SqlIntoUserModeStr += " ... WITH TRUNCATE";
    }
    const auto iterMode = str2Mode.find(SqlIntoModeStr);
    YQL_ENSURE(iterMode != str2Mode.end(), "Invalid sql write mode string: " << SqlIntoModeStr);
    const auto SqlIntoMode = iterMode->second;

    TPosition pos(Ctx.Pos());
    TTableRef table(Ctx.MakeName("table"), service, cluster, nullptr);
    if (isBinding) {
        const TString* binding = nameOrAt.second.GetLiteral();
        YQL_ENSURE(binding);
        YQL_ENSURE(!nameOrAt.first);
        if (!ApplyTableBinding(*binding, table, tableHints)) {
            return nullptr;
        }
    } else {
        table.Keys = BuildTableKey(pos, service, cluster, nameOrAt.second, {nameOrAt.first ? "@" : ""});
    }

    Ctx.IncrementMonCounter("sql_insert_clusters", table.Cluster.GetLiteral() ? *table.Cluster.GetLiteral() : "unknown");

    auto values = TSqlIntoValues(Ctx, Mode).Build(node.GetRule_into_values_source4(), SqlIntoUserModeStr);
    if (!values) {
        return nullptr;
    }
    if (!ValidateServiceName(node, table, SqlIntoMode, GetPos(modeTokens[0]))) {
        return nullptr;
    }
    Ctx.IncrementMonCounter("sql_features", SqlIntoModeStr);

    auto options = BuildIntoTableOptions(pos, eraseColumns, tableHints);

    if (node.HasBlock5()) {
        options = options->L(options, ReturningList(node.GetBlock5().GetRule_returning_columns_list1()));
    }

    return BuildWriteColumns(pos, Ctx.Scoped, table,
                             ToWriteColumnsMode(SqlIntoMode), std::move(values),
                             options);
}

bool TSqlIntoTable::ValidateServiceName(const TRule_into_table_stmt& node, const TTableRef& table,
    ESQLWriteColumnMode mode, const TPosition& pos) {
    Y_UNUSED(node);
    auto serviceName = table.Service;
    const bool isMapReduce = serviceName == YtProviderName;
    const bool isKikimr = serviceName == KikimrProviderName || serviceName == YdbProviderName;
    const bool isRtmr = serviceName == RtmrProviderName;
    const bool isStat = serviceName == StatProviderName;

    if (!isKikimr) {
        if (mode == ESQLWriteColumnMode::InsertOrAbortInto ||
            mode == ESQLWriteColumnMode::InsertOrIgnoreInto ||
            mode == ESQLWriteColumnMode::InsertOrRevertInto ||
            mode == ESQLWriteColumnMode::UpsertInto && !isStat)
        {
            Ctx.Error(pos) << SqlIntoUserModeStr << " is not supported for " << serviceName << " tables";
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    }

    if (isMapReduce) {
        if (mode == ESQLWriteColumnMode::ReplaceInto) {
            Ctx.Error(pos) << "Meaning of REPLACE INTO has been changed, now you should use INSERT INTO <table> WITH TRUNCATE ... for " << serviceName;
            Ctx.IncrementMonCounter("sql_errors", "ReplaceIntoConflictUsage");
            return false;
        }
    } else if (isKikimr) {
        if (mode == ESQLWriteColumnMode::InsertIntoWithTruncate) {
            Ctx.Error(pos) << "INSERT INTO WITH TRUNCATE is not supported for " << serviceName << " tables";
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    } else if (isRtmr) {
        if (mode != ESQLWriteColumnMode::InsertInto) {
            Ctx.Error(pos) << SqlIntoUserModeStr << " is unsupported for " << serviceName;
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    } else if (isStat) {
        if (mode != ESQLWriteColumnMode::UpsertInto) {
            Ctx.Error(pos) << SqlIntoUserModeStr << " is unsupported for " << serviceName;
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    }

    return true;
}

} // namespace NSQLTranslationV1
