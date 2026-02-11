#include "sql_into_tables.h"
#include "sql_values.h"

#include <util/string/join.h>

using namespace NYql;

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

TNodePtr TSqlIntoTable::Build(const TRule_into_table_stmt& node) {
    static const TMap<TString, ESQLWriteColumnMode> Str2Mode = {
        {"InsertInto", ESQLWriteColumnMode::InsertInto},
        {"InsertOrAbortInto", ESQLWriteColumnMode::InsertOrAbortInto},
        {"InsertOrIgnoreInto", ESQLWriteColumnMode::InsertOrIgnoreInto},
        {"InsertOrRevertInto", ESQLWriteColumnMode::InsertOrRevertInto},
        {"UpsertInto", ESQLWriteColumnMode::UpsertInto},
        {"ReplaceInto", ESQLWriteColumnMode::ReplaceInto},
        {"InsertIntoWithTruncate", ESQLWriteColumnMode::InsertIntoWithTruncate}};

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
                modeBlock.GetAlt2().GetToken3()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt3:
            modeTokens = {
                modeBlock.GetAlt3().GetToken1(),
                modeBlock.GetAlt3().GetToken2(),
                modeBlock.GetAlt3().GetToken3()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt4:
            modeTokens = {
                modeBlock.GetAlt4().GetToken1(),
                modeBlock.GetAlt4().GetToken2(),
                modeBlock.GetAlt4().GetToken3()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt5:
            modeTokens = {modeBlock.GetAlt5().GetToken1()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt6:
            modeTokens = {modeBlock.GetAlt6().GetToken1()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::ALT_NOT_SET:
            Y_UNREACHABLE();
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

    SqlIntoModeStr_ = JoinRange("", modeStrings.begin(), modeStrings.end());
    SqlIntoUserModeStr_ = JoinRange(" ", userModeStrings.begin(), userModeStrings.end());

    const auto& intoTableRef = node.GetRule_into_simple_table_ref3();
    const auto& tableRef = intoTableRef.GetRule_simple_table_ref1();
    const auto& tableRefCore = tableRef.GetRule_simple_table_ref_core1();

    auto service = Ctx_.Scoped->CurrService;
    auto cluster = Ctx_.Scoped->CurrCluster;
    std::pair<bool, TDeferredAtom> nameOrAt;
    bool isBinding = false;

    const bool isClusterSpecified = ((tableRefCore.HasAlt_simple_table_ref_core1() &&
                                      tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().HasBlock1()) ||
                                     (tableRefCore.HasAlt_simple_table_ref_core2() &&
                                      tableRefCore.GetAlt_simple_table_ref_core2().HasBlock1()));

    const bool hasAt = ((tableRefCore.HasAlt_simple_table_ref_core1() &&
                         tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetRule_id_or_at2().HasBlock1()) ||
                        (tableRefCore.HasAlt_simple_table_ref_core2() &&
                         tableRefCore.GetAlt_simple_table_ref_core2().HasBlock2()));

    if (isClusterSpecified) {
        const auto& clusterExpr = tableRefCore.HasAlt_simple_table_ref_core1()
                                      ? tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetBlock1().GetRule_cluster_expr1()
                                      : tableRefCore.GetAlt_simple_table_ref_core2().GetBlock1().GetRule_cluster_expr1();

        const bool result = !hasAt
                                ? ClusterExprOrBinding(clusterExpr, service, cluster, isBinding)
                                : ClusterExpr(clusterExpr, false, service, cluster);

        if (!result) {
            return nullptr;
        }
    }

    if (!isBinding && cluster.Empty()) {
        Ctx_.Error() << "No cluster name given and no default cluster is selected";
        return nullptr;
    }

    switch (tableRefCore.Alt_case()) {
        case TRule_simple_table_ref_core::AltCase::kAltSimpleTableRefCore1: {
            auto id = Id(tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetRule_id_or_at2(), *this);
            nameOrAt = std::make_pair(id.first, TDeferredAtom(Ctx_.Pos(), id.second));
            break;
        }
        case TRule_simple_table_ref_core::AltCase::kAltSimpleTableRefCore2: {
            TString name;
            if (!NamedNodeImpl(tableRefCore.GetAlt_simple_table_ref_core2().GetRule_bind_parameter3(), name, *this)) {
                return nullptr;
            }
            auto named = GetNamedNode(name);
            if (!named) {
                return nullptr;
            }

            named->SetRefPos(Ctx_.Pos());

            TDeferredAtom table;
            MakeTableFromExpression(Ctx_.Pos(), Ctx_, named, table);
            nameOrAt = std::make_pair(hasAt, table);
            break;
        }
        case TRule_simple_table_ref_core::AltCase::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    bool withTruncate = false;
    TTableHints tableHints;
    if (tableRef.HasBlock2()) {
        auto hints = TableHintsImpl(tableRef.GetBlock2().GetRule_table_hints1(), service);
        if (!hints) {
            Ctx_.Error() << "Failed to parse table hints";
            return nullptr;
        }
        for (const auto& hint : *hints) {
            if (to_upper(hint.first) == "TRUNCATE") {
                withTruncate = true;
            }
        }
        std::erase_if(*hints, [](const auto& hint) { return to_upper(hint.first) == "TRUNCATE"; });
        tableHints = std::move(*hints);
    }

    TVector<TString> eraseColumns;
    if (intoTableRef.HasBlock2()) {
        if (service != StatProviderName && service != UnknownProviderName) {
            Ctx_.Error() << "ERASE BY is unsupported for " << service;
            return nullptr;
        }

        PureColumnListStr(
            intoTableRef.GetBlock2().GetRule_pure_column_list3(), *this, eraseColumns);
    }

    if (withTruncate) {
        if (SqlIntoModeStr_ != "InsertInto") {
            Error() << "Unable " << SqlIntoUserModeStr_ << " with truncate mode";
            return nullptr;
        }
        SqlIntoModeStr_ += "WithTruncate";
        SqlIntoUserModeStr_ += " ... WITH TRUNCATE";
    }
    const auto iterMode = Str2Mode.find(SqlIntoModeStr_);
    YQL_ENSURE(iterMode != Str2Mode.end(), "Invalid sql write mode string: " << SqlIntoModeStr_);
    const auto SqlIntoMode = iterMode->second;

    TPosition pos(Ctx_.Pos());
    TTableRef table(Ctx_.MakeName("table"), service, cluster, nullptr);
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

    Ctx_.IncrementMonCounter("sql_insert_clusters", table.Cluster.GetLiteral() ? *table.Cluster.GetLiteral() : "unknown");

    auto values = TSqlIntoValues(Ctx_, Mode_).Build(node.GetRule_into_values_source4(), SqlIntoUserModeStr_);
    if (!values) {
        return nullptr;
    }
    if (!ValidateServiceName(node, table, SqlIntoMode, GetPos(modeTokens[0]))) {
        return nullptr;
    }
    Ctx_.IncrementMonCounter("sql_features", SqlIntoModeStr_);

    auto options = BuildIntoTableOptions(pos, eraseColumns, tableHints);

    if (node.HasBlock5()) {
        options = options->L(options, ReturningList(node.GetBlock5().GetRule_returning_columns_list1()));
    }

    return BuildWriteColumns(pos, Ctx_.Scoped, table,
                             ToWriteColumnsMode(SqlIntoMode), std::move(values),
                             options);
}

bool TSqlIntoTable::ValidateServiceName(const TRule_into_table_stmt& node, const TTableRef& table,
                                        ESQLWriteColumnMode mode, const TPosition& pos) {
    Y_UNUSED(node);
    auto serviceName = table.Service;
    if (serviceName == UnknownProviderName) {
        return true;
    }

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
            Ctx_.Error(pos) << SqlIntoUserModeStr_ << " is not supported for " << serviceName << " tables";
            Ctx_.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr_ << "UnsupportedFor" << serviceName);
            return false;
        }
    }

    if (isMapReduce) {
        if (mode == ESQLWriteColumnMode::ReplaceInto) {
            if (!Ctx_.EnsureBackwardCompatibleFeatureAvailable(
                    pos, "REPLACE", MakeLangVersion(2025, 4)))
            {
                return false;
            }
        }
    } else if (isKikimr) {
        if (mode == ESQLWriteColumnMode::InsertIntoWithTruncate) {
            Ctx_.Error(pos) << "INSERT INTO WITH TRUNCATE is not supported for " << serviceName << " tables";
            Ctx_.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr_ << "UnsupportedFor" << serviceName);
            return false;
        }
    } else if (isRtmr) {
        if (mode != ESQLWriteColumnMode::InsertInto) {
            Ctx_.Error(pos) << SqlIntoUserModeStr_ << " is unsupported for " << serviceName;
            Ctx_.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr_ << "UnsupportedFor" << serviceName);
            return false;
        }
    } else if (isStat) {
        if (mode != ESQLWriteColumnMode::UpsertInto) {
            Ctx_.Error(pos) << SqlIntoUserModeStr_ << " is unsupported for " << serviceName;
            Ctx_.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr_ << "UnsupportedFor" << serviceName);
            return false;
        }
    }

    return true;
}

} // namespace NSQLTranslationV1
