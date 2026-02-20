#include "sql_translation.h"
#include "sql_expression.h"
#include "sql_call_expr.h"
#include "sql_query.h"
#include "sql_values.h"
#include "sql_select_yql.h"
#include "select_yql.h"
#include "sql_select.h"
#include "object_processing.h"
#include "source.h"
#include "antlr_token.h"
#include "secret_settings.h"

#include <yql/essentials/sql/settings/partitioning.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/utils/yql_paths.h>

#include <util/generic/scope.h>
#include <util/string/join.h>

#include <library/cpp/protobuf/util/simple_reflection.h>

namespace {

using namespace NSQLTranslationV1;

using NSQLTranslation::ESqlMode;

template <typename Callback>
void VisitAllFields(const NProtoBuf::Message& msg, Callback& callback) {
    const auto* descr = msg.GetDescriptor();
    for (int i = 0; i < descr->field_count(); ++i) {
        const auto* fd = descr->field(i);
        NProtoBuf::TConstField field(msg, fd);
        if (field.IsMessage()) {
            for (size_t j = 0; j < field.Size(); ++j) {
                const auto& message = *field.Get<NProtoBuf::Message>(j);
                callback(message);
                VisitAllFields(message, callback);
            }
        }
    }
}

struct TTokenCollector {
    void operator()(const NProtoBuf::Message& message) {
        if (const auto* token = dynamic_cast<const NSQLv1Generated::TToken*>(&message)) {
            if (!Tokens.empty()) {
                Tokens << ' ';
            }
            Tokens << token->GetValue();
        }
    }

    TStringBuilder Tokens;
};

TString CollectTokens(const TRule_select_stmt& selectStatement) {
    TTokenCollector tokenCollector;
    VisitAllFields(selectStatement, tokenCollector);
    return tokenCollector.Tokens;
}

bool BuildContextRecreationQuery(TContext& context, TStringBuilder& query) {
    TVector<TString> statements;
    if (!SplitQueryToStatements(context.Lexers, context.Parsers, context.Query, statements, context.Issues, context.Settings)) {
        return false;
    }

    for (size_t id : context.ForAllStatementsParts) {
        query << statements[id] << '\n';
    }
    return true;
}

// ensures that the parsing mode is restored to the original value
class TModeGuard {
    ESqlMode& Mode_;
    ESqlMode OriginalMode_;

public:
    TModeGuard(ESqlMode& mode, ESqlMode newMode)
        : Mode_(mode)
        , OriginalMode_(std::exchange(mode, newMode))
    {
    }

    ~TModeGuard() {
        Mode_ = OriginalMode_;
    }
};

TNodePtr BuildViewSelect(const TRule_select_stmt& selectStatement, TContext& context) {
    TModeGuard guard(context.Settings.Mode, ESqlMode::LIMITED_VIEW);
    TSqlSelect selectTranslator(context, context.Settings.Mode);
    auto position = context.Pos();
    auto source = selectTranslator.Build(selectStatement, position);
    if (!source) {
        return nullptr;
    }
    return BuildSelectResult(
        position,
        source,
        false, /* write result */
        false, /* in subquery */
        context.Scoped);
}

} // namespace

namespace NSQLTranslationV1 {

using NALPDefaultAntlr4::SQLv1Antlr4Lexer;

using namespace NSQLv1Generated;

TIdentifier GetKeywordId(TTranslation& ctx, const TRule_keyword& node) {
    // keyword:
    //    keyword_compat
    //  | keyword_expr_uncompat
    //  | keyword_table_uncompat
    //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_keyword::kAltKeyword1:
            return GetIdentifier(ctx, node.GetAlt_keyword1().GetRule_keyword_compat1());
        case TRule_keyword::kAltKeyword2:
            return GetIdentifier(ctx, node.GetAlt_keyword2().GetRule_keyword_expr_uncompat1());
        case TRule_keyword::kAltKeyword3:
            return GetIdentifier(ctx, node.GetAlt_keyword3().GetRule_keyword_table_uncompat1());
        case TRule_keyword::kAltKeyword4:
            return GetIdentifier(ctx, node.GetAlt_keyword4().GetRule_keyword_select_uncompat1());
        case TRule_keyword::kAltKeyword5:
            return GetIdentifier(ctx, node.GetAlt_keyword5().GetRule_keyword_alter_uncompat1());
        case TRule_keyword::kAltKeyword6:
            return GetIdentifier(ctx, node.GetAlt_keyword6().GetRule_keyword_in_uncompat1());
        case TRule_keyword::kAltKeyword7:
            return GetIdentifier(ctx, node.GetAlt_keyword7().GetRule_keyword_window_uncompat1());
        case TRule_keyword::kAltKeyword8:
            return GetIdentifier(ctx, node.GetAlt_keyword8().GetRule_keyword_hint_uncompat1());
        case TRule_keyword::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_id& node, TTranslation& ctx) {
    // id: identifier | keyword;
    switch (node.Alt_case()) {
        case TRule_id::kAltId1:
            return Id(node.GetAlt_id1().GetRule_identifier1(), ctx);
        case TRule_id::kAltId2:
            return GetKeyword(ctx, node.GetAlt_id2().GetRule_keyword1());
        case TRule_id::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_id_or_type& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_or_type::kAltIdOrType1:
            return Id(node.GetAlt_id_or_type1().GetRule_id1(), ctx);
        case TRule_id_or_type::kAltIdOrType2:
            return ctx.Identifier(node.GetAlt_id_or_type2().GetRule_type_id1().GetToken1());
        case TRule_id_or_type::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_id_as_compat& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_as_compat::kAltIdAsCompat1:
            return Id(node.GetAlt_id_as_compat1().GetRule_identifier1(), ctx);
        case TRule_id_as_compat::kAltIdAsCompat2:
            return ctx.Token(node.GetAlt_id_as_compat2().GetRule_keyword_as_compat1().GetToken1());
        case TRule_id_as_compat::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_an_id_as_compat& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_an_id_as_compat::kAltAnIdAsCompat1:
            return Id(node.GetAlt_an_id_as_compat1().GetRule_id_as_compat1(), ctx);
        case TRule_an_id_as_compat::kAltAnIdAsCompat2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_as_compat2().GetToken1()));
        case TRule_an_id_as_compat::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_id_schema& node, TTranslation& ctx) {
    // id_schema:
    //     identifier
    //   | keyword_compat
    //   | keyword_expr_uncompat
    //  //  | keyword_table_uncompat
    //   | keyword_select_uncompat
    //  //  | keyword_alter_uncompat
    //   | keyword_in_uncompat
    //   | keyword_window_uncompat
    //   | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_schema::kAltIdSchema1:
            return Id(node.GetAlt_id_schema1().GetRule_identifier1(), ctx);
        case TRule_id_schema::kAltIdSchema2:
            return GetKeyword(ctx, node.GetAlt_id_schema2().GetRule_keyword_compat1());
        case TRule_id_schema::kAltIdSchema3:
            return GetKeyword(ctx, node.GetAlt_id_schema3().GetRule_keyword_expr_uncompat1());
        case TRule_id_schema::kAltIdSchema4:
            return GetKeyword(ctx, node.GetAlt_id_schema4().GetRule_keyword_select_uncompat1());
        case TRule_id_schema::kAltIdSchema5:
            return GetKeyword(ctx, node.GetAlt_id_schema5().GetRule_keyword_in_uncompat1());
        case TRule_id_schema::kAltIdSchema6:
            return GetKeyword(ctx, node.GetAlt_id_schema6().GetRule_keyword_window_uncompat1());
        case TRule_id_schema::kAltIdSchema7:
            return GetKeyword(ctx, node.GetAlt_id_schema7().GetRule_keyword_hint_uncompat1());
        case TRule_id_schema::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_an_id_or_type& node, TTranslation& ctx) {
    // an_id_or_type: id_or_type | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_or_type::kAltAnIdOrType1:
            return Id(node.GetAlt_an_id_or_type1().GetRule_id_or_type1(), ctx);
        case TRule_an_id_or_type::kAltAnIdOrType2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_or_type2().GetToken1()));
        case TRule_an_id_or_type::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

std::pair<bool, TString> Id(const TRule_id_or_at& node, TTranslation& ctx) {
    bool hasAt = node.HasBlock1();
    return std::make_pair(hasAt, Id(node.GetRule_an_id_or_type2(), ctx));
}

TString Id(const TRule_id_table& node, TTranslation& ctx) {
    // id_table:
    //     identifier
    //   | keyword_compat
    //   | keyword_expr_uncompat
    //  //  | keyword_table_uncompat
    //   | keyword_select_uncompat
    //  //  | keyword_alter_uncompat
    //   | keyword_in_uncompat
    //   | keyword_window_uncompat
    //   | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_table::kAltIdTable1:
            return Id(node.GetAlt_id_table1().GetRule_identifier1(), ctx);
        case TRule_id_table::kAltIdTable2:
            return GetKeyword(ctx, node.GetAlt_id_table2().GetRule_keyword_compat1());
        case TRule_id_table::kAltIdTable3:
            return GetKeyword(ctx, node.GetAlt_id_table3().GetRule_keyword_expr_uncompat1());
        case TRule_id_table::kAltIdTable4:
            return GetKeyword(ctx, node.GetAlt_id_table4().GetRule_keyword_select_uncompat1());
        case TRule_id_table::kAltIdTable5:
            return GetKeyword(ctx, node.GetAlt_id_table5().GetRule_keyword_in_uncompat1());
        case TRule_id_table::kAltIdTable6:
            return GetKeyword(ctx, node.GetAlt_id_table6().GetRule_keyword_window_uncompat1());
        case TRule_id_table::kAltIdTable7:
            return GetKeyword(ctx, node.GetAlt_id_table7().GetRule_keyword_hint_uncompat1());
        case TRule_id_table::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_an_id_table& node, TTranslation& ctx) {
    // an_id_table: id_table | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_table::kAltAnIdTable1:
            return Id(node.GetAlt_an_id_table1().GetRule_id_table1(), ctx);
        case TRule_an_id_table::kAltAnIdTable2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_table2().GetToken1()));
        case TRule_an_id_table::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_id_table_or_type& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_table_or_type::kAltIdTableOrType1:
            return Id(node.GetAlt_id_table_or_type1().GetRule_an_id_table1(), ctx);
        case TRule_id_table_or_type::kAltIdTableOrType2:
            return ctx.Identifier(node.GetAlt_id_table_or_type2().GetRule_type_id1().GetToken1());
        case TRule_id_table_or_type::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_id_expr& node, TTranslation& ctx) {
    // id_expr:
    //     identifier
    //   | keyword_compat
    //  //  | keyword_expr_uncompat
    //  //  | keyword_table_uncompat
    //  //  | keyword_select_uncompat
    //   | keyword_alter_uncompat
    //   | keyword_in_uncompat
    //   | keyword_window_uncompat
    //   | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_expr::kAltIdExpr1:
            return Id(node.GetAlt_id_expr1().GetRule_identifier1(), ctx);
        case TRule_id_expr::kAltIdExpr2:
            return GetKeyword(ctx, node.GetAlt_id_expr2().GetRule_keyword_compat1());
        case TRule_id_expr::kAltIdExpr3:
            return GetKeyword(ctx, node.GetAlt_id_expr3().GetRule_keyword_alter_uncompat1());
        case TRule_id_expr::kAltIdExpr4:
            return GetKeyword(ctx, node.GetAlt_id_expr4().GetRule_keyword_in_uncompat1());
        case TRule_id_expr::kAltIdExpr5:
            return GetKeyword(ctx, node.GetAlt_id_expr5().GetRule_keyword_window_uncompat1());
        case TRule_id_expr::kAltIdExpr6:
            return GetKeyword(ctx, node.GetAlt_id_expr6().GetRule_keyword_hint_uncompat1());
        case TRule_id_expr::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

bool IsQuotedId(const TRule_id_expr& node, TTranslation& ctx) {
    if (node.Alt_case() != TRule_id_expr::kAltIdExpr1) {
        return false;
    }
    const auto& id = ctx.Token(node.GetAlt_id_expr1().GetRule_identifier1().GetToken1());
    // identifier: ID_PLAIN | ID_QUOTED;
    return id.StartsWith('`');
}

TString Id(const TRule_id_expr_in& node, TTranslation& ctx) {
    // id_expr_in:
    //     identifier
    //   | keyword_compat
    //  //  | keyword_expr_uncompat
    //  //  | keyword_table_uncompat
    //  //  | keyword_select_uncompat
    //   | keyword_alter_uncompat
    //  //  | keyword_in_uncompat
    //   | keyword_window_uncompat
    //   | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_expr_in::kAltIdExprIn1:
            return Id(node.GetAlt_id_expr_in1().GetRule_identifier1(), ctx);
        case TRule_id_expr_in::kAltIdExprIn2:
            return GetKeyword(ctx, node.GetAlt_id_expr_in2().GetRule_keyword_compat1());
        case TRule_id_expr_in::kAltIdExprIn3:
            return GetKeyword(ctx, node.GetAlt_id_expr_in3().GetRule_keyword_alter_uncompat1());
        case TRule_id_expr_in::kAltIdExprIn4:
            return GetKeyword(ctx, node.GetAlt_id_expr_in4().GetRule_keyword_window_uncompat1());
        case TRule_id_expr_in::kAltIdExprIn5:
            return GetKeyword(ctx, node.GetAlt_id_expr_in5().GetRule_keyword_hint_uncompat1());
        case TRule_id_expr_in::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_id_window& node, TTranslation& ctx) {
    // id_window:
    //     identifier
    //   | keyword_compat
    //   | keyword_expr_uncompat
    //   | keyword_table_uncompat
    //   | keyword_select_uncompat
    //   | keyword_alter_uncompat
    //   | keyword_in_uncompat
    //  //  | keyword_window_uncompat
    //   | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_window::kAltIdWindow1:
            return Id(node.GetAlt_id_window1().GetRule_identifier1(), ctx);
        case TRule_id_window::kAltIdWindow2:
            return GetKeyword(ctx, node.GetAlt_id_window2().GetRule_keyword_compat1());
        case TRule_id_window::kAltIdWindow3:
            return GetKeyword(ctx, node.GetAlt_id_window3().GetRule_keyword_expr_uncompat1());
        case TRule_id_window::kAltIdWindow4:
            return GetKeyword(ctx, node.GetAlt_id_window4().GetRule_keyword_table_uncompat1());
        case TRule_id_window::kAltIdWindow5:
            return GetKeyword(ctx, node.GetAlt_id_window5().GetRule_keyword_select_uncompat1());
        case TRule_id_window::kAltIdWindow6:
            return GetKeyword(ctx, node.GetAlt_id_window6().GetRule_keyword_alter_uncompat1());
        case TRule_id_window::kAltIdWindow7:
            return GetKeyword(ctx, node.GetAlt_id_window7().GetRule_keyword_in_uncompat1());
        case TRule_id_window::kAltIdWindow8:
            return GetKeyword(ctx, node.GetAlt_id_window8().GetRule_keyword_hint_uncompat1());
        case TRule_id_window::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_id_without& node, TTranslation& ctx) {
    // id_without:
    //     identifier
    //   | keyword_compat
    //  //  | keyword_expr_uncompat
    //   | keyword_table_uncompat
    //  //  | keyword_select_uncompat
    //   | keyword_alter_uncompat
    //   | keyword_in_uncompat
    //   | keyword_window_uncompat
    //   | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_without::kAltIdWithout1:
            return Id(node.GetAlt_id_without1().GetRule_identifier1(), ctx);
        case TRule_id_without::kAltIdWithout2:
            return GetKeyword(ctx, node.GetAlt_id_without2().GetRule_keyword_compat1());
        case TRule_id_without::kAltIdWithout3:
            return GetKeyword(ctx, node.GetAlt_id_without3().GetRule_keyword_table_uncompat1());
        case TRule_id_without::kAltIdWithout4:
            return GetKeyword(ctx, node.GetAlt_id_without4().GetRule_keyword_alter_uncompat1());
        case TRule_id_without::kAltIdWithout5:
            return GetKeyword(ctx, node.GetAlt_id_without5().GetRule_keyword_in_uncompat1());
        case TRule_id_without::kAltIdWithout6:
            return GetKeyword(ctx, node.GetAlt_id_without6().GetRule_keyword_window_uncompat1());
        case TRule_id_without::kAltIdWithout7:
            return GetKeyword(ctx, node.GetAlt_id_without7().GetRule_keyword_hint_uncompat1());
        case TRule_id_without::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_id_hint& node, TTranslation& ctx) {
    // id_hint:
    //     identifier
    //   | keyword_compat
    //   | keyword_expr_uncompat
    //   | keyword_table_uncompat
    //   | keyword_select_uncompat
    //   | keyword_alter_uncompat
    //   | keyword_in_uncompat
    //   | keyword_window_uncompat
    //  //  | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_hint::kAltIdHint1:
            return Id(node.GetAlt_id_hint1().GetRule_identifier1(), ctx);
        case TRule_id_hint::kAltIdHint2:
            return GetKeyword(ctx, node.GetAlt_id_hint2().GetRule_keyword_compat1());
        case TRule_id_hint::kAltIdHint3:
            return GetKeyword(ctx, node.GetAlt_id_hint3().GetRule_keyword_expr_uncompat1());
        case TRule_id_hint::kAltIdHint4:
            return GetKeyword(ctx, node.GetAlt_id_hint4().GetRule_keyword_table_uncompat1());
        case TRule_id_hint::kAltIdHint5:
            return GetKeyword(ctx, node.GetAlt_id_hint5().GetRule_keyword_select_uncompat1());
        case TRule_id_hint::kAltIdHint6:
            return GetKeyword(ctx, node.GetAlt_id_hint6().GetRule_keyword_alter_uncompat1());
        case TRule_id_hint::kAltIdHint7:
            return GetKeyword(ctx, node.GetAlt_id_hint7().GetRule_keyword_in_uncompat1());
        case TRule_id_hint::kAltIdHint8:
            return GetKeyword(ctx, node.GetAlt_id_hint8().GetRule_keyword_window_uncompat1());
        case TRule_id_hint::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_an_id& node, TTranslation& ctx) {
    // an_id: id | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id::kAltAnId1:
            return Id(node.GetAlt_an_id1().GetRule_id1(), ctx);
        case TRule_an_id::kAltAnId2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id2().GetToken1()));
        case TRule_an_id::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_an_id_schema& node, TTranslation& ctx) {
    // an_id_schema: id_schema | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_schema::kAltAnIdSchema1:
            return Id(node.GetAlt_an_id_schema1().GetRule_id_schema1(), ctx);
        case TRule_an_id_schema::kAltAnIdSchema2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_schema2().GetToken1()));
        case TRule_an_id_schema::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_an_id_expr& node, TTranslation& ctx) {
    // an_id_expr: id_expr | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_expr::kAltAnIdExpr1:
            return Id(node.GetAlt_an_id_expr1().GetRule_id_expr1(), ctx);
        case TRule_an_id_expr::kAltAnIdExpr2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_expr2().GetToken1()));
        case TRule_an_id_expr::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_an_id_window& node, TTranslation& ctx) {
    // an_id_window: id_window | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_window::kAltAnIdWindow1:
            return Id(node.GetAlt_an_id_window1().GetRule_id_window1(), ctx);
        case TRule_an_id_window::kAltAnIdWindow2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_window2().GetToken1()));
        case TRule_an_id_window::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_an_id_without& node, TTranslation& ctx) {
    // an_id_without: id_without | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_without::kAltAnIdWithout1:
            return Id(node.GetAlt_an_id_without1().GetRule_id_without1(), ctx);
        case TRule_an_id_without::kAltAnIdWithout2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_without2().GetToken1()));
        case TRule_an_id_without::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_an_id_hint& node, TTranslation& ctx) {
    // an_id_hint: id_hint | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_hint::kAltAnIdHint1:
            return Id(node.GetAlt_an_id_hint1().GetRule_id_hint1(), ctx);
        case TRule_an_id_hint::kAltAnIdHint2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_hint2().GetToken1()));
        case TRule_an_id_hint::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TString Id(const TRule_an_id_pure& node, TTranslation& ctx) {
    // an_id_pure: identifier | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_pure::kAltAnIdPure1:
            return Id(node.GetAlt_an_id_pure1().GetRule_identifier1(), ctx);
        case TRule_an_id_pure::kAltAnIdPure2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_pure2().GetToken1()));
        case TRule_an_id_pure::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TViewDescription Id(const TRule_view_name& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_view_name::kAltViewName1:
            return {Id(node.GetAlt_view_name1().GetRule_an_id1(), ctx)};
        case TRule_view_name::kAltViewName2:
            return {"", true};
        case TRule_view_name::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

bool NamedNodeImpl(const TRule_bind_parameter& node, TString& name, TTranslation& ctx) {
    // bind_parameter: DOLLAR (an_id_or_type | TRUE | FALSE);
    TString id;
    switch (node.GetBlock2().Alt_case()) {
        case TRule_bind_parameter::TBlock2::kAlt1:
            id = Id(node.GetBlock2().GetAlt1().GetRule_an_id_or_type1(), ctx);
            break;
        case TRule_bind_parameter::TBlock2::kAlt2:
            id = ctx.Token(node.GetBlock2().GetAlt2().GetToken1());
            break;
        case TRule_bind_parameter::TBlock2::kAlt3:
            id = ctx.Token(node.GetBlock2().GetAlt3().GetToken1());
            break;
        case TRule_bind_parameter::TBlock2::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    auto dollar = ctx.Token(node.GetToken1());
    if (id.empty()) {
        ctx.Error() << "Empty symbol name is not allowed";
        return false;
    }

    name = dollar + id;
    return true;
}

TString OptIdPrefixAsStr(const TRule_opt_id_prefix& node, TTranslation& ctx, const TString& defaultStr) {
    if (!node.HasBlock1()) {
        return defaultStr;
    }
    return Id(node.GetBlock1().GetRule_an_id1(), ctx);
}

TString OptIdPrefixAsStr(const TRule_opt_id_prefix_or_type& node, TTranslation& ctx, const TString& defaultStr) {
    if (!node.HasBlock1()) {
        return defaultStr;
    }
    return Id(node.GetBlock1().GetRule_an_id_or_type1(), ctx);
}

void PureColumnListStr(const TRule_pure_column_list& node, TTranslation& ctx, TVector<TString>& outList) {
    outList.push_back(Id(node.GetRule_an_id2(), ctx));
    for (auto& block : node.GetBlock3()) {
        outList.push_back(Id(block.GetRule_an_id2(), ctx));
    }
}

bool NamedNodeImpl(const TRule_opt_bind_parameter& node, TString& name, bool& isOptional, TTranslation& ctx) {
    // opt_bind_parameter: bind_parameter QUESTION?;
    isOptional = false;
    if (!NamedNodeImpl(node.GetRule_bind_parameter1(), name, ctx)) {
        return false;
    }
    isOptional = node.HasBlock2();
    return true;
}

TDeferredAtom PureColumnOrNamed(const TRule_pure_column_or_named& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_pure_column_or_named::kAltPureColumnOrNamed1: {
            TString named;
            if (!NamedNodeImpl(node.GetAlt_pure_column_or_named1().GetRule_bind_parameter1(), named, ctx)) {
                return {};
            }
            auto namedNode = ctx.GetNamedNode(named);
            if (!namedNode) {
                return {};
            }

            return TDeferredAtom(namedNode, ctx.Context());
        }

        case TRule_pure_column_or_named::kAltPureColumnOrNamed2:
            return TDeferredAtom(ctx.Context().Pos(), Id(node.GetAlt_pure_column_or_named2().GetRule_an_id1(), ctx));
        case TRule_pure_column_or_named::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

bool PureColumnOrNamedListStr(const TRule_pure_column_or_named_list& node, TTranslation& ctx, TVector<TDeferredAtom>& outList) {
    outList.push_back(PureColumnOrNamed(node.GetRule_pure_column_or_named2(), ctx));
    if (outList.back().Empty()) {
        return false;
    }

    for (auto& block : node.GetBlock3()) {
        outList.push_back(PureColumnOrNamed(block.GetRule_pure_column_or_named2(), ctx));
        if (outList.back().Empty()) {
            return false;
        }
    }

    return true;
}

bool TSqlTranslation::CreateTableIndex(const TRule_table_index& node, TVector<TIndexDescription>& indexes) {
    indexes.emplace_back(IdEx(node.GetRule_an_id2(), *this));

    const auto& indexType = node.GetRule_table_index_type3().GetBlock1();
    bool isLocalIndex = false;
    switch (indexType.Alt_case()) {
        // "GLOBAL"
        case TRule_table_index_type_TBlock1::kAlt1: {
            auto globalIndex = indexType.GetAlt1().GetRule_global_index1();
            bool uniqIndex = false;
            if (globalIndex.HasBlock2()) {
                uniqIndex = true;
            }
            bool sync = true;
            if (globalIndex.HasBlock3()) {
                const TString token = to_lower(Ctx_.Token(globalIndex.GetBlock3().GetToken1()));
                if (token == "sync") {
                    sync = true;
                } else if (token == "async") {
                    sync = false;
                } else {
                    Y_UNREACHABLE();
                }
            }
            if (sync) {
                if (uniqIndex) {
                    indexes.back().Type = TIndexDescription::EType::GlobalSyncUnique;
                } else {
                    indexes.back().Type = TIndexDescription::EType::GlobalSync;
                }
            } else {
                if (uniqIndex) {
                    AltNotImplemented("unique", indexType);
                    return false;
                }
                indexes.back().Type = TIndexDescription::EType::GlobalAsync;
            }
        } break;
        // "LOCAL"
        case TRule_table_index_type_TBlock1::kAlt2:
            isLocalIndex = true;
            break;
        case TRule_table_index_type_TBlock1::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    if (node.GetRule_table_index_type3().HasBlock2()) {
        const TString subType = to_upper(IdEx(node.GetRule_table_index_type3().GetBlock2().GetRule_index_subtype2().GetRule_an_id1(), *this).Name);
        if (subType == "VECTOR_KMEANS_TREE" || subType == "FULLTEXT_PLAIN" || subType == "FULLTEXT_RELEVANCE") {
            if (isLocalIndex || indexes.back().Type != TIndexDescription::EType::GlobalSync) {
                Ctx_.Error() << subType << " index can only be GLOBAL [SYNC]";
                return false;
            }

            if (subType == "VECTOR_KMEANS_TREE") {
                indexes.back().Type = TIndexDescription::EType::GlobalVectorKmeansTree;
            } else if (subType == "FULLTEXT_PLAIN") {
                indexes.back().Type = TIndexDescription::EType::GlobalFulltextPlain;
            } else if (subType == "FULLTEXT_RELEVANCE") {
                indexes.back().Type = TIndexDescription::EType::GlobalFulltextRelevance;
            } else {
                Y_ABORT("Unreachable");
            }
        } else if (subType == "BLOOM_FILTER" || subType == "BLOOM_NGRAM_FILTER") {
            if (!isLocalIndex) {
                Ctx_.Error() << subType << " index can only be LOCAL";
                return false;
            }

            if (subType == "BLOOM_FILTER") {
                indexes.back().Type = TIndexDescription::EType::LocalBloomFilter;
            } else if (subType == "BLOOM_NGRAM_FILTER") {
                indexes.back().Type = TIndexDescription::EType::LocalBloomNgramFilter;
            } else {
                Y_UNREACHABLE();
            }
        } else {
            Ctx_.Error() << subType << " index subtype is not supported";
            return false;
        }
    } else if (isLocalIndex) {
        AltNotImplemented("local", indexType);
        return false;
    }

    // WITH
    if (node.HasBlock10()) {
        // const auto& with = node.GetBlock4();
        auto& index = indexes.back();
        if (index.Type == TIndexDescription::EType::GlobalVectorKmeansTree ||
            index.Type == TIndexDescription::EType::GlobalFulltextPlain ||
            index.Type == TIndexDescription::EType::GlobalFulltextRelevance ||
            index.Type == TIndexDescription::EType::LocalBloomFilter ||
            index.Type == TIndexDescription::EType::LocalBloomNgramFilter) {
            if (!FillIndexSettings(node.GetBlock10().GetRule_with_index_settings1(), index.IndexSettings)) {
                return false;
            }
        } else {
            AltNotImplemented("with", indexType);
            return false;
        }
    }

    indexes.back().IndexColumns.emplace_back(IdEx(node.GetRule_an_id_schema6(), *this));
    for (const auto& block : node.GetBlock7()) {
        indexes.back().IndexColumns.emplace_back(IdEx(block.GetRule_an_id_schema2(), *this));
    }

    if (node.HasBlock9()) {
        if (indexes.back().Type == TIndexDescription::EType::LocalBloomFilter ||
            indexes.back().Type == TIndexDescription::EType::LocalBloomNgramFilter) {
            Ctx_.Error() << "COVER is not supported for local bloom indexes";
            return false;
        }

        const auto& block = node.GetBlock9();
        indexes.back().DataColumns.emplace_back(IdEx(block.GetRule_an_id_schema3(), *this));
        for (const auto& inner : block.GetBlock4()) {
            indexes.back().DataColumns.emplace_back(IdEx(inner.GetRule_an_id_schema2(), *this));
        }
    }

    return true;
}

bool TSqlTranslation::ParseDatabaseSettings(const TRule_database_settings& in, THashMap<TString, TNodePtr>& out) {
    if (!ParseDatabaseSetting(in.GetRule_database_setting1(), out)) {
        return false;
    }
    for (const auto& setting : in.GetBlock2()) {
        if (!ParseDatabaseSetting(setting.GetRule_database_setting2(), out)) {
            return false;
        }
    }
    return true;
}

namespace {

TMaybe<bool> ParseBool(TContext& ctx, const TRule_bool_value& node) {
    bool value = false;
    const TString& token = ctx.Token(node.GetToken1());
    if (!TryFromString<bool>(token, value)) {
        ctx.Error() << "Cannot parse bool from " << token;
        return Nothing();
    }
    return value;
}

TMaybe<ui64> ParseInteger(TContext& ctx, const TRule_integer& node) {
    ui64 value = 0;
    const TString& token = ctx.Token(node.GetToken1());
    TString suffix;
    if (!ParseNumbers(ctx, token, value, suffix)) {
        ctx.Error() << "Cannot parse integer from " << token;
        return Nothing();
    }
    return value;
}

TNodePtr ParseDatabaseSettingValue(TContext& ctx, const TRule_database_setting_value& node) {
    switch (node.GetAltCase()) {
        case TRule_database_setting_value::kAltDatabaseSettingValue1:
            // bool
            if (auto result = ParseBool(ctx, node.GetAlt_database_setting_value1().GetRule_bool_value1())) {
                return BuildLiteralBool(ctx.Pos(), *result);
            }
            return nullptr;

        case TRule_database_setting_value::kAltDatabaseSettingValue2:
            // integer
            if (auto result = ParseInteger(ctx, node.GetAlt_database_setting_value2().GetRule_integer1())) {
                return MakeIntrusive<TLiteralNumberNode<ui64>>(ctx.Pos(), "Uint64", ToString(*result));
            }
            return nullptr;

        case TRule_database_setting_value::kAltDatabaseSettingValue3: {
            // string
            const auto& token = node.GetAlt_database_setting_value3().GetToken1();
            if (auto result = BuildLiteralSmartString(ctx, ctx.Token(token))) {
                return result;
            }
            return nullptr;
        }
        case TRule_database_setting_value::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TNodePtr ParseLiteral(const TRule_compact_setting_value& value, TContext& ctx, NSQLTranslation::ESqlMode mode, const TString& expectedType) {
    TSqlExpression sqlExpr(ctx, mode);
    auto exprOrId = sqlExpr.LiteralExpr(value.GetRule_literal_value1());
    if (!exprOrId || !exprOrId->Expr || !exprOrId->Expr->IsLiteral() || exprOrId->Expr->GetLiteralType() != expectedType) {
        return nullptr;
    }
    return exprOrId->Expr;
}

} // namespace

bool TSqlTranslation::ParseDatabaseSetting(const TRule_database_setting& in, THashMap<TString, TNodePtr>& out) {
    const auto setting = to_upper(Id(in.GetRule_an_id1(), *this));

    if (out.contains(setting)) {
        Ctx_.Error() << "Duplicate setting: " << setting;
        return false;
    }

    auto node = ParseDatabaseSettingValue(Ctx_, in.GetRule_database_setting_value3());
    if (!node) {
        return false;
    }
    out[setting] = node;
    return true;
}

bool TSqlTranslation::FillIndexSettings(const TRule_with_index_settings& settingsNode,
                                        TIndexDescription::TIndexSettings& indexSettings) {
    const auto& firstEntry = settingsNode.GetRule_index_setting_entry3();
    if (!AddIndexSetting(IdEx(firstEntry.GetRule_an_id1(), *this), firstEntry.GetRule_index_setting_value3(), indexSettings)) {
        return false;
    }
    for (auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_index_setting_entry2();
        if (!AddIndexSetting(IdEx(entry.GetRule_an_id1(), *this), entry.GetRule_index_setting_value3(), indexSettings)) {
            return false;
        }
    }
    return true;
}

TString TSqlTranslation::GetIndexSettingStringValue(const TRule_index_setting_value& node) {
    switch (node.GetAltCase()) {
        case NSQLv1Generated::TRule_index_setting_value::kAltIndexSettingValue1: // id_or_type
            return IdEx(node.GetAlt_index_setting_value1().GetRule_id_or_type1(), *this).Name;
        case NSQLv1Generated::TRule_index_setting_value::kAltIndexSettingValue2: { // STRING_VALUE
            const TString stringValue = Token(node.GetAlt_index_setting_value2().GetToken1());
            const auto unescaped = StringContent(Ctx_, Ctx_.Pos(), stringValue);
            return unescaped ? unescaped->Content : "";
        }
        case NSQLv1Generated::TRule_index_setting_value::kAltIndexSettingValue3: // integer
            return Token(node.GetAlt_index_setting_value3().GetRule_integer1().GetToken1());
        case NSQLv1Generated::TRule_index_setting_value::kAltIndexSettingValue4: // bool_value
            return Token(node.GetAlt_index_setting_value4().GetRule_bool_value1().GetToken1());
        case NSQLv1Generated::TRule_index_setting_value::kAltIndexSettingValue5: // real
            return Token(node.GetAlt_index_setting_value5().GetRule_real1().GetToken1());
        case NSQLv1Generated::TRule_index_setting_value::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

bool TSqlTranslation::AddIndexSetting(const TIdentifier& id,
                                      const TRule_index_setting_value& node,
                                      TIndexDescription::TIndexSettings& indexSettings) {
    // TODO: remove to_lower transformation after the next release to keep backward compatibility
    const auto name = to_lower(id.Name);
    const auto value = to_lower(GetIndexSettingStringValue(node));

    TIndexDescription::TIndexSetting indexSetting{
        .Name = name,
        .NamePosition = id.Pos,
        .Value = value,
        .ValuePosition = Ctx_.Pos()};

    if (!indexSettings.emplace(name, indexSetting).second) {
        Ctx_.Error() << "Duplicated " << name;
        return false;
    }

    return true;
}

bool TSqlTranslation::AddCompactSetting(const TIdentifier& id, const TRule_compact_setting_value& value, TCompactEntry& compactEntry) {
    if (to_lower(id.Name) == "cascade") {
        if (compactEntry.Cascade) {
            Ctx_.Error() << "Duplicated " << to_upper(id.Name);
            return false;
        }
        compactEntry.Cascade = ParseLiteral(value, Ctx_, Mode_, "Bool");
        if (!compactEntry.Cascade) {
            Ctx_.Error() << to_upper(id.Name) << " value should be a boolean";
            return false;
        }
    } else if (to_lower(id.Name) == "max_shards_in_flight") {
        if (compactEntry.MaxShardsInFlight) {
            Ctx_.Error() << "Duplicated " << to_upper(id.Name);
            return false;
        }
        compactEntry.MaxShardsInFlight = ParseLiteral(value, Ctx_, Mode_, "Int32");
        if (!compactEntry.MaxShardsInFlight) {
            Ctx_.Error() << to_upper(id.Name) << " value should be a Int32";
            return false;
        }
        i32 value = FromString<i32>(compactEntry.MaxShardsInFlight->GetLiteralValue());
        if (value <= 0) {
            Ctx_.Error() << to_upper(id.Name) << " value should be positive" << value;
            return false;
        }
    } else {
        Ctx_.Error() << to_upper(id.Name) << ": unknown setting for compact";
        return false;
    }
    return true;
}

std::pair<TString, TViewDescription> TableKeyImpl(const std::pair<bool, TString>& nameWithAt, TViewDescription view, TTranslation& ctx) {
    if (nameWithAt.first) {
        view = {"@"};
        ctx.Context().IncrementMonCounter("sql_features", "AnonymousTable");
    }

    return std::make_pair(nameWithAt.second, view);
}

std::pair<TString, TViewDescription> TableKeyImpl(const TRule_table_key& node, TTranslation& ctx, bool hasAt) {
    auto name(Id(node.GetRule_id_table_or_type1(), ctx));
    TViewDescription view;
    if (node.HasBlock2()) {
        view = Id(node.GetBlock2().GetRule_view_name2(), ctx);
        ctx.Context().IncrementMonCounter("sql_features", "View");
    }

    return TableKeyImpl(std::make_pair(hasAt, name), view, ctx);
}

/// \return optional prefix
TString ColumnNameAsStr(TTranslation& ctx, const TRule_column_name& node, TString& id) {
    id = Id(node.GetRule_an_id2(), ctx);
    return OptIdPrefixAsStr(node.GetRule_opt_id_prefix1(), ctx);
}

TString ColumnNameAsSingleStr(TTranslation& ctx, const TRule_column_name& node) {
    TString body;
    const TString prefix = ColumnNameAsStr(ctx, node, body);
    return prefix ? prefix + '.' + body : body;
}

TTableHints GetContextHints(TContext& ctx) {
    TTableHints hints;
    if (ctx.PragmaInferSchema) {
        hints["infer_schema"] = {};
    }
    if (ctx.PragmaDirectRead) {
        hints["direct_read"] = {};
    }

    return hints;
}

TTableHints GetTableFuncHints(TStringBuf funcName) {
    TCiString func(funcName);
    TTableHints res;
    if (func.StartsWith("range") || func.StartsWith("like") || func.StartsWith("regexp") || func.StartsWith("filter")) {
        res.emplace("ignore_non_existing", TVector<TNodePtr>{});
    } else if (func.StartsWith("each")) {
        res.emplace("ignore_non_existing", TVector<TNodePtr>{});
        res.emplace("warn_non_existing", TVector<TNodePtr>{});
    }

    return res;
}

TNodeResult TSqlTranslation::NamedExpr(
    const TRule_expr& exprTree,
    const TRule_an_id_or_type* nameTree,
    EExpr exprMode)
{
    TSqlExpression expr(Ctx_, Mode_);
    expr.SetYqlSelectProduced(IsYqlSelectProduced_);
    if (exprMode == EExpr::GroupBy) {
        expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::GroupBy);
    } else if (exprMode == EExpr::SqlLambdaParams) {
        expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::SqlLambdaParams);
    }
    if (nameTree) {
        expr.MarkAsNamed();
    }

    TNodeResult exprNode = expr.Build(exprTree);
    if (!exprNode && exprNode.error() == ESQLError::Basic) {
        Ctx_.IncrementMonCounter("sql_errors", "NamedExprInvalid");
        return std::unexpected(ESQLError::Basic);
    }
    if (!exprNode) {
        return std::unexpected(exprNode.error());
    }

    if (nameTree) {
        exprNode = Wrap(SafeClone(TNodePtr(*exprNode)));
        (*exprNode)->SetLabel(Id(*nameTree, *this));
    }

    return exprNode;
}

TNodeResult TSqlTranslation::NamedExpr(const TRule_named_expr& node, EExpr exprMode) {
    return NamedExpr(
        node.GetRule_expr1(),
        (node.HasBlock2() ? &node.GetBlock2().GetRule_an_id_or_type2() : nullptr),
        exprMode);
}

TSQLStatus TSqlTranslation::NamedExprList(const TRule_named_expr_list& node, TVector<TNodePtr>& exprs, EExpr exprMode) {
    if (auto result = NamedExpr(node.GetRule_named_expr1(), exprMode)) {
        exprs.emplace_back(std::move(*result));
    } else {
        return std::unexpected(result.error());
    }

    for (auto& b : node.GetBlock2()) {
        if (auto result = NamedExpr(b.GetRule_named_expr2(), exprMode)) {
            exprs.emplace_back(std::move(*result));
        } else {
            return std::unexpected(result.error());
        }
    }

    return std::monostate();
}

bool TSqlTranslation::BindList(const TRule_bind_parameter_list& node, TVector<TSymbolNameWithPos>& bindNames) {
    bindNames.clear();

    TString name;
    if (!NamedNodeImpl(node.GetRule_bind_parameter1(), name, *this)) {
        return false;
    }

    bindNames.emplace_back(TSymbolNameWithPos{name, Ctx_.Pos()});
    for (auto& b : node.GetBlock2()) {
        if (!NamedNodeImpl(b.GetRule_bind_parameter2(), name, *this)) {
            return false;
        }

        bindNames.emplace_back(TSymbolNameWithPos{name, Ctx_.Pos()});
    }
    return true;
}

bool TSqlTranslation::ActionOrSubqueryArgs(const TRule_action_or_subquery_args& node, TVector<TSymbolNameWithPos>& bindNames, ui32& optionalArgsCount) {
    bindNames.clear();
    optionalArgsCount = 0;

    TString name;
    bool isOptional = false;
    if (!NamedNodeImpl(node.GetRule_opt_bind_parameter1(), name, isOptional, *this)) {
        return false;
    }

    if (isOptional) {
        optionalArgsCount++;
    }
    bindNames.emplace_back(TSymbolNameWithPos{name, Ctx_.Pos()});

    for (auto& b : node.GetBlock2()) {
        if (!NamedNodeImpl(b.GetRule_opt_bind_parameter2(), name, isOptional, *this)) {
            return false;
        }

        if (isOptional) {
            optionalArgsCount++;
        } else if (optionalArgsCount > 0) {
            Context().Error() << "Non-optional argument can not follow optional one";
            return false;
        }
        bindNames.emplace_back(TSymbolNameWithPos{name, Ctx_.Pos()});
    }
    return true;
}

bool TSqlTranslation::ModulePath(const TRule_module_path& node, TVector<TString>& path) {
    if (node.HasBlock1()) {
        path.emplace_back(TString());
    }
    path.emplace_back(Id(node.GetRule_an_id2(), *this));
    for (auto& b : node.GetBlock3()) {
        path.emplace_back(Id(b.GetRule_an_id2(), *this));
    }
    return true;
}

bool TSqlTranslation::NamedBindList(const TRule_named_bind_parameter_list& node, TVector<TSymbolNameWithPos>& names,
                                    TVector<TSymbolNameWithPos>& aliases)
{
    names.clear();
    aliases.clear();
    TSymbolNameWithPos name;
    TSymbolNameWithPos alias;

    if (!NamedBindParam(node.GetRule_named_bind_parameter1(), name, alias)) {
        return false;
    }
    names.push_back(name);
    aliases.push_back(alias);

    for (auto& b : node.GetBlock2()) {
        if (!NamedBindParam(b.GetRule_named_bind_parameter2(), name, alias)) {
            return false;
        }
        names.push_back(name);
        aliases.push_back(alias);
    }
    return true;
}

bool TSqlTranslation::NamedBindParam(const TRule_named_bind_parameter& node, TSymbolNameWithPos& name, TSymbolNameWithPos& alias) {
    name = alias = {};
    if (!NamedNodeImpl(node.GetRule_bind_parameter1(), name.Name, *this)) {
        return false;
    }
    name.Pos = Ctx_.Pos();
    if (node.HasBlock2()) {
        if (!NamedNodeImpl(node.GetBlock2().GetRule_bind_parameter2(), alias.Name, *this)) {
            return false;
        }
        alias.Pos = Ctx_.Pos();
    }
    return true;
}

TSQLResult<TTableArg> TSqlTranslation::TableArgImpl(const TRule_table_arg& node) {
    TTableArg ret;

    ret.HasAt = node.HasBlock1();

    TColumnRefScope scope(Ctx_, EColumnRefState::AsStringLiteral);
    if (auto result = NamedExpr(node.GetRule_named_expr2())) {
        ret.Expr = std::move(*result);
    } else {
        return std::unexpected(result.error());
    }

    if (node.HasBlock3()) {
        ret.View = Id(node.GetBlock3().GetRule_view_name2(), *this);
        Context().IncrementMonCounter("sql_features", "View");
    }

    return ret;
}

bool TSqlTranslation::ClusterExpr(const TRule_cluster_expr& node, bool allowWildcard, TString& service, TDeferredAtom& cluster) {
    bool allowBinding = false;
    bool isBinding;
    return ClusterExpr(node, allowWildcard, allowBinding, service, cluster, isBinding);
}

bool TSqlTranslation::ClusterExprOrBinding(const TRule_cluster_expr& node, TString& service, TDeferredAtom& cluster, bool& isBinding) {
    bool allowWildcard = false;
    bool allowBinding = true;
    return ClusterExpr(node, allowWildcard, allowBinding, service, cluster, isBinding);
}

bool TSqlTranslation::ClusterExpr(const TRule_cluster_expr& node, bool allowWildcard, bool allowBinding, TString& service,
                                  TDeferredAtom& cluster, bool& isBinding)
{
    service = "";
    cluster = TDeferredAtom();
    isBinding = false;
    if (node.HasBlock1()) {
        service = to_lower(Id(node.GetBlock1().GetRule_an_id1(), *this));
        allowBinding = false;
        if (service != YtProviderName &&
            service != KikimrProviderName &&
            service != RtmrProviderName && service != StatProviderName) {
            Ctx_.Error() << "Unknown service: " << service;
            return false;
        }
    }

    switch (node.GetBlock2().Alt_case()) {
        case TRule_cluster_expr::TBlock2::kAlt1: {
            auto value = PureColumnOrNamed(node.GetBlock2().GetAlt1().GetRule_pure_column_or_named1(), *this);
            if (value.Empty()) {
                return false;
            }

            if (value.GetLiteral()) {
                TString clusterName = *value.GetLiteral();
                if (allowBinding && to_lower(clusterName) == "bindings") {
                    switch (Ctx_.Settings.BindingsMode) {
                        case NSQLTranslation::EBindingsMode::DISABLED:
                            Ctx_.Error(Ctx_.Pos(), TIssuesIds::YQL_DISABLED_BINDINGS) << "Please remove 'bindings.' from your query, the support for this syntax has ended";
                            Ctx_.IncrementMonCounter("sql_errors", "DisabledBinding");
                            return false;
                        case NSQLTranslation::EBindingsMode::ENABLED:
                            isBinding = true;
                            break;
                        case NSQLTranslation::EBindingsMode::DROP_WITH_WARNING:
                            if (!Ctx_.Warning(Ctx_.Pos(), TIssuesIds::YQL_DEPRECATED_BINDINGS, [](auto& out) {
                                    out << "Please remove 'bindings.' from your query, "
                                        << "the support for this syntax will be dropped soon";
                                })) {
                                return false;
                            }
                            Ctx_.IncrementMonCounter("sql_errors", "DeprecatedBinding");
                            [[fallthrough]];
                        case NSQLTranslation::EBindingsMode::DROP:
                            service = Context().Scoped->CurrService;
                            cluster = Context().Scoped->CurrCluster;
                            break;
                    }

                    return true;
                }
                TString normalizedClusterName;
                auto foundProvider = Ctx_.GetClusterProvider(clusterName, normalizedClusterName);
                if (!foundProvider) {
                    Ctx_.Error() << "Unknown cluster: " << clusterName;
                    return false;
                }

                if (service && *foundProvider != service) {
                    Ctx_.Error() << "Mismatch of cluster " << clusterName << " service, expected: "
                                 << *foundProvider << ", got: " << service;
                    return false;
                }

                if (!service) {
                    service = *foundProvider;
                }

                value = TDeferredAtom(Ctx_.Pos(), normalizedClusterName);
            } else {
                if (!service) {
                    Ctx_.Error() << "Cluster service is not set";
                    return false;
                }
            }

            cluster = value;
            return true;
        }
        case TRule_cluster_expr::TBlock2::kAlt2: {
            if (!allowWildcard) {
                Ctx_.Error() << "Cluster wildcards allowed only in USE statement";
                return false;
            }

            return true;
        }
        case TRule_cluster_expr::TBlock2::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

bool TSqlTranslation::ApplyTableBinding(const TString& binding, TTableRef& tr, TTableHints& hints) {
    NSQLTranslation::TBindingInfo bindingInfo;
    if (const auto& error = ExtractBindingInfo(Context().Settings, binding, bindingInfo)) {
        Ctx_.Error() << error;
        return false;
    }

    if (bindingInfo.Schema) {
        TNodePtr schema = BuildQuotedAtom(Ctx_.Pos(), bindingInfo.Schema);

        TNodePtr type = new TCallNodeImpl(Ctx_.Pos(), "SqlTypeFromYson", {schema});
        TNodePtr columns = new TCallNodeImpl(Ctx_.Pos(), "SqlColumnOrderFromYson", {schema});

        hints["user_schema"] = {type, columns};
    }

    for (auto& [key, values] : bindingInfo.Attributes) {
        TVector<TNodePtr> hintValue;
        for (auto& column : values) {
            hintValue.push_back(BuildQuotedAtom(Ctx_.Pos(), column));
        }
        hints[key] = std::move(hintValue);
    }

    tr.Service = bindingInfo.ClusterType;
    tr.Cluster = TDeferredAtom(Ctx_.Pos(), bindingInfo.Cluster);

    const TString view = "";
    tr.Keys = BuildTableKey(Ctx_.Pos(), tr.Service, tr.Cluster, TDeferredAtom(Ctx_.Pos(), bindingInfo.Path), {view});

    return true;
}

bool TSqlTranslation::TableRefImpl(const TRule_table_ref& node, TTableRef& result, bool unorderedSubquery) {
    // table_ref:
    //   (cluster_expr DOT)? AT?
    //   (table_key | an_id_expr LPAREN (table_arg (COMMA table_arg)*)? RPAREN |
    //    bind_parameter (LPAREN expr_list? RPAREN)? (VIEW an_id)?)
    //   table_hints?;
    if (Mode_ == NSQLTranslation::ESqlMode::LIMITED_VIEW && node.HasBlock1()) {
        Ctx_.Error() << "Cluster should not be used in limited view";
        return false;
    }
    auto service = Context().Scoped->CurrService;
    auto cluster = Context().Scoped->CurrCluster;
    const bool hasAt = node.HasBlock2();
    bool isBinding = false;
    if (node.HasBlock1()) {
        const auto& clusterExpr = node.GetBlock1().GetRule_cluster_expr1();
        bool result = !hasAt ? ClusterExprOrBinding(clusterExpr, service, cluster, isBinding) : ClusterExpr(clusterExpr, false, service, cluster);
        if (!result) {
            return false;
        }
    }

    TTableRef tr(Context().MakeName("table"), service, cluster, nullptr);
    TPosition pos(Context().Pos());
    TTableHints hints = GetContextHints(Ctx_);
    TTableHints tableHints;

    TMaybe<TString> keyFunc;

    auto& block = node.GetBlock3();
    switch (block.Alt_case()) {
        case TRule_table_ref::TBlock3::kAlt1: {
            if (!isBinding && cluster.Empty()) {
                Ctx_.Error() << "No cluster name given and no default cluster is selected";
                return false;
            }

            auto pair = TableKeyImpl(block.GetAlt1().GetRule_table_key1(), *this, hasAt);
            if (isBinding) {
                TString binding = pair.first;
                auto view = pair.second;
                if (!view.ViewName.empty()) {
                    YQL_ENSURE(view != TViewDescription{"@"});
                    Ctx_.Error() << "VIEW is not supported for table bindings";
                    return false;
                }

                if (!ApplyTableBinding(binding, tr, tableHints)) {
                    return false;
                }
            } else {
                tr.Keys = BuildTableKey(pos, service, cluster, TDeferredAtom(pos, pair.first), pair.second);
            }
            break;
        }
        case TRule_table_ref::TBlock3::kAlt2: {
            if (cluster.Empty()) {
                Ctx_.Error() << "No cluster name given and no default cluster is selected";
                return false;
            }

            auto& alt = block.GetAlt2();
            keyFunc = Id(alt.GetRule_an_id_expr1(), *this);
            TVector<TTableArg> args;
            if (alt.HasBlock3()) {
                auto& argsBlock = alt.GetBlock3();
                auto arg = TableArgImpl(argsBlock.GetRule_table_arg1());
                if (!arg) {
                    return false;
                }

                args.push_back(std::move(*arg));
                for (auto& b : argsBlock.GetBlock2()) {
                    arg = TableArgImpl(b.GetRule_table_arg2());
                    if (!arg) {
                        return false;
                    }

                    args.push_back(std::move(*arg));
                }
            }
            tableHints = GetTableFuncHints(*keyFunc);
            tr.Keys = BuildTableKeys(pos, service, cluster, *keyFunc, args);
            break;
        }
        case TRule_table_ref::TBlock3::kAlt3: {
            auto& alt = block.GetAlt3();
            Ctx_.IncrementMonCounter("sql_features", "NamedNodeUseSource");
            TString named;
            if (!NamedNodeImpl(alt.GetRule_bind_parameter1(), named, *this)) {
                return false;
            }
            if (hasAt) {
                if (alt.HasBlock2()) {
                    Ctx_.Error() << "Subquery must not be used as anonymous table name";
                    return false;
                }

                if (alt.HasBlock3()) {
                    Ctx_.Error() << "View is not supported for anonymous tables";
                    return false;
                }

                if (node.HasBlock4()) {
                    Ctx_.Error() << "Hints are not supported for anonymous tables";
                    return false;
                }

                auto namedNode = GetNamedNode(named);
                if (!namedNode) {
                    return false;
                }

                auto source = TryMakeSourceFromExpression(Ctx_.Pos(), Ctx_, service, cluster, namedNode, "@");
                if (!source) {
                    Ctx_.Error() << "Cannot infer cluster and table name";
                    return false;
                }

                result.Source = source;
                return true;
            }
            auto nodePtr = GetNamedNode(named);
            if (!nodePtr) {
                Ctx_.IncrementMonCounter("sql_errors", "NamedNodeSourceError");
                return false;
            }
            if (alt.HasBlock2()) {
                if (alt.HasBlock3()) {
                    Ctx_.Error() << "View is not supported for subqueries";
                    return false;
                }

                if (node.HasBlock4()) {
                    Ctx_.Error() << "Hints are not supported for subqueries";
                    return false;
                }

                TVector<TNodePtr> values;
                values.push_back(new TAstAtomNodeImpl(Ctx_.Pos(), "Apply", TNodeFlags::Default));
                values.push_back(nodePtr);
                values.push_back(new TAstAtomNodeImpl(Ctx_.Pos(), "world", TNodeFlags::Default));

                TSqlExpression sqlExpr(Ctx_, Mode_);
                if (alt.GetBlock2().HasBlock2() && !Unwrap(ExprList(sqlExpr, values, alt.GetBlock2().GetBlock2().GetRule_expr_list1()))) {
                    return false;
                }

                TNodePtr apply = new TAstListNodeImpl(Ctx_.Pos(), std::move(values));
                if (unorderedSubquery && Ctx_.UnorderedSubqueries) {
                    apply = new TCallNodeImpl(Ctx_.Pos(), "UnorderedSubquery", {apply});
                }
                result.Source = BuildNodeSource(Ctx_.Pos(), apply);
                return true;
            }

            TTableHints hints;
            TTableHints contextHints = GetContextHints(Ctx_);
            auto ret = BuildInnerSource(Ctx_.Pos(), nodePtr, service, cluster);
            if (alt.HasBlock3()) {
                auto view = Id(alt.GetBlock3().GetRule_view_name2(), *this);
                Ctx_.IncrementMonCounter("sql_features", "View");
                bool result = view.PrimaryFlag
                                  ? ret->SetPrimaryView(Ctx_, Ctx_.Pos())
                                  : ret->SetViewName(Ctx_, Ctx_.Pos(), view.ViewName);
                if (!result) {
                    return false;
                }
            }

            if (node.HasBlock4()) {
                auto tmp = TableHintsImpl(node.GetBlock4().GetRule_table_hints1(), service, keyFunc.GetOrElse(""));
                if (!tmp) {
                    return false;
                }

                hints = *tmp;
            }

            if (hints || contextHints) {
                if (!ret->SetTableHints(Ctx_, Ctx_.Pos(), hints, contextHints)) {
                    return false;
                }
            }

            result.Source = ret;
            return true;
        }
        case TRule_table_ref::TBlock3::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    MergeHints(hints, tableHints);

    if (node.HasBlock4()) {
        auto tmp = TableHintsImpl(node.GetBlock4().GetRule_table_hints1(), service, keyFunc.GetOrElse(""));
        if (!tmp) {
            Ctx_.Error() << "Failed to parse table hints";
            return false;
        }

        MergeHints(hints, *tmp);
    }

    if (!hints.empty()) {
        tr.Options = BuildInputOptions(pos, hints);
    }

    if (!tr.Keys) {
        return false;
    }

    result = tr;
    return true;
}

TMaybe<TSourcePtr> TSqlTranslation::AsTableImpl(const TRule_table_ref& node) {
    const auto& block = node.GetBlock3();

    if (block.Alt_case() == TRule_table_ref::TBlock3::kAlt2) {
        auto& alt = block.GetAlt2();

        TString func = Id(alt.GetRule_an_id_expr1(), *this);
        if (auto issue = NormalizeName(Ctx_.Pos(), func)) {
            Error() << issue->GetMessage();
            Ctx_.IncrementMonCounter("sql_errors", "NormalizeTableFunctionError");
            return nullptr;
        }

        if (func == "astable") {
            if (node.HasBlock1()) {
                Ctx_.Error() << "Cluster shouldn't be specified for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (!alt.HasBlock3() || !alt.GetBlock3().GetBlock2().empty()) {
                Ctx_.Error() << "Expected single argument for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (node.HasBlock4()) {
                Ctx_.Error() << "No hints expected for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            auto arg = TableArgImpl(alt.GetBlock3().GetRule_table_arg1());
            if (!arg) {
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (arg->Expr->GetSource()) {
                Ctx_.Error() << "AS_TABLE shouldn't be used for table sources";
                return TMaybe<TSourcePtr>(nullptr);
            }

            return BuildNodeSource(Ctx_.Pos(), arg->Expr, true, Ctx_.EmitTableSource);
        }
    }

    return Nothing();
}

bool ColumnCompression(const TRule_compression_setting_entry& node, TTranslation& ctx, TCompression& compression) {
    const auto id = to_lower(Id(node.GetRule_an_id1(), ctx));

    const auto& value = node.GetRule_compression_setting_value3(); // compression_setting_value

    if (compression.Entries.contains(id)) {
        ctx.Context().Error() << "'" << id << "' setting can be specified only once";
        return false;
    }

    switch (value.Alt_case()) {
        case TRule_compression_setting_value::AltCase::kAltCompressionSettingValue1: {
            const auto result = ParseInteger(ctx.Context(), value.GetAlt_compression_setting_value1().GetRule_integer1());
            if (!result) {
                return false; // ParseInteger has already set error to context
            }
            const auto literal = MakeIntrusive<TLiteralNumberNode<ui64>>(ctx.Context().Pos(), "Uint64", ToString(*result));

            compression.Entries[std::move(id)] = std::move(literal);
            break;
        }
        case TRule_compression_setting_value::AltCase::kAltCompressionSettingValue2: {
            const auto result = Id(value.GetAlt_compression_setting_value2().GetRule_id1(), ctx);
            const TNodePtr literal = BuildLiteralRawString(ctx.Context().Pos(), result);

            compression.Entries[std::move(id)] = std::move(literal);
            break;
        }
        case TRule_compression_setting_value::AltCase::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    return true;
}

TMaybe<TCompression> ColumnCompression(const TRule_compression& node, TTranslation& ctx) {
    // compression: COMPRESSION LPAREN (compression_setting_entry (COMMA compression_setting_entry)*)? COMMA? RPAREN;

    TCompression compression;

    if (!node.HasBlock3()) { // compression_setting_entry
        return compression;
    }

    const auto& block = node.GetBlock3();
    const auto& entry = block.GetRule_compression_setting_entry1();
    if (!ColumnCompression(entry, ctx, compression)) {
        return Nothing();
    }

    for (const auto& block2 : block.GetBlock2()) {
        const auto& entry2 = block2.GetRule_compression_setting_entry2();
        if (!ColumnCompression(entry2, ctx, compression)) {
            return Nothing();
        }
    }

    return compression;
}

TMaybe<TColumnOptions> ColumnOptions(const TRule_column_schema& node, TTranslation& ctx) {
    TNodePtr defaultExpr;
    TVector<TIdentifier> families;
    TMaybe<TCompression> compression;
    bool nullable = true;

    const auto& optionsList = node.GetRule_column_option_list3();

    enum class EOption {
        Family,
        NotNull,
        DefaultValue,
        Compression,
    };

    TVector<TRule_column_option> columnOptions(Reserve(static_cast<size_t>(EOption::Compression) + 1));

    {
        switch (optionsList.Alt_case()) {
            case TRule_column_option_list::kAltColumnOptionList1: {
                const auto& block = optionsList.GetAlt_column_option_list1().GetRule_column_option_list_space1().GetBlock1();
                for (const auto& item : block) {
                    const auto& rule = item.GetRule_column_option1();
                    columnOptions.push_back(rule);
                }

                break;
            }

            case TRule_column_option_list::kAltColumnOptionList2: {
                const auto& optionsNode = optionsList.GetAlt_column_option_list2().GetRule_column_option_list_comma1();
                const auto& firstColumnOption = optionsNode.GetRule_column_option2();
                columnOptions.push_back(firstColumnOption);

                {
                    auto block = optionsNode.GetBlock3();

                    for (const auto& item : block) {
                        const auto& rule = item.GetRule_column_option2();
                        columnOptions.push_back(rule);
                    }
                }

                break;
            }

            case TRule_column_option_list::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    {
        std::vector<EOption> usedOptions;
        usedOptions.reserve(static_cast<size_t>(EOption::DefaultValue) + 1);

        for (const auto& rule : columnOptions) {
            switch (rule.Alt_case()) {
                case TRule_column_option::kAltColumnOption1: { // family_relation
                    const auto opt = rule.GetAlt_column_option1().GetRule_family_relation1();
                    if (std::find(usedOptions.begin(), usedOptions.end(), EOption::Family) != usedOptions.end()) {
                        TPosition pos = ctx.Context().TokenPosition(opt.GetToken1());
                        ctx.Context().Error(pos) << "'FAMILY' option can be specified only once";
                        return {};
                    }

                    usedOptions.push_back(EOption::Family);

                    families.push_back(IdEx(opt.GetRule_an_id2(), ctx));
                    break;
                }
                case TRule_column_option::kAltColumnOption2: { // nullability
                    const auto opt = rule.GetAlt_column_option2().GetRule_nullability1();
                    if (std::find(usedOptions.begin(), usedOptions.end(), EOption::NotNull) != usedOptions.end()) {
                        TPosition pos = ctx.Context().TokenPosition(opt.GetToken2());
                        ctx.Context().Error(pos) << "'NOT NULL' option can be specified only once";
                        return {};
                    }

                    usedOptions.push_back(EOption::NotNull);

                    nullable = !opt.HasBlock1();
                    break;
                }
                case TRule_column_option::kAltColumnOption3: { // default_value
                    const auto opt = rule.GetAlt_column_option3().GetRule_default_value1();
                    if (std::find(usedOptions.begin(), usedOptions.end(), EOption::DefaultValue) != usedOptions.end()) {
                        TPosition pos = ctx.Context().TokenPosition(opt.GetToken1());
                        ctx.Context().Error(pos) << "'DEFAULT' option can be specified only once";
                        return {};
                    }

                    usedOptions.push_back(EOption::DefaultValue);

                    TSqlExpression expr(ctx.Context(), ctx.Context().Settings.Mode);

                    // TODO: We need to refact it
                    auto legacyNotNullLastValue = ctx.Context().DisableLegacyNotNull;
                    Y_DEFER {
                        ctx.Context().DisableLegacyNotNull = legacyNotNullLastValue;
                    };

                    ctx.Context().DisableLegacyNotNull = true;

                    defaultExpr = Unwrap(expr.Build(opt.GetRule_expr2()));

                    if (AnyOf(ctx.Context().Issues.begin(), ctx.Context().Issues.end(), [](const auto& issue) {
                            return issue.GetCode() == TIssuesIds::YQL_MISSING_IS_BEFORE_NOT_NULL;
                        })) {
                        TPosition pos = ctx.Context().TokenPosition(opt.GetToken1());
                        ctx.Context().Error(pos) << "'DEFAULT' option can not use expr which contains literall 'NOT NULL'."
                                                 << " If you wanted to use two different options 'DEFAULT' and 'NOT NULL',"
                                                 << " it is recommended to use the syntax '(DEFAULT value, NOT NULL, ...)'";

                        return {};
                    }

                    if (!defaultExpr) {
                        return {};
                    }

                    break;
                }
                case TRule_column_option::kAltColumnOption4: { // compression
                    const auto opt = rule.GetAlt_column_option4().GetRule_compression1();
                    if (std::find(usedOptions.begin(), usedOptions.end(), EOption::Compression) != usedOptions.end()) {
                        TPosition pos = ctx.Context().TokenPosition(opt.GetToken1());
                        ctx.Context().Error(pos) << "'COMPRESSION' option can be specified only once";
                        return {};
                    }

                    usedOptions.push_back(EOption::Compression);

                    compression = ColumnCompression(opt, ctx);
                    break;
                }
                case TRule_column_option::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }
        }
    }

    return TColumnOptions{
        .DefaultExpr = std::move(defaultExpr),
        .Families = std::move(families),
        .Compression = std::move(compression),
        .Nullable = nullable};
}

TMaybe<TColumnSchema> TSqlTranslation::ColumnSchemaImpl(const TRule_column_schema& node) {
    const TString name(Id(node.GetRule_an_id_schema1(), *this));
    const TPosition pos(Context().Pos());
    TNodePtr type = SerialTypeNode(node.GetRule_type_name_or_bind2());
    const bool serial = (type != nullptr);

    const auto columnOptions = ColumnOptions(node, *this);
    if (!columnOptions) {
        return {};
    }

    const auto [defaultExpr, families, compression, nullable] = columnOptions.GetRef();

    if (!type) {
        type = TypeNodeOrBind(node.GetRule_type_name_or_bind2());
    }

    if (!type) {
        return {};
    }

    return TColumnSchema{
        .Pos = std::move(pos),
        .Name = std::move(name),
        .Type = std::move(type),
        .Families = std::move(families),
        .DefaultExpr = std::move(defaultExpr),
        .Compression = compression,
        .Nullable = nullable,
        .Serial = serial,
    };
}

TNodePtr TSqlTranslation::SerialTypeNode(const TRule_type_name_or_bind& node) {
    if (node.Alt_case() != TRule_type_name_or_bind::kAltTypeNameOrBind1) {
        return nullptr;
    }

    TPosition pos = Ctx_.Pos();

    auto typeNameNode = node.GetAlt_type_name_or_bind1().GetRule_type_name1();
    if (typeNameNode.Alt_case() != TRule_type_name::kAltTypeName2) {
        return nullptr;
    }

    auto alt = typeNameNode.GetAlt_type_name2();
    auto& block = alt.GetBlock1();
    if (block.Alt_case() != TRule_type_name::TAlt2::TBlock1::kAlt2) {
        return nullptr;
    }

    auto alt2 = block.GetAlt2().GetRule_type_name_simple1();
    const TString name = Id(alt2.GetRule_an_id_pure1(), *this);
    if (name.empty()) {
        return nullptr;
    }

    const auto res = to_lower(name);
    if (res == "bigserial" || res == "serial8") {
        return new TCallNodeImpl(pos, "DataType", {BuildQuotedAtom(pos, "Int64", TNodeFlags::Default)});
    } else if (res == "serial" || res == "serial4") {
        return new TCallNodeImpl(pos, "DataType", {BuildQuotedAtom(pos, "Int32", TNodeFlags::Default)});
    } else if (res == "smallserial" || res == "serial2") {
        return new TCallNodeImpl(pos, "DataType", {BuildQuotedAtom(pos, "Int16", TNodeFlags::Default)});
    }

    return nullptr;
}

bool StoreString(const TRule_family_setting_value& from, TNodePtr& to, TContext& ctx) {
    switch (from.Alt_case()) {
        case TRule_family_setting_value::kAltFamilySettingValue1: {
            // STRING_VALUE
            const TString stringValue(ctx.Token(from.GetAlt_family_setting_value1().GetToken1()));
            TNodePtr literal = BuildLiteralSmartString(ctx, stringValue);
            if (!literal) {
                return false;
            }
            to = literal;
            break;
        }
        default:
            return false;
    }
    return true;
}

bool StoreInt(const TRule_family_setting_value& from, TNodePtr& to, TContext& ctx) {
    switch (from.Alt_case()) {
        case TRule_family_setting_value::kAltFamilySettingValue2: {
            // integer
            TNodePtr literal = LiteralNumber(ctx, from.GetAlt_family_setting_value2().GetRule_integer1());
            if (!literal) {
                return false;
            }
            to = literal;
            break;
        }
        default:
            return false;
    }
    return true;
}

bool TSqlTranslation::FillFamilySettingsEntry(const TRule_family_settings_entry& settingNode, TFamilyEntry& family) {
    TIdentifier id = IdEx(settingNode.GetRule_an_id1(), *this);
    const TRule_family_setting_value& value = settingNode.GetRule_family_setting_value3();
    if (to_lower(id.Name) == "data") {
        if (!StoreString(value, family.Data, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else if (to_lower(id.Name) == "compression") {
        if (!StoreString(value, family.Compression, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else if (to_lower(id.Name) == "compression_level") {
        if (!StoreInt(value, family.CompressionLevel, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "cache_mode") {
        if (!StoreString(value, family.CacheMode, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else {
        Ctx_.Error() << "Unknown table setting: " << id.Name;
        return false;
    }
    return true;
}

bool TSqlTranslation::FillFamilySettings(const TRule_family_settings& settingsNode, TFamilyEntry& family) {
    // family_settings: LPAREN (family_settings_entry (COMMA family_settings_entry)*)? RPAREN;
    if (settingsNode.HasBlock2()) {
        auto& settings = settingsNode.GetBlock2();
        if (!FillFamilySettingsEntry(settings.GetRule_family_settings_entry1(), family)) {
            return false;
        }
        for (auto& block : settings.GetBlock2()) {
            if (!FillFamilySettingsEntry(block.GetRule_family_settings_entry2(), family)) {
                return false;
            }
        }
    }
    return true;
}

bool TSqlTranslation::CreateTableEntry(const TRule_create_table_entry& node, TCreateTableParameters& params, const bool isCreateTableAs)
{
    switch (node.Alt_case()) {
        case TRule_create_table_entry::kAltCreateTableEntry1: {
            if (isCreateTableAs) {
                Ctx_.Error() << "Column types are not supported for CREATE TABLE AS";
                return false;
            }
            // column_schema
            auto columnSchema = ColumnSchemaImpl(node.GetAlt_create_table_entry1().GetRule_column_schema1());
            if (!columnSchema) {
                return false;
            }
            if (columnSchema->Families.size() > 1) {
                Ctx_.Error() << "Several column families for a single column are not yet supported";
                return false;
            }
            params.Columns.push_back(*columnSchema);
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry2: {
            // table_constraint
            auto& constraint = node.GetAlt_create_table_entry2().GetRule_table_constraint1();
            switch (constraint.Alt_case()) {
                case TRule_table_constraint::kAltTableConstraint1: {
                    if (!params.PkColumns.empty()) {
                        Ctx_.Error() << "PRIMARY KEY statement must be specified only once";
                        return false;
                    }
                    auto& pkConstraint = constraint.GetAlt_table_constraint1();
                    params.PkColumns.push_back(IdEx(pkConstraint.GetRule_an_id4(), *this));
                    for (auto& block : pkConstraint.GetBlock5()) {
                        params.PkColumns.push_back(IdEx(block.GetRule_an_id2(), *this));
                    }
                    break;
                }
                case TRule_table_constraint::kAltTableConstraint2: {
                    if (!params.PartitionByColumns.empty()) {
                        Ctx_.Error() << "PARTITION BY statement must be specified only once";
                        return false;
                    }
                    auto& pbConstraint = constraint.GetAlt_table_constraint2();
                    params.PartitionByColumns.push_back(IdEx(pbConstraint.GetRule_an_id4(), *this));
                    for (auto& block : pbConstraint.GetBlock5()) {
                        params.PartitionByColumns.push_back(IdEx(block.GetRule_an_id2(), *this));
                    }
                    break;
                }
                case TRule_table_constraint::kAltTableConstraint3: {
                    if (!params.OrderByColumns.empty()) {
                        Ctx_.Error() << "ORDER BY statement must be specified only once";
                        return false;
                    }
                    auto& obConstraint = constraint.GetAlt_table_constraint3();
                    auto extractDirection = [this](const TRule_column_order_by_specification& spec, bool& desc) {
                        desc = false;
                        if (!spec.HasBlock2()) {
                            return true;
                        }

                        auto& token = spec.GetBlock2().GetToken1();
                        auto tokenId = token.GetId();
                        if (IS_TOKEN(tokenId, ASC)) {
                            return true;
                        } else if (IS_TOKEN(tokenId, DESC)) {
                            desc = true;
                            return true;
                        } else {
                            Ctx_.Error() << "Unsupported direction token: " << token.GetId();
                            return false;
                        }
                    };

                    bool desc = false;
                    auto& obSpec = obConstraint.GetRule_column_order_by_specification4();
                    if (!extractDirection(obSpec, desc)) {
                        return false;
                    }
                    params.OrderByColumns.push_back(std::make_pair(IdEx(obSpec.GetRule_an_id1(), *this), desc));

                    for (auto& block : obConstraint.GetBlock5()) {
                        auto& obSpec = block.GetRule_column_order_by_specification2();
                        if (!extractDirection(obSpec, desc)) {
                            return false;
                        }
                        params.OrderByColumns.push_back(std::make_pair(IdEx(obSpec.GetRule_an_id1(), *this), desc));
                    }
                    break;
                }
                case NSQLv1Generated::TRule_table_constraint::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry3: {
            // table_index
            auto& table_index = node.GetAlt_create_table_entry3().GetRule_table_index1();
            if (!CreateTableIndex(table_index, params.Indexes)) {
                return false;
            }
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry4: {
            if (isCreateTableAs) {
                Ctx_.Error() << "Column families are not supported for CREATE TABLE AS";
                return false;
            }
            // family_entry
            auto& family_entry = node.GetAlt_create_table_entry4().GetRule_family_entry1();
            TFamilyEntry family(IdEx(family_entry.GetRule_an_id2(), *this));
            if (!FillFamilySettings(family_entry.GetRule_family_settings3(), family)) {
                return false;
            }
            params.ColumnFamilies.push_back(family);
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry5: {
            // changefeed
            auto& changefeed = node.GetAlt_create_table_entry5().GetRule_changefeed1();
            TSqlExpression expr(Ctx_, Mode_);
            if (!CreateChangefeed(changefeed, expr, params.Changefeeds)) {
                return false;
            }
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry6: {
            if (!isCreateTableAs) {
                Ctx_.Error() << "Column requires a type";
                return false;
            }
            // an_id_schema
            const TString name(Id(node.GetAlt_create_table_entry6().GetRule_an_id_schema1(), *this));
            const TPosition pos(Context().Pos());

            params.Columns.push_back({.Pos = std::move(pos), .Name = std::move(name), .Nullable = true});
            break;
        }
        case NSQLv1Generated::TRule_create_table_entry::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return true;
}

namespace {
bool StoreId(const TRule_table_setting_value& from, TMaybe<TIdentifier>& to, TTranslation& ctx) {
    switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue1: {
            // id
            to = IdEx(from.GetAlt_table_setting_value1().GetRule_id1(), ctx);
            break;
        }
        default:
            return false;
    }
    return true;
}

bool StoreString(const TRule_table_setting_value& from, TNodePtr& to, TContext& ctx) {
    switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2: {
            // STRING_VALUE
            const TString stringValue(ctx.Token(from.GetAlt_table_setting_value2().GetToken1()));
            to = BuildLiteralSmartString(ctx, stringValue);
            break;
        }
        default:
            return false;
    }
    return true;
}

bool StoreString(const TRule_table_setting_value& from, TDeferredAtom& to, TContext& ctx, const TString& errorPrefix = {}) {
    switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2: {
            // STRING_VALUE
            const TString stringValue(ctx.Token(from.GetAlt_table_setting_value2().GetToken1()));
            auto unescaped = StringContent(ctx, ctx.Pos(), stringValue);
            if (!unescaped) {
                ctx.Error() << errorPrefix << " value cannot be unescaped";
                return false;
            }
            to = TDeferredAtom(ctx.Pos(), unescaped->Content);
            break;
        }
        default:
            ctx.Error() << errorPrefix << " value should be a string literal";
            return false;
    }
    return true;
}

bool StoreInt(const TRule_table_setting_value& from, TNodePtr& to, TContext& ctx) {
    switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue3: {
            // integer
            to = LiteralNumber(ctx, from.GetAlt_table_setting_value3().GetRule_integer1());
            break;
        }
        default:
            return false;
    }
    return true;
}

bool StoreInt(const TRule_table_setting_value& from, TDeferredAtom& to, TContext& ctx, const TString& errorPrefix = {}) {
    switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue3: {
            // integer
            to = TDeferredAtom(LiteralNumber(ctx, from.GetAlt_table_setting_value3().GetRule_integer1()), ctx);
            break;
        }
        default:
            ctx.Error() << errorPrefix << " value should be an integer";
            return false;
    }
    return true;
}

bool StoreSplitBoundary(const TRule_literal_value_list& boundary, TVector<TVector<TNodePtr>>& to,
                        TSqlExpression& expr, TContext& ctx) {
    TVector<TNodePtr> boundaryKeys;
    auto first_key = expr.LiteralExpr(boundary.GetRule_literal_value2());
    if (!first_key) {
        ctx.Error() << "Empty key in partition at keys";
        return false;
    }
    if (!first_key->Expr) {
        ctx.Error() << "Identifier is not expected in partition at keys";
        return false;
    }
    boundaryKeys.emplace_back(first_key->Expr);
    for (auto& key : boundary.GetBlock3()) {
        auto keyExprOrIdent = expr.LiteralExpr(key.GetRule_literal_value2());
        if (!keyExprOrIdent) {
            ctx.Error() << "Empty key in partition at keys";
            return false;
        }
        if (!keyExprOrIdent->Expr) {
            ctx.Error() << "Identifier is not expected in partition at keys";
            return false;
        }
        boundaryKeys.emplace_back(keyExprOrIdent->Expr);
    }
    to.push_back(boundaryKeys);
    return true;
}

bool StoreSplitBoundaries(const TRule_table_setting_value& from, TVector<TVector<TNodePtr>>& to,
                          TSqlExpression& expr, TContext& ctx) {
    switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue4: {
            // split_boundaries
            const auto& boundariesNode = from.GetAlt_table_setting_value4().GetRule_split_boundaries1();
            switch (boundariesNode.Alt_case()) {
                case TRule_split_boundaries::kAltSplitBoundaries1: {
                    // literal_value_list (COMMA literal_value_list)*
                    auto& complexBoundaries = boundariesNode.GetAlt_split_boundaries1();

                    auto& first_boundary = complexBoundaries.GetRule_literal_value_list2();
                    if (!StoreSplitBoundary(first_boundary, to, expr, ctx)) {
                        return false;
                    }

                    for (auto& boundary : complexBoundaries.GetBlock3()) {
                        if (!StoreSplitBoundary(boundary.GetRule_literal_value_list2(), to, expr, ctx)) {
                            return false;
                        }
                    }
                    break;
                }
                case TRule_split_boundaries::kAltSplitBoundaries2: {
                    // literal_value_list
                    auto& simpleBoundaries = boundariesNode.GetAlt_split_boundaries2().GetRule_literal_value_list1();
                    auto first_key = expr.LiteralExpr(simpleBoundaries.GetRule_literal_value2());
                    if (!first_key) {
                        ctx.Error() << "Empty key in partition at keys";
                        return false;
                    }
                    if (!first_key->Expr) {
                        ctx.Error() << "Identifier is not expected in partition at keys";
                        return false;
                    }
                    to.push_back(TVector<TNodePtr>(1, first_key->Expr));
                    for (auto& key : simpleBoundaries.GetBlock3()) {
                        auto keyExprOrIdent = expr.LiteralExpr(key.GetRule_literal_value2());
                        if (!keyExprOrIdent) {
                            ctx.Error() << "Empty key in partition at keys";
                            return false;
                        }
                        if (!first_key->Expr) {
                            ctx.Error() << "Identifier is not expected in partition at keys";
                            return false;
                        }
                        to.push_back(
                            TVector<TNodePtr>(1, keyExprOrIdent->Expr));
                    }
                    break;
                }
                default:
                    return false;
            }
            break;
        }
        default:
            return false;
    }
    return true;
}

bool FillTieringInterval(const TRule_expr& from, TNodePtr& tieringInterval, TSqlExpression& expr, TContext& ctx) {
    auto exprNode = Unwrap(expr.Build(from));
    if (!exprNode) {
        return false;
    }

    if (exprNode->GetOpName() != "Interval") {
        ctx.Error() << "Literal of Interval type is expected for TTL";
        return false;
    }

    tieringInterval = exprNode;
    return true;
}

bool FillTierAction(const TRule_ttl_tier_action& from, std::optional<TIdentifier>& storageName, TTranslation& txc) {
    switch (from.GetAltCase()) {
        case TRule_ttl_tier_action::kAltTtlTierAction1:
            storageName = IdEx(from.GetAlt_ttl_tier_action1().GetRule_an_id5(), txc);
            break;
        case TRule_ttl_tier_action::kAltTtlTierAction2:
            storageName.reset();
            break;
        case TRule_ttl_tier_action::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return true;
}

bool StoreTtlSettings(const TRule_table_setting_value& from, TResetableSetting<TTtlSettings, void>& to, TSqlExpression& expr, TContext& ctx,
                      TTranslation& txc) {
    switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue5: {
            auto columnName = IdEx(from.GetAlt_table_setting_value5().GetRule_an_id3(), txc);
            auto tiersLiteral = from.GetAlt_table_setting_value5().GetRule_ttl_tier_list1();

            TNodePtr firstInterval;
            if (!FillTieringInterval(tiersLiteral.GetRule_expr1(), firstInterval, expr, ctx)) {
                return false;
            }

            std::vector<TTtlSettings::TTierSettings> tiers;
            if (!tiersLiteral.HasBlock2()) {
                tiers.emplace_back(firstInterval);
            } else {
                std::optional<TIdentifier> firstStorageName;
                if (!FillTierAction(tiersLiteral.GetBlock2().GetRule_ttl_tier_action1(), firstStorageName, txc)) {
                    return false;
                }
                tiers.emplace_back(firstInterval, firstStorageName);

                for (const auto& tierLiteral : tiersLiteral.GetBlock2().GetBlock2()) {
                    TNodePtr intervalExpr;
                    if (!FillTieringInterval(tierLiteral.GetRule_expr2(), intervalExpr, expr, ctx)) {
                        return false;
                    }
                    std::optional<TIdentifier> storageName;
                    if (!FillTierAction(tierLiteral.GetRule_ttl_tier_action3(), storageName, txc)) {
                        return false;
                    }
                    tiers.emplace_back(intervalExpr, storageName);
                }
            }

            TMaybe<TTtlSettings::EUnit> columnUnit;
            if (from.GetAlt_table_setting_value5().HasBlock4()) {
                const TString unit = to_lower(ctx.Token(from.GetAlt_table_setting_value5().GetBlock4().GetToken2()));
                columnUnit.ConstructInPlace();
                if (!TryFromString<TTtlSettings::EUnit>(unit, *columnUnit)) {
                    ctx.Error() << "Invalid unit: " << unit;
                    return false;
                }
            }

            to.Set(TTtlSettings(columnName, tiers, columnUnit));
            break;
        }
        default:
            return false;
    }
    return true;
}

template <typename TChar>
struct TPatternComponent {
    TBasicString<TChar> Prefix;
    TBasicString<TChar> Suffix;
    bool IsSimple = true;

    void AppendPlain(TChar c) {
        if (IsSimple) {
            Prefix.push_back(c);
        }
        Suffix.push_back(c);
    }

    void AppendAnyChar() {
        IsSimple = false;
        Suffix.clear();
    }
};

template <typename TChar>
TVector<TPatternComponent<TChar>> SplitPattern(const TBasicString<TChar>& pattern, TMaybe<char> escape, bool& inEscape) {
    inEscape = false;
    TVector<TPatternComponent<TChar>> result;
    TPatternComponent<TChar> current;
    bool prevIsPercentChar = false;
    for (const TChar c : pattern) {
        if (inEscape) {
            current.AppendPlain(c);
            inEscape = false;
            prevIsPercentChar = false;
        } else if (escape && c == static_cast<TChar>(*escape)) {
            inEscape = true;
        } else if (c == '%') {
            if (!prevIsPercentChar) {
                result.push_back(std::move(current));
            }
            current = {};
            prevIsPercentChar = true;
        } else if (c == '_') {
            current.AppendAnyChar();
            prevIsPercentChar = false;
        } else {
            current.AppendPlain(c);
            prevIsPercentChar = false;
        }
    }
    result.push_back(std::move(current));
    return result;
}
} // namespace

bool TSqlTranslation::StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value,
                                              TTableSettings& settings, ETableType tableType, bool alter, bool reset) {
    switch (tableType) {
        case ETableType::ExternalTable:
            return StoreExternalTableSettingsEntry(id, value, settings, alter, reset);
        case ETableType::Table:
        case ETableType::TableStore:
            return StoreTableSettingsEntry(id, value, settings, alter, reset);
    }
}

bool TSqlTranslation::StoreExternalTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value,
                                                      TTableSettings& settings, bool alter, bool reset) {
    YQL_ENSURE(value || reset);
    YQL_ENSURE(!reset || reset && alter);
    if (to_lower(id.Name) == "data_source") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        TDeferredAtom dataSource;
        if (!StoreString(*value, dataSource, Ctx_, to_upper(id.Name))) {
            return false;
        }
        TString service = Context().Scoped->CurrService;
        TDeferredAtom cluster = Context().Scoped->CurrCluster;
        TNodePtr root = new TAstListNodeImpl(Ctx_.Pos());
        root->Add("String", Ctx_.GetPrefixedPath(service, cluster, dataSource));
        settings.DataSourcePath = root;
    } else if (to_lower(id.Name) == "location") {
        if (reset) {
            settings.Location.Reset();
        } else {
            TNodePtr location;
            if (!StoreString(*value, location, Ctx_)) {
                Ctx_.Error() << to_upper(id.Name) << " value should be a string literal";
                return false;
            }
            settings.Location.Set(location);
        }
    } else {
        auto& setting = settings.ExternalSourceParameters.emplace_back();
        if (reset) {
            setting.Reset(id);
        } else {
            TNodePtr node;
            if (!StoreString(*value, node, Ctx_)) {
                Ctx_.Error() << to_upper(id.Name) << " value should be a string literal";
                return false;
            }
            setting.Set(std::pair<TIdentifier, TNodePtr>{id, std::move(node)});
        }
    }
    return true;
}

bool TSqlTranslation::ValidateTableSettings(const TTableSettings& settings) {
    if (settings.PartitionCount) {
        if (!settings.StoreType || to_lower(settings.StoreType->Name) != "column") {
            Ctx_.Error() << " PARTITION_COUNT can be used only with STORE=COLUMN";
            return false;
        }
    }

    return true;
}

bool TSqlTranslation::StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value,
                                              TTableSettings& settings, bool alter, bool reset) {
    YQL_ENSURE(value || reset);
    YQL_ENSURE(!reset || reset && alter);
    if (to_lower(id.Name) == "compaction_policy") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreString(*value, settings.CompactionPolicy, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_by_size") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.AutoPartitioningBySize, *this)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_partition_size_mb") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.PartitionSizeMb, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_by_load") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.AutoPartitioningByLoad, *this)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_min_partitions_count") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.MinPartitions, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_max_partitions_count") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.MaxPartitions, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "partition_count") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }

        if (!StoreInt(*value, settings.PartitionCount, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "uniform_partitions") {
        if (alter) {
            Ctx_.Error() << to_upper(id.Name) << " alter is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.UniformPartitions, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "partition_at_keys") {
        if (alter) {
            Ctx_.Error() << to_upper(id.Name) << " alter is not supported";
            return false;
        }
        TSqlExpression expr(Ctx_, Mode_);
        if (!StoreSplitBoundaries(*value, settings.PartitionAtKeys, expr, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be a list of keys. "
                         << "Example1: (10, 1000)  Example2: ((10), (1000, \"abc\"))";
            return false;
        }
    } else if (to_lower(id.Name) == "key_bloom_filter") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.KeyBloomFilter, *this)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "read_replicas_settings") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreString(*value, settings.ReadReplicasSettings, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else if (to_lower(id.Name) == "ttl") {
        if (!reset) {
            TSqlExpression expr(Ctx_, Mode_);
            if (!StoreTtlSettings(*value, settings.TtlSettings, expr, Ctx_, *this)) {
                Ctx_.Error() << "Invalid TTL settings";
                return false;
            }
        } else {
            settings.TtlSettings.Reset();
        }
    } else if (to_lower(id.Name) == "tiering") {
        if (!reset) {
            TNodePtr tieringNode;
            if (!StoreString(*value, tieringNode, Ctx_)) {
                Ctx_.Error() << to_upper(id.Name) << " value should be a string literal";
                return false;
            }
            settings.Tiering.Set(tieringNode);
        } else {
            settings.Tiering.Reset();
        }
    } else if (to_lower(id.Name) == "store") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.StoreType, *this)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "partition_by_hash_function") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreString(*value, settings.PartitionByHashFunction, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else if (to_lower(id.Name) == "store_external_blobs") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.StoreExternalBlobs, *this)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "external_data_channels_count") {
        if (reset) {
            Ctx_.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.ExternalDataChannelsCount, Ctx_)) {
            Ctx_.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else {
        Ctx_.Error() << "Unknown table setting: " << id.Name;
        return false;
    }

    return ValidateTableSettings(settings);
}

bool TSqlTranslation::StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value& value,
                                              TTableSettings& settings, ETableType tableType, bool alter) {
    return StoreTableSettingsEntry(id, &value, settings, tableType, alter, false);
}

bool TSqlTranslation::ResetTableSettingsEntry(const TIdentifier& id, TTableSettings& settings, ETableType tableType) {
    return StoreTableSettingsEntry(id, nullptr, settings, tableType, true, true);
}

bool TSqlTranslation::CreateTableSettings(const TRule_with_table_settings& settingsNode, TCreateTableParameters& params) {
    const auto& firstEntry = settingsNode.GetRule_table_settings_entry3();
    if (!StoreTableSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), firstEntry.GetRule_table_setting_value3(),
                                 params.TableSettings, params.TableType)) {
        return false;
    }
    for (auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_table_settings_entry2();
        if (!StoreTableSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), entry.GetRule_table_setting_value3(), params.TableSettings, params.TableType)) {
            return false;
        }
    }
    return true;
}

namespace {

bool StoreConsumerSettingsEntry(
    const TIdentifier& id, const TRule_topic_consumer_setting_value* value, TSqlExpression& ctx,
    TTopicConsumerSettings& settings,
    bool reset, bool alter) {
    YQL_ENSURE(value || reset);
    const TStringBuf statement = alter ? "ALTER CONSUMER"sv : "CONSUMER"sv;
    TNodePtr valueExprNode;
    if (value) {
        valueExprNode = Unwrap(ctx.Build(value->GetRule_expr1()));
        if (!valueExprNode) {
            ctx.Error() << "invalid value for setting: " << id.Name;
            return false;
        }
    }
    auto name = to_lower(id.Name);
    if (name == "important") {
        if (settings.Important) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "Bool") {
            ctx.Error() << to_upper(id.Name) << " value should be boolean";
            return false;
        }
        settings.Important = valueExprNode;
    } else if (name == "availability_period") {
        if (settings.AvailabilityPeriod) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            settings.AvailabilityPeriod.Reset();
        } else {
            if (valueExprNode->GetOpName() != "Interval") {
                ctx.Error() << "Literal of Interval type is expected for " << to_upper(id.Name) << " setting";
                return false;
            }
            settings.AvailabilityPeriod.Set(valueExprNode);
        }
    } else if (name == "read_from") {
        if (settings.ReadFromTs) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            settings.ReadFromTs.Reset();
        } else {
            settings.ReadFromTs.Set(valueExprNode);
        }
    } else if (name == "supported_codecs") {
        if (settings.SupportedCodecs) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            settings.SupportedCodecs.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be a string literal";
                return false;
            }
            settings.SupportedCodecs.Set(valueExprNode);
        }
    } else if (name == "type") {
        if (settings.Type) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (alter) {
            ctx.Error() << to_upper(id.Name) << " alter is not supported";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || (valueExprNode->GetLiteralType() != "String" && valueExprNode->GetLiteralType() != "Enum")) {
            ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
        TString value = to_upper(valueExprNode->GetLiteralValue());
        if (value != "STREAMING" && value != "SHARED") {
            ctx.Error() << to_upper(id.Name) << " value should be 'STREAMING' or 'SHARED', got: " << valueExprNode->GetLiteralValue();
            return false;
        }
        settings.Type = valueExprNode;
    } else if (name == "keep_messages_order") {
        if (settings.KeepMessagesOrder) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (alter) {
            ctx.Error() << to_upper(id.Name) << " alter is not supported";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "Bool") {
            ctx.Error() << to_upper(id.Name) << " value should be boolean";
            return false;
        }
        settings.KeepMessagesOrder = valueExprNode;
    } else if (name == "default_processing_timeout") {
        if (settings.DefaultProcessingTimeout) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (valueExprNode->GetOpName() != "Interval") {
            ctx.Error() << "Literal of Interval type is expected for " << to_upper(id.Name) << " setting";
            return false;
        }
        settings.DefaultProcessingTimeout = valueExprNode;
    } else if (name == "max_processing_attempts") {
        if (settings.MaxProcessingAttempts) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsIntegerLiteral()) {
            ctx.Error() << to_upper(id.Name) << " value should be a integer";
            return false;
        }
        settings.MaxProcessingAttempts = valueExprNode;
    } else if (name == "dead_letter_policy") {
        if (settings.DeadLetterPolicy) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || (valueExprNode->GetLiteralType() != "String" && valueExprNode->GetLiteralType() != "Enum")) {
            ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
        TString value = to_upper(valueExprNode->GetLiteralValue());
        if (value != "MOVE" && value != "DELETE" && value != "NONE") {
            ctx.Error() << to_upper(id.Name) << " value should be 'MOVE', 'DELETE' or 'NONE', got: " << valueExprNode->GetLiteralValue();
            return false;
        }
        settings.DeadLetterPolicy = valueExprNode;
    } else if (name == "dead_letter_queue") {
        if (settings.DeadLetterQueue) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
            ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
        settings.DeadLetterQueue = valueExprNode;
    } else {
        ctx.Error() << to_upper(id.Name) << ": unknown option for consumer";
        return false;
    }
    return true;
}

} // namespace

TIdentifier TSqlTranslation::GetTopicConsumerId(const TRule_topic_consumer_ref& node) {
    return IdEx(node.GetRule_an_id_pure1(), *this);
}

bool TSqlTranslation::CreateConsumerSettings(
    const TRule_topic_consumer_settings& node, TTopicConsumerSettings& settings) {
    const auto& firstEntry = node.GetRule_topic_consumer_settings_entry1();
    TSqlExpression expr(Ctx_, Mode_);
    if (!StoreConsumerSettingsEntry(
            IdEx(firstEntry.GetRule_an_id1(), *this),
            &firstEntry.GetRule_topic_consumer_setting_value3(),
            expr, settings, false,
            /* alter = */ false)) {
        return false;
    }
    for (auto& block : node.GetBlock2()) {
        const auto& entry = block.GetRule_topic_consumer_settings_entry2();
        if (!StoreConsumerSettingsEntry(
                IdEx(entry.GetRule_an_id1(), *this),
                &entry.GetRule_topic_consumer_setting_value3(),
                expr, settings, false,
                /* alter = */ false)) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::CreateTopicConsumer(
    const TRule_topic_create_consumer_entry& node,
    TVector<TTopicConsumerDescription>& consumers) {
    consumers.emplace_back(IdEx(node.GetRule_an_id2(), *this));

    if (node.HasBlock3()) {
        auto& settings = node.GetBlock3().GetRule_topic_consumer_with_settings1().GetRule_topic_consumer_settings3();
        if (!CreateConsumerSettings(settings, consumers.back().Settings)) {
            return false;
        }
    }

    return true;
}

bool TSqlTranslation::AlterTopicConsumerEntry(
    const TRule_alter_topic_alter_consumer_entry& node, TTopicConsumerDescription& alterConsumer) {
    switch (node.Alt_case()) {
        case TRule_alter_topic_alter_consumer_entry::kAltAlterTopicAlterConsumerEntry1:
            return CreateConsumerSettings(
                node.GetAlt_alter_topic_alter_consumer_entry1().GetRule_topic_alter_consumer_set1().GetRule_topic_consumer_settings3(),
                alterConsumer.Settings);
        // case TRule_alter_topic_alter_consumer_entry::ALT_NOT_SET:
        case TRule_alter_topic_alter_consumer_entry::kAltAlterTopicAlterConsumerEntry2: {
            auto& resetNode = node.GetAlt_alter_topic_alter_consumer_entry2().GetRule_topic_alter_consumer_reset1();
            TSqlExpression expr(Ctx_, Mode_);
            if (!StoreConsumerSettingsEntry(
                    IdEx(resetNode.GetRule_an_id3(), *this),
                    nullptr,
                    expr, alterConsumer.Settings, true,
                    /* alter = */ true)) {
                return false;
            }

            for (auto& resetItem : resetNode.GetBlock4()) {
                if (!StoreConsumerSettingsEntry(
                        IdEx(resetItem.GetRule_an_id2(), *this),
                        nullptr,
                        expr, alterConsumer.Settings, true,
                        /* alter = */ true)) {
                    return false;
                }
            }
            return true;
        }
        case NSQLv1Generated::TRule_alter_topic_alter_consumer_entry::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return true;
}

bool TSqlTranslation::AlterTopicConsumer(
    const TRule_alter_topic_alter_consumer& node,
    THashMap<TString, TTopicConsumerDescription>& alterConsumers) {
    auto consumerId = GetTopicConsumerId(node.GetRule_topic_consumer_ref3());
    TString name = to_lower(consumerId.Name);
    auto iter = alterConsumers.insert(std::make_pair(
                                          name, TTopicConsumerDescription(std::move(consumerId))))
                    .first;
    if (!AlterTopicConsumerEntry(node.GetRule_alter_topic_alter_consumer_entry4(), iter->second)) {
        return false;
    }
    return true;
}

bool TSqlTranslation::CreateTopicEntry(const TRule_create_topic_entry& node, TCreateTopicParameters& params) {
    // Will need a switch() here if (ever) create_topic_entry gets more than 1 type of statement
    auto& consumer = node.GetRule_topic_create_consumer_entry1();
    if (!CreateTopicConsumer(consumer, params.Consumers)) {
        return false;
    }
    return true;
}

namespace {

bool StoreTopicSettingsEntry(
    const TIdentifier& id, const TRule_topic_setting_value* value, TSqlExpression& ctx,
    TTopicSettings& settings, bool reset) {
    YQL_ENSURE(value || reset);
    TNodePtr valueExprNode;
    if (value) {
        valueExprNode = Unwrap(ctx.Build(value->GetRule_expr1()));
        if (!valueExprNode) {
            ctx.Error() << "invalid value for setting: " << id.Name;
            return false;
        }
    }

    if (to_lower(id.Name) == "min_active_partitions") {
        if (reset) {
            settings.MinPartitions.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.MinPartitions.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "partition_count_limit" || to_lower(id.Name) == "max_active_partitions") {
        if (reset) {
            settings.MaxPartitions.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.MaxPartitions.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "retention_period") {
        if (reset) {
            settings.RetentionPeriod.Reset();
        } else {
            if (valueExprNode->GetOpName() != "Interval") {
                ctx.Error() << "Literal of Interval type is expected for retention";
                return false;
            }
            settings.RetentionPeriod.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "retention_storage_mb") {
        if (reset) {
            settings.RetentionStorage.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.RetentionStorage.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "partition_write_speed_bytes_per_second") {
        if (reset) {
            settings.PartitionWriteSpeed.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.PartitionWriteSpeed.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "partition_write_burst_bytes") {
        if (reset) {
            settings.PartitionWriteBurstSpeed.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.PartitionWriteBurstSpeed.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "metering_mode") {
        if (reset) {
            settings.MeteringMode.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be string";
                return false;
            }
            settings.MeteringMode.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "supported_codecs") {
        if (reset) {
            settings.SupportedCodecs.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be string";
                return false;
            }
            settings.SupportedCodecs.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "auto_partitioning_stabilization_window") {
        if (reset) {
            settings.AutoPartitioningStabilizationWindow.Reset();
        } else {
            if (valueExprNode->GetOpName() != "Interval") {
                ctx.Error() << "Literal of Interval type is expected for retention";
                return false;
            }
            settings.AutoPartitioningStabilizationWindow.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "auto_partitioning_up_utilization_percent") {
        if (reset) {
            settings.AutoPartitioningUpUtilizationPercent.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.AutoPartitioningUpUtilizationPercent.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "auto_partitioning_down_utilization_percent") {
        if (reset) {
            settings.AutoPartitioningDownUtilizationPercent.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.AutoPartitioningDownUtilizationPercent.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "auto_partitioning_strategy") {
        if (reset) {
            settings.AutoPartitioningStrategy.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be string";
                return false;
            }
            settings.AutoPartitioningStrategy.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "metrics_level") {
        if (reset) {
            settings.MetricsLevel.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.MetricsLevel.Set(valueExprNode);
        }
    } else {
        ctx.Error() << "unknown topic setting: " << id.Name;
        return false;
    }
    return true;
}

} // namespace

bool TSqlTranslation::AlterTopicAction(const TRule_alter_topic_action& node, TAlterTopicParameters& params) {
    // alter_topic_action:
    // alter_topic_add_consumer
    // | alter_topic_alter_consumer
    // | alter_topic_drop_consumer
    // | alter_topic_set_settings
    // | alter_topic_reset_settings

    switch (node.Alt_case()) {
        case TRule_alter_topic_action::kAltAlterTopicAction1: // alter_topic_add_consumer
            return CreateTopicConsumer(
                node.GetAlt_alter_topic_action1().GetRule_alter_topic_add_consumer1().GetRule_topic_create_consumer_entry2(),
                params.AddConsumers);

        case TRule_alter_topic_action::kAltAlterTopicAction2: // alter_topic_alter_consumer
            return AlterTopicConsumer(
                node.GetAlt_alter_topic_action2().GetRule_alter_topic_alter_consumer1(),
                params.AlterConsumers);

        case TRule_alter_topic_action::kAltAlterTopicAction3: // drop_consumer
            params.DropConsumers.emplace_back(GetTopicConsumerId(
                node.GetAlt_alter_topic_action3().GetRule_alter_topic_drop_consumer1().GetRule_topic_consumer_ref3()));
            return true;

        case TRule_alter_topic_action::kAltAlterTopicAction4: // set_settings
            return CreateTopicSettings(
                node.GetAlt_alter_topic_action4().GetRule_alter_topic_set_settings1().GetRule_topic_settings3(),
                params.TopicSettings);

        case TRule_alter_topic_action::kAltAlterTopicAction5: { // reset_settings
            auto& resetNode = node.GetAlt_alter_topic_action5().GetRule_alter_topic_reset_settings1();
            TSqlExpression expr(Ctx_, Mode_);
            if (!StoreTopicSettingsEntry(
                    IdEx(resetNode.GetRule_an_id3(), *this),
                    nullptr, expr,
                    params.TopicSettings, true)) {
                return false;
            }

            for (auto& resetItem : resetNode.GetBlock4()) {
                if (!StoreTopicSettingsEntry(
                        IdEx(resetItem.GetRule_an_id_pure2(), *this),
                        nullptr, expr,
                        params.TopicSettings, true)) {
                    return false;
                }
            }
            return true;
        }

        case NSQLv1Generated::TRule_alter_topic_action::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return true;
}

bool TSqlTranslation::CreateTopicSettings(const TRule_topic_settings& node, TTopicSettings& settings) {
    const auto& firstEntry = node.GetRule_topic_settings_entry1();
    TSqlExpression expr(Ctx_, Mode_);

    if (!StoreTopicSettingsEntry(
            IdEx(firstEntry.GetRule_an_id1(), *this),
            &firstEntry.GetRule_topic_setting_value3(),
            expr, settings, false)) {
        return false;
    }
    for (auto& block : node.GetBlock2()) {
        const auto& entry = block.GetRule_topic_settings_entry2();
        if (!StoreTopicSettingsEntry(
                IdEx(entry.GetRule_an_id1(), *this),
                &entry.GetRule_topic_setting_value3(),
                expr, settings, false)) {
            return false;
        }
    }
    return true;
}

TNodePtr TSqlTranslation::IntegerOrBind(const TRule_integer_or_bind& node) {
    switch (node.Alt_case()) {
        case TRule_integer_or_bind::kAltIntegerOrBind1: {
            const TString intString = Ctx_.Token(node.GetAlt_integer_or_bind1().GetRule_integer1().GetToken1());
            ui64 value;
            TString suffix;
            if (!ParseNumbers(Ctx_, intString, value, suffix)) {
                return {};
            }
            return BuildQuotedAtom(Ctx_.Pos(), ToString(value), TNodeFlags::ArbitraryContent);
        }
        case TRule_integer_or_bind::kAltIntegerOrBind2: {
            TString bindName;
            if (!NamedNodeImpl(node.GetAlt_integer_or_bind2().GetRule_bind_parameter1(), bindName, *this)) {
                return {};
            }
            auto namedNode = GetNamedNode(bindName);
            if (!namedNode) {
                return {};
            }
            auto atom = MakeAtomFromExpression(Ctx_.Pos(), Ctx_, namedNode);
            return atom.Build();
        }
        case TRule_integer_or_bind::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TNodePtr TSqlTranslation::TypeNameTag(const TRule_type_name_tag& node) {
    switch (node.Alt_case()) {
        case TRule_type_name_tag::kAltTypeNameTag1: {
            auto content = Id(node.GetAlt_type_name_tag1().GetRule_id1(), *this);
            auto atom = TDeferredAtom(Ctx_.Pos(), content);
            return atom.Build();
        }
        case TRule_type_name_tag::kAltTypeNameTag2: {
            auto value = Token(node.GetAlt_type_name_tag2().GetToken1());
            auto parsed = StringContentOrIdContent(Ctx_, Ctx_.Pos(), value);
            if (!parsed) {
                return {};
            }
            auto atom = TDeferredAtom(Ctx_.Pos(), parsed->Content);
            return atom.Build();
        }
        case TRule_type_name_tag::kAltTypeNameTag3: {
            TString bindName;
            if (!NamedNodeImpl(node.GetAlt_type_name_tag3().GetRule_bind_parameter1(), bindName, *this)) {
                return {};
            }
            auto namedNode = GetNamedNode(bindName);
            if (!namedNode) {
                return {};
            }
            TDeferredAtom atom;
            MakeTableFromExpression(Ctx_.Pos(), Ctx_, namedNode, atom);
            return atom.Build();
        }
        case TRule_type_name_tag::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TNodePtr TSqlTranslation::TypeSimple(const TRule_type_name_simple& node, bool onlyDataAllowed) {
    const TString origName = Id(node.GetRule_an_id_pure1(), *this);
    if (origName.empty()) {
        return {};
    }
    return BuildSimpleType(Ctx_, Ctx_.Pos(), origName, onlyDataAllowed);
}

TNodePtr TSqlTranslation::TypeDecimal(const TRule_type_name_decimal& node) {
    auto pos = Ctx_.Pos();
    auto flags = TNodeFlags::Default;

    auto paramOne = IntegerOrBind(node.GetRule_integer_or_bind3());
    if (!paramOne) {
        return {};
    }
    auto paramTwo = IntegerOrBind(node.GetRule_integer_or_bind5());
    if (!paramTwo) {
        return {};
    }
    return new TCallNodeImpl(pos, "DataType", {BuildQuotedAtom(pos, "Decimal", flags), paramOne, paramTwo});
}

TNodePtr TSqlTranslation::AddOptionals(const TNodePtr& node, size_t optionalCount) {
    TNodePtr result = node;
    if (node) {
        TPosition pos = node->GetPos();
        for (size_t i = 0; i < optionalCount; ++i) {
            result = new TCallNodeImpl(pos, "OptionalType", {result});
        }
    }
    return result;
}

TMaybe<std::pair<TVector<TNodePtr>, bool>> TSqlTranslation::CallableArgList(const TRule_callable_arg_list& argList, bool namedArgsStarted) {
    auto pos = Ctx_.Pos();
    auto flags = TNodeFlags::Default;
    auto& arg1 = argList.GetRule_callable_arg1();
    auto& varArg = arg1.GetRule_variant_arg1();
    TVector<TNodePtr> result;
    TVector<TNodePtr> items;
    auto typeNode = TypeNodeOrBind(varArg.GetRule_type_name_or_bind2());
    if (!typeNode) {
        return {};
    }
    items.push_back(typeNode);
    if (varArg.HasBlock1()) {
        namedArgsStarted = true;
        auto tag = TypeNameTag(varArg.GetBlock1().GetRule_type_name_tag1());
        if (!tag) {
            return {};
        }
        items.push_back(tag);
    }
    if (arg1.HasBlock2()) {
        if (!varArg.HasBlock1()) {
            items.push_back(BuildQuotedAtom(pos, "", flags));
        }
        items.push_back(BuildQuotedAtom(pos, "1", flags));
    }
    result.push_back(new TAstListNodeImpl(pos, items));

    for (auto& arg : argList.GetBlock2()) {
        auto& varArg = arg.GetRule_callable_arg2().GetRule_variant_arg1();
        TVector<TNodePtr> items;
        auto typeNode = TypeNodeOrBind(varArg.GetRule_type_name_or_bind2());
        if (!typeNode) {
            return {};
        }
        items.push_back(typeNode);
        if (varArg.HasBlock1()) {
            auto tag = TypeNameTag(varArg.GetBlock1().GetRule_type_name_tag1());
            if (!tag) {
                return {};
            }
            items.push_back(tag);
        } else {
            if (namedArgsStarted) {
                Ctx_.Error() << "Expected named argument, previous argument was named";
                return {};
            }
            items.push_back(BuildQuotedAtom(pos, "", flags));
        }
        if (arg.GetRule_callable_arg2().HasBlock2()) {
            if (!varArg.HasBlock1()) {
                items.push_back(BuildQuotedAtom(pos, "", flags));
            }
            items.push_back(BuildQuotedAtom(pos, "1", flags));
        }
        result.push_back(new TAstListNodeImpl(pos, items));
    }
    return std::make_pair(result, namedArgsStarted);
}

TNodePtr TSqlTranslation::TypeNodeOrBind(const TRule_type_name_or_bind& node) {
    switch (node.Alt_case()) {
        case TRule_type_name_or_bind::kAltTypeNameOrBind1: {
            return TypeNode(node.GetAlt_type_name_or_bind1().GetRule_type_name1());
        }
        case TRule_type_name_or_bind::kAltTypeNameOrBind2: {
            TString bindName;
            if (!NamedNodeImpl(node.GetAlt_type_name_or_bind2().GetRule_bind_parameter1(), bindName, *this)) {
                return {};
            }
            return GetNamedNode(bindName);
        }
        case TRule_type_name_or_bind::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TNodePtr TSqlTranslation::TypeNode(const TRule_type_name& node) {
    // type_name:
    //     type_name_composite
    //   | (type_name_decimal | type_name_simple) QUESTION*;
    if (node.Alt_case() == TRule_type_name::kAltTypeName1) {
        return TypeNode(node.GetAlt_type_name1().GetRule_type_name_composite1());
    }

    TNodePtr result;
    TPosition pos = Ctx_.Pos();

    auto& alt = node.GetAlt_type_name2();
    auto& block = alt.GetBlock1();
    switch (block.Alt_case()) {
        case TRule_type_name::TAlt2::TBlock1::kAlt1: {
            auto& decimalType = block.GetAlt1().GetRule_type_name_decimal1();
            result = TypeDecimal(decimalType);
            break;
        }
        case TRule_type_name::TAlt2::TBlock1::kAlt2: {
            auto& simpleType = block.GetAlt2().GetRule_type_name_simple1();
            result = TypeSimple(simpleType, false);
            break;
        }
        case TRule_type_name::TAlt2::TBlock1::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    return AddOptionals(result, alt.GetBlock2().size());
}

TNodePtr TSqlTranslation::TypeNode(const TRule_type_name_composite& node) {
    // type_name_composite:
    //   ( type_name_optional
    //   | type_name_tuple
    //   | type_name_struct
    //   | type_name_variant
    //   | type_name_list
    //   | type_name_stream
    //   | type_name_flow
    //   | type_name_dict
    //   | type_name_set
    //   | type_name_enum
    //   | type_name_resource
    //   | type_name_tagged
    //   | type_name_callable
    //   ) QUESTION*;
    TNodePtr result;
    TPosition pos = Ctx_.Pos();
    auto flags = TNodeFlags::Default;

    auto wrapOneParamType = [&](const TRule_type_name_or_bind& param, const char* type) -> TNodePtr {
        auto node = TypeNodeOrBind(param);
        return node ? new TAstListNodeImpl(pos, {BuildAtom(pos, type, flags), node}) : nullptr;
    };
    auto makeVoid = [&]() -> TNodePtr {
        return new TAstListNodeImpl(pos, {BuildAtom(pos, "VoidType", flags)});
    };
    auto makeQuote = [&](const TNodePtr& node) -> TNodePtr {
        return new TAstListNodeImpl(pos, {new TAstAtomNodeImpl(pos, "quote", 0), node});
    };

    auto& block = node.GetBlock1();
    switch (block.Alt_case()) {
        case TRule_type_name_composite_TBlock1::kAlt1: {
            auto& optionalType = block.GetAlt1().GetRule_type_name_optional1();
            result = wrapOneParamType(optionalType.GetRule_type_name_or_bind3(), "OptionalType");
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt2: {
            auto& tupleType = block.GetAlt2().GetRule_type_name_tuple1();
            TVector<TNodePtr> items;
            items.push_back(BuildAtom(pos, "TupleType", flags));

            switch (tupleType.GetBlock2().Alt_case()) {
                case TRule_type_name_tuple::TBlock2::kAlt1: {
                    if (tupleType.GetBlock2().GetAlt1().HasBlock2()) {
                        auto typeNode = TypeNodeOrBind(tupleType.GetBlock2().GetAlt1().GetBlock2().GetRule_type_name_or_bind1());
                        if (!typeNode) {
                            return {};
                        }
                        items.push_back(typeNode);
                        for (auto& arg : tupleType.GetBlock2().GetAlt1().GetBlock2().GetBlock2()) {
                            auto typeNode = TypeNodeOrBind(arg.GetRule_type_name_or_bind2());
                            if (!typeNode) {
                                return {};
                            }
                            items.push_back(typeNode);
                        }
                    }
                    [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
                }
                case TRule_type_name_tuple::TBlock2::kAlt2:
                    break;
                case TRule_type_name_tuple::TBlock2::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }

            result = new TAstListNodeImpl(pos, items);
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt3: {
            auto& structType = block.GetAlt3().GetRule_type_name_struct1();
            TVector<TNodePtr> items;
            items.push_back(BuildAtom(pos, "StructType", flags));

            switch (structType.GetBlock2().Alt_case()) {
                case TRule_type_name_struct::TBlock2::kAlt1: {
                    if (structType.GetBlock2().GetAlt1().HasBlock2()) {
                        auto& structArg = structType.GetBlock2().GetAlt1().GetBlock2().GetRule_struct_arg1();
                        auto typeNode = TypeNodeOrBind(structArg.GetRule_type_name_or_bind3());
                        if (!typeNode) {
                            return {};
                        }
                        auto tag = TypeNameTag(structArg.GetRule_type_name_tag1());
                        if (!tag) {
                            return {};
                        }

                        items.push_back(makeQuote(new TAstListNodeImpl(pos, {tag, typeNode})));
                        for (auto& arg : structType.GetBlock2().GetAlt1().GetBlock2().GetBlock2()) {
                            auto typeNode = TypeNodeOrBind(arg.GetRule_struct_arg2().GetRule_type_name_or_bind3());
                            if (!typeNode) {
                                return {};
                            }
                            auto tag = TypeNameTag(arg.GetRule_struct_arg2().GetRule_type_name_tag1());
                            if (!tag) {
                                return {};
                            }
                            items.push_back(makeQuote(new TAstListNodeImpl(pos, {tag, typeNode})));
                        }
                    }
                    [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
                }
                case TRule_type_name_struct::TBlock2::kAlt2:
                    break;
                case TRule_type_name_struct::TBlock2::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }

            result = new TAstListNodeImpl(pos, items);
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt4: {
            auto& variantType = block.GetAlt4().GetRule_type_name_variant1();
            TVector<TNodePtr> items;
            bool overStruct = false;
            auto& variantArg = variantType.GetRule_variant_arg3();
            auto typeNode = TypeNodeOrBind(variantArg.GetRule_type_name_or_bind2());
            if (!typeNode) {
                return {};
            }
            if (variantArg.HasBlock1()) {
                items.push_back(BuildAtom(pos, "StructType", flags));
                overStruct = true;
                auto tag = TypeNameTag(variantArg.GetBlock1().GetRule_type_name_tag1());
                if (!tag) {
                    return {};
                }
                items.push_back(makeQuote(new TAstListNodeImpl(pos, {tag, typeNode})));
            } else {
                items.push_back(BuildAtom(pos, "TupleType", flags));
                items.push_back(typeNode);
            }

            for (auto& arg : variantType.GetBlock4()) {
                auto typeNode = TypeNodeOrBind(arg.GetRule_variant_arg2().GetRule_type_name_or_bind2());
                if (!typeNode) {
                    return {};
                }
                if (overStruct) {
                    if (!arg.GetRule_variant_arg2().HasBlock1()) {
                        Ctx_.Error() << "Variant over struct and tuple mixture";
                        return {};
                    }
                    auto tag = TypeNameTag(arg.GetRule_variant_arg2().GetBlock1().GetRule_type_name_tag1());
                    if (!tag) {
                        return {};
                    }
                    items.push_back(makeQuote(new TAstListNodeImpl(pos, {tag, typeNode})));
                } else {
                    if (arg.GetRule_variant_arg2().HasBlock1()) {
                        Ctx_.Error() << "Variant over struct and tuple mixture";
                        return {};
                    }
                    items.push_back(typeNode);
                }
            }
            typeNode = new TAstListNodeImpl(pos, items);
            result = new TAstListNodeImpl(pos, {BuildAtom(pos, "VariantType", flags), typeNode});
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt5: {
            auto& listType = block.GetAlt5().GetRule_type_name_list1();
            result = wrapOneParamType(listType.GetRule_type_name_or_bind3(), "ListType");
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt6: {
            auto& streamType = block.GetAlt6().GetRule_type_name_stream1();
            result = wrapOneParamType(streamType.GetRule_type_name_or_bind3(), "StreamType");
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt7: {
            auto& flowType = block.GetAlt7().GetRule_type_name_flow1();
            result = wrapOneParamType(flowType.GetRule_type_name_or_bind3(), "FlowType");
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt8: {
            auto& dictType = block.GetAlt8().GetRule_type_name_dict1();
            TVector<TNodePtr> items;
            items.push_back(BuildAtom(pos, "DictType", flags));
            auto typeNode = TypeNodeOrBind(dictType.GetRule_type_name_or_bind3());
            if (!typeNode) {
                return {};
            }
            items.push_back(typeNode);
            typeNode = TypeNodeOrBind(dictType.GetRule_type_name_or_bind5());
            if (!typeNode) {
                return {};
            }
            items.push_back(typeNode);
            result = new TAstListNodeImpl(pos, items);
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt9: {
            auto& setType = block.GetAlt9().GetRule_type_name_set1();
            auto typeNode = TypeNodeOrBind(setType.GetRule_type_name_or_bind3());
            if (!typeNode) {
                return {};
            }
            result = new TAstListNodeImpl(pos, {BuildAtom(pos, "DictType", flags), typeNode, makeVoid()});
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt10: {
            auto& enumType = block.GetAlt10().GetRule_type_name_enum1();
            TVector<TNodePtr> items;
            items.push_back(BuildAtom(pos, "StructType", flags));
            auto tag = TypeNameTag(enumType.GetRule_type_name_tag3());
            if (!tag) {
                return {};
            }
            items.push_back(makeQuote(new TAstListNodeImpl(pos, {tag, makeVoid()})));
            for (auto& arg : enumType.GetBlock4()) {
                auto tag = TypeNameTag(arg.GetRule_type_name_tag2());
                if (!tag) {
                    return {};
                }
                items.push_back(makeQuote(new TAstListNodeImpl(pos, {tag, makeVoid()})));
            }
            auto typeNode = new TAstListNodeImpl(pos, items);
            result = new TAstListNodeImpl(pos, {BuildAtom(pos, "VariantType", flags), typeNode});
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt11: {
            auto& resourceType = block.GetAlt11().GetRule_type_name_resource1();
            auto tag = TypeNameTag(resourceType.GetRule_type_name_tag3());
            if (!tag) {
                return {};
            }
            result = new TAstListNodeImpl(pos, {BuildAtom(pos, "ResourceType", flags), tag});
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt12: {
            auto& taggedType = block.GetAlt12().GetRule_type_name_tagged1();
            auto typeNode = TypeNodeOrBind(taggedType.GetRule_type_name_or_bind3());
            if (!typeNode) {
                return {};
            }
            auto tag = TypeNameTag(taggedType.GetRule_type_name_tag5());
            if (!tag) {
                return {};
            }
            result = new TAstListNodeImpl(pos, {BuildAtom(pos, "TaggedType", flags), typeNode, tag});
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt13: {
            auto& callableType = block.GetAlt13().GetRule_type_name_callable1();
            TMaybe<std::pair<TVector<TNodePtr>, bool>> requiredArgs, optionalArgs;
            bool namedArgsStarted = false;
            size_t optionalArgsCount = 0;
            if (callableType.HasBlock4()) {
                auto& argList = callableType.GetBlock4().GetRule_callable_arg_list1();
                requiredArgs = CallableArgList(argList, namedArgsStarted);
                if (!requiredArgs) {
                    return {};
                }
                namedArgsStarted = requiredArgs->second;
            }
            if (callableType.HasBlock6()) {
                auto& argList = callableType.GetBlock6().GetRule_callable_arg_list2();
                optionalArgs = CallableArgList(argList, namedArgsStarted);
                if (!optionalArgs) {
                    return {};
                }
                optionalArgsCount = optionalArgs->first.size();
            }
            auto returnType = TypeNodeOrBind(callableType.GetRule_type_name_or_bind9());
            if (!returnType) {
                return {};
            }
            TVector<TNodePtr> items;
            items.push_back(BuildAtom(pos, "CallableType", flags));
            if (optionalArgsCount) {
                items.push_back(makeQuote(new TAstListNodeImpl(pos,
                                                               {BuildQuotedAtom(pos, ToString(optionalArgsCount), flags)})));
            } else {
                items.push_back(makeQuote(new TAstListNodeImpl(pos, {})));
            }
            items.push_back(makeQuote(new TAstListNodeImpl(pos, {returnType})));
            if (requiredArgs) {
                for (auto& arg : requiredArgs->first) {
                    items.push_back(makeQuote(arg));
                }
            }
            if (optionalArgs) {
                for (auto& arg : optionalArgs->first) {
                    items.push_back(makeQuote(arg));
                }
            }
            result = new TAstListNodeImpl(pos, items);
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt14: {
            auto& linearType = block.GetAlt14().GetRule_type_name_linear1();
            result = wrapOneParamType(linearType.GetRule_type_name_or_bind3(), "LinearType");
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt15: {
            auto& linearType = block.GetAlt15().GetRule_type_name_dynamiclinear1();
            result = wrapOneParamType(linearType.GetRule_type_name_or_bind3(), "DynamicLinearType");
            break;
        }
        case TRule_type_name_composite_TBlock1::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    return AddOptionals(result, node.GetBlock2().size());
}

TNodePtr TSqlTranslation::ValueConstructorLiteral(const TRule_value_constructor_literal& node) {
    return BuildLiteralSmartString(Ctx_, Token(node.GetToken1()));
}

TNodePtr TSqlTranslation::ValueConstructor(const TRule_value_constructor& node) {
    TSqlCallExpr call(Ctx_, Mode_);
    if (!call.Init(node)) {
        return {};
    }
    return Unwrap(call.BuildCall());
}

TNodePtr TSqlTranslation::ListLiteral(const TRule_list_literal& node) {
    TVector<TNodePtr> values;
    values.push_back(new TAstAtomNodeImpl(Ctx_.Pos(), "AsListMayWarn", TNodeFlags::Default));

    TSqlExpression sqlExpr(Ctx_, Mode_);
    if (node.HasBlock2() && !Unwrap(ExprList(sqlExpr, values, node.GetBlock2().GetRule_expr_list1()))) {
        return nullptr;
    }

    return new TAstListNodeImpl(Ctx_.Pos(), std::move(values));
}

TNodePtr TSqlTranslation::DictLiteral(const TRule_dict_literal& node) {
    TVector<TNodePtr> values;
    if (node.HasBlock2()) {
        const auto& list = node.GetBlock2().GetRule_expr_dict_list1();
        const bool isSet = !list.HasBlock2();
        values.push_back(new TAstAtomNodeImpl(Ctx_.Pos(), isSet ? "AsSet" : "AsDict", TNodeFlags::Default));
        TSqlExpression sqlExpr(Ctx_, Mode_);
        if (isSet) {
            if (!Unwrap(Expr(sqlExpr, values, list.GetRule_expr1()))) {
                return nullptr;
            }
        } else {
            TVector<TNodePtr> tupleItems;
            if (!Unwrap(Expr(sqlExpr, tupleItems, list.GetRule_expr1()))) {
                return nullptr;
            }

            if (!Unwrap(Expr(sqlExpr, tupleItems, list.GetBlock2().GetRule_expr2()))) {
                return nullptr;
            }

            values.push_back(new TTupleNode(Ctx_.Pos(), std::move(tupleItems)));
        }

        for (auto& b : list.GetBlock3()) {
            sqlExpr.Token(b.GetToken1());
            const bool isSetCurr = !b.HasBlock3();
            if (isSetCurr != isSet) {
                Error() << "Expected keys/values pair or keys, but got mix of them";
                return nullptr;
            }

            if (isSet) {
                if (!Unwrap(Expr(sqlExpr, values, b.GetRule_expr2()))) {
                    return nullptr;
                }
            } else {
                TVector<TNodePtr> tupleItems;
                if (!Unwrap(Expr(sqlExpr, tupleItems, b.GetRule_expr2()))) {
                    return nullptr;
                }

                if (!Unwrap(Expr(sqlExpr, tupleItems, b.GetBlock3().GetRule_expr2()))) {
                    return nullptr;
                }

                values.push_back(new TTupleNode(Ctx_.Pos(), std::move(tupleItems)));
            }
        }
    } else {
        values.push_back(new TAstAtomNodeImpl(Ctx_.Pos(), "AsDict", TNodeFlags::Default));
    }

    return new TAstListNodeImpl(Ctx_.Pos(), std::move(values));
}

bool TSqlTranslation::StructLiteralItem(TVector<TNodePtr>& labels, const TRule_expr& label, TVector<TNodePtr>& values, const TRule_expr& value) {
    // label expr
    {
        TColumnRefScope scope(Ctx_, EColumnRefState::AsStringLiteral, /* topLevel */ false);
        TSqlExpression sqlExpr(Ctx_, Mode_);
        if (!Unwrap(Expr(sqlExpr, labels, label))) {
            return false;
        }

        TDeferredAtom atom;
        MakeTableFromExpression(Ctx_.Pos(), Ctx_, labels.back(), atom);
        labels.back() = atom.Build();
        if (!labels.back()) {
            return false;
        }
    }

    // value expr
    {
        TSqlExpression sqlExpr(Ctx_, Mode_);
        if (!Unwrap(Expr(sqlExpr, values, value))) {
            return false;
        }
    }

    return true;
}

TNodePtr TSqlTranslation::StructLiteral(const TRule_struct_literal& node) {
    TVector<TNodePtr> labels;
    TVector<TNodePtr> values;
    TPosition pos = Ctx_.TokenPosition(node.GetToken1());
    if (node.HasBlock2()) {
        const auto& list = node.GetBlock2().GetRule_expr_struct_list1();

        if (!StructLiteralItem(labels, list.GetRule_expr1(), values, list.GetRule_expr3())) {
            return {};
        }

        for (auto& b : list.GetBlock4()) {
            if (!StructLiteralItem(labels, b.GetRule_expr2(), values, b.GetRule_expr4())) {
                return {};
            }
        }
    }
    return BuildStructure(pos, values, labels);
}

bool TSqlTranslation::TableHintImpl(const TRule_table_hint& rule, TTableHints& hints, const TString& provider, const TString& keyFunc) {
    // table_hint:
    //      an_id_hint (EQUALS (type_name_tag | LPAREN type_name_tag (COMMA type_name_tag)* COMMA? RPAREN))?
    //    | (SCHEMA | COLUMNS) EQUALS? type_name_or_bind
    //    | SCHEMA EQUALS? LPAREN (struct_arg_positional (COMMA struct_arg_positional)*)? COMMA? RPAREN
    //    | WATERMARK AS LPAREN expr RPAREN
    //    | WATERMARK EQUALS expr

    switch (rule.Alt_case()) {
        case TRule_table_hint::kAltTableHint1: {
            const auto& alt = rule.GetAlt_table_hint1();
            const TString id = Id(alt.GetRule_an_id_hint1(), *this);
            const auto idLower = to_lower(id);
            if (idLower == "schema" || idLower == "columns") {
                Error() << "Expected type after " << to_upper(id);
                return false;
            }
            TVector<TNodePtr> hint_val;
            if (alt.HasBlock2()) {
                auto& tags = alt.GetBlock2().GetBlock2();
                switch (tags.Alt_case()) {
                    case TRule_table_hint_TAlt1_TBlock2_TBlock2::kAlt1:
                        hint_val.push_back(TypeNameTag(tags.GetAlt1().GetRule_type_name_tag1()));
                        break;
                    case TRule_table_hint_TAlt1_TBlock2_TBlock2::kAlt2: {
                        hint_val.push_back(TypeNameTag(tags.GetAlt2().GetRule_type_name_tag2()));
                        for (auto& tag : tags.GetAlt2().GetBlock3()) {
                            hint_val.push_back(TypeNameTag(tag.GetRule_type_name_tag2()));
                        }
                        break;
                    }
                    case TRule_table_hint_TAlt1_TBlock2_TBlock2::ALT_NOT_SET:
                        Y_UNREACHABLE();
                }
            }
            hints[id] = hint_val;
            break;
        }

        case TRule_table_hint::kAltTableHint2: {
            const auto& alt2 = rule.GetAlt_table_hint2();
            auto node = TypeNodeOrBind(alt2.GetRule_type_name_or_bind3());
            if (!node) {
                return false;
            }

            hints["user_" + to_lower(alt2.GetToken1().GetValue())] = {node};
            break;
        }

        case TRule_table_hint::kAltTableHint3: {
            const auto& alt = rule.GetAlt_table_hint3();
            TVector<TNodePtr> labels;
            TVector<TNodePtr> structTypeItems;
            if (alt.HasBlock4()) {
                bool warn = false;
                auto processItem = [&](const TRule_struct_arg_positional& arg) {
                    // struct_arg_positional:
                    //     type_name_tag type_name_or_bind (NOT? NULL)?
                    //   | type_name_or_bind AS type_name_tag; //deprecated
                    const bool altCurrent = arg.Alt_case() == TRule_struct_arg_positional::kAltStructArgPositional1;
                    auto& typeNameOrBind = altCurrent ? arg.GetAlt_struct_arg_positional1().GetRule_type_name_or_bind2() : arg.GetAlt_struct_arg_positional2().GetRule_type_name_or_bind1();
                    auto typeNode = TypeNodeOrBind(typeNameOrBind);
                    if (!typeNode) {
                        return false;
                    }

                    auto pos = Ctx_.Pos();
                    if (!altCurrent && !warn) {
                        if (!Ctx_.Warning(pos, TIssuesIds::YQL_DEPRECATED_POSITIONAL_SCHEMA, [](auto& out) {
                                out << "Deprecated syntax for positional schema: "
                                    << "please use 'column type' instead of 'type AS column'";
                            })) {
                            return false;
                        }
                        warn = true;
                    }

                    if (altCurrent) {
                        bool notNull = arg.GetAlt_struct_arg_positional1().HasBlock3() && arg.GetAlt_struct_arg_positional1().GetBlock3().HasBlock1();
                        if (!notNull) {
                            typeNode = new TCallNodeImpl(pos, "AsOptionalType", {typeNode});
                        }
                    }

                    auto& typeNameTag = altCurrent ? arg.GetAlt_struct_arg_positional1().GetRule_type_name_tag1() : arg.GetAlt_struct_arg_positional2().GetRule_type_name_tag3();
                    auto tag = TypeNameTag(typeNameTag);
                    if (!tag) {
                        return false;
                    }

                    labels.push_back(tag);
                    structTypeItems.push_back(BuildTuple(pos, {tag, typeNode}));
                    return true;
                };

                if (!processItem(alt.GetBlock4().GetRule_struct_arg_positional1())) {
                    return false;
                }

                for (auto& entry : alt.GetBlock4().GetBlock2()) {
                    if (!processItem(entry.GetRule_struct_arg_positional2())) {
                        return false;
                    }
                }
            }

            TPosition pos = Ctx_.TokenPosition(alt.GetToken1());
            TNodePtr structType = new TCallNodeImpl(pos, "StructType", structTypeItems);
            bool shouldEmitLabel = provider != YtProviderName || TCiString(keyFunc) == "object";
            if (shouldEmitLabel) {
                auto labelsTuple = BuildTuple(pos, labels);
                hints["user_" + to_lower(alt.GetToken1().GetValue())] = {structType, labelsTuple};
                break;
            } else {
                hints["user_" + to_lower(alt.GetToken1().GetValue())] = {structType};
                break;
            }
        }

        case TRule_table_hint::kAltTableHint4: {
            const auto& alt = rule.GetAlt_table_hint4();
            const auto pos = Ctx_.TokenPosition(alt.GetToken1());
            TColumnRefScope scope(Ctx_, EColumnRefState::Allow);
            TNodePtr expr = Unwrap(TSqlExpression(Ctx_, Mode_).Build(alt.GetRule_expr4()));
            if (!expr) {
                return false;
            }
            hints["watermark"] = {BuildLambda(pos, BuildList(pos, {BuildAtom(pos, "row")}), std::move(expr))};
            break;
        }

        case TRule_table_hint::kAltTableHint5: {
            const auto& alt = rule.GetAlt_table_hint5();
            const auto pos = Ctx_.TokenPosition(alt.GetToken1());
            TColumnRefScope scope(Ctx_, EColumnRefState::Allow);
            TNodePtr expr = Unwrap(TSqlExpression(Ctx_, Mode_).Build(alt.GetRule_expr3()));
            if (!expr) {
                return false;
            }
            hints["watermark"] = {BuildLambda(pos, BuildList(pos, {BuildAtom(pos, "row")}), std::move(expr))};
            break;
        }

        case TRule_table_hint::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    return true;
}

TMaybe<TTableHints> TSqlTranslation::TableHintsImpl(const TRule_table_hints& node, const TString& provider, const TString& keyFunc) {
    TTableHints hints;
    auto& block = node.GetBlock2();
    bool hasErrors = false;
    switch (block.Alt_case()) {
        case TRule_table_hints::TBlock2::kAlt1: {
            hasErrors = !TableHintImpl(block.GetAlt1().GetRule_table_hint1(), hints, provider, keyFunc);
            break;
        }
        case TRule_table_hints::TBlock2::kAlt2: {
            hasErrors = !TableHintImpl(block.GetAlt2().GetRule_table_hint2(), hints, provider, keyFunc);
            for (const auto& x : block.GetAlt2().GetBlock3()) {
                hasErrors = hasErrors || !TableHintImpl(x.GetRule_table_hint2(), hints, provider, keyFunc);
            }

            break;
        }
        case TRule_table_hints::TBlock2::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    if (hasErrors) {
        return Nothing();
    }

    return hints;
}

bool TSqlTranslation::SimpleTableRefImpl(const TRule_simple_table_ref& node, TTableRef& result) {
    // simple_table_ref: simple_table_ref_core table_hints?;
    if (!SimpleTableRefCoreImpl(node.GetRule_simple_table_ref_core1(), result)) {
        return false;
    }

    TTableHints hints = GetContextHints(Context());
    if (node.HasBlock2()) {
        const TString& service = Context().Scoped->CurrService;
        auto tmp = TableHintsImpl(node.GetBlock2().GetRule_table_hints1(), service);
        if (!tmp) {
            Error() << "Failed to parse table hints";
            return false;
        }

        hints = *tmp;
    }

    if (!hints.empty()) {
        result.Options = BuildInputOptions(Context().Pos(), hints);
    }

    return true;
}

bool TSqlTranslation::SimpleTableRefCoreImpl(const TRule_simple_table_ref_core& node, TTableRef& result) {
    // simple_table_ref_core: ((cluster_expr DOT)? id_or_at) | AT? bind_parameter;
    TString service = Context().Scoped->CurrService;
    TDeferredAtom cluster = Context().Scoped->CurrCluster;
    switch (node.Alt_case()) {
        case TRule_simple_table_ref_core::AltCase::kAltSimpleTableRefCore1: {
            if (node.GetAlt_simple_table_ref_core1().GetRule_object_ref1().HasBlock1()) {
                if (!ClusterExpr(node.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetBlock1().GetRule_cluster_expr1(), false, service, cluster)) {
                    return false;
                }
            }

            if (cluster.Empty()) {
                Error() << "No cluster name given and no default cluster is selected";
                return false;
            }

            result = TTableRef(Context().MakeName("table"), service, cluster, nullptr);
            auto tableOrAt = Id(node.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetRule_id_or_at2(), *this);
            auto tableAndView = TableKeyImpl(tableOrAt, {}, *this);
            result.Keys = BuildTableKey(Context().Pos(), result.Service, result.Cluster,
                                        TDeferredAtom(Context().Pos(), tableAndView.first), tableAndView.second);
            break;
        }
        case TRule_simple_table_ref_core::AltCase::kAltSimpleTableRefCore2: {
            if (node.GetAlt_simple_table_ref_core2().HasBlock1()) {
                if (!ClusterExpr(node.GetAlt_simple_table_ref_core2().GetBlock1().GetRule_cluster_expr1(), false, service, cluster)) {
                    return false;
                }
            }

            if (cluster.Empty()) {
                Error() << "No cluster name given and no default cluster is selected";
                return false;
            }

            auto at = node.GetAlt_simple_table_ref_core2().HasBlock2();
            TString bindName;
            if (!NamedNodeImpl(node.GetAlt_simple_table_ref_core2().GetRule_bind_parameter3(), bindName, *this)) {
                return false;
            }
            auto named = GetNamedNode(bindName);
            if (!named) {
                return false;
            }

            TDeferredAtom table;
            MakeTableFromExpression(Context().Pos(), Context(), named, table);
            result = TTableRef(Context().MakeName("table"), service, cluster, nullptr);
            result.Keys = BuildTableKey(Context().Pos(), result.Service, result.Cluster, table, {at ? "@" : ""});
            break;
        }
        case TRule_simple_table_ref_core::AltCase::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    return result.Keys != nullptr;
}

bool TSqlTranslation::TopicRefImpl(const TRule_topic_ref& node, TTopicRef& result) {
    TString service = Context().Scoped->CurrService;
    TDeferredAtom cluster = Context().Scoped->CurrCluster;
    if (node.HasBlock1()) {
        if (Mode_ == NSQLTranslation::ESqlMode::LIMITED_VIEW) {
            Error() << "Cluster should not be used in limited view";
            return false;
        }

        if (!ClusterExpr(node.GetBlock1().GetRule_cluster_expr1(), false, service, cluster)) {
            return false;
        }
    }

    if (cluster.Empty()) {
        Error() << "No cluster name given and no default cluster is selected";
        return false;
    }

    result = TTopicRef(Context().MakeName("topic"), cluster, nullptr);
    auto topic = Id(node.GetRule_an_id2(), *this);
    result.Keys = BuildTopicKey(Context().Pos(), result.Cluster, TDeferredAtom(Context().Pos(), topic));

    return true;
}

TNodePtr TSqlTranslation::NamedNode(const TRule_named_nodes_stmt& rule, TVector<TSymbolNameWithPos>& names) {
    // named_nodes_stmt: bind_parameter_list EQUALS (expr | select_unparenthesized_stmt);
    if (!BindList(rule.GetRule_bind_parameter_list1(), names)) {
        return {};
    }

    TNodePtr nodeExpr = nullptr;
    switch (rule.GetBlock3().Alt_case()) {
        case TRule_named_nodes_stmt::TBlock3::kAlt1: {
            const auto& alt = rule.GetBlock3().GetAlt1().GetRule_expr1();
            return YqlSelectOrLegacy(
                [&]() -> TNodeResult {
                    TSqlExpression expr(Ctx_, Mode_);
                    expr.SetPure(IsPure_);
                    expr.SetYqlSelectProduced(true);

                    TNodeResult node = expr.Build(alt);
                    if (!node) {
                        return std::unexpected(node.error());
                    }

                    if (TNodePtr source = GetYqlSource(*node)) {
                        node = TNonNull(ToTableExpression(std::move(source)));
                    }

                    return node;
                },
                [&]() -> TNodePtr {
                    TSqlExpression expr(Ctx_, Mode_);
                    expr.SetPure(IsPure_);
                    expr.SetYqlSelectProduced(false);

                    TNodePtr result = Unwrap(expr.BuildSourceOrNode(alt));
                    if (TSourcePtr source = MoveOutIfSource(result)) {
                        result = BuildSourceNode(Ctx_.Pos(), std::move(source));
                    }

                    return result;
                });
        }

        case TRule_named_nodes_stmt::TBlock3::kAlt2: {
            const auto& alt = rule.GetBlock3().GetAlt2().GetRule_select_unparenthesized_stmt1();
            return YqlSelectOrLegacy(
                [&]() -> TNodeResult {
                    TNodeResult node = BuildYqlSelectSubExpr(Ctx_, Mode_, alt);
                    if (!node) {
                        return std::unexpected(node.error());
                    }

                    return TNonNull(ToTableExpression(GetYqlSource(std::move(*node))));
                },
                [&]() -> TNodePtr {
                    TSqlSelect select(Ctx_, Mode_);
                    select.SetPure(IsPure_);

                    TPosition pos;
                    TSourcePtr source = select.Build(alt, pos);
                    if (!source) {
                        return nullptr;
                    }

                    return BuildSourceNode(pos, std::move(source));
                });
        }

        case TRule_named_nodes_stmt::TBlock3::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

bool TSqlTranslation::ImportStatement(const TRule_import_stmt& stmt, TVector<TString>* namesPtr) {
    TVector<TString> modulePath;
    if (!ModulePath(stmt.GetRule_module_path2(), modulePath)) {
        return false;
    }

    TVector<TSymbolNameWithPos> names;
    TVector<TSymbolNameWithPos> aliases;
    if (!NamedBindList(stmt.GetRule_named_bind_parameter_list4(), names, aliases)) {
        return false;
    }
    YQL_ENSURE(names.size() == aliases.size());
    const TString moduleAlias = Ctx_.AddImport(std::move(modulePath));
    if (!moduleAlias) {
        return false;
    }

    for (size_t i = 0; i < names.size(); ++i) {
        auto& name = names[i];
        auto& alias = aliases[i];

        auto& var = alias.Name ? alias : name;
        if (IsAnonymousName(var.Name)) {
            Ctx_.Error(var.Pos) << "Can not import anonymous name " << var.Name;
            return false;
        }

        auto builder = [&](const TString& realName) {
            YQL_ENSURE(realName == var.Name);
            auto atom = BuildQuotedAtom(name.Pos, name.Name);
            return atom->Y("bind", moduleAlias, atom);
        };

        var.Name = PushNamedNode(var.Pos, var.Name, builder);
        if (namesPtr) {
            namesPtr->push_back(var.Name);
        }
    }
    return true;
}

bool TSqlTranslation::SortSpecification(const TRule_sort_specification& node, TVector<TSortSpecificationPtr>& sortSpecs) {
    bool asc = true;
    TSqlExpression expr(Ctx_, Mode_);
    TColumnRefScope scope(Ctx_, EColumnRefState::Allow);
    TNodePtr exprNode = Unwrap(expr.Build(node.GetRule_expr1()));
    if (!exprNode) {
        return false;
    }
    if (node.HasBlock2()) {
        const auto& token = node.GetBlock2().GetToken1();
        Token(token);
        auto tokenId = token.GetId();
        if (IS_TOKEN(tokenId, ASC)) {
            Ctx_.IncrementMonCounter("sql_features", "OrderByAsc");
        } else if (IS_TOKEN(tokenId, DESC)) {
            asc = false;
            Ctx_.IncrementMonCounter("sql_features", "OrderByDesc");
        } else {
            Ctx_.IncrementMonCounter("sql_errors", "UnknownOrderBy");
            Error() << "Unsupported direction token: " << token.GetId();
            return false;
        }
    } else {
        Ctx_.IncrementMonCounter("sql_features", "OrderByDefault");
    }
    sortSpecs.emplace_back(MakeIntrusive<TSortSpecification>(exprNode, asc));
    return true;
}

bool TSqlTranslation::SortSpecificationList(const TRule_sort_specification_list& node, TVector<TSortSpecificationPtr>& sortSpecs) {
    if (!SortSpecification(node.GetRule_sort_specification1(), sortSpecs)) {
        return false;
    }
    for (auto sortSpec : node.GetBlock2()) {
        Token(sortSpec.GetToken1());
        if (!SortSpecification(sortSpec.GetRule_sort_specification2(), sortSpecs)) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::IsDistinctOptSet(const TRule_opt_set_quantifier& node) const {
    TPosition pos;
    return node.HasBlock1() && IS_TOKEN(node.GetBlock1().GetToken1().GetId(), DISTINCT);
}

bool TSqlTranslation::IsDistinctOptSet(const TRule_opt_set_quantifier& node, TPosition& distinctPos) const {
    if (node.HasBlock1() && IS_TOKEN(node.GetBlock1().GetToken1().GetId(), DISTINCT)) {
        distinctPos = Ctx_.TokenPosition(node.GetBlock1().GetToken1());
        return true;
    }
    return false;
}

bool TSqlTranslation::RoleNameClause(const TRule_role_name& node, TDeferredAtom& result, bool allowSystemRoles) {
    // role_name: an_id_or_type | bind_parameter;
    switch (node.Alt_case()) {
        case TRule_role_name::kAltRoleName1: {
            TString name = Id(node.GetAlt_role_name1().GetRule_an_id_or_type1(), *this);
            result = TDeferredAtom(Ctx_.Pos(), name);
            break;
        }
        case TRule_role_name::kAltRoleName2: {
            if (!BindParameterClause(node.GetAlt_role_name2().GetRule_bind_parameter1(), result)) {
                return false;
            }
            break;
        }
        case TRule_role_name::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    if (auto literalName = result.GetLiteral(); literalName && !allowSystemRoles) {
        static const THashSet<TStringBuf> SystemRoles = {"current_role", "current_user", "session_user"};
        if (SystemRoles.contains(to_lower(*literalName))) {
            Ctx_.Error() << "System role " << to_upper(*literalName) << " can not be used here";
            return false;
        }
    }

    return true;
}

bool TSqlTranslation::PasswordParameter(const TRule_password_option& passwordOption, TUserParameters& result) {
    // password_option: ENCRYPTED? PASSWORD password_value;
    // password_value: STRING_VALUE | NULL;

    const auto& token = passwordOption.GetRule_password_value3().GetToken1();
    TString stringValue(Ctx_.Token(token));

    if (to_lower(stringValue) == "null") {
        result.IsPasswordNull = true;
    } else {
        auto password = StringContent(Ctx_, Ctx_.Pos(), stringValue);

        if (!password) {
            Error() << "Password should be enclosed into quotation marks.";
            return false;
        }

        result.Password = TDeferredAtom(Ctx_.Pos(), std::move(password->Content));
    }

    result.IsPasswordEncrypted = passwordOption.HasBlock1();

    return true;
}

bool TSqlTranslation::HashParameter(const TRule_hash_option& hashOption, TUserParameters& result) {
    // hash_option: HASH STRING_VALUE;

    const auto& token = hashOption.GetToken2();
    TString stringValue(Ctx_.Token(token));

    auto hash = StringContent(Ctx_, Ctx_.Pos(), stringValue);

    if (!hash) {
        Error() << "Hash should be enclosed into quotation marks.";
        return false;
    }

    result.Hash = TDeferredAtom(Ctx_.Pos(), std::move(hash->Content));

    return true;
}

void TSqlTranslation::LoginParameter(const TRule_login_option& loginOption, std::optional<bool>& canLogin) {
    // login_option: LOGIN | NOLOGIN;

    auto token = loginOption.GetToken1().GetId();
    if (IS_TOKEN(token, LOGIN)) {
        canLogin = true;
    } else if (IS_TOKEN(token, NOLOGIN)) {
        canLogin = false;
    } else {
        Y_UNREACHABLE();
    }
}

bool TSqlTranslation::UserParameters(const std::vector<TRule_user_option>& optionsList, TUserParameters& result, bool isCreateUser) {
    enum class EUserOption {
        Login,
        Authentication
    };

    std::set<EUserOption> used;

    auto ParseUserOption = [&used, this](const TRule_user_option& option, TUserParameters& result) -> bool {
        // user_option: authentication_option | login_option;
        //      authentication_option: password_option | hash_option;

        switch (option.Alt_case()) {
            case TRule_user_option::kAltUserOption1: {
                if (used.contains(EUserOption::Authentication)) {
                    Error() << "Conflicting or redundant options";
                    return false;
                }

                used.insert(EUserOption::Authentication);

                const auto& authenticationOption = option.GetAlt_user_option1().GetRule_authentication_option1();

                switch (authenticationOption.Alt_case()) {
                    case TRule_authentication_option::kAltAuthenticationOption1: {
                        if (!PasswordParameter(authenticationOption.GetAlt_authentication_option1().GetRule_password_option1(), result)) {
                            return false;
                        }

                        break;
                    }
                    case TRule_authentication_option::kAltAuthenticationOption2: {
                        if (!HashParameter(authenticationOption.GetAlt_authentication_option2().GetRule_hash_option1(), result)) {
                            return false;
                        }

                        break;
                    }
                    case TRule_authentication_option::ALT_NOT_SET:
                        Y_UNREACHABLE();
                }

                break;
            }
            case TRule_user_option::kAltUserOption2: {
                if (used.contains(EUserOption::Login)) {
                    Error() << "Conflicting or redundant options";
                    return false;
                }

                used.insert(EUserOption::Login);

                LoginParameter(option.GetAlt_user_option2().GetRule_login_option1(), result.CanLogin);

                break;
            }
            case TRule_user_option::ALT_NOT_SET:
                Y_UNREACHABLE();
        }

        return true;
    };

    if (isCreateUser) {
        result.CanLogin = true;
        result.IsPasswordNull = true;
    }

    for (const auto& option : optionsList) {
        if (!ParseUserOption(option, result)) {
            return false;
        }
    }

    return true;
}

bool TSqlTranslation::PermissionNameClause(const TRule_permission_id& node, TDeferredAtom& result) {
    // permission_id:
    //   CONNECT
    // | LIST
    // | INSERT
    // | MANAGE
    // | DROP
    // | GRANT
    // | MODIFY (TABLES | ATTRIBUTES)
    // | (UPDATE | ERASE) ROW
    // | (REMOVE | DESCRIBE | ALTER) SCHEMA
    // | SELECT (TABLES | ATTRIBUTES | ROW)?
    // | (USE | FULL) LEGACY?
    // | CREATE (DIRECTORY | TABLE | QUEUE)?

    auto handleOneIdentifier = [&result, this](const auto& permissionNameKeyword) {
        result = TDeferredAtom(Ctx_.Pos(), GetIdentifier(*this, permissionNameKeyword).Name);
    };

    auto handleTwoIdentifiers = [&result, this](const auto& permissionNameKeyword) {
        const auto& token1 = permissionNameKeyword.GetToken1();
        const auto& token2 = permissionNameKeyword.GetToken2();
        TString identifierName = TIdentifier(TPosition(token1.GetColumn(), token1.GetLine()), Identifier(token1)).Name +
                                 "_" +
                                 TIdentifier(TPosition(token2.GetColumn(), token2.GetLine()), Identifier(token2)).Name;
        result = TDeferredAtom(Ctx_.Pos(), identifierName);
    };

    auto handleOneOrTwoIdentifiers = [&result, this](const auto& permissionNameKeyword) {
        TString identifierName = GetIdentifier(*this, permissionNameKeyword).Name;
        if (permissionNameKeyword.HasBlock2()) {
            identifierName += "_" + GetIdentifier(*this, permissionNameKeyword.GetBlock2()).Name;
        }
        result = TDeferredAtom(Ctx_.Pos(), identifierName);
    };

    switch (node.GetAltCase()) {
        case TRule_permission_id::kAltPermissionId1: {
            // CONNECT
            handleOneIdentifier(node.GetAlt_permission_id1());
            break;
        }
        case TRule_permission_id::kAltPermissionId2: {
            // LIST
            handleOneIdentifier(node.GetAlt_permission_id2());
            break;
        }
        case TRule_permission_id::kAltPermissionId3: {
            // INSERT
            handleOneIdentifier(node.GetAlt_permission_id3());
            break;
        }
        case TRule_permission_id::kAltPermissionId4: {
            // MANAGE
            handleOneIdentifier(node.GetAlt_permission_id4());
            break;
        }
        case TRule_permission_id::kAltPermissionId5: {
            // DROP
            handleOneIdentifier(node.GetAlt_permission_id5());
            break;
        }
        case TRule_permission_id::kAltPermissionId6: {
            // GRANT
            handleOneIdentifier(node.GetAlt_permission_id6());
            break;
        }
        case TRule_permission_id::kAltPermissionId7: {
            // MODIFY (TABLES | ATTRIBUTES)
            handleTwoIdentifiers(node.GetAlt_permission_id7());
            break;
        }
        case TRule_permission_id::kAltPermissionId8: {
            // (UPDATE | ERASE) ROW
            handleTwoIdentifiers(node.GetAlt_permission_id8());
            break;
        }
        case TRule_permission_id::kAltPermissionId9: {
            // (REMOVE | DESCRIBE | ALTER) SCHEMA
            handleTwoIdentifiers(node.GetAlt_permission_id9());
            break;
        }
        case TRule_permission_id::kAltPermissionId10: {
            // SELECT (TABLES | ATTRIBUTES | ROW)?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id10());
            break;
        }
        case TRule_permission_id::kAltPermissionId11: {
            // (USE | FULL) LEGACY?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id11());
            break;
        }
        case TRule_permission_id::kAltPermissionId12: {
            // CREATE (DIRECTORY | TABLE | QUEUE)?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id12());
            break;
        }
        case TRule_permission_id::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return true;
}

bool TSqlTranslation::PermissionNameClause(const TRule_permission_name& node, TDeferredAtom& result) {
    // permission_name: permission_id | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_permission_name::kAltPermissionName1: {
            return PermissionNameClause(node.GetAlt_permission_name1().GetRule_permission_id1(), result);
            break;
        }
        case TRule_permission_name::kAltPermissionName2: {
            const TString stringValue(Ctx_.Token(node.GetAlt_permission_name2().GetToken1()));
            auto unescaped = StringContent(Ctx_, Ctx_.Pos(), stringValue);
            if (!unescaped) {
                return false;
            }
            result = TDeferredAtom(Ctx_.Pos(), unescaped->Content);
            break;
        }
        case TRule_permission_name::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return true;
}

bool TSqlTranslation::PermissionNameClause(const TRule_permission_name_target& node, TVector<TDeferredAtom>& result, bool withGrantOption) {
    // permission_name_target: permission_name (COMMA permission_name)* COMMA? | ALL PRIVILEGES?;
    switch (node.Alt_case()) {
        case TRule_permission_name_target::kAltPermissionNameTarget1: {
            const auto& permissionNameRule = node.GetAlt_permission_name_target1();
            result.emplace_back();
            if (!PermissionNameClause(permissionNameRule.GetRule_permission_name1(), result.back())) {
                return false;
            }
            for (const auto& item : permissionNameRule.GetBlock2()) {
                result.emplace_back();
                if (!PermissionNameClause(item.GetRule_permission_name2(), result.back())) {
                    return false;
                }
            }
            break;
        }
        case TRule_permission_name_target::kAltPermissionNameTarget2: {
            result.emplace_back(Ctx_.Pos(), "all_privileges");
            break;
        }
        case TRule_permission_name_target::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    if (withGrantOption) {
        result.emplace_back(Ctx_.Pos(), "grant");
    }
    return true;
}

bool TSqlTranslation::StoreStringSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result) {
    YQL_ENSURE(value);

    const TString key = to_lower(id.Name);
    if (result.find(key) != result.end()) {
        Ctx_.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    switch (value->Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2:
            return StoreString(*value, result[key], Ctx_, to_upper(key));

        default:
            Ctx_.Error() << to_upper(key) << " value should be a string literal";
            return false;
    }

    return true;
}

bool TSqlTranslation::StoreStringSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result) {
    const TIdentifier id = IdEx(entry.GetRule_an_id1(), *this);
    return StoreStringSettingsEntry(id, &entry.GetRule_table_setting_value3(), result);
}

bool TSqlTranslation::ParseBackupCollectionSettings(std::map<TString, TDeferredAtom>& result, const TRule_backup_collection_settings& settings) {
    const auto& firstEntry = settings.GetRule_backup_collection_settings_entry1();
    if (!StoreStringSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), &firstEntry.GetRule_table_setting_value3(), result)) {
        return false;
    }
    for (const auto& block : settings.GetBlock2()) {
        const auto& entry = block.GetRule_backup_collection_settings_entry2();
        if (!StoreStringSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), &entry.GetRule_table_setting_value3(), result)) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::ParseBackupCollectionSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_backup_collection_actions& actions) {
    auto parseAction = [&](auto& actionVariant) {
        switch (actionVariant.Alt_case()) {
            case TRule_alter_backup_collection_action::kAltAlterBackupCollectionAction1: {
                const auto& action = actionVariant.GetAlt_alter_backup_collection_action1().GetRule_alter_table_set_table_setting_compat1();
                if (!StoreStringSettingsEntry(action.GetRule_alter_table_setting_entry3(), result)) {
                    return false;
                }
                for (const auto& entry : action.GetBlock4()) {
                    if (!StoreStringSettingsEntry(entry.GetRule_alter_table_setting_entry2(), result)) {
                        return false;
                    }
                }
                return true;
            }
            case TRule_alter_backup_collection_action::kAltAlterBackupCollectionAction2: {
                const auto& action = actionVariant.GetAlt_alter_backup_collection_action2().GetRule_alter_table_reset_table_setting1();
                const TString firstKey = to_lower(IdEx(action.GetRule_an_id3(), *this).Name);
                toReset.insert(firstKey);
                for (const auto& key : action.GetBlock4()) {
                    toReset.insert(to_lower(IdEx(key.GetRule_an_id2(), *this).Name));
                }
                return true;
            }
            case TRule_alter_backup_collection_action::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    };

    const auto& firstAction = actions.GetRule_alter_backup_collection_action1();
    if (!parseAction(firstAction)) {
        return false;
    }

    for (const auto& action : actions.GetBlock2()) {
        if (!parseAction(action.GetRule_alter_backup_collection_action2())) {
            return false;
        }
    }

    return true;
}

bool TSqlTranslation::ParseBackupCollectionTables(TVector<TDeferredAtom>& result, const TRule_table_list& tables) {
    const auto& firstEntry = tables.GetRule_an_id_table2();
    result.push_back(TDeferredAtom(Ctx_.Pos(), Id(firstEntry, *this)));
    for (const auto& block : tables.GetBlock3()) {
        const auto& entry = block.GetRule_an_id_table3();
        result.push_back(TDeferredAtom(Ctx_.Pos(), Id(entry, *this)));
    }
    return true;
}

bool TSqlTranslation::ParseBackupCollectionEntry(
    bool& addDatabase,
    bool& removeDatabase,
    TVector<TDeferredAtom>& addTables,
    TVector<TDeferredAtom>& removeTables,
    const TRule_alter_backup_collection_entry& entry)
{
    switch (entry.Alt_case()) {
        case TRule_alter_backup_collection_entry::kAltAlterBackupCollectionEntry1: {
            addDatabase = true;
            return true;
        }
        case TRule_alter_backup_collection_entry::kAltAlterBackupCollectionEntry2: {
            removeDatabase = true;
            return true;
        }
        case TRule_alter_backup_collection_entry::kAltAlterBackupCollectionEntry3: {
            auto table = entry.GetAlt_alter_backup_collection_entry3().GetRule_an_id_table3();
            addTables.push_back(TDeferredAtom(Ctx_.Pos(), Id(table, *this)));
            return true;
        }
        case TRule_alter_backup_collection_entry::kAltAlterBackupCollectionEntry4: {
            auto table = entry.GetAlt_alter_backup_collection_entry4().GetRule_an_id_table3();
            removeTables.push_back(TDeferredAtom(Ctx_.Pos(), Id(table, *this)));
            return true;
        }
        case TRule_alter_backup_collection_entry::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return true;
}

bool TSqlTranslation::ParseBackupCollectionEntries(
    bool& addDatabase,
    bool& removeDatabase,
    TVector<TDeferredAtom>& addTables,
    TVector<TDeferredAtom>& removeTables,
    const TRule_alter_backup_collection_entries& entries)
{
    const auto& firstEntry = entries.GetRule_alter_backup_collection_entry1();
    if (!ParseBackupCollectionEntry(addDatabase, removeDatabase, addTables, removeTables, firstEntry)) {
        return false;
    }
    for (const auto& block : entries.GetBlock2()) {
        const auto& entry = block.GetRule_alter_backup_collection_entry2();
        if (!ParseBackupCollectionEntry(addDatabase, removeDatabase, addTables, removeTables, entry)) {
            return false;
        }
    }
    return true;
}

TString TSqlTranslation::FrameSettingsToString(EFrameSettings settings, bool isUnbounded) {
    TString result;
    switch (settings) {
        case FramePreceding:
            result = "PRECEDING";
            break;
        case FrameCurrentRow:
            YQL_ENSURE(!isUnbounded);
            result = "CURRENT ROW";
            break;
        case FrameFollowing:
            result = "FOLLOWING";
            break;
        case FrameUndefined:
            Y_ABORT("Unexpected frame settings");
    }

    return (isUnbounded ? "UNBOUNDED " : "") + result;
}

bool CheckFrameBoundLiteral(TContext& ctx, const TFrameBound& bound, TMaybe<i32>& boundValue) {
    boundValue = {};
    auto node = bound.Bound;
    if (node && node->IsLiteral()) {
        auto type = node->GetLiteralType();
        if (type != "Int32") {
            ctx.Error(node->GetPos()) << "Expecting Int32 as frame bound value, but got " << type << " literal";
            return false;
        }

        i32 value = FromString<i32>(node->GetLiteralValue());
        if (value < 0) {
            ctx.Error(node->GetPos()) << "Expecting non-negative value for frame bound, but got " << value;
            return false;
        }

        boundValue = value;
    }

    return true;
}

bool TSqlTranslation::IsValidFrameSettings(TContext& ctx, const TFrameSpecification& frameSpec, size_t sortSpecSize) {
    const TFrameBound& begin = *frameSpec.FrameBegin;
    const TFrameBound& end = *frameSpec.FrameEnd;

    YQL_ENSURE(begin.Settings != FrameUndefined);
    YQL_ENSURE(end.Settings != FrameUndefined);

    const bool beginUnbounded = !begin.Bound && begin.Settings != FrameCurrentRow;
    const bool endUnbounded = !end.Bound && end.Settings != FrameCurrentRow;

    if (beginUnbounded && begin.Settings == FrameFollowing) {
        ctx.Error(begin.Pos) << "Frame cannot start from " << FrameSettingsToString(begin.Settings, beginUnbounded);
        return false;
    }

    if (endUnbounded && end.Settings == FramePreceding) {
        ctx.Error(end.Pos) << "Frame cannot end with " << FrameSettingsToString(end.Settings, endUnbounded);
        return false;
    }

    if (begin.Settings > end.Settings) {
        ctx.Error(begin.Pos) << "Frame cannot start from " << FrameSettingsToString(begin.Settings, beginUnbounded)
                             << " and end with " << FrameSettingsToString(end.Settings, endUnbounded);
        return false;
    }

    if (frameSpec.FrameType == FrameByRange && sortSpecSize != 1) {
        TStringBuf msg = "RANGE with <offset> PRECEDING/FOLLOWING requires exactly one expression in ORDER BY partition clause";
        if (begin.Bound) {
            ctx.Error(begin.Bound->GetPos()) << msg;
            return false;
        }
        if (end.Bound) {
            ctx.Error(end.Bound->GetPos()) << msg;
            return false;
        }
    }

    TMaybe<i32> beginValue;
    TMaybe<i32> endValue;

    if (frameSpec.FrameType != EFrameType::FrameByRange) {
        if (!CheckFrameBoundLiteral(ctx, begin, beginValue) || !CheckFrameBoundLiteral(ctx, end, endValue)) {
            return false;
        }
    }

    if (beginValue.Defined() && endValue.Defined()) {
        if (begin.Settings == FramePreceding) {
            beginValue = 0 - *beginValue;
        }
        if (end.Settings == FramePreceding) {
            endValue = 0 - *endValue;
        }

        if (*beginValue > *endValue) {
            YQL_ENSURE(begin.Bound);
            if (!ctx.Warning(begin.Bound->GetPos(), TIssuesIds::YQL_EMPTY_WINDOW_FRAME, [](auto& out) {
                    out << "Used frame specification implies empty window frame";
                })) {
                return false;
            }
        }
    }

    return true;
}

bool TSqlTranslation::FrameBound(const TRule_window_frame_bound& rule, TFrameBoundPtr& bound) {
    // window_frame_bound:
    //    CURRENT ROW
    //  | (expr | UNBOUNDED) (PRECEDING | FOLLOWING)
    // ;
    bound = new TFrameBound;
    switch (rule.Alt_case()) {
        case TRule_window_frame_bound::kAltWindowFrameBound1:
            bound->Pos = GetPos(rule.GetAlt_window_frame_bound1().GetToken1());
            bound->Settings = FrameCurrentRow;
            break;
        case TRule_window_frame_bound::kAltWindowFrameBound2: {
            auto block = rule.GetAlt_window_frame_bound2().GetBlock1();
            switch (block.Alt_case()) {
                case TRule_window_frame_bound_TAlt2_TBlock1::kAlt1: {
                    TSqlExpression boundExpr(Ctx_, Mode_);
                    bound->Bound = Unwrap(boundExpr.Build(block.GetAlt1().GetRule_expr1()));
                    if (!bound->Bound) {
                        return false;
                    }
                    bound->Pos = bound->Bound->GetPos();
                    break;
                }
                case TRule_window_frame_bound_TAlt2_TBlock1::kAlt2:
                    bound->Pos = GetPos(block.GetAlt2().GetToken1());
                    break;
                case TRule_window_frame_bound_TAlt2_TBlock1::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }

            const TString settingToken = to_lower(Token(rule.GetAlt_window_frame_bound2().GetToken2()));
            if (settingToken == "preceding") {
                bound->Settings = FramePreceding;
            } else if (settingToken == "following") {
                bound->Settings = FrameFollowing;
            } else {
                Y_UNREACHABLE();
            }
            break;
        }
        case TRule_window_frame_bound::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return true;
}

bool TSqlTranslation::FrameClause(const TRule_window_frame_clause& rule, TFrameSpecificationPtr& frameSpec, size_t sortSpecSize) {
    // window_frame_clause: window_frame_units window_frame_extent window_frame_exclusion?;
    frameSpec = new TFrameSpecification;
    const TString frameUnitStr = to_lower(Token(rule.GetRule_window_frame_units1().GetToken1()));
    if (frameUnitStr == "rows") {
        frameSpec->FrameType = EFrameType::FrameByRows;
    } else if (frameUnitStr == "range") {
        frameSpec->FrameType = EFrameType::FrameByRange;
    } else {
        YQL_ENSURE(frameUnitStr == "groups");
        frameSpec->FrameType = EFrameType::FrameByGroups;
    }

    auto frameExtent = rule.GetRule_window_frame_extent2();
    // window_frame_extent: window_frame_bound | window_frame_between;
    switch (frameExtent.Alt_case()) {
        case TRule_window_frame_extent::kAltWindowFrameExtent1: {
            auto start = frameExtent.GetAlt_window_frame_extent1().GetRule_window_frame_bound1();
            if (!FrameBound(start, frameSpec->FrameBegin)) {
                return false;
            }

            // frame end is CURRENT ROW
            frameSpec->FrameEnd = new TFrameBound;
            frameSpec->FrameEnd->Pos = frameSpec->FrameBegin->Pos;
            frameSpec->FrameEnd->Settings = FrameCurrentRow;
            break;
        }
        case TRule_window_frame_extent::kAltWindowFrameExtent2: {
            // window_frame_between: BETWEEN window_frame_bound AND window_frame_bound;
            auto between = frameExtent.GetAlt_window_frame_extent2().GetRule_window_frame_between1();
            if (!FrameBound(between.GetRule_window_frame_bound2(), frameSpec->FrameBegin)) {
                return false;
            }
            if (!FrameBound(between.GetRule_window_frame_bound4(), frameSpec->FrameEnd)) {
                return false;
            }
            break;
        }
        case TRule_window_frame_extent::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    YQL_ENSURE(frameSpec->FrameBegin);
    YQL_ENSURE(frameSpec->FrameEnd);
    if (!IsValidFrameSettings(Ctx_, *frameSpec, sortSpecSize)) {
        return false;
    }

    if (rule.HasBlock3()) {
        // window_frame_exclusion: EXCLUDE CURRENT ROW | EXCLUDE GROUP | EXCLUDE TIES | EXCLUDE NO OTHERS;
        switch (rule.GetBlock3().GetRule_window_frame_exclusion1().Alt_case()) {
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion1:
                frameSpec->FrameExclusion = FrameExclCurRow;
                break;
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion2:
                frameSpec->FrameExclusion = FrameExclGroup;
                break;
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion3:
                frameSpec->FrameExclusion = FrameExclTies;
                break;
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion4:
                frameSpec->FrameExclusion = FrameExclNone;
                break;
            case TRule_window_frame_exclusion::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    if (frameSpec->FrameExclusion != FrameExclNone) {
        Ctx_.Error() << "Frame exclusion is not supported yet";
        return false;
    }

    return true;
}

TWindowSpecificationPtr TSqlTranslation::WindowSpecification(const TRule_window_specification_details& rule) {
    /*
    window_specification_details:
        existing_window_name?
        window_partition_clause?
        window_order_clause?
        window_frame_clause?
    */
    TWindowSpecificationPtr winSpecPtr = new TWindowSpecification;
    if (rule.HasBlock1()) {
        Ctx_.Error() << "Existing window name is not supported in window specification yet!";
        return {};
    }
    if (rule.HasBlock2()) {
        /*
        window_partition_clause: PARTITION COMPACT? BY named_expr_list;
        */
        auto& partitionClause = rule.GetBlock2().GetRule_window_partition_clause1();
        winSpecPtr->IsCompact = partitionClause.HasBlock2();
        if (!winSpecPtr->IsCompact) {
            auto hints = Ctx_.PullHintForToken(Ctx_.TokenPosition(partitionClause.GetToken1()));
            winSpecPtr->IsCompact = AnyOf(hints, [](const NSQLTranslation::TSQLHint& hint) { return to_lower(hint.Name) == "compact"; });
        }
        TColumnRefScope scope(Ctx_, EColumnRefState::Allow);
        if (!NamedExprList(partitionClause.GetRule_named_expr_list4(), winSpecPtr->Partitions)) {
            return {};
        }
        // ignore empty unnamed tuples:
        // "PARTITION BY (), foo(x) as y, (), (z)" is allowed and will work exactly the same as
        // "PARTITION BY foo(x) as y, z"
        auto removed = std::remove_if(winSpecPtr->Partitions.begin(), winSpecPtr->Partitions.end(),
                                      [](const TNodePtr& partitionNode) {
                                          return !partitionNode->GetLabel() && !partitionNode->GetColumnName() &&
                                                 partitionNode->GetTupleNode() != nullptr &&
                                                 partitionNode->GetTupleSize() == 0;
                                      });
        winSpecPtr->Partitions.erase(removed, winSpecPtr->Partitions.end());
    }
    if (rule.HasBlock3()) {
        if (!OrderByClause(rule.GetBlock3().GetRule_window_order_clause1().GetRule_order_by_clause1(), winSpecPtr->OrderBy)) {
            return {};
        }
    }
    const bool ordered = !winSpecPtr->OrderBy.empty();
    if (rule.HasBlock4()) {
        if (!FrameClause(rule.GetBlock4().GetRule_window_frame_clause1(), winSpecPtr->Frame, winSpecPtr->OrderBy.size())) {
            return {};
        }
    } else {
        winSpecPtr->Frame = new TFrameSpecification;
        winSpecPtr->Frame->FrameBegin = new TFrameBound;
        winSpecPtr->Frame->FrameEnd = new TFrameBound;
        winSpecPtr->Frame->FrameBegin->Pos = winSpecPtr->Frame->FrameEnd->Pos = Ctx_.Pos();
        winSpecPtr->Frame->FrameExclusion = EFrameExclusions::FrameExclNone;

        winSpecPtr->Frame->FrameBegin->Settings = EFrameSettings::FramePreceding;
        if (Ctx_.AnsiCurrentRow) {
            // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            winSpecPtr->Frame->FrameType = EFrameType::FrameByRange;
            winSpecPtr->Frame->FrameEnd->Settings = EFrameSettings::FrameCurrentRow;
        } else if (ordered) {
            // legacy behavior
            // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            winSpecPtr->Frame->FrameType = EFrameType::FrameByRows;
            winSpecPtr->Frame->FrameEnd->Settings = EFrameSettings::FrameCurrentRow;
        } else {
            // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            winSpecPtr->Frame->FrameType = EFrameType::FrameByRows;
            winSpecPtr->Frame->FrameEnd->Settings = EFrameSettings::FrameFollowing;
        }
    }

    // Normalize and simplify
    auto replaceCurrentWith = [](TFrameBound& frame, bool preceding, TNodePtr value) {
        frame.Settings = preceding ? EFrameSettings::FramePreceding : EFrameSettings::FrameFollowing;
        frame.Bound = value;
    };

    const auto frameSpec = winSpecPtr->Frame;
    if (!ordered && frameSpec->FrameType != EFrameType::FrameByRows) {
        // CURRENT ROW -> UNBOUNDED
        if (frameSpec->FrameBegin->Settings == EFrameSettings::FrameCurrentRow) {
            replaceCurrentWith(*frameSpec->FrameBegin, true, nullptr);
        }
        if (frameSpec->FrameEnd->Settings == EFrameSettings::FrameCurrentRow) {
            replaceCurrentWith(*frameSpec->FrameBegin, false, nullptr);
        }
    }

    // RANGE/GROUPS UNBOUNDED -> ROWS UNBOUNDED
    if (frameSpec->FrameBegin->Settings == EFrameSettings::FramePreceding && !frameSpec->FrameBegin->Bound &&
        frameSpec->FrameEnd->Settings == EFrameSettings::FrameFollowing && !frameSpec->FrameEnd->Bound)
    {
        frameSpec->FrameType = EFrameType::FrameByRows;
    }

    if (frameSpec->FrameType != EFrameType::FrameByRange) {
        // replace FrameCurrentRow for ROWS/GROUPS with 0 preceding/following
        // FrameCurrentRow has special meaning ( = first/last peer row)
        if (frameSpec->FrameBegin->Settings == EFrameSettings::FrameCurrentRow) {
            TNodePtr zero = new TLiteralNumberNode<i32>(winSpecPtr->Frame->FrameBegin->Pos, "Int32", "0");
            replaceCurrentWith(*frameSpec->FrameBegin, true, zero);
        }

        if (frameSpec->FrameEnd->Settings == EFrameSettings::FrameCurrentRow) {
            TNodePtr zero = new TLiteralNumberNode<i32>(winSpecPtr->Frame->FrameEnd->Pos, "Int32", "0");
            replaceCurrentWith(*frameSpec->FrameEnd, false, zero);
        }
    }

    return winSpecPtr;
}

TNodePtr TSqlTranslation::DoStatement(const TRule_do_stmt& stmt, bool makeLambda, const TVector<TString>& args) {
    switch (stmt.GetBlock2().Alt_case()) {
        case TRule_do_stmt_TBlock2::kAlt1: {
            const auto& callAction = stmt.GetBlock2().GetAlt1().GetRule_call_action1();
            TNodePtr action;
            switch (callAction.GetBlock1().GetAltCase()) {
                case TRule_call_action_TBlock1::kAlt1: {
                    TString bindName;
                    if (!NamedNodeImpl(callAction.GetBlock1().GetAlt1().GetRule_bind_parameter1(), bindName, *this)) {
                        return nullptr;
                    }
                    action = GetNamedNode(bindName);
                    if (!action) {
                        return nullptr;
                    }
                    break;
                }
                case TRule_call_action_TBlock1::kAlt2:
                    action = BuildEmptyAction(Ctx_.Pos());
                    break;
                case TRule_call_action_TBlock1::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }

            TVector<TNodePtr> values;
            values.push_back(new TAstAtomNodeImpl(Ctx_.Pos(), "Apply", TNodeFlags::Default));
            values.push_back(action);
            values.push_back(new TAstAtomNodeImpl(Ctx_.Pos(), "world", TNodeFlags::Default));

            TSqlExpression sqlExpr(Ctx_, Mode_);
            if (callAction.HasBlock3() && !Unwrap(ExprList(sqlExpr, values, callAction.GetBlock3().GetRule_expr_list1()))) {
                return nullptr;
            }

            TNodePtr apply = new TAstListNodeImpl(Ctx_.Pos(), std::move(values));
            if (!makeLambda) {
                return BuildDoCall(Ctx_.Pos(), apply);
            }

            TNodePtr params = new TAstListNodeImpl(Ctx_.Pos());
            params->Add("world");
            for (const auto& arg : args) {
                params->Add(new TAstAtomNodeImpl(Ctx_.Pos(), arg, TNodeFlags::ArbitraryContent));
            }

            return BuildDoCall(Ctx_.Pos(), BuildLambda(Ctx_.Pos(), params, apply));
        }
        case TRule_do_stmt_TBlock2::kAlt2: {
            const auto& inlineAction = stmt.GetBlock2().GetAlt2().GetRule_inline_action1();
            const auto& body = inlineAction.GetRule_define_action_or_subquery_body2();

            auto saveScoped = Ctx_.Scoped;
            Ctx_.Scoped = MakeIntrusive<TScopedState>();
            Ctx_.AllScopes.push_back(Ctx_.Scoped);
            *Ctx_.Scoped = *saveScoped;
            Ctx_.Scoped->Local = TScopedState::TLocal{};
            Ctx_.ScopeLevel++;
            TSqlQuery query(Ctx_, Ctx_.Settings.Mode, false);
            TBlocks innerBlocks;

            const bool hasValidBody = DefineActionOrSubqueryBody(query, innerBlocks, body);
            auto ret = hasValidBody ? BuildQuery(Ctx_.Pos(), innerBlocks, false, Ctx_.Scoped, Ctx_.SeqMode) : nullptr;
            if (!WarnUnusedNodes()) {
                return nullptr;
            }
            Ctx_.ScopeLevel--;
            Ctx_.Scoped = saveScoped;

            if (!ret) {
                return {};
            }

            TNodePtr blockNode = new TAstListNodeImpl(Ctx_.Pos());
            blockNode->Add("block");
            blockNode->Add(blockNode->Q(ret));
            if (!makeLambda) {
                return blockNode;
            }

            TNodePtr params = new TAstListNodeImpl(Ctx_.Pos());
            params->Add("world");
            for (const auto& arg : args) {
                params->Add(new TAstAtomNodeImpl(Ctx_.Pos(), arg, TNodeFlags::ArbitraryContent));
            }

            return BuildLambda(Ctx_.Pos(), params, blockNode);
        }
        case TRule_do_stmt_TBlock2::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

bool TSqlTranslation::DefineActionOrSubqueryBody(TSqlQuery& query, TBlocks& blocks, const TRule_define_action_or_subquery_body& body) {
    if (body.HasBlock2()) {
        Ctx_.PushCurrentBlocks(&blocks);
        Y_DEFER {
            Ctx_.PopCurrentBlocks();
        };

        size_t statementNumber = 0;
        if (!query.Statement(blocks, body.GetBlock2().GetRule_sql_stmt_core1(), statementNumber++)) {
            return false;
        }

        for (const auto& nestedStmtItem : body.GetBlock2().GetBlock2()) {
            const auto& nestedStmt = nestedStmtItem.GetRule_sql_stmt_core2();
            if (!query.Statement(blocks, nestedStmt, statementNumber++)) {
                return false;
            }
        }
    }

    return true;
}

bool TSqlTranslation::DefineActionOrSubqueryStatement(const TRule_define_action_or_subquery_stmt& stmt, TSymbolNameWithPos& nameAndPos, TNodePtr& lambda) {
    auto kind = Ctx_.Token(stmt.GetToken2());
    const bool isSubquery = to_lower(kind) == "subquery";
    if (!isSubquery && Mode_ == NSQLTranslation::ESqlMode::SUBQUERY) {
        Error() << "Definition of actions is not allowed in the subquery";
        return false;
    }

    TString actionName;
    if (!NamedNodeImpl(stmt.GetRule_bind_parameter3(), actionName, *this)) {
        return false;
    }
    if (IsAnonymousName(actionName)) {
        Error() << "Can not use anonymous name '" << actionName << "' as " << to_upper(kind) << " name";
        return false;
    }
    TPosition actionNamePos = Ctx_.Pos();

    TVector<TSymbolNameWithPos> argNames;
    ui32 optionalArgumentsCount = 0;
    if (stmt.HasBlock5() && !ActionOrSubqueryArgs(stmt.GetBlock5().GetRule_action_or_subquery_args1(), argNames, optionalArgumentsCount)) {
        return false;
    }

    auto saveScoped = Ctx_.Scoped;
    Ctx_.Scoped = MakeIntrusive<TScopedState>();
    Ctx_.AllScopes.push_back(Ctx_.Scoped);
    *Ctx_.Scoped = *saveScoped;
    Ctx_.Scoped->Local = TScopedState::TLocal{};
    Ctx_.ScopeLevel++;

    for (auto& arg : argNames) {
        arg.Name = PushNamedAtom(arg.Pos, arg.Name);
    }

    auto saveMode = Ctx_.Settings.Mode;
    if (isSubquery) {
        Ctx_.Settings.Mode = NSQLTranslation::ESqlMode::SUBQUERY;
    }

    TSqlQuery query(Ctx_, Ctx_.Settings.Mode, false);
    TBlocks innerBlocks;
    const bool hasValidBody = DefineActionOrSubqueryBody(query, innerBlocks, stmt.GetRule_define_action_or_subquery_body8());

    if (isSubquery && !ValidateSubqueryOrViewBody(innerBlocks)) {
        return false;
    }

    auto ret = hasValidBody ? BuildQuery(Ctx_.Pos(), innerBlocks, false, Ctx_.Scoped, !isSubquery && Ctx_.SeqMode) : nullptr;
    if (!WarnUnusedNodes()) {
        return false;
    }
    Ctx_.Scoped = saveScoped;
    Ctx_.ScopeLevel--;
    Ctx_.Settings.Mode = saveMode;

    if (!ret) {
        return false;
    }

    TNodePtr blockNode = new TAstListNodeImpl(Ctx_.Pos());
    blockNode->Add("block");
    blockNode->Add(blockNode->Q(ret));

    TNodePtr params = new TAstListNodeImpl(Ctx_.Pos());
    params->Add("world");
    for (const auto& arg : argNames) {
        params->Add(BuildAtom(arg.Pos, arg.Name));
    }

    lambda = BuildLambda(Ctx_.Pos(), params, blockNode);
    if (optionalArgumentsCount > 0) {
        lambda = new TCallNodeImpl(Ctx_.Pos(), "WithOptionalArgs", {lambda,
                                                                    BuildQuotedAtom(Ctx_.Pos(), ToString(optionalArgumentsCount), TNodeFlags::Default)});
    }

    nameAndPos.Name = actionName;
    nameAndPos.Pos = actionNamePos;
    return true;
}

TNodePtr TSqlTranslation::IfStatement(const TRule_if_stmt& stmt) {
    bool isEvaluate = stmt.HasBlock1();

    if (!isEvaluate &&
        !Ctx_.EnsureBackwardCompatibleFeatureAvailable(
            GetPos(stmt.GetToken2()),
            "IF without EVALUATE",
            GetMaxLangVersion()))
    {
        return {};
    }

    TSqlExpression expr(Ctx_, Mode_);
    auto exprNode = Unwrap(expr.Build(stmt.GetRule_expr3()));
    if (!exprNode) {
        return {};
    }

    auto thenNode = DoStatement(stmt.GetRule_do_stmt4(), isEvaluate);
    if (!thenNode) {
        return {};
    }

    TNodePtr elseNode;
    if (stmt.HasBlock5()) {
        elseNode = DoStatement(stmt.GetBlock5().GetRule_do_stmt2(), isEvaluate);
        if (!elseNode) {
            return {};
        }
    }

    return BuildWorldIfNode(Ctx_.Pos(), exprNode, thenNode, elseNode, isEvaluate);
}

TNodePtr TSqlTranslation::ForStatement(const TRule_for_stmt& stmt) {
    bool isEvaluate = stmt.HasBlock1();
    bool isParallel = stmt.HasBlock2();

    if (isParallel &&
        !Ctx_.EnsureBackwardCompatibleFeatureAvailable(
            GetPos(stmt.GetBlock2().GetToken1()),
            "PARALLEL FOR",
            GetMaxLangVersion()))
    {
        return {};
    }

    if (!isEvaluate &&
        !Ctx_.EnsureBackwardCompatibleFeatureAvailable(
            GetPos(stmt.GetToken3()),
            "FOR without EVALUATE",
            GetMaxLangVersion()))
    {
        return {};
    }

    TSqlExpression expr(Ctx_, Mode_);
    TString itemArgName;
    if (!NamedNodeImpl(stmt.GetRule_bind_parameter4(), itemArgName, *this)) {
        return {};
    }
    TPosition itemArgNamePos = Ctx_.Pos();

    auto exprNode = Unwrap(expr.Build(stmt.GetRule_expr6()));
    if (!exprNode) {
        return {};
    }

    itemArgName = PushNamedAtom(itemArgNamePos, itemArgName);
    if (isParallel) {
        ++Ctx_.ParallelModeCount;
    }

    auto bodyNode = DoStatement(stmt.GetRule_do_stmt7(), true, {itemArgName});
    if (isParallel) {
        --Ctx_.ParallelModeCount;
    }

    if (!PopNamedNode(itemArgName)) {
        return {};
    }

    if (!bodyNode) {
        return {};
    }

    TNodePtr elseNode;
    if (stmt.HasBlock8()) {
        elseNode = DoStatement(stmt.GetBlock8().GetRule_do_stmt2(), true);
        if (!elseNode) {
            return {};
        }
    }

    return BuildWorldForNode(Ctx_.Pos(), exprNode, bodyNode, elseNode, isEvaluate, isParallel);
}

bool TSqlTranslation::BindParameterClause(const TRule_bind_parameter& node, TDeferredAtom& result) {
    TString paramName;
    if (!NamedNodeImpl(node, paramName, *this)) {
        return false;
    }
    auto named = GetNamedNode(paramName);
    if (!named) {
        return false;
    }

    result = MakeAtomFromExpression(Ctx_.Pos(), Ctx_, named);
    return true;
}

bool TSqlTranslation::ObjectFeatureValueClause(const TRule_object_feature_value& node, TDeferredAtom& result) {
    // object_feature_value: id_or_type | bind_parameter | STRING_VALUE | bool_value;
    switch (node.Alt_case()) {
        case TRule_object_feature_value::kAltObjectFeatureValue1: {
            TString name = Id(node.GetAlt_object_feature_value1().GetRule_id_or_type1(), *this);
            result = TDeferredAtom(Ctx_.Pos(), name);
            break;
        }
        case TRule_object_feature_value::kAltObjectFeatureValue2: {
            if (!BindParameterClause(node.GetAlt_object_feature_value2().GetRule_bind_parameter1(), result)) {
                return false;
            }
            break;
        }
        case TRule_object_feature_value::kAltObjectFeatureValue3: {
            auto strValue = StringContent(Ctx_, Ctx_.Pos(), Ctx_.Token(node.GetAlt_object_feature_value3().GetToken1()));
            if (!strValue) {
                Error() << "Cannot parse string correctly: " << Ctx_.Token(node.GetAlt_object_feature_value3().GetToken1());
                return false;
            }
            result = TDeferredAtom(Ctx_.Pos(), strValue->Content);
            break;
        }
        case TRule_object_feature_value::kAltObjectFeatureValue4: {
            TString value = Ctx_.Token(node.GetAlt_object_feature_value4().GetRule_bool_value1().GetToken1());
            result = TDeferredAtom(BuildLiteralBool(Ctx_.Pos(), FromString<bool>(value)), Ctx_);
            break;
        }
        case TRule_object_feature_value::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
    return true;
}

bool TSqlTranslation::AddObjectFeature(std::map<TString, TDeferredAtom>& result, const TRule_object_feature& feature) {
    if (feature.has_alt_object_feature1()) {
        auto& kv = feature.GetAlt_object_feature1().GetRule_object_feature_kv1();
        const TString& key = Id(kv.GetRule_an_id_or_type1(), *this);
        auto& ruleValue = kv.GetRule_object_feature_value3();
        TDeferredAtom value;
        if (!ObjectFeatureValueClause(ruleValue, value)) {
            return false;
        }
        result[key] = value;
    } else if (feature.has_alt_object_feature2()) {
        result[Id(feature.GetAlt_object_feature2().GetRule_object_feature_flag1().GetRule_an_id_or_type1(), *this)] = TDeferredAtom();
    }
    return true;
}

bool TSqlTranslation::ParseObjectFeatures(std::map<TString, TDeferredAtom>& result, const TRule_object_features& features) {
    if (features.has_alt_object_features1()) {
        if (!AddObjectFeature(result, features.alt_object_features1().GetRule_object_feature1())) {
            return false;
        }

    } else if (features.has_alt_object_features2()) {
        if (!AddObjectFeature(result, features.alt_object_features2().GetRule_object_feature2())) {
            return false;
        }
        for (auto&& i : features.alt_object_features2().GetBlock3()) {
            if (!AddObjectFeature(result, i.GetRule_object_feature2())) {
                return false;
            }
        }
    } else {
        return false;
    }
    return true;
}

bool TSqlTranslation::StoreDataSourceSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result) {
    YQL_ENSURE(value);

    const TString key = to_lower(id.Name);
    if (result.find(key) != result.end()) {
        Ctx_.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    if (!StoreString(*value, result[key], Ctx_, to_upper(key))) {
        return false;
    }

    return true;
}

bool TSqlTranslation::StoreDataSourceSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result) {
    const TIdentifier id = IdEx(entry.GetRule_an_id1(), *this);
    return StoreDataSourceSettingsEntry(id, &entry.GetRule_table_setting_value3(), result);
}

bool TSqlTranslation::ParseExternalDataSourceSettings(std::map<TString, TDeferredAtom>& result, const TRule_with_table_settings& settingsNode) {
    const auto& firstEntry = settingsNode.GetRule_table_settings_entry3();
    if (!StoreDataSourceSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), &firstEntry.GetRule_table_setting_value3(),
                                      result)) {
        return false;
    }
    for (auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_table_settings_entry2();
        if (!StoreDataSourceSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), &entry.GetRule_table_setting_value3(), result)) {
            return false;
        }
    }
    if (result.find("source_type") == result.end()) {
        Ctx_.Error() << "SOURCE_TYPE requires key";
        return false;
    }
    if (!ValidateExternalDataSourceAuthMethod(result, Ctx_)) {
        return false;
    }
    return true;
}

bool TSqlTranslation::ParseExternalDataSourceSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_external_data_source_action& alterAction) {
    switch (alterAction.Alt_case()) {
        case TRule_alter_external_data_source_action::kAltAlterExternalDataSourceAction1: {
            const auto& action = alterAction.GetAlt_alter_external_data_source_action1().GetRule_alter_table_set_table_setting_uncompat1();
            if (!StoreDataSourceSettingsEntry(IdEx(action.GetRule_an_id2(), *this), &action.GetRule_table_setting_value3(), result)) {
                return false;
            }
            return true;
        }
        case TRule_alter_external_data_source_action::kAltAlterExternalDataSourceAction2: {
            const auto& action = alterAction.GetAlt_alter_external_data_source_action2().GetRule_alter_table_set_table_setting_compat1();
            if (!StoreDataSourceSettingsEntry(action.GetRule_alter_table_setting_entry3(), result)) {
                return false;
            }
            for (const auto& entry : action.GetBlock4()) {
                if (!StoreDataSourceSettingsEntry(entry.GetRule_alter_table_setting_entry2(), result)) {
                    return false;
                }
            }
            return true;
        }
        case TRule_alter_external_data_source_action::kAltAlterExternalDataSourceAction3: {
            const auto& action = alterAction.GetAlt_alter_external_data_source_action3().GetRule_alter_table_reset_table_setting1();
            const TString key = to_lower(IdEx(action.GetRule_an_id3(), *this).Name);
            toReset.insert(key);
            for (const auto& keys : action.GetBlock4()) {
                const TString key = to_lower(IdEx(keys.GetRule_an_id2(), *this).Name);
                toReset.insert(key);
            }
            return true;
        }
        case TRule_alter_external_data_source_action::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    // no ValidateExternalDataSourceAuthMethod is called b/c its impossible to check format with previously saved settings
}

bool TSqlTranslation::StoreSecretInheritPermissions(
    const TRule_secret_setting_value& value,
    const TString& key,
    TSecretParameters& secretParams) {
    if (secretParams.InheritPermissions) {
        Error() << "Duplicate parameter: " << key;
        return false;
    }
    const NSQLv1Generated::TToken* errToken = nullptr;
    switch (value.Alt_case()) {
        // secret_setting_value: STRING_VALUE | bool_value | bind_parameter
        case TRule_secret_setting_value::kAltSecretSettingValue2: {
            if (auto inheritPermissions = ParseBool(Ctx_, value.GetAlt_secret_setting_value2().GetRule_bool_value1())) {
                secretParams.InheritPermissions = TDeferredAtom(Ctx_.Pos(), *inheritPermissions ? "1" : "0");
            } else {
                errToken = &value.GetAlt_secret_setting_value2().GetRule_bool_value1().GetToken1();
            }
            break;
        }
        case TRule_secret_setting_value::kAltSecretSettingValue1: {
            errToken = &value.GetAlt_secret_setting_value1().GetToken1();
            break;
        }
        case TRule_secret_setting_value::kAltSecretSettingValue3: {
            errToken = &value.GetAlt_secret_setting_value3().GetRule_bind_parameter1().GetToken1();
            break;
        }
        default: {
            return false;
        }
    }
    if (errToken) {
        Ctx_.Error(Ctx_.TokenPosition(*errToken)) << "Unsupported type for parameter: " << key << ". Bool was expected";
        return false;
    }
    return true;
}

bool TSqlTranslation::StoreSecretValue(
    const TRule_secret_setting_value& value,
    const TString& key,
    TSecretParameters& secretParams) {
    if (secretParams.Value) {
        Error() << "Duplicate parameter: " << key;
        return false;
    }
    const NSQLv1Generated::TToken* errToken = nullptr;
    switch (value.Alt_case()) {
        case TRule_secret_setting_value::kAltSecretSettingValue1: {
            const auto& token = value.GetAlt_secret_setting_value1().GetToken1();
            auto content = StringContent(Ctx_, Ctx_.Pos(), Ctx_.Token(token));
            if (!content) {
                errToken = &value.GetAlt_secret_setting_value1().GetToken1();
            } else {
                secretParams.Value = TDeferredAtom(Ctx_.Pos(), std::move(content->Content));
            }
            break;
        }
        case TRule_secret_setting_value::kAltSecretSettingValue3: {
            TDeferredAtom result;
            if (!BindParameterClause(value.GetAlt_secret_setting_value3().GetRule_bind_parameter1(), result)) {
                errToken = &value.GetAlt_secret_setting_value3().GetRule_bind_parameter1().GetToken1();
            } else {
                secretParams.Value = std::move(result);
            }
            break;
        }
        case TRule_secret_setting_value::kAltSecretSettingValue2: {
            errToken = &value.GetAlt_secret_setting_value2().GetRule_bool_value1().GetToken1();
            break;
        }
        default: {
            return false;
        }
    }
    if (errToken) {
        Ctx_.Error(Ctx_.TokenPosition(*errToken)) << "Unsupported type for parameter: " << key << ". String (or named expression with type String) was expected";
        return false;
    }
    return true;
}

bool TSqlTranslation::StoreSecretSettingEntry(const TIdentifier& id, const TRule_secret_setting_value& value, TSecretParameters& secretParams) {
    const TString key = to_upper(id.Name);
    if (key == "INHERIT_PERMISSIONS") {
        return StoreSecretInheritPermissions(value, key, secretParams);
    } else if (key == "VALUE") {
        return StoreSecretValue(value, key, secretParams);
    }

    Error() << "Unknown parameter: " << key;
    return false;
}

bool TSqlTranslation::ParseSecretSettings(
    const TPosition pos,
    const TRule_with_secret_settings& settingsNode,
    TSecretParameters& secretParams,
    const TSecretParameters::EOperationMode mode) {
    // with_secret_settings: WITH LPAREN secret_setting_entry (COMMA secret_setting_entry)* RPAREN;
    auto tryStoreEntry = [&](const auto& entry) -> bool {
        return StoreSecretSettingEntry(
            IdEx(entry.GetRule_an_id1(), *this),
            entry.GetRule_secret_setting_value3(),
            secretParams);
    };

    const auto& firstEntry = settingsNode.GetRule_secret_setting_entry3();
    if (!tryStoreEntry(firstEntry)) {
        return false;
    }

    for (auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_secret_setting_entry2();
        if (!tryStoreEntry(entry)) {
            return false;
        }
    }

    return secretParams.ValidateParameters(Ctx_, pos, mode);
}

bool TSqlTranslation::ParseSecretId(const TRule_id_or_at& node, TString& objectId) {
    const auto idOrAt = Id(node, *this);
    if (idOrAt.first) { // has @
        Error() << "'@' is not allowed prefix for secret name";
        return false;
    }
    objectId = idOrAt.second;
    if (objectId.empty()) {
        Error() << "Empty secret name";
        return false;
    }
    return true;
}

bool TSqlTranslation::ValidateExternalTable(const TCreateTableParameters& params) {
    if (params.TableType != ETableType::ExternalTable) {
        return true;
    }

    if (!params.TableSettings.DataSourcePath) {
        Ctx_.Error() << "DATA_SOURCE requires key";
        return false;
    }

    if (!params.TableSettings.Location) {
        Ctx_.Error() << "LOCATION requires key";
        return false;
    }

    if (params.PkColumns) {
        Ctx_.Error() << "PRIMARY KEY is not supported for external table";
        return false;
    }

    return true;
}

bool TSqlTranslation::ValidateSubqueryOrViewBody(const TBlocks& blocks) {
    ui32 topLevelSelects = 0;
    bool hasTailOps = false;
    for (auto& block : blocks) {
        if (block->SubqueryAlias()) {
            continue;
        }

        if (block->HasSelectResult()) {
            ++topLevelSelects;
        } else if (topLevelSelects) {
            hasTailOps = true;
        }
    }

    if (topLevelSelects != 1 || hasTailOps) {
        Error() << "Strictly one select/process/reduce statement is expected at the end of "
                << (Mode_ == NSQLTranslation::ESqlMode::LIMITED_VIEW ? "view" : "subquery");
        return false;
    }

    return true;
}

bool TSqlTranslation::ParseViewQuery(
    std::map<TString, TDeferredAtom>& features,
    const TRule_select_stmt& query) {
    TStringBuilder queryText;
    if (!BuildContextRecreationQuery(Ctx_, queryText)) {
        return false;
    }
    queryText << CollectTokens(query);
    features["query_text"] = {Ctx_.Pos(), queryText};

    // The AST is needed solely for the validation of the CREATE VIEW statement.
    // The final storage format for the query is a plain text, not an AST.
    const auto viewSelect = BuildViewSelect(query, Ctx_);
    if (!viewSelect) {
        return false;
    }
    features["query_ast"] = {viewSelect, Ctx_};

    return true;
}

bool TSqlTranslation::ParseViewQuery(
    std::map<TString, TDeferredAtom>& features,
    const TRule_define_action_or_subquery_body& body,
    const NSQLv1Generated::TToken& beforeToken,
    const NSQLv1Generated::TToken& afterToken,
    const TString& service,
    const TDeferredAtom& cluster)
{
    if (!body.HasBlock2()) {
        Error() << "Empty view body is not allowed";
        return false;
    }
    const auto saveScoped = Ctx_.Scoped;
    const auto saveMode = Ctx_.Settings.Mode;
    Ctx_.Scoped = Ctx_.CreateScopedState();
    Ctx_.AllScopes.push_back(Ctx_.Scoped);
    Ctx_.Scoped->CurrCluster = cluster;
    Ctx_.Scoped->CurrService = service;
    Ctx_.Settings.Mode = ESqlMode::LIMITED_VIEW;

    Y_DEFER {
        Ctx_.ScopeLevel--;
        Ctx_.Scoped = saveScoped;
        Ctx_.Settings.Mode = saveMode;
    };

    TSqlQuery query(Ctx_, Ctx_.Settings.Mode, /* topLevel */ false, /* allowTopLevelPragmas */ true);
    TBlocks innerBlocks;

    TNodePtr clearWorldNode = new TAstListNodeImpl(Ctx_.Pos());
    clearWorldNode->Add("World");
    innerBlocks.push_back(clearWorldNode);

    if (!DefineActionOrSubqueryBody(query, innerBlocks, body)) {
        return false;
    }

    if (!ValidateSubqueryOrViewBody(innerBlocks)) {
        return false;
    }

    auto queryNode = BuildQuery(Ctx_.Pos(), innerBlocks, false, Ctx_.Scoped, Ctx_.SeqMode);
    if (!queryNode) {
        return false;
    }

    if (!WarnUnusedNodes()) {
        return false;
    }

    TNodePtr blockNode = new TAstListNodeImpl(Ctx_.Pos());
    blockNode->Add("block", blockNode->Q(queryNode));
    features[TStreamingQuerySettings::QUERY_AST_FEATURE] = TDeferredAtom(blockNode, Ctx_);

    auto begin = GetQueryPosition(Ctx_.Query, beforeToken);
    auto end = GetQueryPosition(Ctx_.Query, afterToken);
    YQL_ENSURE(begin < Ctx_.Query.size() && end < Ctx_.Query.size());
    begin += beforeToken.value().size();
    YQL_ENSURE(begin < end);
    features[TStreamingQuerySettings::QUERY_TEXT_FEATURE] = TDeferredAtom(Ctx_.Pos(), Ctx_.Query.substr(begin, end - begin));

    return true;
}

namespace {

static TString GetLambdaText(TTranslation& ctx, TContext& Ctx, const TRule_lambda_or_parameter& lambdaOrParameter) {
    static const TString StatementSeparator = ";\n";

    TVector<TString> statements;
    NYql::TIssues issues;
    if (!SplitQueryToStatements(Ctx.Lexers, Ctx.Parsers, Ctx.Query, statements, issues, Ctx.Settings)) {
        return {};
    }

    TStringBuilder result;
    for (const auto id : Ctx.ForAllStatementsParts) {
        result << statements[id] << "\n";
    }

    switch (lambdaOrParameter.Alt_case()) {
        case NSQLv1Generated::TRule_lambda_or_parameter::kAltLambdaOrParameter1: {
            const auto& lambda = lambdaOrParameter.GetAlt_lambda_or_parameter1().GetRule_lambda1();

            auto& beginToken = lambda.GetRule_smart_parenthesis1().GetToken1();
            const NSQLv1Generated::TToken* endToken = nullptr;
            switch (lambda.GetBlock2().GetBlock2().GetAltCase()) {
                case TRule_lambda_TBlock2_TBlock2::AltCase::kAlt1:
                    endToken = &lambda.GetBlock2().GetBlock2().GetAlt1().GetToken3();
                    break;
                case TRule_lambda_TBlock2_TBlock2::AltCase::kAlt2:
                    endToken = &lambda.GetBlock2().GetBlock2().GetAlt2().GetToken3();
                    break;
                case TRule_lambda_TBlock2_TBlock2::AltCase::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }

            auto begin = GetQueryPosition(Ctx.Query, beginToken);
            auto end = GetQueryPosition(Ctx.Query, *endToken);
            if (begin == std::string::npos || end == std::string::npos) {
                return {};
            }

            result << "$__ydb_transfer_lambda = " << Ctx.Query.substr(begin, end - begin + endToken->value().size()) << StatementSeparator;

            return result;
        }
        case NSQLv1Generated::TRule_lambda_or_parameter::kAltLambdaOrParameter2: {
            const auto& valueBlock = lambdaOrParameter.GetAlt_lambda_or_parameter2().GetRule_bind_parameter1().GetBlock2();
            const auto id = Id(valueBlock.GetAlt1().GetRule_an_id_or_type1(), ctx);
            result << "$__ydb_transfer_lambda = $" << id << StatementSeparator;
            return result;
        }
        case NSQLv1Generated::TRule_lambda_or_parameter::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

} // anonymous namespace

std::string::size_type GetQueryPosition(const TString& query, const NSQLv1Generated::TToken& token) {
    if (1 == token.GetLine() && 0 == token.GetColumn()) {
        return 0;
    }

    TPosition pos = {0, 1};
    TTextWalker walker(pos, /*utf8Aware=*/true);

    std::string::size_type position = 0;
    for (char c : query) {
        walker.Advance(c);
        ++position;

        if (pos.Row == token.GetLine() && pos.Column == token.GetColumn()) {
            return position;
        }
    }

    return std::string::npos;
}

bool TSqlTranslation::ParseTransferLambda(
    TString& lambdaText,
    const TRule_lambda_or_parameter& lambdaOrParameter) {
    TSqlExpression expr(Ctx_, Ctx_.Settings.Mode);
    auto result = expr.Build(lambdaOrParameter);
    if (!result) {
        return false;
    }

    lambdaText = GetLambdaText(*this, Ctx_, lambdaOrParameter);
    if (lambdaText.empty()) {
        Ctx_.Error() << "Cannot parse lambda correctly";
    }

    return !lambdaText.empty();
}

class TReturningListColumns: public INode {
public:
    explicit TReturningListColumns(TPosition pos)
        : INode(pos)
    {
    }

    void SetStar() {
        ColumnNames_.clear();
        Star_ = true;
    }

    void AddColumn(const NSQLv1Generated::TRule_an_id& rule, TTranslation& ctx) {
        ColumnNames_.push_back(NSQLTranslationV1::Id(rule, ctx));
    }

    bool DoInit(TContext& ctx, ISource* source) override {
        Node_ = Y();
        if (Star_) {
            Node_->Add(Y("ReturningStar"));
        } else {
            for (auto&& column : ColumnNames_) {
                Node_->Add(Y("ReturningListItem", Q(column)));
            }
        }
        Node_ = Q(Y(Q("returning"), Q(Node_)));
        return Node_->Init(ctx, source);
    }

    TNodePtr DoClone() const override {
        return new TReturningListColumns(GetPos());
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node_->Translate(ctx);
    }

private:
    TNodePtr Node_;
    TVector<TString> ColumnNames_;
    bool Star_ = false;
};

TNodePtr TSqlTranslation::ReturningList(const ::NSQLv1Generated::TRule_returning_columns_list& columns) {
    auto result = MakeHolder<TReturningListColumns>(Ctx_.Pos());

    if (columns.GetBlock2().Alt_case() == TRule_returning_columns_list_TBlock2::AltCase::kAlt1) {
        result->SetStar();
    } else if (columns.GetBlock2().Alt_case() == TRule_returning_columns_list_TBlock2::AltCase::kAlt2) {
        result->AddColumn(columns.GetBlock2().alt2().GetRule_an_id1(), *this);
        for (auto& block : columns.GetBlock2().alt2().GetBlock2()) {
            result->AddColumn(block.GetRule_an_id2(), *this);
        }
    }

    return result.Release();
}

bool TSqlTranslation::StoreResourcePoolSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result) {
    YQL_ENSURE(value);

    const TString key = to_lower(id.Name);
    if (result.find(key) != result.end()) {
        Ctx_.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    switch (value->Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2:
            return StoreString(*value, result[key], Ctx_, to_upper(key));

        case TRule_table_setting_value::kAltTableSettingValue3:
            return StoreInt(*value, result[key], Ctx_, to_upper(key));

        default:
            Ctx_.Error() << to_upper(key) << " value should be a string literal or integer";
            return false;
    }

    return true;
}

bool TSqlTranslation::StoreResourcePoolSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result) {
    const TIdentifier id = IdEx(entry.GetRule_an_id1(), *this);
    return StoreResourcePoolSettingsEntry(id, &entry.GetRule_table_setting_value3(), result);
}

bool TSqlTranslation::ParseResourcePoolSettings(std::map<TString, TDeferredAtom>& result, const TRule_with_table_settings& settingsNode) {
    const auto& firstEntry = settingsNode.GetRule_table_settings_entry3();
    if (!StoreResourcePoolSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), &firstEntry.GetRule_table_setting_value3(), result)) {
        return false;
    }
    for (const auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_table_settings_entry2();
        if (!StoreResourcePoolSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), &entry.GetRule_table_setting_value3(), result)) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::ParseResourcePoolSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_resource_pool_action& alterAction) {
    switch (alterAction.Alt_case()) {
        case TRule_alter_resource_pool_action::kAltAlterResourcePoolAction1: {
            const auto& action = alterAction.GetAlt_alter_resource_pool_action1().GetRule_alter_table_set_table_setting_compat1();
            if (!StoreResourcePoolSettingsEntry(action.GetRule_alter_table_setting_entry3(), result)) {
                return false;
            }
            for (const auto& entry : action.GetBlock4()) {
                if (!StoreResourcePoolSettingsEntry(entry.GetRule_alter_table_setting_entry2(), result)) {
                    return false;
                }
            }
            return true;
        }
        case TRule_alter_resource_pool_action::kAltAlterResourcePoolAction2: {
            const auto& action = alterAction.GetAlt_alter_resource_pool_action2().GetRule_alter_table_reset_table_setting1();
            const TString firstKey = to_lower(IdEx(action.GetRule_an_id3(), *this).Name);
            toReset.insert(firstKey);
            for (const auto& key : action.GetBlock4()) {
                toReset.insert(to_lower(IdEx(key.GetRule_an_id2(), *this).Name));
            }
            return true;
        }
        case TRule_alter_resource_pool_action::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

bool TSqlTranslation::StoreResourcePoolClassifierSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result) {
    YQL_ENSURE(value);

    const TString key = to_lower(id.Name);
    if (result.find(key) != result.end()) {
        Ctx_.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    switch (value->Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2:
            return StoreString(*value, result[key], Ctx_, to_upper(key));

        case TRule_table_setting_value::kAltTableSettingValue3:
            return StoreInt(*value, result[key], Ctx_, to_upper(key));

        default:
            Ctx_.Error() << to_upper(key) << " value should be a string literal or integer";
            return false;
    }

    return true;
}

bool TSqlTranslation::StoreResourcePoolClassifierSettingsEntry(const TRule_alter_table_setting_entry& entry, std::map<TString, TDeferredAtom>& result) {
    const TIdentifier id = IdEx(entry.GetRule_an_id1(), *this);
    return StoreResourcePoolClassifierSettingsEntry(id, &entry.GetRule_table_setting_value3(), result);
}

bool TSqlTranslation::ParseResourcePoolClassifierSettings(std::map<TString, TDeferredAtom>& result, const TRule_with_table_settings& settingsNode) {
    const auto& firstEntry = settingsNode.GetRule_table_settings_entry3();
    if (!StoreResourcePoolClassifierSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), &firstEntry.GetRule_table_setting_value3(), result)) {
        return false;
    }
    for (const auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_table_settings_entry2();
        if (!StoreResourcePoolClassifierSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), &entry.GetRule_table_setting_value3(), result)) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::ParseResourcePoolClassifierSettings(std::map<TString, TDeferredAtom>& result, std::set<TString>& toReset, const TRule_alter_resource_pool_classifier_action& alterAction) {
    switch (alterAction.Alt_case()) {
        case TRule_alter_resource_pool_classifier_action::kAltAlterResourcePoolClassifierAction1: {
            const auto& action = alterAction.GetAlt_alter_resource_pool_classifier_action1().GetRule_alter_table_set_table_setting_compat1();
            if (!StoreResourcePoolClassifierSettingsEntry(action.GetRule_alter_table_setting_entry3(), result)) {
                return false;
            }
            for (const auto& entry : action.GetBlock4()) {
                if (!StoreResourcePoolClassifierSettingsEntry(entry.GetRule_alter_table_setting_entry2(), result)) {
                    return false;
                }
            }
            return true;
        }
        case TRule_alter_resource_pool_classifier_action::kAltAlterResourcePoolClassifierAction2: {
            const auto& action = alterAction.GetAlt_alter_resource_pool_classifier_action2().GetRule_alter_table_reset_table_setting1();
            const TString firstKey = to_lower(IdEx(action.GetRule_an_id3(), *this).Name);
            toReset.insert(firstKey);
            for (const auto& key : action.GetBlock4()) {
                toReset.insert(to_lower(IdEx(key.GetRule_an_id2(), *this).Name));
            }
            return true;
        }
        case TRule_alter_resource_pool_classifier_action::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

TMaybe<TString> TSqlTranslation::ParseObjectPath(const TRule_object_ref& node, TObjectOperatorContext& context) {
    // object_ref: (cluster_expr .)? id_or_at

    if (node.HasBlock1()) {
        if (!ClusterExpr(node.GetBlock1().GetRule_cluster_expr1(), false, context.ServiceId, context.Cluster)) {
            return Nothing();
        }
    }

    const auto& [hasAt, objectId] = Id(node.GetRule_id_or_at2(), *this);
    if (hasAt) {
        Error() << "'@' is not allowed prefix for object name";
        return Nothing();
    }

    return BuildTablePath(Ctx_.GetPrefixPath(context.ServiceId, context.Cluster), objectId);
}

bool TSqlTranslation::ParseStreamingQuerySetting(const TRule_streaming_query_setting& node, TStreamingQuerySettings& settings) {
    // streaming_query_setting: an_id_or_type = (id_or_type | STRING_VALUE | bool_value | streaming_query_settings)

    const auto& id = to_lower(Id(node.GetRule_an_id_or_type1(), *this));
    if (id.StartsWith(TStreamingQuerySettings::RESERVED_FEATURE_PREFIX)) {
        Error() << "Streaming query parameter name should not start with prefix '" << TStreamingQuerySettings::RESERVED_FEATURE_PREFIX << "': " << to_upper(id);
        return false;
    }

    YQL_ENSURE(settings.Features);
    const auto [feature, inserted] = settings.Features->AddFeature(id, Ctx_.Pos());
    if (!inserted) {
        Error() << "Found duplicated parameter: " << to_upper(id);
        return false;
    }

    const auto& valueNode = node.GetRule_streaming_query_setting_value3();
    switch (valueNode.GetAltCase()) {
        case TRule_streaming_query_setting_value::kAltStreamingQuerySettingValue1: {
            const auto& value = Id(valueNode.GetAlt_streaming_query_setting_value1().GetRule_id_or_type1(), *this);
            feature = BuildQuotedAtom(Ctx_.Pos(), value);
            break;
        }
        case TRule_streaming_query_setting_value::kAltStreamingQuerySettingValue2: {
            const auto& strToken = Ctx_.Token(valueNode.GetAlt_streaming_query_setting_value2().GetToken1());
            const auto& strValue = StringContent(Ctx_, Ctx_.Pos(), strToken);
            if (!strValue) {
                return false;
            }

            feature = BuildQuotedAtom(Ctx_.Pos(), strValue->Content);
            break;
        }
        case TRule_streaming_query_setting_value::kAltStreamingQuerySettingValue3: {
            const auto& alt = valueNode.GetAlt_streaming_query_setting_value3();
            const auto& token = Ctx_.Token(alt.GetRule_bool_value1().GetToken1());
            feature = BuildLiteralBool(Ctx_.Pos(), FromString<bool>(token));
            break;
        }
        case TRule_streaming_query_setting_value::kAltStreamingQuerySettingValue4: {
            TStreamingQuerySettings settings;
            if (!ParseStreamingQuerySettings(valueNode.GetAlt_streaming_query_setting_value4().GetRule_streaming_query_settings1(), settings)) {
                return false;
            }

            feature = settings.Features;
            break;
        }
        case TRule_streaming_query_setting_value::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    return true;
}

bool TSqlTranslation::ParseStreamingQuerySettings(const TRule_streaming_query_settings& node, TStreamingQuerySettings& settings) {
    // streaming_query_settings: (
    //     streaming_query_setting
    //     (, streaming_query_setting)* ,?
    // )

    Ctx_.Token(node.GetToken1());

    if (!settings.Features) {
        settings.Features = new TObjectFeatureNode(Ctx_.Pos());
    }

    if (!ParseStreamingQuerySetting(node.GetRule_streaming_query_setting2(), settings)) {
        return false;
    }

    for (const auto& setting : node.GetBlock3()) {
        if (!ParseStreamingQuerySetting(setting.GetRule_streaming_query_setting2(), settings)) {
            return false;
        }
    }

    return true;
}

bool TSqlTranslation::ParseStreamingQueryDefinition(const TRule_streaming_query_definition& node, TStreamingQuerySettings& settings) {
    // streaming_query_definition: AS DO (BEGIN define_action_or_subquery_body END DO);

    const auto& inlineAction = node.GetRule_inline_action3();
    const auto& queryBegin = inlineAction.GetToken1();
    Ctx_.Token(queryBegin);

    if (!settings.Features) {
        settings.Features = new TObjectFeatureNode(Ctx_.Pos());
    }

    // Save query ast to perform type check and validation of allowed expressions

    const auto saveScoped = Ctx_.Scoped;
    Ctx_.Scoped = Ctx_.CreateScopedState(); // Reset scoped context to interrupt inheritance of global settings and named nodes
    Ctx_.AllScopes.push_back(Ctx_.Scoped);
    Ctx_.Scoped->Local = TScopedState::TLocal{};
    Ctx_.ScopeLevel++;
    TSqlQuery query(Ctx_, Ctx_.Settings.Mode, /* topLevel */ false, /* allowTopLevelPragmas */ true);
    TBlocks innerBlocks;

    TNodePtr clearWorldNode = new TAstListNodeImpl(Ctx_.Pos());
    clearWorldNode->Add("World");
    innerBlocks.push_back(clearWorldNode);

    const bool hasValidBody = DefineActionOrSubqueryBody(query, innerBlocks, inlineAction.GetRule_define_action_or_subquery_body2());
    auto queryNode = hasValidBody ? BuildQuery(Ctx_.Pos(), innerBlocks, false, Ctx_.Scoped, Ctx_.SeqMode) : nullptr;
    if (!WarnUnusedNodes()) {
        return false;
    }
    Ctx_.ScopeLevel--;
    Ctx_.Scoped = saveScoped;

    if (!queryNode) {
        return false;
    }

    TNodePtr blockNode = new TAstListNodeImpl(Ctx_.Pos());
    blockNode->Add("block", blockNode->Q(queryNode));
    settings.Features->AddFeature(TStreamingQuerySettings::QUERY_AST_FEATURE, Ctx_.Pos()).first = blockNode;

    // Extract whole query text between BEGIN and END tokens

    const auto& queryEnd = inlineAction.GetToken3();
    Y_DEBUG_ABORT_UNLESS(IS_TOKEN(queryBegin.GetId(), BEGIN));
    Y_DEBUG_ABORT_UNLESS(IS_TOKEN(queryEnd.GetId(), END));

    auto beginPos = GetQueryPosition(Ctx_.Query, queryBegin);
    const auto endPos = GetQueryPosition(Ctx_.Query, queryEnd);
    if (beginPos == std::string::npos || endPos == std::string::npos) {
        Error() << "Failed to parse streaming query definition";
        return false;
    }

    beginPos += queryBegin.value().size();
    settings.Features->AddFeature(TStreamingQuerySettings::QUERY_TEXT_FEATURE, Ctx_.Pos()).first = BuildQuotedAtom(Ctx_.Pos(), Ctx_.Query.substr(beginPos, endPos - beginPos));

    return true;
}

bool TSqlTranslation::ParseAlterStreamingQueryAction(const TRule_alter_streaming_query_action& node, TStreamingQuerySettings& settings) {
    // alter_streaming_query_action:
    //     (SET streaming_query_settings)
    //   | (SET streaming_query_settings)? streaming_query_definition

    switch (node.GetAltCase()) {
        case TRule_alter_streaming_query_action::kAltAlterStreamingQueryAction1: {
            const auto& alterSettingsNode = node.GetAlt_alter_streaming_query_action1().GetRule_alter_streaming_query_set_settings1();
            Ctx_.Token(alterSettingsNode.GetToken1());

            if (!ParseStreamingQuerySettings(alterSettingsNode.GetRule_streaming_query_settings2(), settings)) {
                return false;
            }

            break;
        }
        case TRule_alter_streaming_query_action::kAltAlterStreamingQueryAction2: {
            const auto& action = node.GetAlt_alter_streaming_query_action2();

            if (action.HasBlock1()) {
                const auto& alterSettingsNode = action.GetBlock1().GetRule_alter_streaming_query_set_settings1();
                Ctx_.Token(alterSettingsNode.GetToken1());

                if (!ParseStreamingQuerySettings(alterSettingsNode.GetRule_streaming_query_settings2(), settings)) {
                    return false;
                }
            }

            if (!ParseStreamingQueryDefinition(action.GetRule_streaming_query_definition2(), settings)) {
                return false;
            }

            break;
        }
        case TRule_alter_streaming_query_action::ALT_NOT_SET:
            Y_UNREACHABLE();
    }

    return true;
}

TNodePtr TSqlTranslation::YqlSelectOrLegacy(
    std::function<TNodeResult()> yqlSelect,
    std::function<TNodePtr()> legacy)
{
    const EYqlSelectMode mode = Ctx_.GetYqlSelectMode();
    if (mode == EYqlSelectMode::Disable) {
        return legacy();
    }

    if (!Ctx_.EnsureBackwardCompatibleFeatureAvailable(
            Ctx_.Pos(), "YqlSelect", YqlSelectLangVersion()))
    {
        return nullptr;
    }

    TNodeResult result = yqlSelect();
    if (result) {
        return std::move(*result);
    }

    switch (result.error()) {
        case ESQLError::Basic: {
            return nullptr;
        }
        case ESQLError::UnsupportedYqlSelect: {
            if (mode == EYqlSelectMode::Force) {
                Error() << "Translation of the statement "
                        << "to YqlSelect was forced, but unsupported";
                return nullptr;
            }

            YQL_ENSURE(mode == EYqlSelectMode::Auto);
            return legacy();
        }
    }
}

} // namespace NSQLTranslationV1
