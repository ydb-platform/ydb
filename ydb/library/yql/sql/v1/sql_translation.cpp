#include "sql_translation.h"
#include "sql_expression.h"
#include "sql_call_expr.h"
#include "sql_query.h"
#include "sql_values.h"
#include "sql_select.h"
#include "source.h"

#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <ydb/library/yql/sql/settings/partitioning.h>

#include <util/generic/scope.h>
#include <util/string/join.h>

#include <library/cpp/protobuf/util/simple_reflection.h>

namespace {

using namespace NSQLTranslationV1;

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
            if (!Tokens.Empty()) {
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

NSQLTranslation::TTranslationSettings CreateViewTranslationSettings(const NSQLTranslation::TTranslationSettings& base) {
    NSQLTranslation::TTranslationSettings settings;

    settings.ClusterMapping = base.ClusterMapping;
    settings.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;

    return settings;
}

TNodePtr BuildViewSelect(const TRule_select_stmt& query, TContext& ctx) {
    const auto viewTranslationSettings = CreateViewTranslationSettings(ctx.Settings);
    TContext viewParsingContext(viewTranslationSettings, {}, ctx.Issues);
    TSqlSelect select(viewParsingContext, viewTranslationSettings.Mode);
    TPosition pos;
    auto source = select.Build(query, pos);
    if (!source) {
        return nullptr;
    }
    return BuildSelectResult(
        pos,
        std::move(source),
        false,
        false,
        viewParsingContext.Scoped
    );
}

}

namespace NSQLTranslationV1 {

using NALPDefault::SQLv1LexerTokens;

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
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TString Id(const TRule_id_or_type& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_or_type::kAltIdOrType1:
            return Id(node.GetAlt_id_or_type1().GetRule_id1(), ctx);
        case TRule_id_or_type::kAltIdOrType2:
            return ctx.Identifier(node.GetAlt_id_or_type2().GetRule_type_id1().GetToken1());
        case TRule_id_or_type::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TString Id(const TRule_id_as_compat& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_as_compat::kAltIdAsCompat1:
            return Id(node.GetAlt_id_as_compat1().GetRule_identifier1(), ctx);
        case TRule_id_as_compat::kAltIdAsCompat2:
            return ctx.Token(node.GetAlt_id_as_compat2().GetRule_keyword_as_compat1().GetToken1());
        case TRule_id_as_compat::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TString Id(const TRule_an_id_as_compat& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_an_id_as_compat::kAltAnIdAsCompat1:
            return Id(node.GetAlt_an_id_as_compat1().GetRule_id_as_compat1(), ctx);
        case TRule_an_id_as_compat::kAltAnIdAsCompat2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_as_compat2().GetToken1()));
        case TRule_an_id_as_compat::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TString Id(const TRule_id_schema& node, TTranslation& ctx) {
    //id_schema:
    //    identifier
    //  | keyword_compat
    //  | keyword_expr_uncompat
    // //  | keyword_table_uncompat
    //  | keyword_select_uncompat
    // //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
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
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

std::pair<bool, TString> Id(const TRule_id_or_at& node, TTranslation& ctx) {
    bool hasAt = node.HasBlock1();
    return std::make_pair(hasAt, Id(node.GetRule_an_id_or_type2(), ctx) );
}

TString Id(const TRule_id_table& node, TTranslation& ctx) {
    //id_table:
    //    identifier
    //  | keyword_compat
    //  | keyword_expr_uncompat
    // //  | keyword_table_uncompat
    //  | keyword_select_uncompat
    // //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
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
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TString Id(const TRule_id_table_or_type& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_table_or_type::kAltIdTableOrType1:
            return Id(node.GetAlt_id_table_or_type1().GetRule_an_id_table1(), ctx);
        case TRule_id_table_or_type::kAltIdTableOrType2:
            return ctx.Identifier(node.GetAlt_id_table_or_type2().GetRule_type_id1().GetToken1());
        case TRule_id_table_or_type::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TString Id(const TRule_id_expr& node, TTranslation& ctx) {
    //id_expr:
    //    identifier
    //  | keyword_compat
    // //  | keyword_expr_uncompat
    // //  | keyword_table_uncompat
    // //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
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
            Y_ABORT("You should change implementation according to grammar changes");
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
    //id_expr_in:
    //    identifier
    //  | keyword_compat
    // //  | keyword_expr_uncompat
    // //  | keyword_table_uncompat
    // //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    // //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
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
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TString Id(const TRule_id_window& node, TTranslation& ctx) {
    //id_window:
    //    identifier
    //  | keyword_compat
    //  | keyword_expr_uncompat
    //  | keyword_table_uncompat
    //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    // //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
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
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TString Id(const TRule_id_without& node, TTranslation& ctx) {
    //id_without:
    //    identifier
    //  | keyword_compat
    // //  | keyword_expr_uncompat
    //  | keyword_table_uncompat
    // //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
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
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TString Id(const TRule_id_hint& node, TTranslation& ctx) {
    //id_hint:
    //    identifier
    //  | keyword_compat
    //  | keyword_expr_uncompat
    //  | keyword_table_uncompat
    //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    // //  | keyword_hint_uncompat
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
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TViewDescription Id(const TRule_view_name& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_view_name::kAltViewName1:
            return {Id(node.GetAlt_view_name1().GetRule_an_id1(), ctx)};
        case TRule_view_name::kAltViewName2:
            return {"", true};
        case TRule_view_name::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
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
            Y_ABORT("You should change implementation according to grammar changes");
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
    for (auto& block: node.GetBlock3()) {
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
        Y_ABORT("You should change implementation according to grammar changes");
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
    switch (indexType.Alt_case()) {
        // "GLOBAL"
        case TRule_table_index_type_TBlock1::kAlt1: {
            auto globalIndex = indexType.GetAlt1().GetRule_global_index1();
            bool uniqIndex = false;
            if (globalIndex.HasBlock2()) {
                uniqIndex = true;
            }
            if (globalIndex.HasBlock3()) {
                const TString token = to_lower(Ctx.Token(globalIndex.GetBlock3().GetToken1()));
                if (token == "sync") {
                    if (uniqIndex) {
                        indexes.back().Type = TIndexDescription::EType::GlobalSyncUnique;
                    } else {
                        indexes.back().Type = TIndexDescription::EType::GlobalSync;
                    }
                } else if (token == "async") {
                    if (uniqIndex) {
                        AltNotImplemented("unique", indexType);
                        return false;
                    }
                    indexes.back().Type = TIndexDescription::EType::GlobalAsync;
                } else {
                    Y_ABORT("You should change implementation according to grammar changes");
                }
            }
        }
        break;
        // "LOCAL"
        case TRule_table_index_type_TBlock1::kAlt2:
            AltNotImplemented("local", indexType);
            return false;
        case TRule_table_index_type_TBlock1::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }

    if (node.GetRule_table_index_type3().HasBlock2()) {
        const TString subType = to_upper(IdEx(node.GetRule_table_index_type3().GetBlock2().GetRule_index_subtype2().GetRule_an_id1(), *this).Name) ;
        if (subType == "VECTOR_KMEANS_TREE") {
            if (indexes.back().Type != TIndexDescription::EType::GlobalSync) {
                Ctx.Error() << subType << " index can only be GLOBAL [SYNC]";
                return false;
            }

            indexes.back().Type = TIndexDescription::EType::GlobalVectorKmeansTree;
        } else {
            Ctx.Error() << subType << " index subtype is not supported";
            return false;
        }
    }

    // WITH
    if (node.HasBlock10()) {
        //const auto& with = node.GetBlock4();
        auto& index = indexes.back();
        if (index.Type == TIndexDescription::EType::GlobalVectorKmeansTree) {
            index.IndexSettings = TVectorIndexSettings();
            if (!CreateIndexSettings(node.GetBlock10().GetRule_with_index_settings1(), index.Type, index.IndexSettings)) {
                return false;
            }
            const auto &vectorSettings = std::get<TVectorIndexSettings>(index.IndexSettings);
            if (!vectorSettings.Validate(Ctx)) {
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
        const auto& block = node.GetBlock9();
        indexes.back().DataColumns.emplace_back(IdEx(block.GetRule_an_id_schema3(), *this));
        for (const auto& inner : block.GetBlock4()) {
            indexes.back().DataColumns.emplace_back(IdEx(inner.GetRule_an_id_schema2(), *this));
        }
    }

    return true;
}

bool TSqlTranslation::CreateIndexSettings(const TRule_with_index_settings& settingsNode,
        TIndexDescription::EType indexType,
        TIndexDescription::TIndexSettings& indexSettings) {
    const auto& firstEntry = settingsNode.GetRule_index_setting_entry3();
    if (!CreateIndexSettingEntry(IdEx(firstEntry.GetRule_an_id1(), *this), firstEntry.GetRule_index_setting_value3(), indexType, indexSettings)) {
        return false;
    }
    for (auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_index_setting_entry2();
        if (!CreateIndexSettingEntry(IdEx(entry.GetRule_an_id1(), *this), entry.GetRule_index_setting_value3(), indexType, indexSettings)) {
            return false;
        }
    }
    return true;
}

template<typename T>
std::tuple<bool, T, TString> TSqlTranslation::GetIndexSettingValue(const TRule_index_setting_value& node) {
    T value;
    // id_or_type
    if (node.HasAlt_index_setting_value1()) {
        const TString stringValue = to_lower(IdEx(node.GetAlt_index_setting_value1().GetRule_id_or_type1(), *this).Name);
        if (!TryFromString<T>(stringValue, value)) {
            return {false, value, stringValue};
        }
        return {true, value, stringValue};
    }
    // STRING_VALUE
    else if (node.HasAlt_index_setting_value2()) {
        const TString stringValue = to_lower(Token(node.GetAlt_index_setting_value2().GetToken1()));
        const auto unescaped = StringContent(Ctx, Ctx.Pos(), stringValue);
        if (!unescaped) {
            return {false, value, stringValue};
        }
        if (!TryFromString<T>(unescaped->Content, value)) {
            return {false, value, stringValue};
        }
        return {true, value, unescaped->Content};
    } else {
        Y_ABORT("You should change implementation according to grammar changes");
    }
}

template<>
std::tuple<bool, ui64, TString> TSqlTranslation::GetIndexSettingValue(const TRule_index_setting_value& node) {
    const auto& intNode = node.GetAlt_index_setting_value3().GetRule_integer1();
    const TString stringValue = Token(intNode.GetToken1());
    ui64 value;
    TString suffix;
    if (!ParseNumbers(Ctx, stringValue, value, suffix)) {
        return {false, value, stringValue};
    }
    return {true, value, stringValue};
}

template<>
std::tuple<bool, bool, TString> TSqlTranslation::GetIndexSettingValue(const TRule_index_setting_value& node) {
    bool value;
    const TString stringValue = to_lower(Token(node.GetAlt_index_setting_value4().GetRule_bool_value1().GetToken1()));;
    if (!TryFromString<bool>(stringValue, value)) {
        return {false, value, stringValue};
    }
    return {true, value, stringValue};
}

bool TSqlTranslation::CreateIndexSettingEntry(const TIdentifier &id, 
        const TRule_index_setting_value& node, 
        TIndexDescription::EType indexType,
        TIndexDescription::TIndexSettings& indexSettings) {


    if (indexType == TIndexDescription::EType::GlobalVectorKmeansTree) {
        TVectorIndexSettings &vectorIndexSettings = std::get<TVectorIndexSettings>(indexSettings);

        if (to_lower(id.Name) == "distance") {
            const auto [success, value, stringValue] = GetIndexSettingValue<TVectorIndexSettings::EDistance>(node);
            if (!success) {
                Ctx.Error() << "Invalid distance: " << stringValue;
                return false;
            }
            vectorIndexSettings.Metric = value;
        } else if (to_lower(id.Name) == "similarity") {
            const auto [success, value, stringValue] = GetIndexSettingValue<TVectorIndexSettings::ESimilarity>(node);
            if (!success) {
                Ctx.Error() << "Invalid similarity: " << stringValue;
                return false;
            }
            vectorIndexSettings.Metric = value;
        } else if (to_lower(id.Name) == "vector_type") {
            const auto [success, value, stringValue] = GetIndexSettingValue<TVectorIndexSettings::EVectorType>(node);
            if (!success) {
                Ctx.Error() << "Invalid vector_type: " << stringValue;
                return false;
            }
            vectorIndexSettings.VectorType = value;
        } else if (to_lower(id.Name) == "vector_dimension") {
            const auto [success, value, stringValue] = GetIndexSettingValue<ui64>(node);
            if (!success || value > Max<ui32>()) {
                Ctx.Error() << "Invalid vector_dimension: " << stringValue;
                return false;
            }
            vectorIndexSettings.VectorDimension = value;
        } else {
            Ctx.Error() << "Unknown index setting: " << id.Name;
            return false;
        }
    } else {
        Ctx.Error() << "Unknown index setting: " << id.Name;
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


TNodePtr TSqlTranslation::NamedExpr(const TRule_named_expr& node, EExpr exprMode) {
    TSqlExpression expr(Ctx, Mode);
    if (exprMode == EExpr::GroupBy) {
        expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::GroupBy);
    } else if (exprMode == EExpr::SqlLambdaParams) {
        expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::SqlLambdaParams);
    }
    if (node.HasBlock2()) {
        expr.MarkAsNamed();
    }
    TNodePtr exprNode(expr.Build(node.GetRule_expr1()));
    if (!exprNode) {
        Ctx.IncrementMonCounter("sql_errors", "NamedExprInvalid");
        return nullptr;
    }
    if (node.HasBlock2()) {
        exprNode = SafeClone(exprNode);
        exprNode->SetLabel(Id(node.GetBlock2().GetRule_an_id_or_type2(), *this));
    }
    return exprNode;
}

bool TSqlTranslation::NamedExprList(const TRule_named_expr_list& node, TVector<TNodePtr>& exprs, EExpr exprMode) {
    exprs.emplace_back(NamedExpr(node.GetRule_named_expr1(), exprMode));
    if (!exprs.back()) {
        return false;
    }
    for (auto& b: node.GetBlock2()) {
        exprs.emplace_back(NamedExpr(b.GetRule_named_expr2(), exprMode));
        if (!exprs.back()) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::BindList(const TRule_bind_parameter_list& node, TVector<TSymbolNameWithPos>& bindNames) {
    bindNames.clear();

    TString name;
    if (!NamedNodeImpl(node.GetRule_bind_parameter1(), name, *this)) {
        return false;
    }

    bindNames.emplace_back(TSymbolNameWithPos{name, Ctx.Pos()});
    for (auto& b: node.GetBlock2()) {
        if (!NamedNodeImpl(b.GetRule_bind_parameter2(), name, *this)) {
            return false;
        }

        bindNames.emplace_back(TSymbolNameWithPos{name, Ctx.Pos()});
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
    bindNames.emplace_back(TSymbolNameWithPos{name, Ctx.Pos()});

    for (auto& b: node.GetBlock2()) {
        if (!NamedNodeImpl(b.GetRule_opt_bind_parameter2(), name, isOptional, *this)) {
            return false;
        }

        if (isOptional) {
            optionalArgsCount++;
        } else if (optionalArgsCount > 0) {
            Context().Error() << "Non-optional argument can not follow optional one";
            return false;
        }
        bindNames.emplace_back(TSymbolNameWithPos{name, Ctx.Pos()});
    }
    return true;
}

bool TSqlTranslation::ModulePath(const TRule_module_path& node, TVector<TString>& path) {
    if (node.HasBlock1()) {
        path.emplace_back(TString());
    }
    path.emplace_back(Id(node.GetRule_an_id2(), *this));
    for (auto& b: node.GetBlock3()) {
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

    for (auto& b: node.GetBlock2()) {
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
    name.Pos = Ctx.Pos();
    if (node.HasBlock2()) {
        if (!NamedNodeImpl(node.GetBlock2().GetRule_bind_parameter2(), alias.Name, *this)) {
            return false;
        }
        alias.Pos = Ctx.Pos();
    }
    return true;
}

TMaybe<TTableArg> TSqlTranslation::TableArgImpl(const TRule_table_arg& node) {
    TTableArg ret;
    ret.HasAt = node.HasBlock1();
    TColumnRefScope scope(Ctx, EColumnRefState::AsStringLiteral);
    ret.Expr = NamedExpr(node.GetRule_named_expr2());
    if (!ret.Expr) {
        return Nothing();
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
            Ctx.Error() << "Unknown service: " << service;
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
                switch (Ctx.Settings.BindingsMode) {
                case NSQLTranslation::EBindingsMode::DISABLED:
                    Ctx.Error(Ctx.Pos(), TIssuesIds::YQL_DISABLED_BINDINGS) << "Please remove 'bindings.' from your query, the support for this syntax has ended";
                    Ctx.IncrementMonCounter("sql_errors", "DisabledBinding");
                    return false;
                case NSQLTranslation::EBindingsMode::ENABLED:
                    isBinding = true;
                    break;
                case NSQLTranslation::EBindingsMode::DROP_WITH_WARNING:
                    Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_DEPRECATED_BINDINGS) << "Please remove 'bindings.' from your query, the support for this syntax will be dropped soon";
                    Ctx.IncrementMonCounter("sql_errors", "DeprecatedBinding");
                    [[fallthrough]];
                case NSQLTranslation::EBindingsMode::DROP:
                    service = Context().Scoped->CurrService;
                    cluster = Context().Scoped->CurrCluster;
                    break;
                }

                return true;
            }
            TString normalizedClusterName;
            auto foundProvider = Ctx.GetClusterProvider(clusterName, normalizedClusterName);
            if (!foundProvider) {
                Ctx.Error() << "Unknown cluster: " << clusterName;
                return false;
            }

            if (service && *foundProvider != service) {
                Ctx.Error() << "Mismatch of cluster " << clusterName << " service, expected: "
                    << *foundProvider << ", got: " << service;
                return false;
            }

            if (!service) {
                service = *foundProvider;
            }

            value = TDeferredAtom(Ctx.Pos(), normalizedClusterName);
        } else {
            if (!service) {
                Ctx.Error() << "Cluster service is not set";
                return false;
            }
        }

        cluster = value;
        return true;
    }
    case TRule_cluster_expr::TBlock2::kAlt2: {
        if (!allowWildcard) {
            Ctx.Error() << "Cluster wildcards allowed only in USE statement";
            return false;
        }

        return true;
    }
    case TRule_cluster_expr::TBlock2::ALT_NOT_SET:
        Y_ABORT("You should change implementation according to grammar changes");
    }
}


bool TSqlTranslation::ApplyTableBinding(const TString& binding, TTableRef& tr, TTableHints& hints) {
    NSQLTranslation::TBindingInfo bindingInfo;
    if (const auto& error = ExtractBindingInfo(Context().Settings, binding, bindingInfo)) {
        Ctx.Error() << error;
        return false;
    }

    if (bindingInfo.Schema) {
        TNodePtr schema = BuildQuotedAtom(Ctx.Pos(), bindingInfo.Schema);

        TNodePtr type = new TCallNodeImpl(Ctx.Pos(), "SqlTypeFromYson", { schema });
        TNodePtr columns = new TCallNodeImpl(Ctx.Pos(), "SqlColumnOrderFromYson", { schema });

        hints["user_schema"] = { type, columns };
    }

    for (auto& [key, values] : bindingInfo.Attributes) {
        TVector<TNodePtr> hintValue;
        for (auto& column : values) {
            hintValue.push_back(BuildQuotedAtom(Ctx.Pos(), column));
        }
        hints[key] = std::move(hintValue);
    }

    tr.Service = bindingInfo.ClusterType;
    tr.Cluster = TDeferredAtom(Ctx.Pos(), bindingInfo.Cluster);

    const TString view = "";
    tr.Keys = BuildTableKey(Ctx.Pos(), tr.Service, tr.Cluster, TDeferredAtom(Ctx.Pos(), bindingInfo.Path), {view});

    return true;
}

bool TSqlTranslation::TableRefImpl(const TRule_table_ref& node, TTableRef& result, bool unorderedSubquery) {
    // table_ref:
    //   (cluster_expr DOT)? AT?
    //   (table_key | an_id_expr LPAREN (table_arg (COMMA table_arg)*)? RPAREN |
    //    bind_parameter (LPAREN expr_list? RPAREN)? (VIEW an_id)?)
    //   table_hints?;
    if (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW && node.HasBlock1()) {
        Ctx.Error() << "Cluster should not be used in limited view";
        return false;
    }
    auto service = Context().Scoped->CurrService;
    auto cluster = Context().Scoped->CurrCluster;
    const bool hasAt = node.HasBlock2();
    bool isBinding = false;
    if (node.HasBlock1()) {
        const auto& clusterExpr = node.GetBlock1().GetRule_cluster_expr1();
        bool result = !hasAt ?
            ClusterExprOrBinding(clusterExpr, service, cluster, isBinding) : ClusterExpr(clusterExpr, false, service, cluster);
        if (!result) {
            return false;
        }
    }

    TTableRef tr(Context().MakeName("table"), service, cluster, nullptr);
    TPosition pos(Context().Pos());
    TTableHints hints = GetContextHints(Ctx);
    TTableHints tableHints;

    TMaybe<TString> keyFunc;

    auto& block = node.GetBlock3();
    switch (block.Alt_case()) {
        case TRule_table_ref::TBlock3::kAlt1: {
            if (!isBinding && cluster.Empty()) {
                Ctx.Error() << "No cluster name given and no default cluster is selected";
                return false;
            }

            auto pair = TableKeyImpl(block.GetAlt1().GetRule_table_key1(), *this, hasAt);
            if (isBinding) {
                TString binding = pair.first;
                auto view = pair.second;
                if (!view.ViewName.empty()) {
                    YQL_ENSURE(view != TViewDescription{"@"});
                    Ctx.Error() << "VIEW is not supported for table bindings";
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
                Ctx.Error() << "No cluster name given and no default cluster is selected";
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
            Ctx.IncrementMonCounter("sql_features", "NamedNodeUseSource");
            TString named;
            if (!NamedNodeImpl(alt.GetRule_bind_parameter1(), named, *this)) {
                return false;
            }
            if (hasAt) {
                if (alt.HasBlock2()) {
                    Ctx.Error() << "Subquery must not be used as anonymous table name";
                    return false;
                }

                if (alt.HasBlock3()) {
                    Ctx.Error() << "View is not supported for anonymous tables";
                    return false;
                }

                if (node.HasBlock4()) {
                    Ctx.Error() << "Hints are not supported for anonymous tables";
                    return false;
                }

                auto namedNode = GetNamedNode(named);
                if (!namedNode) {
                    return false;
                }

                auto source = TryMakeSourceFromExpression(Ctx.Pos(), Ctx, service, cluster, namedNode, "@");
                if (!source) {
                    Ctx.Error() << "Cannot infer cluster and table name";
                    return false;
                }

                result.Source = source;
                return true;
            }
            auto nodePtr = GetNamedNode(named);
            if (!nodePtr) {
                Ctx.IncrementMonCounter("sql_errors", "NamedNodeSourceError");
                return false;
            }
            if (alt.HasBlock2()) {
                if (alt.HasBlock3()) {
                    Ctx.Error() << "View is not supported for subqueries";
                    return false;
                }

                if (node.HasBlock4()) {
                    Ctx.Error() << "Hints are not supported for subqueries";
                    return false;
                }

                TVector<TNodePtr> values;
                values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "Apply", TNodeFlags::Default));
                values.push_back(nodePtr);
                values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "world", TNodeFlags::Default));

                TSqlExpression sqlExpr(Ctx, Mode);
                if (alt.GetBlock2().HasBlock2() && !ExprList(sqlExpr, values, alt.GetBlock2().GetBlock2().GetRule_expr_list1())) {
                    return false;
                }

                TNodePtr apply = new TAstListNodeImpl(Ctx.Pos(), std::move(values));
                if (unorderedSubquery && Ctx.UnorderedSubqueries) {
                    apply = new TCallNodeImpl(Ctx.Pos(), "UnorderedSubquery", { apply });
                }
                result.Source = BuildNodeSource(Ctx.Pos(), apply);
                return true;
            }

            TTableHints hints;
            TTableHints contextHints = GetContextHints(Ctx);
            auto ret = BuildInnerSource(Ctx.Pos(), nodePtr, service, cluster);
            if (alt.HasBlock3()) {
                auto view = Id(alt.GetBlock3().GetRule_view_name2(), *this);
                Ctx.IncrementMonCounter("sql_features", "View");
                bool result = view.PrimaryFlag
                    ? ret->SetPrimaryView(Ctx, Ctx.Pos())
                    : ret->SetViewName(Ctx, Ctx.Pos(), view.ViewName);
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
                if (!ret->SetTableHints(Ctx, Ctx.Pos(), hints, contextHints)) {
                    return false;
                }
            }

            result.Source = ret;
            return true;
        }
        case TRule_table_ref::TBlock3::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }

    MergeHints(hints, tableHints);

    if (node.HasBlock4()) {
        auto tmp = TableHintsImpl(node.GetBlock4().GetRule_table_hints1(), service, keyFunc.GetOrElse(""));
        if (!tmp) {
            Ctx.Error() << "Failed to parse table hints";
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
        TCiString func(Id(alt.GetRule_an_id_expr1(), *this));

        if (func == "as_table") {
            if (node.HasBlock1()) {
                Ctx.Error() << "Cluster shouldn't be specified for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (!alt.HasBlock3() || !alt.GetBlock3().GetBlock2().empty()) {
                Ctx.Error() << "Expected single argument for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (node.HasBlock4()) {
                Ctx.Error() << "No hints expected for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            auto arg = TableArgImpl(alt.GetBlock3().GetRule_table_arg1());
            if (!arg) {
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (arg->Expr->GetSource()) {
                Ctx.Error() << "AS_TABLE shouldn't be used for table sources";
                return TMaybe<TSourcePtr>(nullptr);
            }

            return BuildNodeSource(Ctx.Pos(), arg->Expr, true);
        }
    }

    return Nothing();
}

TMaybe<TColumnConstraints> ColumnConstraints(const TRule_column_schema& node, TTranslation& ctx) {
    TNodePtr defaultExpr = nullptr;
    bool nullable = true;

    auto constraintsNode = node.GetRule_opt_column_constraints4();
    if (constraintsNode.HasBlock1()) {
        nullable = !constraintsNode.GetBlock1().HasBlock1();
    }
    if (constraintsNode.HasBlock2()) {
        TSqlExpression expr(ctx.Context(), ctx.Context().Settings.Mode);
        defaultExpr = expr.Build(constraintsNode.GetBlock2().GetRule_expr2());
        if (!defaultExpr) {
            return {};
        }
    }

    return TColumnConstraints(defaultExpr, nullable);
}

TMaybe<TColumnSchema> TSqlTranslation::ColumnSchemaImpl(const TRule_column_schema& node) {
    const TString name(Id(node.GetRule_an_id_schema1(), *this));
    const TPosition pos(Context().Pos());
    TNodePtr type = SerialTypeNode(node.GetRule_type_name_or_bind2());
    const bool serial = (type != nullptr);

    const auto constraints = ColumnConstraints(node, *this);
    if (!constraints){
        return {};
    }

    if (!type) {
        type = TypeNodeOrBind(node.GetRule_type_name_or_bind2());
    }

    if (!type) {
        return {};
    }
    TVector<TIdentifier> families;
    if (node.HasBlock3()) {
        const auto& familyRelation = node.GetBlock3().GetRule_family_relation1();
        families.push_back(IdEx(familyRelation.GetRule_an_id2(), *this));
    }
    return TColumnSchema(pos, name, type, constraints->Nullable, families, serial, constraints->DefaultExpr);
}

TNodePtr TSqlTranslation::SerialTypeNode(const TRule_type_name_or_bind& node) {
    if (node.Alt_case() != TRule_type_name_or_bind::kAltTypeNameOrBind1) {
        return nullptr;
    }

    TPosition pos = Ctx.Pos();

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
        return new TCallNodeImpl(pos, "DataType", { BuildQuotedAtom(pos, "Int64", TNodeFlags::Default) });
    } else if (res == "serial" || res == "serial4") {
        return new TCallNodeImpl(pos, "DataType", { BuildQuotedAtom(pos, "Int32", TNodeFlags::Default) });
    } else if (res == "smallserial" || res == "serial2") {
        return new TCallNodeImpl(pos, "DataType", { BuildQuotedAtom(pos, "Int16", TNodeFlags::Default) });
    }

    return nullptr;
}

bool TSqlTranslation::FillFamilySettingsEntry(const TRule_family_settings_entry& settingNode, TFamilyEntry& family) {
    TIdentifier id = IdEx(settingNode.GetRule_an_id1(), *this);
    const TRule_family_setting_value& value = settingNode.GetRule_family_setting_value3();
    if (to_lower(id.Name) == "data") {
        const TString stringValue(Ctx.Token(value.GetToken1()));
        family.Data = BuildLiteralSmartString(Ctx, stringValue);
    } else if (to_lower(id.Name) == "compression") {
        const TString stringValue(Ctx.Token(value.GetToken1()));
        family.Compression = BuildLiteralSmartString(Ctx, stringValue);
    } else {
        Ctx.Error() << "Unknown table setting: " << id.Name;
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
        case TRule_create_table_entry::kAltCreateTableEntry1:
        {
            if (isCreateTableAs) {
                Ctx.Error() << "Column types are not supported for CREATE TABLE AS";
                return false;
            }
            // column_schema
            auto columnSchema = ColumnSchemaImpl(node.GetAlt_create_table_entry1().GetRule_column_schema1());
            if (!columnSchema) {
                return false;
            }
            if (columnSchema->Families.size() > 1) {
                Ctx.Error() << "Several column families for a single column are not yet supported";
                return false;
            }
            params.Columns.push_back(*columnSchema);
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry2:
        {
            // table_constraint
            auto& constraint = node.GetAlt_create_table_entry2().GetRule_table_constraint1();
            switch (constraint.Alt_case()) {
                case TRule_table_constraint::kAltTableConstraint1: {
                    if (!params.PkColumns.empty()) {
                        Ctx.Error() << "PRIMARY KEY statement must be specified only once";
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
                        Ctx.Error() << "PARTITION BY statement must be specified only once";
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
                        Ctx.Error() << "ORDER BY statement must be specified only once";
                        return false;
                    }
                    auto& obConstraint = constraint.GetAlt_table_constraint3();
                    auto extractDirection = [this] (const TRule_column_order_by_specification& spec, bool& desc) {
                        desc = false;
                        if (!spec.HasBlock2()) {
                            return true;
                        }

                        auto& token = spec.GetBlock2().GetToken1();
                        switch (token.GetId()) {
                            case SQLv1LexerTokens::TOKEN_ASC:
                                return true;
                            case SQLv1LexerTokens::TOKEN_DESC:
                                desc = true;
                                return true;
                            default:
                                Ctx.Error() << "Unsupported direction token: " << token.GetId();
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
                default:
                    AltNotImplemented("table_constraint", constraint);
                    return false;
            }
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry3:
        {
            // table_index
            auto& table_index = node.GetAlt_create_table_entry3().GetRule_table_index1();
            if (!CreateTableIndex(table_index, params.Indexes)) {
                return false;
            }
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry4:
        {
            if (isCreateTableAs) {
                Ctx.Error() << "Column families are not supported for CREATE TABLE AS";
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
        case TRule_create_table_entry::kAltCreateTableEntry5:
        {
            // changefeed
            auto& changefeed = node.GetAlt_create_table_entry5().GetRule_changefeed1();
            TSqlExpression expr(Ctx, Mode);
            if (!CreateChangefeed(changefeed, expr, params.Changefeeds)) {
                return false;
            }
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry6:
        {
            if (!isCreateTableAs) {
                Ctx.Error() << "Column requires a type";
                return false;
            }
            // an_id_schema
            const TString name(Id(node.GetAlt_create_table_entry6().GetRule_an_id_schema1(), *this));
            const TPosition pos(Context().Pos());

            params.Columns.push_back(TColumnSchema(pos, name, nullptr, true, {}, false, nullptr));
            break;
        }
        default:
            AltNotImplemented("create_table_entry", node);
            return false;
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

    bool StoreBool(const TRule_table_setting_value& from, TDeferredAtom& to, TContext& ctx) {
        if (!from.HasAlt_table_setting_value6()) {
            return false;
        }
        // bool_value
        const TString value = to_lower(ctx.Token(from.GetAlt_table_setting_value6().GetRule_bool_value1().GetToken1()));
        to = TDeferredAtom(BuildLiteralBool(ctx.Pos(), FromString<bool>(value)), ctx);
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
                        TVector<TNodePtr>(1, keyExprOrIdent->Expr)
                    );
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

    bool StoreTtlSettings(const TRule_table_setting_value& from, TResetableSetting<TTtlSettings, void>& to,
            TSqlExpression& expr, TContext& ctx, TTranslation& txc) {
        switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue5: {
            auto columnName = IdEx(from.GetAlt_table_setting_value5().GetRule_an_id3(), txc);
            auto exprNode = expr.Build(from.GetAlt_table_setting_value5().GetRule_expr1());
            if (!exprNode) {
                return false;
            }

            if (exprNode->GetOpName() != "Interval") {
                ctx.Error() << "Literal of Interval type is expected for TTL";
                return false;
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

            to.Set(TTtlSettings(columnName, exprNode, columnUnit));
            break;
        }
        default:
            return false;
        }
        return true;
    }

    bool StoreViewOptionsEntry(const TIdentifier& id,
                               const TRule_table_setting_value& value,
                               std::map<TString, TDeferredAtom>& features,
                               TContext& ctx) {
        const auto name = to_lower(id.Name);
        const auto publicName = to_upper(name);

        if (features.find(name) != features.end()) {
            ctx.Error(ctx.Pos()) << publicName << " is a duplicate";
            return false;
        }

        if (!StoreBool(value, features[name], ctx)) {
            ctx.Error(ctx.Pos()) << "Value of " << publicName << " must be a bool";
            return false;
        }

        return true;
    }

    template<typename TChar>
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

    template<typename TChar>
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
}

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
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        TDeferredAtom dataSource;
        if (!StoreString(*value, dataSource, Ctx, to_upper(id.Name))) {
            return false;
        }
        TString service = Context().Scoped->CurrService;
        TDeferredAtom cluster = Context().Scoped->CurrCluster;
        TNodePtr root = new TAstListNodeImpl(Ctx.Pos());
        root->Add("String", Ctx.GetPrefixedPath(service, cluster, dataSource));
        settings.DataSourcePath = root;
    } else if (to_lower(id.Name) == "location") {
        if (reset) {
            settings.Location.Reset();
        } else {
            TNodePtr location;
            if (!StoreString(*value, location, Ctx)) {
                Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
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
            if (!StoreString(*value, node, Ctx)) {
                Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
                return false;
            }
            setting.Set(std::pair<TIdentifier, TNodePtr>{id, std::move(node)});
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
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreString(*value, settings.CompactionPolicy, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_by_size") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.AutoPartitioningBySize, *this)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_partition_size_mb") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.PartitionSizeMb, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_by_load") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.AutoPartitioningByLoad, *this)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_min_partitions_count") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.MinPartitions, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_max_partitions_count") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.MaxPartitions, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "uniform_partitions") {
        if (alter) {
            Ctx.Error() << to_upper(id.Name) << " alter is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.UniformPartitions, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "partition_at_keys") {
        if (alter) {
            Ctx.Error() << to_upper(id.Name) << " alter is not supported";
            return false;
        }
        TSqlExpression expr(Ctx, Mode);
        if (!StoreSplitBoundaries(*value, settings.PartitionAtKeys, expr, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be a list of keys. "
                << "Example1: (10, 1000)  Example2: ((10), (1000, \"abc\"))";
            return false;
        }
    } else if (to_lower(id.Name) == "key_bloom_filter") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.KeyBloomFilter, *this)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "read_replicas_settings") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreString(*value, settings.ReadReplicasSettings, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else if (to_lower(id.Name) == "ttl") {
        if (!reset) {
            TSqlExpression expr(Ctx, Mode);
            if (!StoreTtlSettings(*value, settings.TtlSettings, expr, Ctx, *this)) {
                Ctx.Error() << "Invalid TTL settings";
                return false;
            }
        } else {
            settings.TtlSettings.Reset();
        }
    } else if (to_lower(id.Name) == "tiering") {
        if (!reset) {
            TNodePtr tieringNode;
            if (!StoreString(*value, tieringNode, Ctx)) {
                Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
                return false;
            }
            settings.Tiering.Set(tieringNode);
        } else {
            settings.Tiering.Reset();
        }
    } else if (to_lower(id.Name) == "store") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.StoreType, *this)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "partition_by_hash_function") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreString(*value, settings.PartitionByHashFunction, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else if (to_lower(id.Name) == "store_external_blobs") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.StoreExternalBlobs, *this)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else {
        Ctx.Error() << "Unknown table setting: " << id.Name;
        return false;
    }
    return true;
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

bool StoreConsumerSettingsEntry(
        const TIdentifier& id, const TRule_topic_consumer_setting_value* value, TSqlExpression& ctx,
        TTopicConsumerSettings& settings,
        bool reset
) {
    YQL_ENSURE(value || reset);
    TNodePtr valueExprNode;
    if (value) {
        valueExprNode = ctx.Build(value->GetRule_expr1());
        if (!valueExprNode) {
            ctx.Error() << "invalid value for setting: " << id.Name;
            return false;
        }
    }
    if (to_lower(id.Name) == "important") {
        if (settings.Important) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in ALTER CONSUMER statements for single consumer";
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

    } else if (to_lower(id.Name) == "read_from") {
        if (settings.ReadFromTs) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in ALTER CONSUMER statements for single consumer";
            return false;
        }
        if (reset) {
            settings.ReadFromTs.Reset();
        } else {
            //ToDo: !! validate
            settings.ReadFromTs.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "supported_codecs") {
        if (settings.SupportedCodecs) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in ALTER CONSUMER statements for single consumer";
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
    } else {
        ctx.Error() << to_upper(id.Name) << ": unknown option for consumer";
        return false;
    }
    return true;
}

TIdentifier TSqlTranslation::GetTopicConsumerId(const TRule_topic_consumer_ref& node) {
    return IdEx(node.GetRule_an_id_pure1(), *this);
}

bool TSqlTranslation::CreateConsumerSettings(
        const TRule_topic_consumer_settings& node, TTopicConsumerSettings& settings
) {
    const auto& firstEntry = node.GetRule_topic_consumer_settings_entry1();
    TSqlExpression expr(Ctx, Mode);
    if (!StoreConsumerSettingsEntry(
            IdEx(firstEntry.GetRule_an_id1(), *this),
            &firstEntry.GetRule_topic_consumer_setting_value3(),
            expr, settings, false
    )) {
        return false;
    }
    for (auto& block : node.GetBlock2()) {
        const auto& entry = block.GetRule_topic_consumer_settings_entry2();
        if (!StoreConsumerSettingsEntry(
                IdEx(entry.GetRule_an_id1(), *this),
                &entry.GetRule_topic_consumer_setting_value3(),
                expr, settings, false
        )) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::CreateTopicConsumer(
        const TRule_topic_create_consumer_entry& node,
        TVector<TTopicConsumerDescription>& consumers
) {
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
        const TRule_alter_topic_alter_consumer_entry& node, TTopicConsumerDescription& alterConsumer
) {
    switch (node.Alt_case()) {
        case TRule_alter_topic_alter_consumer_entry::kAltAlterTopicAlterConsumerEntry1:
            return CreateConsumerSettings(
                    node.GetAlt_alter_topic_alter_consumer_entry1().GetRule_topic_alter_consumer_set1()
                            .GetRule_topic_consumer_settings3(),
                    alterConsumer.Settings
            );
        //case TRule_alter_topic_alter_consumer_entry::ALT_NOT_SET:
        case TRule_alter_topic_alter_consumer_entry::kAltAlterTopicAlterConsumerEntry2: {
            auto& resetNode = node.GetAlt_alter_topic_alter_consumer_entry2().GetRule_topic_alter_consumer_reset1();
            TSqlExpression expr(Ctx, Mode);
            if (!StoreConsumerSettingsEntry(
                    IdEx(resetNode.GetRule_an_id3(), *this),
                    nullptr,
                    expr, alterConsumer.Settings, true
            )) {
                return false;
            }

            for (auto& resetItem: resetNode.GetBlock4()) {
                if (!StoreConsumerSettingsEntry(
                        IdEx(resetItem.GetRule_an_id2(), *this),
                        nullptr,
                        expr, alterConsumer.Settings, true
                )) {
                    return false;
                }
            }
            return true;
        }
        default:
            Ctx.Error() << "unknown alter consumer action";
            return false;
    }
    return true;
}

bool TSqlTranslation::AlterTopicConsumer(
        const TRule_alter_topic_alter_consumer& node,
        THashMap<TString, TTopicConsumerDescription>& alterConsumers
) {
    auto consumerId = GetTopicConsumerId(node.GetRule_topic_consumer_ref3());
    TString name = to_lower(consumerId.Name);
    auto iter = alterConsumers.insert(std::make_pair(
            name, TTopicConsumerDescription(std::move(consumerId))
    )).first;
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

static bool StoreTopicSettingsEntry(
        const TIdentifier& id, const TRule_topic_setting_value* value, TSqlExpression& ctx,
        TTopicSettings& settings, bool reset
) {
    YQL_ENSURE(value || reset);
    TNodePtr valueExprNode;
    if (value) {
        valueExprNode = ctx.Build(value->GetRule_expr1());
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
    } else {
        ctx.Error() << "unknown topic setting: " << id.Name;
        return false;
    }
    return true;
}

bool TSqlTranslation::AlterTopicAction(const TRule_alter_topic_action& node, TAlterTopicParameters& params) {
//    alter_topic_action:
//    alter_topic_add_consumer
//    | alter_topic_alter_consumer
//    | alter_topic_drop_consumer
//    | alter_topic_set_settings
//    | alter_topic_reset_settings

    switch (node.Alt_case()) {
        case TRule_alter_topic_action::kAltAlterTopicAction1: // alter_topic_add_consumer
            return CreateTopicConsumer(
                    node.GetAlt_alter_topic_action1().GetRule_alter_topic_add_consumer1()
                                                     .GetRule_topic_create_consumer_entry2(),
                    params.AddConsumers
           );

        case TRule_alter_topic_action::kAltAlterTopicAction2: // alter_topic_alter_consumer
            return AlterTopicConsumer(
                    node.GetAlt_alter_topic_action2().GetRule_alter_topic_alter_consumer1(),
                    params.AlterConsumers
            );

        case TRule_alter_topic_action::kAltAlterTopicAction3: // drop_consumer
            params.DropConsumers.emplace_back(GetTopicConsumerId(
                    node.GetAlt_alter_topic_action3().GetRule_alter_topic_drop_consumer1()
                                                     .GetRule_topic_consumer_ref3()
            ));
            return true;

        case TRule_alter_topic_action::kAltAlterTopicAction4: // set_settings
            return CreateTopicSettings(
                    node.GetAlt_alter_topic_action4().GetRule_alter_topic_set_settings1()
                                                     .GetRule_topic_settings3(),
                    params.TopicSettings
            );

        case TRule_alter_topic_action::kAltAlterTopicAction5: { // reset_settings
            auto& resetNode = node.GetAlt_alter_topic_action5().GetRule_alter_topic_reset_settings1();
            TSqlExpression expr(Ctx, Mode);
            if (!StoreTopicSettingsEntry(
                    IdEx(resetNode.GetRule_an_id3(), *this),
                    nullptr, expr,
                    params.TopicSettings, true
            )) {
                return false;
            }

            for (auto& resetItem: resetNode.GetBlock4()) {
                if (!StoreTopicSettingsEntry(
                        IdEx(resetItem.GetRule_an_id_pure2(), *this),
                        nullptr, expr,
                        params.TopicSettings, true
                )) {
                    return false;
                }
            }
            return true;
        }
        default:
            Ctx.Error() << "unknown alter topic action";
            return false;
    }
    return true;
}

bool TSqlTranslation::CreateTopicSettings(const TRule_topic_settings& node, TTopicSettings& settings) {
    const auto& firstEntry = node.GetRule_topic_settings_entry1();
    TSqlExpression expr(Ctx, Mode);

    if (!StoreTopicSettingsEntry(
            IdEx(firstEntry.GetRule_an_id1(), *this),
            &firstEntry.GetRule_topic_setting_value3(),
            expr, settings, false
    )) {
        return false;
    }
    for (auto& block : node.GetBlock2()) {
        const auto& entry = block.GetRule_topic_settings_entry2();
        if (!StoreTopicSettingsEntry(
                IdEx(entry.GetRule_an_id1(), *this),
                &entry.GetRule_topic_setting_value3(),
                expr, settings, false
        )) {
            return false;
        }
    }
    return true;
}

TNodePtr TSqlTranslation::IntegerOrBind(const TRule_integer_or_bind& node) {
    switch (node.Alt_case()) {
        case TRule_integer_or_bind::kAltIntegerOrBind1: {
            const TString intString = Ctx.Token(node.GetAlt_integer_or_bind1().GetRule_integer1().GetToken1());
            ui64 value;
            TString suffix;
            if (!ParseNumbers(Ctx, intString, value, suffix)) {
                return {};
            }
            return BuildQuotedAtom(Ctx.Pos(), ToString(value), TNodeFlags::ArbitraryContent);
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
            auto atom = MakeAtomFromExpression(Ctx.Pos(), Ctx, namedNode);
            return atom.Build();
        }
        case TRule_integer_or_bind::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TNodePtr TSqlTranslation::TypeNameTag(const TRule_type_name_tag& node) {
    switch (node.Alt_case()) {
        case TRule_type_name_tag::kAltTypeNameTag1: {
            auto content = Id(node.GetAlt_type_name_tag1().GetRule_id1(), *this);
            auto atom = TDeferredAtom(Ctx.Pos(), content);
            return atom.Build();
        }
        case TRule_type_name_tag::kAltTypeNameTag2: {
            auto value = Token(node.GetAlt_type_name_tag2().GetToken1());
            auto parsed = StringContentOrIdContent(Ctx, Ctx.Pos(), value);
            if (!parsed) {
                return {};
            }
            auto atom = TDeferredAtom(Ctx.Pos(), parsed->Content);
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
            MakeTableFromExpression(Ctx.Pos(), Ctx, namedNode, atom);
            return atom.Build();
        }
        case TRule_type_name_tag::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TNodePtr TSqlTranslation::TypeSimple(const TRule_type_name_simple& node, bool onlyDataAllowed) {
    const TString origName = Id(node.GetRule_an_id_pure1(), *this);
    if (origName.empty()) {
        return {};
    }
    return BuildSimpleType(Ctx, Ctx.Pos(), origName, onlyDataAllowed);
}

TNodePtr TSqlTranslation::TypeDecimal(const TRule_type_name_decimal& node) {
    auto pos = Ctx.Pos();
    auto flags = TNodeFlags::Default;

    auto paramOne = IntegerOrBind(node.GetRule_integer_or_bind3());
    if (!paramOne) {
        return {};
    }
    auto paramTwo = IntegerOrBind(node.GetRule_integer_or_bind5());
    if (!paramTwo) {
        return {};
    }
    return new TCallNodeImpl(pos, "DataType", { BuildQuotedAtom(pos, "Decimal", flags), paramOne, paramTwo });
}

TNodePtr TSqlTranslation::AddOptionals(const TNodePtr& node, size_t optionalCount) {
    TNodePtr result = node;
    if (node) {
        TPosition pos = node->GetPos();
        for (size_t i = 0; i < optionalCount; ++i) {
            result = new TCallNodeImpl(pos, "OptionalType", { result });
        }
    }
    return result;
}


TMaybe<std::pair<TVector<TNodePtr>, bool>> TSqlTranslation::CallableArgList(const TRule_callable_arg_list& argList, bool namedArgsStarted) {
    auto pos = Ctx.Pos();
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
                Ctx.Error() << "Expected named argument, previous argument was named";
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
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

TNodePtr TSqlTranslation::TypeNode(const TRule_type_name& node) {
    //type_name:
    //    type_name_composite
    //  | (type_name_decimal | type_name_simple) QUESTION*;
    if (node.Alt_case() == TRule_type_name::kAltTypeName1) {
        return TypeNode(node.GetAlt_type_name1().GetRule_type_name_composite1());
    }

    TNodePtr result;
    TPosition pos = Ctx.Pos();

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
            Y_ABORT("You should change implementation according to grammar changes");
    }

    return AddOptionals(result, alt.GetBlock2().size());
}

TNodePtr TSqlTranslation::TypeNode(const TRule_type_name_composite& node) {
    //type_name_composite:
    //  ( type_name_optional
    //  | type_name_tuple
    //  | type_name_struct
    //  | type_name_variant
    //  | type_name_list
    //  | type_name_stream
    //  | type_name_flow
    //  | type_name_dict
    //  | type_name_set
    //  | type_name_enum
    //  | type_name_resource
    //  | type_name_tagged
    //  | type_name_callable
    //  ) QUESTION*;
    TNodePtr result;
    TPosition pos = Ctx.Pos();
    auto flags = TNodeFlags::Default;

    auto wrapOneParamType = [&] (const TRule_type_name_or_bind& param, const char* type) -> TNodePtr {
        auto node = TypeNodeOrBind(param);
        return node ? new TAstListNodeImpl(pos, { BuildAtom(pos, type, flags), node }) : nullptr;
    };
    auto makeVoid = [&] () -> TNodePtr {
        return new TAstListNodeImpl(pos, { BuildAtom(pos, "VoidType", flags) });
    };
    auto makeQuote = [&] (const TNodePtr& node) -> TNodePtr {
        return new TAstListNodeImpl(pos, { new TAstAtomNodeImpl(pos, "quote", 0), node });
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
                Y_ABORT("You should change implementation according to grammar changes");
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

                        items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, typeNode })));
                        for (auto& arg : structType.GetBlock2().GetAlt1().GetBlock2().GetBlock2()) {
                            auto typeNode = TypeNodeOrBind(arg.GetRule_struct_arg2().GetRule_type_name_or_bind3());
                            if (!typeNode) {
                                return {};
                            }
                            auto tag = TypeNameTag(arg.GetRule_struct_arg2().GetRule_type_name_tag1());
                            if (!tag) {
                                return {};
                            }
                            items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, typeNode })));
                        }
                    }
                    [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
                }
                case TRule_type_name_struct::TBlock2::kAlt2:
                    break;
                case TRule_type_name_struct::TBlock2::ALT_NOT_SET:
                    Y_ABORT("You should change implementation according to grammar changes");
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
                items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, typeNode })));
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
                        Ctx.Error() << "Variant over struct and tuple mixture";
                        return {};
                    }
                    auto tag = TypeNameTag(arg.GetRule_variant_arg2().GetBlock1().GetRule_type_name_tag1());
                    if (!tag) {
                        return {};
                    }
                    items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, typeNode })));
                } else {
                    if (arg.GetRule_variant_arg2().HasBlock1()) {
                        Ctx.Error() << "Variant over struct and tuple mixture";
                        return {};
                    }
                    items.push_back(typeNode);
                }
            }
            typeNode = new TAstListNodeImpl(pos, items);
            result = new TAstListNodeImpl(pos, { BuildAtom(pos, "VariantType", flags), typeNode });
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
            result = new TAstListNodeImpl(pos, { BuildAtom(pos, "DictType", flags), typeNode, makeVoid() });
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
            items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, makeVoid() })));
            for (auto& arg : enumType.GetBlock4()) {
                auto tag = TypeNameTag(arg.GetRule_type_name_tag2());
                if (!tag) {
                    return {};
                }
                items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, makeVoid() })));
            }
            auto typeNode = new TAstListNodeImpl(pos, items);
            result = new TAstListNodeImpl(pos, { BuildAtom(pos, "VariantType", flags), typeNode });
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt11: {
            auto& resourceType = block.GetAlt11().GetRule_type_name_resource1();
            auto tag = TypeNameTag(resourceType.GetRule_type_name_tag3());
            if (!tag) {
                return {};
            }
            result = new TAstListNodeImpl(pos, { BuildAtom(pos, "ResourceType", flags), tag });
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
            result = new TAstListNodeImpl(pos, { BuildAtom(pos, "TaggedType", flags), typeNode, tag });
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
                    { BuildQuotedAtom(pos, ToString(optionalArgsCount), flags) })));
            } else {
                items.push_back(makeQuote(new TAstListNodeImpl(pos, {})));
            }
            items.push_back(makeQuote(new TAstListNodeImpl(pos, { returnType })));
            if (requiredArgs) {
                for (auto& arg: requiredArgs->first) {
                    items.push_back(makeQuote(arg));
                }
            }
            if (optionalArgs) {
                for (auto& arg: optionalArgs->first) {
                    items.push_back(makeQuote(arg));
                }
            }
            result = new TAstListNodeImpl(pos, items);
            break;
        }
        case TRule_type_name_composite_TBlock1::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }

    return AddOptionals(result, node.GetBlock2().size());
}

TNodePtr TSqlTranslation::ValueConstructorLiteral(const TRule_value_constructor_literal& node) {
    return BuildLiteralSmartString(Ctx, Token(node.GetToken1()));
}

TNodePtr TSqlTranslation::ValueConstructor(const TRule_value_constructor& node) {
    TSqlCallExpr call(Ctx, Mode);
    if (!call.Init(node)) {
        return {};
    }
    return call.BuildCall();
}

TNodePtr TSqlTranslation::ListLiteral(const TRule_list_literal& node) {
    TVector<TNodePtr> values;
    values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "AsListMayWarn", TNodeFlags::Default));

    TSqlExpression sqlExpr(Ctx, Mode);
    if (node.HasBlock2() && !ExprList(sqlExpr, values, node.GetBlock2().GetRule_expr_list1())) {
        return nullptr;
    }

    return new TAstListNodeImpl(Ctx.Pos(), std::move(values));
}

TNodePtr TSqlTranslation::DictLiteral(const TRule_dict_literal& node) {
    TVector<TNodePtr> values;
    if (node.HasBlock2()) {
        const auto& list = node.GetBlock2().GetRule_expr_dict_list1();
        const bool isSet = !list.HasBlock2();
        values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), isSet ? "AsSet" : "AsDict", TNodeFlags::Default));
        TSqlExpression sqlExpr(Ctx, Mode);
        if (isSet) {
            if (!Expr(sqlExpr, values, list.GetRule_expr1())) {
                return nullptr;
            }
        } else {
            TVector<TNodePtr> tupleItems;
            if (!Expr(sqlExpr, tupleItems, list.GetRule_expr1())) {
                return nullptr;
            }

            if (!Expr(sqlExpr, tupleItems, list.GetBlock2().GetRule_expr2())) {
                return nullptr;
            }

            values.push_back(new TTupleNode(Ctx.Pos(), std::move(tupleItems)));
        }

        for (auto& b : list.GetBlock3()) {
            sqlExpr.Token(b.GetToken1());
            const bool isSetCurr = !b.HasBlock3();
            if (isSetCurr != isSet) {
                Error() << "Expected keys/values pair or keys, but got mix of them";
                return nullptr;
            }

            if (isSet) {
                if (!Expr(sqlExpr, values, b.GetRule_expr2())) {
                    return nullptr;
                }
            } else {
                TVector<TNodePtr> tupleItems;
                if (!Expr(sqlExpr, tupleItems, b.GetRule_expr2())) {
                    return nullptr;
                }

                if (!Expr(sqlExpr, tupleItems, b.GetBlock3().GetRule_expr2())) {
                    return nullptr;
                }

                values.push_back(new TTupleNode(Ctx.Pos(), std::move(tupleItems)));
            }
        }
    } else {
        values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "AsDict", TNodeFlags::Default));
    }

    return new TAstListNodeImpl(Ctx.Pos(), std::move(values));
}

bool TSqlTranslation::StructLiteralItem(TVector<TNodePtr>& labels, const TRule_expr& label, TVector<TNodePtr>& values, const TRule_expr& value) {
    // label expr
    {
        TColumnRefScope scope(Ctx, EColumnRefState::AsStringLiteral, /* topLevel */ false);
        TSqlExpression sqlExpr(Ctx, Mode);
        if (!Expr(sqlExpr, labels, label)) {
            return false;
        }

        TDeferredAtom atom;
        MakeTableFromExpression(Ctx.Pos(), Ctx, labels.back(), atom);
        labels.back() = atom.Build();
        if (!labels.back()) {
            return false;
        }
    }

    // value expr
    {
        TSqlExpression sqlExpr(Ctx, Mode);
        if (!Expr(sqlExpr, values, value)) {
            return false;
        }
    }

    return true;
}

TNodePtr TSqlTranslation::StructLiteral(const TRule_struct_literal& node) {
    TVector<TNodePtr> labels;
    TVector<TNodePtr> values;
    TPosition pos = Ctx.TokenPosition(node.GetToken1());
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
                    Y_ABORT("You should change implementation according to grammar changes");
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

        hints["user_" + to_lower(alt2.GetToken1().GetValue())] = { node };
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
                auto& typeNameOrBind = altCurrent ?
                    arg.GetAlt_struct_arg_positional1().GetRule_type_name_or_bind2() :
                    arg.GetAlt_struct_arg_positional2().GetRule_type_name_or_bind1();
                auto typeNode = TypeNodeOrBind(typeNameOrBind);
                if (!typeNode) {
                    return false;
                }

                auto pos = Ctx.Pos();
                if (!altCurrent && !warn) {
                    Ctx.Warning(pos, TIssuesIds::YQL_DEPRECATED_POSITIONAL_SCHEMA)
                        << "Deprecated syntax for positional schema: please use 'column type' instead of 'type AS column'";
                    warn = true;
                }

                if (altCurrent) {
                    bool notNull = arg.GetAlt_struct_arg_positional1().HasBlock3() && arg.GetAlt_struct_arg_positional1().GetBlock3().HasBlock1();
                    if (!notNull) {
                        typeNode = new TCallNodeImpl(pos, "AsOptionalType", { typeNode });
                    }
                }

                auto& typeNameTag = altCurrent ?
                    arg.GetAlt_struct_arg_positional1().GetRule_type_name_tag1() :
                    arg.GetAlt_struct_arg_positional2().GetRule_type_name_tag3();
                auto tag = TypeNameTag(typeNameTag);
                if (!tag) {
                    return false;
                }

                labels.push_back(tag);
                structTypeItems.push_back(BuildTuple(pos, { tag, typeNode }));
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

        TPosition pos = Ctx.TokenPosition(alt.GetToken1());
        TNodePtr structType = new TCallNodeImpl(pos, "StructType", structTypeItems);
        bool shouldEmitLabel = provider != YtProviderName || TCiString(keyFunc) == "object";
        if (shouldEmitLabel) {
            auto labelsTuple = BuildTuple(pos, labels);
            hints["user_" + to_lower(alt.GetToken1().GetValue())] = { structType, labelsTuple };
            break;
        } else {
            hints["user_" + to_lower(alt.GetToken1().GetValue())] = { structType };
            break;
        }
    }

    case TRule_table_hint::ALT_NOT_SET:
        Y_ABORT("You should change implementation according to grammar changes");
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
        Y_ABORT("You should change implementation according to grammar changes");
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
            if (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) {
                Error() << "Cluster should not be used in limited view";
                return false;
            }

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
        if (cluster.Empty()) {
            Error() << "No cluster name given and no default cluster is selected";
            return false;
        }

        auto at = node.GetAlt_simple_table_ref_core2().HasBlock1();
        TString bindName;
        if (!NamedNodeImpl(node.GetAlt_simple_table_ref_core2().GetRule_bind_parameter2(), bindName, *this)) {
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
        Y_ABORT("You should change implementation according to grammar changes");
    }

    return result.Keys != nullptr;
}

bool TSqlTranslation::TopicRefImpl(const TRule_topic_ref& node, TTopicRef& result) {
    TString service = Context().Scoped->CurrService;
    TDeferredAtom cluster = Context().Scoped->CurrCluster;
    if (node.HasBlock1()) {
        if (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) {
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
    // named_nodes_stmt: bind_parameter_list EQUALS (expr | subselect_stmt);
    // subselect_stmt: (LPAREN select_stmt RPAREN | select_unparenthesized_stmt);
    if (!BindList(rule.GetRule_bind_parameter_list1(), names)) {
        return {};
    }

    TNodePtr nodeExpr = nullptr;
    switch (rule.GetBlock3().Alt_case()) {
    case TRule_named_nodes_stmt::TBlock3::kAlt1: {
        TSqlExpression expr(Ctx, Mode);
        auto result = expr.Build(rule.GetBlock3().GetAlt1().GetRule_expr1());
        return result;
    }

    case TRule_named_nodes_stmt::TBlock3::kAlt2:{
        const auto& subselect_rule = rule.GetBlock3().GetAlt2().GetRule_subselect_stmt1();

        TSqlSelect expr(Ctx, Mode);
        TPosition pos;
        TSourcePtr source = nullptr;
        switch (subselect_rule.GetBlock1().Alt_case()) {
            case TRule_subselect_stmt::TBlock1::kAlt1:
                source = expr.Build(subselect_rule.GetBlock1().GetAlt1().GetRule_select_stmt2(), pos);
                break;

            case TRule_subselect_stmt::TBlock1::kAlt2:
                source = expr.Build(subselect_rule.GetBlock1().GetAlt2().GetRule_select_unparenthesized_stmt1(), pos);
                break;

            case TRule_subselect_stmt::TBlock1::ALT_NOT_SET:
                AltNotImplemented("subselect_stmt", subselect_rule.GetBlock1());
                Ctx.IncrementMonCounter("sql_errors", "UnknownNamedNode");
                return nullptr;
        }

        if (!source) {
            return {};
        }

        return BuildSourceNode(pos, std::move(source));
    }

    case TRule_named_nodes_stmt::TBlock3::ALT_NOT_SET:
        AltNotImplemented("named_node", rule.GetBlock3());
        Ctx.IncrementMonCounter("sql_errors", "UnknownNamedNode");
        return nullptr;
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
    const TString moduleAlias = Ctx.AddImport(std::move(modulePath));
    if (!moduleAlias) {
        return false;
    }

    for (size_t i = 0; i < names.size(); ++i) {
        auto& name = names[i];
        auto& alias = aliases[i];

        auto& var = alias.Name ? alias : name;
        if (IsAnonymousName(var.Name)) {
            Ctx.Error(var.Pos) << "Can not import anonymous name " << var.Name;
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
    TSqlExpression expr(Ctx, Mode);
    TColumnRefScope scope(Ctx, EColumnRefState::Allow);
    TNodePtr exprNode = expr.Build(node.GetRule_expr1());
    if (!exprNode) {
        return false;
    }
    if (node.HasBlock2()) {
        const auto& token = node.GetBlock2().GetToken1();
        Token(token);
        switch (token.GetId()) {
            case SQLv1LexerTokens::TOKEN_ASC:
                Ctx.IncrementMonCounter("sql_features", "OrderByAsc");
                break;
            case SQLv1LexerTokens::TOKEN_DESC:
                asc = false;
                Ctx.IncrementMonCounter("sql_features", "OrderByDesc");
                break;
            default:
                Ctx.IncrementMonCounter("sql_errors", "UnknownOrderBy");
                Error() << "Unsupported direction token: " << token.GetId();
                return false;
        }
    } else {
        Ctx.IncrementMonCounter("sql_features", "OrderByDefault");
    }
    sortSpecs.emplace_back(MakeIntrusive<TSortSpecification>(exprNode, asc));
    return true;
}

bool TSqlTranslation::SortSpecificationList(const TRule_sort_specification_list& node, TVector<TSortSpecificationPtr>& sortSpecs) {
    if (!SortSpecification(node.GetRule_sort_specification1(), sortSpecs)) {
        return false;
    }
    for (auto sortSpec: node.GetBlock2()) {
        Token(sortSpec.GetToken1());
        if (!SortSpecification(sortSpec.GetRule_sort_specification2(), sortSpecs)) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::IsDistinctOptSet(const TRule_opt_set_quantifier& node) const {
    TPosition pos;
    return node.HasBlock1() && node.GetBlock1().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_DISTINCT;
}

bool TSqlTranslation::IsDistinctOptSet(const TRule_opt_set_quantifier& node, TPosition& distinctPos) const {
    if (node.HasBlock1() && node.GetBlock1().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_DISTINCT) {
        distinctPos = Ctx.TokenPosition(node.GetBlock1().GetToken1());
        return true;
    }
    return false;
}

bool TSqlTranslation::RoleNameClause(const TRule_role_name& node, TDeferredAtom& result, bool allowSystemRoles) {
    // role_name: an_id_or_type | bind_parameter;
    switch (node.Alt_case()) {
        case TRule_role_name::kAltRoleName1:
        {
            TString name = Id(node.GetAlt_role_name1().GetRule_an_id_or_type1(), *this);
            result = TDeferredAtom(Ctx.Pos(), name);
            break;
        }
        case TRule_role_name::kAltRoleName2:
        {
            if (!BindParameterClause(node.GetAlt_role_name2().GetRule_bind_parameter1(), result)) {
                return false;
            }
            break;
        }
        case TRule_role_name::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }

    if (auto literalName = result.GetLiteral(); literalName && !allowSystemRoles) {
        static const THashSet<TStringBuf> systemRoles = { "current_role", "current_user", "session_user" };
        if (systemRoles.contains(to_lower(*literalName))) {
            Ctx.Error() << "System role " << to_upper(*literalName) << " can not be used here";
            return false;
        }
    }

    return true;
}

bool TSqlTranslation::RoleParameters(const TRule_create_user_option& node, TRoleParameters& result) {
    // create_user_option: ENCRYPTED? PASSWORD expr;
    result = TRoleParameters{};

    TSqlExpression expr(Ctx, Mode);
    TNodePtr password = expr.Build(node.GetRule_expr3());
    if (!password) {
        return false;
    }

    result.IsPasswordEncrypted = node.HasBlock1();
    if (!password->IsNull()) {
        result.Password = MakeAtomFromExpression(Ctx.Pos(), Ctx, password);
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

    auto handleOneIdentifier = [&result, this] (const auto& permissionNameKeyword) {
        result = TDeferredAtom(Ctx.Pos(), GetIdentifier(*this, permissionNameKeyword).Name);
    };

    auto handleTwoIdentifiers = [&result, this] (const auto& permissionNameKeyword) {
        const auto& token1 = permissionNameKeyword.GetToken1();
        const auto& token2 = permissionNameKeyword.GetToken2();
        TString identifierName = TIdentifier(TPosition(token1.GetColumn(), token1.GetLine()), Identifier(token1)).Name +
                                "_" +
                                TIdentifier(TPosition(token2.GetColumn(), token2.GetLine()), Identifier(token2)).Name;
        result = TDeferredAtom(Ctx.Pos(), identifierName);
    };

    auto handleOneOrTwoIdentifiers = [&result, this] (const auto& permissionNameKeyword) {
        TString identifierName = GetIdentifier(*this, permissionNameKeyword).Name;
        if (permissionNameKeyword.HasBlock2()) {
            identifierName += "_" + GetIdentifier(*this, permissionNameKeyword.GetBlock2()).Name;
        }
        result = TDeferredAtom(Ctx.Pos(), identifierName);
    };

    switch (node.GetAltCase()) {
        case TRule_permission_id::kAltPermissionId1:
        {
            // CONNECT
            handleOneIdentifier(node.GetAlt_permission_id1());
            break;
        }
        case TRule_permission_id::kAltPermissionId2:
        {
            // LIST
            handleOneIdentifier(node.GetAlt_permission_id2());
            break;
        }
        case TRule_permission_id::kAltPermissionId3:
        {
            // INSERT
            handleOneIdentifier(node.GetAlt_permission_id3());
            break;
        }
        case TRule_permission_id::kAltPermissionId4:
        {
            // MANAGE
            handleOneIdentifier(node.GetAlt_permission_id4());
            break;
        }
        case TRule_permission_id::kAltPermissionId5:
        {
            // DROP
            handleOneIdentifier(node.GetAlt_permission_id5());
            break;
        }
        case TRule_permission_id::kAltPermissionId6:
        {
            // GRANT
            handleOneIdentifier(node.GetAlt_permission_id6());
            break;
        }
        case TRule_permission_id::kAltPermissionId7:
        {
            // MODIFY (TABLES | ATTRIBUTES)
            handleTwoIdentifiers(node.GetAlt_permission_id7());
            break;
        }
        case TRule_permission_id::kAltPermissionId8:
        {
            // (UPDATE | ERASE) ROW
            handleTwoIdentifiers(node.GetAlt_permission_id8());
            break;
        }
        case TRule_permission_id::kAltPermissionId9:
        {
            // (REMOVE | DESCRIBE | ALTER) SCHEMA
            handleTwoIdentifiers(node.GetAlt_permission_id9());
            break;
        }
        case TRule_permission_id::kAltPermissionId10:
        {
            // SELECT (TABLES | ATTRIBUTES | ROW)?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id10());
            break;
        }
        case TRule_permission_id::kAltPermissionId11:
        {
            // (USE | FULL) LEGACY?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id11());
            break;
        }
        case TRule_permission_id::kAltPermissionId12:
        {
            // CREATE (DIRECTORY | TABLE | QUEUE)?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id12());
            break;
        }
        case TRule_permission_id::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    return true;
}

bool TSqlTranslation::PermissionNameClause(const TRule_permission_name& node, TDeferredAtom& result) {
    // permission_name: permission_id | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_permission_name::kAltPermissionName1:
        {
            return PermissionNameClause(node.GetAlt_permission_name1().GetRule_permission_id1(), result);
            break;
        }
        case TRule_permission_name::kAltPermissionName2:
        {
            const TString stringValue(Ctx.Token(node.GetAlt_permission_name2().GetToken1()));
            auto unescaped = StringContent(Ctx, Ctx.Pos(), stringValue);
            if (!unescaped) {
                return false;
            }
            result = TDeferredAtom(Ctx.Pos(), unescaped->Content);
            break;
        }
        case TRule_permission_name::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    return true;
}

bool TSqlTranslation::PermissionNameClause(const TRule_permission_name_target& node, TVector<TDeferredAtom>& result, bool withGrantOption) {
    // permission_name_target: permission_name (COMMA permission_name)* COMMA? | ALL PRIVILEGES?;
    switch (node.Alt_case()) {
        case TRule_permission_name_target::kAltPermissionNameTarget1:
        {
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
        case TRule_permission_name_target::kAltPermissionNameTarget2:
        {
            result.emplace_back(Ctx.Pos(), "all_privileges");
            break;
        }
        case TRule_permission_name_target::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    if (withGrantOption) {
        result.emplace_back(Ctx.Pos(), "grant");
    }
    return true;
}

bool TSqlTranslation::StoreStringSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result) {
    YQL_ENSURE(value);

    const TString key = to_lower(id.Name);
    if (result.find(key) != result.end()) {
        Ctx.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    switch (value->Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2:
            return StoreString(*value, result[key], Ctx, to_upper(key));

        default:
            Ctx.Error() << to_upper(key) << " value should be a string literal";
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
                Y_ABORT("You should change implementation according to grammar changes");
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

TString TSqlTranslation::FrameSettingsToString(EFrameSettings settings, bool isUnbounded) {
    TString result;
    switch (settings) {
        case FramePreceding:
            result = "PRECEDING"; break;
        case FrameCurrentRow:
            YQL_ENSURE(!isUnbounded);
            result = "CURRENT ROW"; break;
        case FrameFollowing:
            result = "FOLLOWING"; break;
        default:
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
            ctx.Warning(begin.Bound->GetPos(), TIssuesIds::YQL_EMPTY_WINDOW_FRAME) << "Used frame specification implies empty window frame";
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
                    TSqlExpression boundExpr(Ctx, Mode);
                    bound->Bound = boundExpr.Build(block.GetAlt1().GetRule_expr1());
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
                    Y_ABORT("You should change implementation according to grammar changes");
            }

            const TString settingToken = to_lower(Token(rule.GetAlt_window_frame_bound2().GetToken2()));
            if (settingToken == "preceding") {
                bound->Settings = FramePreceding;
            } else if (settingToken == "following") {
                bound->Settings = FrameFollowing;
            } else {
                Y_ABORT("You should change implementation according to grammar changes");
            }
            break;
        }
        case TRule_window_frame_bound::ALT_NOT_SET:
            Y_ABORT("FrameClause: frame bound not corresond to grammar changes");
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
            Y_ABORT("FrameClause: frame extent not correspond to grammar changes");
    }
    YQL_ENSURE(frameSpec->FrameBegin);
    YQL_ENSURE(frameSpec->FrameEnd);
    if (!IsValidFrameSettings(Ctx, *frameSpec, sortSpecSize)) {
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
                Y_ABORT("FrameClause: frame exclusion not correspond to grammar changes");
        }
    }

    if (frameSpec->FrameExclusion != FrameExclNone) {
        Ctx.Error() << "Frame exclusion is not supported yet";
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
        Ctx.Error() << "Existing window name is not supported in window specification yet!";
        return {};
    }
    if (rule.HasBlock2()) {
        /*
        window_partition_clause: PARTITION COMPACT? BY named_expr_list;
        */
        auto& partitionClause = rule.GetBlock2().GetRule_window_partition_clause1();
        winSpecPtr->IsCompact = partitionClause.HasBlock2();
        if (!winSpecPtr->IsCompact) {
            auto hints = Ctx.PullHintForToken(Ctx.TokenPosition(partitionClause.GetToken1()));
            winSpecPtr->IsCompact = AnyOf(hints, [](const NSQLTranslation::TSQLHint& hint) { return to_lower(hint.Name) == "compact"; });
        }
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
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
        winSpecPtr->Frame->FrameBegin->Pos = winSpecPtr->Frame->FrameEnd->Pos = Ctx.Pos();
        winSpecPtr->Frame->FrameExclusion = EFrameExclusions::FrameExclNone;

        winSpecPtr->Frame->FrameBegin->Settings = EFrameSettings::FramePreceding;
        if (Ctx.AnsiCurrentRow) {
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
    auto replaceCurrentWith = [](TFrameBound& frame, bool preceding, TNodePtr value ) {
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
            action = BuildEmptyAction(Ctx.Pos());
            break;
        case TRule_call_action_TBlock1::ALT_NOT_SET:
            Ctx.IncrementMonCounter("sql_errors", "UnknownDoStmt");
            AltNotImplemented("do_stmt", callAction.GetBlock1());
            return nullptr;
        }

        TVector<TNodePtr> values;
        values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "Apply", TNodeFlags::Default));
        values.push_back(action);
        values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "world", TNodeFlags::Default));

        TSqlExpression sqlExpr(Ctx, Mode);
        if (callAction.HasBlock3() && !ExprList(sqlExpr, values, callAction.GetBlock3().GetRule_expr_list1())) {
            return nullptr;
        }

        TNodePtr apply = new TAstListNodeImpl(Ctx.Pos(), std::move(values));
        if (!makeLambda) {
            return BuildDoCall(Ctx.Pos(), apply);
        }

        TNodePtr params = new TAstListNodeImpl(Ctx.Pos());
        params->Add("world");
        for (const auto& arg : args) {
            params->Add(new TAstAtomNodeImpl(Ctx.Pos(), arg, TNodeFlags::ArbitraryContent));
        }

        return BuildDoCall(Ctx.Pos(), BuildLambda(Ctx.Pos(), params, apply));
    }
    case TRule_do_stmt_TBlock2::kAlt2: {
        const auto& inlineAction = stmt.GetBlock2().GetAlt2().GetRule_inline_action1();
        const auto& body = inlineAction.GetRule_define_action_or_subquery_body2();

        auto saveScoped = Ctx.Scoped;
        Ctx.Scoped = MakeIntrusive<TScopedState>();
        Ctx.AllScopes.push_back(Ctx.Scoped);
        *Ctx.Scoped = *saveScoped;
        Ctx.Scoped->Local = TScopedState::TLocal{};
        Ctx.ScopeLevel++;
        TSqlQuery query(Ctx, Ctx.Settings.Mode, false);
        TBlocks innerBlocks;

        const bool hasValidBody = DefineActionOrSubqueryBody(query, innerBlocks, body);
        auto ret = hasValidBody ? BuildQuery(Ctx.Pos(), innerBlocks, false, Ctx.Scoped) : nullptr;
        WarnUnusedNodes();
        Ctx.ScopeLevel--;
        Ctx.Scoped = saveScoped;

        if (!ret) {
            return {};
        }

        TNodePtr blockNode = new TAstListNodeImpl(Ctx.Pos());
        blockNode->Add("block");
        blockNode->Add(blockNode->Q(ret));
        if (!makeLambda) {
            return blockNode;
        }

        TNodePtr params = new TAstListNodeImpl(Ctx.Pos());
        params->Add("world");
        for (const auto& arg : args) {
            params->Add(new TAstAtomNodeImpl(Ctx.Pos(), arg, TNodeFlags::ArbitraryContent));
        }

        return BuildLambda(Ctx.Pos(), params, blockNode);
    }
    case TRule_do_stmt_TBlock2::ALT_NOT_SET:
        Y_ABORT("You should change implementation according to grammar changes");
    }
}

bool TSqlTranslation::DefineActionOrSubqueryBody(TSqlQuery& query, TBlocks& blocks, const TRule_define_action_or_subquery_body& body) {
    if (body.HasBlock2()) {
        Ctx.PushCurrentBlocks(&blocks);
        Y_DEFER {
            Ctx.PopCurrentBlocks();
        };
        if (!query.Statement(blocks, body.GetBlock2().GetRule_sql_stmt_core1())) {
            return false;
        }

        for (const auto& nestedStmtItem : body.GetBlock2().GetBlock2()) {
            const auto& nestedStmt = nestedStmtItem.GetRule_sql_stmt_core2();
            if (!query.Statement(blocks, nestedStmt)) {
                return false;
            }
        }
    }

    return true;
}

bool TSqlTranslation::DefineActionOrSubqueryStatement(const TRule_define_action_or_subquery_stmt& stmt, TSymbolNameWithPos& nameAndPos, TNodePtr& lambda) {
    auto kind = Ctx.Token(stmt.GetToken2());
    const bool isSubquery = to_lower(kind) == "subquery";
    if (!isSubquery && Mode == NSQLTranslation::ESqlMode::SUBQUERY) {
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
    TPosition actionNamePos = Ctx.Pos();

    TVector<TSymbolNameWithPos> argNames;
    ui32 optionalArgumentsCount = 0;
    if (stmt.HasBlock5() && !ActionOrSubqueryArgs(stmt.GetBlock5().GetRule_action_or_subquery_args1(), argNames, optionalArgumentsCount)) {
        return false;
    }

    auto saveScoped = Ctx.Scoped;
    Ctx.Scoped = MakeIntrusive<TScopedState>();
    Ctx.AllScopes.push_back(Ctx.Scoped);
    *Ctx.Scoped = *saveScoped;
    Ctx.Scoped->Local = TScopedState::TLocal{};
    Ctx.ScopeLevel++;

    for (auto& arg : argNames) {
        arg.Name = PushNamedAtom(arg.Pos, arg.Name);
    }

    auto saveMode = Ctx.Settings.Mode;
    if (isSubquery) {
        Ctx.Settings.Mode = NSQLTranslation::ESqlMode::SUBQUERY;
    }

    TSqlQuery query(Ctx, Ctx.Settings.Mode, false);
    TBlocks innerBlocks;
    const bool hasValidBody = DefineActionOrSubqueryBody(query, innerBlocks, stmt.GetRule_define_action_or_subquery_body8());

    ui32 topLevelSelects = 0;
    bool hasTailOps = false;
    for (auto& block : innerBlocks) {
        if (block->SubqueryAlias()) {
            continue;
        }

        if (block->HasSelectResult()) {
            ++topLevelSelects;
        } else if (topLevelSelects) {
            hasTailOps = true;
        }
    }

    if (isSubquery && (topLevelSelects != 1 || hasTailOps)) {
        Error() << "Strictly one select/process/reduce statement is expected at the end of subquery";
        return false;
    }

    auto ret = hasValidBody ? BuildQuery(Ctx.Pos(), innerBlocks, false, Ctx.Scoped) : nullptr;
    WarnUnusedNodes();
    Ctx.Scoped = saveScoped;
    Ctx.ScopeLevel--;
    Ctx.Settings.Mode = saveMode;

    if (!ret) {
        return false;
    }

    TNodePtr blockNode = new TAstListNodeImpl(Ctx.Pos());
    blockNode->Add("block");
    blockNode->Add(blockNode->Q(ret));

    TNodePtr params = new TAstListNodeImpl(Ctx.Pos());
    params->Add("world");
    for (const auto& arg : argNames) {
        params->Add(BuildAtom(arg.Pos, arg.Name));
    }

    lambda = BuildLambda(Ctx.Pos(), params, blockNode);
    if (optionalArgumentsCount > 0) {
        lambda = new TCallNodeImpl(Ctx.Pos(), "WithOptionalArgs", {
            lambda,
            BuildQuotedAtom(Ctx.Pos(), ToString(optionalArgumentsCount), TNodeFlags::Default)
            });
    }

    nameAndPos.Name = actionName;
    nameAndPos.Pos = actionNamePos;
    return true;
}

TNodePtr TSqlTranslation::IfStatement(const TRule_if_stmt& stmt) {
    bool isEvaluate = stmt.HasBlock1();
    TSqlExpression expr(Ctx, Mode);
    auto exprNode = expr.Build(stmt.GetRule_expr3());
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

    return BuildWorldIfNode(Ctx.Pos(), exprNode, thenNode, elseNode, isEvaluate);
}

TNodePtr TSqlTranslation::ForStatement(const TRule_for_stmt& stmt) {
    bool isEvaluate = stmt.HasBlock1();
    bool isParallel = stmt.HasBlock2();
    TSqlExpression expr(Ctx, Mode);
    TString itemArgName;
    if (!NamedNodeImpl(stmt.GetRule_bind_parameter4(), itemArgName, *this)) {
        return {};
    }
    TPosition itemArgNamePos = Ctx.Pos();

    auto exprNode = expr.Build(stmt.GetRule_expr6());
    if (!exprNode) {
        return{};
    }

    itemArgName = PushNamedAtom(itemArgNamePos, itemArgName);
    if (isParallel) {
        ++Ctx.ParallelModeCount;
    }

    auto bodyNode = DoStatement(stmt.GetRule_do_stmt7(), true, { itemArgName });
    if (isParallel) {
        --Ctx.ParallelModeCount;
    }

    PopNamedNode(itemArgName);
    if (!bodyNode) {
        return{};
    }

    TNodePtr elseNode;
    if (stmt.HasBlock8()) {
        elseNode = DoStatement(stmt.GetBlock8().GetRule_do_stmt2(), true);
        if (!elseNode) {
            return{};
        }
    }

    return BuildWorldForNode(Ctx.Pos(), exprNode, bodyNode, elseNode, isEvaluate, isParallel);
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

    result = MakeAtomFromExpression(Ctx.Pos(), Ctx, named);
    return true;
}

bool TSqlTranslation::ObjectFeatureValueClause(const TRule_object_feature_value& node, TDeferredAtom& result) {
    // object_feature_value: an_id_or_type | bind_parameter;
    switch (node.Alt_case()) {
        case TRule_object_feature_value::kAltObjectFeatureValue1:
        {
            TString name = Id(node.GetAlt_object_feature_value1().GetRule_id_or_type1(), *this);
            result = TDeferredAtom(Ctx.Pos(), name);
            break;
        }
        case TRule_object_feature_value::kAltObjectFeatureValue2:
        {
            if (!BindParameterClause(node.GetAlt_object_feature_value2().GetRule_bind_parameter1(), result)) {
                return false;
            }
            break;
        }
        case TRule_object_feature_value::kAltObjectFeatureValue3:
        {
            auto strValue = StringContent(Ctx, Ctx.Pos(), Ctx.Token(node.GetAlt_object_feature_value3().GetToken1()));
            if (!strValue) {
                Error() << "Cannot parse string correctly: " << Ctx.Token(node.GetAlt_object_feature_value3().GetToken1());
                return false;
            }
            result = TDeferredAtom(Ctx.Pos(), strValue->Content);
            break;
        }
        case TRule_object_feature_value::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
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
        Ctx.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    if (!StoreString(*value, result[key], Ctx, to_upper(key))) {
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
        Ctx.Error() << "SOURCE_TYPE requires key";
        return false;
    }
    if (!ValidateAuthMethod(result)) {
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
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

bool TSqlTranslation::ValidateAuthMethod(const std::map<TString, TDeferredAtom>& result) {
    const static TSet<TStringBuf> allAuthFields{
        "service_account_id",
        "service_account_secret_name",
        "login",
        "password_secret_name",
        "aws_access_key_id_secret_name",
        "aws_secret_access_key_secret_name",
        "aws_region",
        "token_secret_name"
    };
    const static TMap<TStringBuf, TSet<TStringBuf>> authMethodFields{
        {"NONE", {}},
        {"SERVICE_ACCOUNT", {"service_account_id", "service_account_secret_name"}},
        {"BASIC", {"login", "password_secret_name"}},
        {"AWS", {"aws_access_key_id_secret_name", "aws_secret_access_key_secret_name", "aws_region"}},
        {"MDB_BASIC", {"service_account_id", "service_account_secret_name", "login", "password_secret_name"}},
        {"TOKEN", {"token_secret_name"}}
    };
    auto authMethodIt = result.find("auth_method");
    if (authMethodIt == result.end() || authMethodIt->second.GetLiteral() == nullptr) {
        Ctx.Error() << "AUTH_METHOD requires key";
        return false;
    }
    const auto& authMethod = *authMethodIt->second.GetLiteral();
    auto it = authMethodFields.find(authMethod);
    if (it == authMethodFields.end()) {
        Ctx.Error() << "Unknown AUTH_METHOD = " << authMethod;
        return false;
    }
    const auto& currentAuthFields = it->second;
    for (const auto& authField: allAuthFields) {
        if (currentAuthFields.contains(authField) && !result.contains(TString{authField})) {
            Ctx.Error() << to_upper(TString{authField}) << " requires key";
            return false;
        }
        if (!currentAuthFields.contains(authField) && result.contains(TString{authField})) {
            Ctx.Error() << to_upper(TString{authField}) << " key is not supported for AUTH_METHOD = " << authMethod;
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::ValidateExternalTable(const TCreateTableParameters& params) {
    if (params.TableType != ETableType::ExternalTable) {
        return true;
    }

    if (!params.TableSettings.DataSourcePath) {
        Ctx.Error() << "DATA_SOURCE requires key";
        return false;
    }

    if (!params.TableSettings.Location) {
        Ctx.Error() << "LOCATION requires key";
        return false;
    }

    if (params.PkColumns) {
        Ctx.Error() << "PRIMARY KEY is not supported for external table";
        return false;
    }

    return true;
}

bool TSqlTranslation::ParseViewOptions(std::map<TString, TDeferredAtom>& features,
                                       const TRule_with_table_settings& options) {
    const auto& firstEntry = options.GetRule_table_settings_entry3();
    if (!StoreViewOptionsEntry(IdEx(firstEntry.GetRule_an_id1(), *this),
                               firstEntry.GetRule_table_setting_value3(),
                               features,
                               Ctx)) {
        return false;
    }
    for (const auto& block : options.GetBlock4()) {
        const auto& entry = block.GetRule_table_settings_entry2();
        if (!StoreViewOptionsEntry(IdEx(entry.GetRule_an_id1(), *this),
                                   entry.GetRule_table_setting_value3(),
                                   features,
                                   Ctx)) {
            return false;
        }
    }
    if (const auto securityInvoker = features.find("security_invoker");
        securityInvoker == features.end() || securityInvoker->second.Build()->GetLiteralValue() != "true") {
        Ctx.Error(Ctx.Pos()) << "SECURITY_INVOKER option must be explicitly enabled";
        return false;
    }
    return true;
}

bool TSqlTranslation::ParseViewQuery(std::map<TString, TDeferredAtom>& features,
                                     const TRule_select_stmt& query) {
    const TString queryText = CollectTokens(query);
    features["query_text"] = {Ctx.Pos(), queryText};

    const auto viewSelect = BuildViewSelect(query, Ctx);
    if (!viewSelect) {
        return false;
    }
    features["query_ast"] = {viewSelect, Ctx};

    return true;
}

class TReturningListColumns : public INode {
public:
    TReturningListColumns(TPosition pos)
        : INode(pos)
    {
    }

    void SetStar() {
        ColumnNames.clear();
        Star = true;
    }

    void AddColumn(const NSQLv1Generated::TRule_an_id & rule, TTranslation& ctx) {
        ColumnNames.push_back(NSQLTranslationV1::Id(rule, ctx));
    }

    bool DoInit(TContext& ctx, ISource* source) override {
        Node = Y();
        if (Star) {
            Node->Add(Y("ReturningStar"));
        } else {
            for (auto&& column : ColumnNames) {
                Node->Add(Y("ReturningListItem", Q(column)));
            }
        }
        Node = Q(Y(Q("returning"), Q(Node)));
        return Node->Init(ctx, source);
    }

    TNodePtr DoClone() const override {
        return new TReturningListColumns(GetPos());
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Node->Translate(ctx);
    }

private:
    TNodePtr Node;
    TVector<TString> ColumnNames;
    bool Star = false;
};

TNodePtr TSqlTranslation::ReturningList(const ::NSQLv1Generated::TRule_returning_columns_list& columns) {
    auto result = MakeHolder<TReturningListColumns>(Ctx.Pos());

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
        Ctx.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    switch (value->Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2:
            return StoreString(*value, result[key], Ctx, to_upper(key));

        case TRule_table_setting_value::kAltTableSettingValue3:
            return StoreInt(*value, result[key], Ctx, to_upper(key));

        default:
            Ctx.Error() << to_upper(key) << " value should be a string literal or integer";
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
            const auto& action = alterAction.GetAlt_alter_resource_pool_action1().GetRule_alter_table_set_table_setting_uncompat1();
            if (!StoreResourcePoolSettingsEntry(IdEx(action.GetRule_an_id2(), *this), &action.GetRule_table_setting_value3(), result)) {
                return false;
            }
            return true;
        }
        case TRule_alter_resource_pool_action::kAltAlterResourcePoolAction2: {
            const auto& action = alterAction.GetAlt_alter_resource_pool_action2().GetRule_alter_table_set_table_setting_compat1();
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
        case TRule_alter_resource_pool_action::kAltAlterResourcePoolAction3: {
            const auto& action = alterAction.GetAlt_alter_resource_pool_action3().GetRule_alter_table_reset_table_setting1();
            const TString firstKey = to_lower(IdEx(action.GetRule_an_id3(), *this).Name);
            toReset.insert(firstKey);
            for (const auto& key : action.GetBlock4()) {
                toReset.insert(to_lower(IdEx(key.GetRule_an_id2(), *this).Name));
            }
            return true;
        }
        case TRule_alter_resource_pool_action::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
}

} // namespace NSQLTranslationV1
