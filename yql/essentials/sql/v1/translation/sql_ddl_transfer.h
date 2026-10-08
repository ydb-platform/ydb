#pragma once

#include "ddl_transfer.h"
#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TSqlExpression;

class TTransferTranslation final: public TSqlTranslation {
public:
    TTransferTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_create_transfer_stmt& node);
    TNodePtr Build(const TRule_alter_transfer_stmt& node);
    TNodePtr Build(const TRule_drop_transfer_stmt& node);

private:
    bool TransferSettingsEntry(std::map<TString, TNodePtr>& out,
                               const TRule_transfer_settings_entry& in, TSqlExpression& ctx, bool create);
    bool TransferSettings(std::map<TString, TNodePtr>& out,
                          const TRule_transfer_settings& in, TSqlExpression& ctx, bool create,
                          const TStringBuf& tablePathPrefix);
    bool ParseTransferLambda(TString& lambdaText, const TRule_lambda_or_parameter& lambdaOrParameter);
};

} // namespace NSQLTranslationV1
