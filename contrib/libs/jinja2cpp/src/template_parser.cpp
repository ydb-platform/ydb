#include "template_parser.h"
#include "renderer.h"
#include <boost/cast.hpp>

namespace jinja2
{

StatementsParser::ParseResult StatementsParser::Parse(LexScanner& lexer, StatementInfoList& statementsInfo)
{
    Token tok = lexer.NextToken();
    ParseResult result;

    switch (lexer.GetAsKeyword(tok))
    {
    case Keyword::For:
        result = ParseFor(lexer, statementsInfo, tok);
        break;
    case Keyword::Endfor:
        result = ParseEndFor(lexer, statementsInfo, tok);
        break;
    case Keyword::If:
        result = ParseIf(lexer, statementsInfo, tok);
        break;
    case Keyword::Else:
        result = ParseElse(lexer, statementsInfo, tok);
        break;
    case Keyword::ElIf:
        result = ParseElIf(lexer, statementsInfo, tok);
        break;
    case Keyword::EndIf:
        result = ParseEndIf(lexer, statementsInfo, tok);
        break;
    case Keyword::Set:
        result = ParseSet(lexer, statementsInfo, tok);
        break;
    case Keyword::EndSet:
        result = ParseEndSet(lexer, statementsInfo, tok);
        break;
    case Keyword::Block:
        result = ParseBlock(lexer, statementsInfo, tok);
        break;
    case Keyword::EndBlock:
        result = ParseEndBlock(lexer, statementsInfo, tok);
        break;
    case Keyword::Extends:
        result = ParseExtends(lexer, statementsInfo, tok);
        break;
    case Keyword::Macro:
        result = ParseMacro(lexer, statementsInfo, tok);
        break;
    case Keyword::EndMacro:
        result = ParseEndMacro(lexer, statementsInfo, tok);
        break;
    case Keyword::Call:
        result = ParseCall(lexer, statementsInfo, tok);
        break;
    case Keyword::EndCall:
        result = ParseEndCall(lexer, statementsInfo, tok);
        break;
    case Keyword::Include:
        result = ParseInclude(lexer, statementsInfo, tok);
        break;
    case Keyword::Import:
        result = ParseImport(lexer, statementsInfo, tok);
        break;
    case Keyword::From:
        result = ParseFrom(lexer, statementsInfo, tok);
        break;
    case Keyword::Do:
        if (!m_settings.extensions.Do)
            return MakeParseError(ErrorCode::ExtensionDisabled, tok);
        result = ParseDo(lexer, statementsInfo, tok);
        break;
    case Keyword::With:
        result = ParseWith(lexer, statementsInfo, tok);
        break;
    case Keyword::EndWith:
        result = ParseEndWith(lexer, statementsInfo, tok);
        break;
    case Keyword::Filter:
        result = ParseFilter(lexer, statementsInfo, tok);
        break;
    case Keyword::EndFilter:
        result = ParseEndFilter(lexer, statementsInfo, tok);
        break;
    default:
        return MakeParseError(ErrorCode::UnexpectedToken, tok);
    }

    if (result)
    {
        tok = lexer.PeekNextToken();
        if (tok != Token::Eof)
            return MakeParseError(ErrorCode::ExpectedEndOfStatement, tok);
    }

    return result;
}

struct ErrorTokenConverter
{
    const Token& baseTok;
    
    explicit ErrorTokenConverter(const Token& t)
        : baseTok(t)
    {}
    
    Token operator()(const Token& tok) const
    {
        return tok;
    }
    
    template<typename T>
    Token operator()(T tokType) const
    {
        auto newTok = baseTok;
        newTok.type = static_cast<Token::Type>(tokType);
        if (newTok.type == Token::Identifier || newTok.type == Token::String)
            newTok.range.endOffset = newTok.range.startOffset;
        return newTok;
    }
};

template<typename ... Args>
auto MakeParseErrorTL(ErrorCode code, const Token& baseTok, Args ...  expectedTokens)
{
    ErrorTokenConverter tokCvt(baseTok);
    
    return MakeParseError(code, baseTok, {tokCvt(expectedTokens)...});
}

StatementsParser::ParseResult StatementsParser::ParseFor(LexScanner &lexer, StatementInfoList &statementsInfo,
                                                         const Token &stmtTok)
{
    std::vector<std::string> vars;

    while (lexer.PeekNextToken() == Token::Identifier)
    {
        auto tok = lexer.NextToken();
        vars.push_back(AsString(tok.value));
        if (lexer.NextToken() != ',')
        {
            lexer.ReturnToken();
            break;
        }
    }

    if (vars.empty())
        return MakeParseError(ErrorCode::ExpectedIdentifier, lexer.PeekNextToken());

    if (!lexer.EatIfEqual(Keyword::In))
    {
        Token tok1 = lexer.PeekNextToken();
        Token tok2 = tok1;
        tok2.type = Token::Identifier;
        tok2.range.endOffset = tok2.range.startOffset;
        tok2.value = InternalValue();
        return MakeParseErrorTL(ErrorCode::ExpectedToken, tok1, tok2, Token::In, ',');
    }

    auto pivotToken = lexer.PeekNextToken();
    ExpressionParser exprPraser(m_settings);
    auto valueExpr = exprPraser.ParseFullExpression(lexer, false);
    if (!valueExpr)
        return valueExpr.get_unexpected();
        // return MakeParseError(ErrorCode::ExpectedExpression, pivotToken);

    Token flagsTok;
    bool isRecursive = false;
    if (lexer.EatIfEqual(Keyword::Recursive, &flagsTok))
    {
        isRecursive = true;
    }

    ExpressionEvaluatorPtr<> ifExpr;
    if (lexer.EatIfEqual(Keyword::If))
    {
        auto parsedExpr = exprPraser.ParseFullExpression(lexer, false);
        if (!parsedExpr)
            return parsedExpr.get_unexpected();
        ifExpr = *parsedExpr;
    }
    else if (lexer.PeekNextToken() != Token::Eof)
    {
        auto tok1 = lexer.PeekNextToken();
        return MakeParseErrorTL(ErrorCode::ExpectedToken, tok1, Token::If, Token::Recursive, Token::Eof);
    }

    auto renderer = std::make_shared<ForStatement>(vars, *valueExpr, ifExpr, isRecursive);
    StatementInfo statementInfo = StatementInfo::Create(StatementInfo::ForStatement, stmtTok);
    statementInfo.renderer = renderer;
    statementsInfo.push_back(statementInfo);
    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseEndFor(LexScanner&, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.size() <= 1)
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    StatementInfo info = statementsInfo.back();
    RendererPtr elseRenderer;
    if (info.type == StatementInfo::ElseIfStatement)
    {
        auto r = std::static_pointer_cast<ElseBranchStatement>(info.renderer);
        r->SetMainBody(info.compositions[0]);
        elseRenderer = std::static_pointer_cast<IRendererBase>(r);

        statementsInfo.pop_back();
        info = statementsInfo.back();
    }

    if (info.type != StatementInfo::ForStatement)
    {
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);
    }

    statementsInfo.pop_back();
    auto renderer = static_cast<ForStatement*>(info.renderer.get());
    renderer->SetMainBody(info.compositions[0]);
    if (elseRenderer)
        renderer->SetElseBody(elseRenderer);

    statementsInfo.back().currentComposition->AddRenderer(info.renderer);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseIf(LexScanner &lexer, StatementInfoList &statementsInfo,
                                                        const Token &stmtTok)
{
    auto pivotTok = lexer.PeekNextToken();
    ExpressionParser exprParser(m_settings);
    auto valueExpr = exprParser.ParseFullExpression(lexer);
    if (!valueExpr)
        return MakeParseError(ErrorCode::ExpectedExpression, pivotTok);

    auto renderer = std::make_shared<IfStatement>(*valueExpr);
    StatementInfo statementInfo = StatementInfo::Create(StatementInfo::IfStatement, stmtTok);
    statementInfo.renderer = renderer;
    statementsInfo.push_back(statementInfo);
    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseElse(LexScanner& /*lexer*/, StatementInfoList& statementsInfo
                                                          , const Token& stmtTok)
{
    auto renderer = std::make_shared<ElseBranchStatement>(ExpressionEvaluatorPtr<>());
    StatementInfo statementInfo = StatementInfo::Create(StatementInfo::ElseIfStatement, stmtTok);
    statementInfo.renderer = std::static_pointer_cast<IRendererBase>(renderer);
    statementsInfo.push_back(statementInfo);
    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseElIf(LexScanner& lexer, StatementInfoList& statementsInfo
                                                          , const Token& stmtTok)
{
    auto pivotTok = lexer.PeekNextToken();
    ExpressionParser exprParser(m_settings);
    auto valueExpr = exprParser.ParseFullExpression(lexer);
    if (!valueExpr)
        return MakeParseError(ErrorCode::ExpectedExpression, pivotTok);

    auto renderer = std::make_shared<ElseBranchStatement>(*valueExpr);
    StatementInfo statementInfo = StatementInfo::Create(StatementInfo::ElseIfStatement, stmtTok);
    statementInfo.renderer = std::static_pointer_cast<IRendererBase>(renderer);
    statementsInfo.push_back(statementInfo);
    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseEndIf(LexScanner&, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.size() <= 1)
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    auto info = statementsInfo.back();
    statementsInfo.pop_back();

    std::list<StatementPtr<ElseBranchStatement>> elseBranches;

    auto errorTok = stmtTok;
    while (info.type != StatementInfo::IfStatement)
    {
        if (info.type != StatementInfo::ElseIfStatement)
            return MakeParseError(ErrorCode::UnexpectedStatement, errorTok);

        auto elseRenderer = std::static_pointer_cast<ElseBranchStatement>(info.renderer);
        elseRenderer->SetMainBody(info.compositions[0]);

        elseBranches.push_front(elseRenderer);
        errorTok = info.token;
        info = statementsInfo.back();
        statementsInfo.pop_back();
    }

    auto renderer = static_cast<IfStatement*>(info.renderer.get());
    renderer->SetMainBody(info.compositions[0]);

    for (auto& b : elseBranches)
        renderer->AddElseBranch(b);

    statementsInfo.back().currentComposition->AddRenderer(info.renderer);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseSet(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    std::vector<std::string> vars;

    while (lexer.PeekNextToken() == Token::Identifier)
    {
        auto tok = lexer.NextToken();
        vars.push_back(AsString(tok.value));
        if (lexer.NextToken() != ',')
        {
            lexer.ReturnToken();
            break;
        }
    }

    if (vars.empty())
        return MakeParseError(ErrorCode::ExpectedIdentifier, lexer.PeekNextToken());

    ExpressionParser exprParser(m_settings);
    if (lexer.EatIfEqual('='))
    {
        const auto expr = exprParser.ParseFullExpression(lexer);
        if (!expr)
            return expr.get_unexpected();
        statementsInfo.back().currentComposition->AddRenderer(
            std::make_shared<SetLineStatement>(std::move(vars), *expr));
    }
    else if (lexer.EatIfEqual('|'))
    {
         const auto expr = exprParser.ParseFilterExpression(lexer);
         if (!expr)
            return expr.get_unexpected();
         auto statementInfo = StatementInfo::Create(
            StatementInfo::SetStatement, stmtTok);
         statementInfo.renderer = std::make_shared<SetFilteredBlockStatement>(
            std::move(vars), *expr);
         statementsInfo.push_back(std::move(statementInfo));
    }
    else
    {
        auto operTok = lexer.NextToken();
        if (lexer.NextToken() != Token::Eof)
            return MakeParseError(ErrorCode::YetUnsupported, operTok, {std::move(stmtTok)});
        auto statementInfo = StatementInfo::Create(
            StatementInfo::SetStatement, stmtTok);
        statementInfo.renderer = std::make_shared<SetRawBlockStatement>(
            std::move(vars));
        statementsInfo.push_back(std::move(statementInfo));
    }

    return {};
}

StatementsParser::ParseResult StatementsParser::ParseEndSet(LexScanner&
                                                            , StatementInfoList& statementsInfo
                                                            , const Token& stmtTok)
{
    if (statementsInfo.size() <= 1)
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    const auto info = statementsInfo.back();
    if (info.type != StatementInfo::SetStatement)
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    auto &renderer = *boost::polymorphic_downcast<SetBlockStatement*>(
        info.renderer.get());
    renderer.SetBody(info.compositions[0]);

    statementsInfo.pop_back();
    statementsInfo.back().currentComposition->AddRenderer(info.renderer);

    return {};
}

StatementsParser::ParseResult StatementsParser::ParseBlock(LexScanner& lexer, StatementInfoList& statementsInfo
                                                           , const Token& stmtTok)
{
    if (statementsInfo.empty())
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    Token nextTok = lexer.NextToken();
    if (nextTok != Token::Identifier)
        return MakeParseError(ErrorCode::ExpectedIdentifier, nextTok);

    std::string blockName = AsString(nextTok.value);

    auto& info = statementsInfo.back();
    RendererPtr blockRenderer;
    StatementInfo::Type blockType = StatementInfo::ParentBlockStatement;
    if (info.type == StatementInfo::ExtendsStatement)
    {
        blockRenderer = std::make_shared<BlockStatement>(blockName);
        blockType = StatementInfo::BlockStatement;
    }
    else
    {
        bool isScoped = false;
        if (lexer.EatIfEqual(Keyword::Scoped, &nextTok))
            isScoped = true;
        else
        {
            nextTok = lexer.PeekNextToken();
            if (nextTok != Token::Eof)
                return MakeParseErrorTL(ErrorCode::ExpectedToken, nextTok, Token::Scoped);
        }
            
        blockRenderer = std::make_shared<ParentBlockStatement>(blockName, isScoped);
    }

    StatementInfo statementInfo = StatementInfo::Create(blockType, stmtTok);
    statementInfo.renderer = std::move(blockRenderer);
    statementsInfo.push_back(statementInfo);
    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseEndBlock(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.size() <= 1)
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    Token nextTok = lexer.PeekNextToken();
    if (nextTok != Token::Identifier && nextTok != Token::Eof)
    {
        Token tok2;
        tok2.type = Token::Identifier;
        Token tok3;
        tok3.type = Token::Eof;
        return MakeParseError(ErrorCode::ExpectedToken, nextTok, {tok2, tok3});
    }
    
    if (nextTok == Token::Identifier)
        lexer.EatToken();

    auto info = statementsInfo.back();
    statementsInfo.pop_back();

    if (info.type == StatementInfo::BlockStatement)
    {
        auto blockStmt = std::static_pointer_cast<BlockStatement>(info.renderer);
        blockStmt->SetMainBody(info.compositions[0]);
        auto& extendsInfo = statementsInfo.back();
        auto extendsStmt = std::static_pointer_cast<ExtendsStatement>(extendsInfo.renderer);
        extendsStmt->AddBlock(std::static_pointer_cast<BlockStatement>(info.renderer));
    }
    else if (info.type == StatementInfo::ParentBlockStatement)
    {
        auto blockStmt = std::static_pointer_cast<ParentBlockStatement>(info.renderer);
        blockStmt->SetMainBody(info.compositions[0]);
        statementsInfo.back().currentComposition->AddRenderer(info.renderer);
    }
    else
    {
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);
    }

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseExtends(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.empty())
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    if (!m_env)
        return MakeParseError(ErrorCode::TemplateEnvAbsent, stmtTok);

    Token tok = lexer.NextToken();
    if (tok != Token::String && tok != Token::Identifier)
    {
        auto tok2 = tok;
        tok2.type = Token::Identifier;
        tok2.range.endOffset = tok2.range.startOffset;
        tok2.value = EmptyValue{};
        return MakeParseErrorTL(ErrorCode::ExpectedToken, tok, tok2, Token::String);
    }

    auto renderer = std::make_shared<ExtendsStatement>(AsString(tok.value), tok == Token::String);
    statementsInfo.back().currentComposition->AddRenderer(renderer);

    StatementInfo statementInfo = StatementInfo::Create(StatementInfo::ExtendsStatement, stmtTok);
    statementInfo.renderer = renderer;
    statementsInfo.push_back(statementInfo);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseMacro(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.empty())
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    Token nextTok = lexer.NextToken();
    if (nextTok != Token::Identifier)
        return MakeParseError(ErrorCode::ExpectedIdentifier, nextTok);

    std::string macroName = AsString(nextTok.value);
    MacroParams macroParams;

    if (lexer.EatIfEqual('('))
    {
        auto result = ParseMacroParams(lexer);
        if (!result)
            return result.get_unexpected();

        macroParams = std::move(result.value());
    }
    else if (lexer.PeekNextToken() != Token::Eof)
    {
        Token tok = lexer.PeekNextToken();

        return MakeParseErrorTL(ErrorCode::UnexpectedToken, tok, Token::RBracket, Token::Eof);
    }

    auto renderer = std::make_shared<MacroStatement>(std::move(macroName), std::move(macroParams));
    StatementInfo statementInfo = StatementInfo::Create(StatementInfo::MacroStatement, stmtTok);
    statementInfo.renderer = renderer;
    statementsInfo.push_back(statementInfo);

    return ParseResult();
}

nonstd::expected<MacroParams, ParseError> StatementsParser::ParseMacroParams(LexScanner& lexer)
{
    MacroParams items;

    if (lexer.EatIfEqual(')'))
        return std::move(items);

    ExpressionParser exprParser(m_settings);

    do
    {
        Token name = lexer.NextToken();
        if (name != Token::Identifier)
            return MakeParseError(ErrorCode::ExpectedIdentifier, name);

        ExpressionEvaluatorPtr<> defVal;
        if (lexer.EatIfEqual('='))
        {
            auto result = exprParser.ParseFullExpression(lexer, false);
            if (!result)
                return result.get_unexpected();

            defVal = *result;
        }

        MacroParam p;
        p.paramName = AsString(name.value);
        p.defaultValue = std::move(defVal);
        items.push_back(std::move(p));

    } while (lexer.EatIfEqual(','));

    auto tok = lexer.NextToken();
    if (tok != ')')
        return MakeParseError(ErrorCode::ExpectedRoundBracket, tok);

    return std::move(items);
}

StatementsParser::ParseResult StatementsParser::ParseEndMacro(LexScanner&, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.size() <= 1)
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    StatementInfo info = statementsInfo.back();

    if (info.type != StatementInfo::MacroStatement)
    {
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);
    }

    statementsInfo.pop_back();
    auto renderer = static_cast<MacroStatement*>(info.renderer.get());
    renderer->SetMainBody(info.compositions[0]);

    statementsInfo.back().currentComposition->AddRenderer(info.renderer);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseCall(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.empty())
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    MacroParams callbackParams;

    if (lexer.EatIfEqual('('))
    {
        auto result = ParseMacroParams(lexer);
        if (!result)
            return result.get_unexpected();

        callbackParams = std::move(result.value());
    }

    Token nextTok = lexer.NextToken();
    if (nextTok != Token::Identifier)
    {
        Token tok = nextTok;
        Token tok1;
        tok1.type = Token::Identifier;

        return MakeParseError(ErrorCode::UnexpectedToken, tok, {tok1});
    }

    std::string macroName = AsString(nextTok.value);

    CallParamsInfo callParams;
    if (lexer.EatIfEqual('('))
    {
        ExpressionParser exprParser(m_settings);
        auto result = exprParser.ParseCallParams(lexer);
        if (!result)
            return result.get_unexpected();

        callParams = std::move(result.value());
    }

    auto renderer = std::make_shared<MacroCallStatement>(std::move(macroName), std::move(callParams), std::move(callbackParams));
    StatementInfo statementInfo = StatementInfo::Create(StatementInfo::MacroCallStatement, stmtTok);
    statementInfo.renderer = renderer;
    statementsInfo.push_back(statementInfo);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseEndCall(LexScanner&, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.size() <= 1)
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    StatementInfo info = statementsInfo.back();

    if (info.type != StatementInfo::MacroCallStatement)
    {
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);
    }

    statementsInfo.pop_back();
    auto renderer = static_cast<MacroCallStatement*>(info.renderer.get());
    renderer->SetMainBody(info.compositions[0]);

    statementsInfo.back().currentComposition->AddRenderer(info.renderer);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseInclude(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.empty())
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    // auto operTok = lexer.NextToken();
    ExpressionEvaluatorPtr<> valueExpr;
    ExpressionParser exprParser(m_settings);
    auto expr = exprParser.ParseFullExpression(lexer);
    if (!expr)
        return expr.get_unexpected();
    valueExpr = *expr;

    Token nextTok = lexer.PeekNextToken();
    bool isIgnoreMissing = false;
    bool isWithContext = true;
    bool hasIgnoreMissing = false;
    if (lexer.EatIfEqual(Keyword::Ignore))
    {
        if (lexer.EatIfEqual(Keyword::Missing))
            isIgnoreMissing = true;
        else
            return MakeParseErrorTL(ErrorCode::ExpectedToken, lexer.PeekNextToken(), Token::Missing);
            
        hasIgnoreMissing = true;
        nextTok = lexer.PeekNextToken();
    }

    auto kw = lexer.GetAsKeyword(nextTok);
    bool hasContextControl = false;
    if (kw == Keyword::With || kw == Keyword::Without)
    {
        lexer.EatToken();
        isWithContext = kw == Keyword::With;
        if (!lexer.EatIfEqual(Keyword::Context))
            return MakeParseErrorTL(ErrorCode::ExpectedToken, lexer.PeekNextToken(), Token::Context);
            
        nextTok = lexer.PeekNextToken();        
        hasContextControl = true;
    }

    if (nextTok != Token::Eof)
    {
        if (hasContextControl)
            return MakeParseErrorTL(ErrorCode::ExpectedEndOfStatement, nextTok, Token::Eof);

        if (hasIgnoreMissing)
            return MakeParseErrorTL(ErrorCode::UnexpectedToken, nextTok, Token::Eof, Token::With, Token::Without);

        return MakeParseErrorTL(ErrorCode::UnexpectedToken, nextTok, Token::Eof, Token::Ignore, Token::With, Token::Without);
    }

    if (!m_env && !isIgnoreMissing)
        return MakeParseError(ErrorCode::TemplateEnvAbsent, stmtTok);

    auto renderer = std::make_shared<IncludeStatement>(isIgnoreMissing, isWithContext);
    renderer->SetIncludeNamesExpr(valueExpr);
    statementsInfo.back().currentComposition->AddRenderer(renderer);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseImport(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (!m_env)
        return MakeParseError(ErrorCode::TemplateEnvAbsent, stmtTok);

    ExpressionEvaluatorPtr<> valueExpr;
    ExpressionParser exprParser(m_settings);
    auto expr = exprParser.ParseFullExpression(lexer);
    if (!expr)
        return expr.get_unexpected();
    valueExpr = *expr;

    if (!lexer.EatIfEqual(Keyword::As))
        return MakeParseErrorTL(ErrorCode::ExpectedToken, lexer.PeekNextToken(), Token::As);

    Token name;
    if (!lexer.EatIfEqual(Token::Identifier, &name))
        return MakeParseErrorTL(ErrorCode::ExpectedToken, lexer.PeekNextToken(), Token::Identifier);

    Token nextTok = lexer.PeekNextToken();
    auto kw = lexer.GetAsKeyword(nextTok);
    bool hasContextControl = false;
    bool isWithContext = false;
    if (kw == Keyword::With || kw == Keyword::Without)
    {
        lexer.EatToken();
        isWithContext = kw == Keyword::With;
        if (!lexer.EatIfEqual(Keyword::Context))
            return MakeParseErrorTL(ErrorCode::ExpectedToken, lexer.PeekNextToken(), Token::Context);

        nextTok = lexer.PeekNextToken();
        hasContextControl = true;
    }

    if (nextTok != Token::Eof)
    {
        if (hasContextControl)
            return MakeParseErrorTL(ErrorCode::ExpectedEndOfStatement, nextTok, Token::Eof);

        return MakeParseErrorTL(ErrorCode::UnexpectedToken, nextTok, Token::Eof, Token::With, Token::Without);
    }

    auto renderer = std::make_shared<ImportStatement>(isWithContext);
    renderer->SetImportNameExpr(valueExpr);
    renderer->SetNamespace(AsString(name.value));
    statementsInfo.back().currentComposition->AddRenderer(renderer);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseFrom(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (!m_env)
        return MakeParseError(ErrorCode::TemplateEnvAbsent, stmtTok);

    ExpressionEvaluatorPtr<> valueExpr;
    ExpressionParser exprParser(m_settings);
    auto expr = exprParser.ParseFullExpression(lexer);
    if (!expr)
        return expr.get_unexpected();
    valueExpr = *expr;

    if (!lexer.EatIfEqual(Keyword::Import))
        return MakeParseErrorTL(ErrorCode::ExpectedToken, lexer.PeekNextToken(), Token::Identifier);

    std::vector<std::pair<std::string, std::string>> mappedNames;

    Token nextTok;
    bool hasContextControl = false;
    bool isWithContext = false;

    for (;;)
    {
		bool hasComma = false;
        if (!mappedNames.empty())
        {
			if (!lexer.EatIfEqual(Token::Comma))
				hasComma = true;;
        }

        nextTok = lexer.PeekNextToken();
        auto kw = lexer.GetAsKeyword(nextTok);
        if (kw == Keyword::With || kw == Keyword::Without)
        {
            lexer.NextToken();
            if (lexer.EatIfEqual(Keyword::Context))
            {
                hasContextControl = true;
                isWithContext = kw == Keyword::With;
                nextTok = lexer.PeekNextToken();
                break;
            }
            else
            {
                lexer.ReturnToken();
            }
        }

		if (hasComma)
			break;

        std::pair<std::string, std::string> macroMap;
        if (!lexer.EatIfEqual(Token::Identifier, &nextTok))
            return MakeParseErrorTL(ErrorCode::ExpectedToken, nextTok, Token::Identifier);

        macroMap.first = AsString(nextTok.value);

        if (lexer.EatIfEqual(Keyword::As))
        {
            if (!lexer.EatIfEqual(Token::Identifier, &nextTok))
                return MakeParseErrorTL(ErrorCode::ExpectedToken, nextTok, Token::Identifier);
            macroMap.second = AsString(nextTok.value);
        }
        else
        {
            macroMap.second = macroMap.first;
        }
        mappedNames.push_back(std::move(macroMap));
    }

    if (nextTok != Token::Eof)
    {
        if (hasContextControl)
            return MakeParseErrorTL(ErrorCode::ExpectedEndOfStatement, nextTok, Token::Eof);

        if (mappedNames.empty())
            MakeParseErrorTL(ErrorCode::UnexpectedToken, nextTok, Token::Eof, Token::Identifier);
        else
            MakeParseErrorTL(ErrorCode::UnexpectedToken, nextTok, Token::Eof, Token::Comma, Token::With, Token::Without);
    }

    auto renderer = std::make_shared<ImportStatement>(isWithContext);
    renderer->SetImportNameExpr(valueExpr);

    for (auto& nameInfo : mappedNames)
        renderer->AddNameToImport(std::move(nameInfo.first), std::move(nameInfo.second));

    statementsInfo.back().currentComposition->AddRenderer(renderer);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseDo(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& /*stmtTok*/)
{
    ExpressionEvaluatorPtr<> valueExpr;
    ExpressionParser exprParser(m_settings);
    auto expr = exprParser.ParseFullExpression(lexer);
    if (!expr)
        return expr.get_unexpected();
    valueExpr = *expr;

    auto renderer = std::make_shared<DoStatement>(valueExpr);
    statementsInfo.back().currentComposition->AddRenderer(renderer);

    return jinja2::StatementsParser::ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseWith(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    std::vector<std::pair<std::string, ExpressionEvaluatorPtr<>>> vars;

    ExpressionParser exprParser(m_settings);
    while (lexer.PeekNextToken() == Token::Identifier)
    {
        auto nameTok = lexer.NextToken();
        if (!lexer.EatIfEqual('='))
            return MakeParseErrorTL(ErrorCode::ExpectedToken, lexer.PeekNextToken(), '=');

        auto expr = exprParser.ParseFullExpression(lexer);
        if (!expr)
            return expr.get_unexpected();
        auto valueExpr = *expr;

        vars.emplace_back(AsString(nameTok.value), valueExpr);

        if (!lexer.EatIfEqual(','))
            break;
    }

    auto nextTok = lexer.PeekNextToken();
    if (vars.empty())
        return MakeParseError(ErrorCode::ExpectedIdentifier, nextTok);

    if (nextTok != Token::Eof)
        return MakeParseErrorTL(ErrorCode::ExpectedToken, nextTok, Token::Eof, ',');

    auto renderer = std::make_shared<WithStatement>();
    renderer->SetScopeVars(std::move(vars));
    StatementInfo statementInfo = StatementInfo::Create(StatementInfo::WithStatement, stmtTok);
    statementInfo.renderer = renderer;
    statementsInfo.push_back(statementInfo);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseEndWith(LexScanner& /*lexer*/, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.size() <= 1)
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    StatementInfo info = statementsInfo.back();

    if (info.type != StatementInfo::WithStatement)
    {
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);
    }

    statementsInfo.pop_back();
    auto renderer = static_cast<WithStatement*>(info.renderer.get());
    renderer->SetMainBody(info.compositions[0]);

    statementsInfo.back().currentComposition->AddRenderer(info.renderer);

    return ParseResult();
}

StatementsParser::ParseResult StatementsParser::ParseFilter(LexScanner& lexer, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    ExpressionParser exprParser(m_settings);
    auto filterExpr = exprParser.ParseFilterExpression(lexer);
    if (!filterExpr)
    {
        return filterExpr.get_unexpected();
    }

    auto renderer = std::make_shared<FilterStatement>(*filterExpr);
    auto statementInfo = StatementInfo::Create(
        StatementInfo::FilterStatement, stmtTok);
    statementInfo.renderer = std::move(renderer);
    statementsInfo.push_back(std::move(statementInfo));
  
    return {};
}

StatementsParser::ParseResult StatementsParser::ParseEndFilter(LexScanner&, StatementInfoList& statementsInfo, const Token& stmtTok)
{
    if (statementsInfo.size() <= 1)
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);

    const auto info = statementsInfo.back();
    if (info.type != StatementInfo::FilterStatement)
    {
        return MakeParseError(ErrorCode::UnexpectedStatement, stmtTok);
    }

    statementsInfo.pop_back();
    auto &renderer = *boost::polymorphic_downcast<FilterStatement*>(info.renderer.get());
    renderer.SetBody(info.compositions[0]);

    statementsInfo.back().currentComposition->AddRenderer(info.renderer);

    return {};
}

} // namespace jinja2
