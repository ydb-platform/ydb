#pragma once

#include "servlet.h"

#include <ydb/library/yql/ast/yql_errors.h>
#include <ydb/library/yql/ast/yql_ast.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>


#define YQL_ACTION(action)                                         \
    class TYqlAction##action: public ::NYql::NHttp::TYqlAction {   \
    public:                                                        \
        TYqlAction##action(                                        \
                ::NYql::NHttp::TYqlServer& yqlServer,              \
                ::NJson::TJsonWriter& writer,                      \
                const ::NYql::NHttp::TRequest& req,                \
                ::NYql::NHttp::TResponse& resp)                    \
           : ::NYql::NHttp::TYqlAction(yqlServer, writer, req, resp) {} \

namespace NYql {
namespace NHttp {

class TYqlServer;

///////////////////////////////////////////////////////////////////////////////
// TYqlAction
///////////////////////////////////////////////////////////////////////////////
class TYqlAction: private TNonCopyable
{
public:
    enum EOptions {
        YqlProgram = 0x01,
        SqlProgram = 0x02,

        PrintAst = 0x0100,
        PrintExpr = 0x0200,
        PrintTraceOpt = 0x0400,
        WithFinalIssues = 0x0800,
    };

public:
    TYqlAction(
            TYqlServer& yqlServer,
            NJson::TJsonWriter& writer,
            const TRequest& req,
            TResponse& resp)
        : YqlServer(yqlServer)
        , Writer(writer)
        , Req(req)
        , Resp(resp)
    {
    }

protected:
    void WriteStatus(bool success, const TIssues& errors) const;
    void WriteAstTree(const TAstNode* node);

protected:
    TYqlServer& YqlServer;
    NJson::TJsonWriter& Writer;
    const TRequest& Req;
    TResponse& Resp;
};

///////////////////////////////////////////////////////////////////////////////
// TYqlServlet
///////////////////////////////////////////////////////////////////////////////
template <typename TAction>
class TYqlServlet: public IServlet
{
public:
    TYqlServlet(TYqlServer& yqlServer)
        : YqlServer_(yqlServer)
    {
    }

    void DoPost(const TRequest& req, TResponse& resp) const override final {
        NJson::TJsonValue value;
        TStringBuf body(req.Body.AsCharPtr(), req.Body.Size());
        bool parsed = NJson::ReadJsonFastTree(body, &value, true);
        Y_ENSURE_EX(parsed, THttpError(HTTP_BAD_REQUEST) << "can't parse json");

        const TString& program = value[TStringBuf("program")].GetString();
        const TString& input = value[TStringBuf("tableInput")].GetString();
        const TString& attr = value[TStringBuf("tableAttr")].GetString();
        const TString& lang = value[TStringBuf("lang")].GetString();
        const TString& params = value[TStringBuf("parameters")].GetString();

        ui32 options = 0;
        if (req.RD.CgiParam.Has(TStringBuf("printAst"))) {
            options |= TYqlAction::PrintAst;
        }

        if (req.RD.CgiParam.Has(TStringBuf("printExpr"))) {
            options |= TYqlAction::PrintExpr;
        }

        if (req.RD.CgiParam.Has(TStringBuf("traceOpt"))) {
            options |= TYqlAction::PrintTraceOpt;
        }

        if (lang == TStringBuf("yql")) {
            options |= TYqlAction::YqlProgram;
        } else if (lang == TStringBuf("sql")) {
            options |= TYqlAction::SqlProgram;
        }
        options |= TYqlAction::WithFinalIssues;

        TStringStream output;
        NJson::TJsonWriter writer(&output, false);
        writer.OpenMap();

        TAction action(YqlServer_, writer, req, resp);
        action.Perform(program, input, attr, options, params);

        writer.CloseMap();
        writer.Flush();
        resp.Body = TBlob::FromString(output.Str());
        resp.ContentType = TStringBuf("application/json");
    }

private:
    TYqlServer& YqlServer_;
};

} // namspace NNttp
} // namspace NYql
