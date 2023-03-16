#include "graph_params_printer.h"
#include "minikql_program_printer.h"

#include <ydb/library/protobuf_printer/hide_field_printer.h>
#include <ydb/library/protobuf_printer/protobuf_printer.h>
#include <ydb/library/protobuf_printer/stream_helper.h>

#include <ydb/library/yql/providers/dq/api/protos/service.pb.h>

#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

namespace NFq {
namespace {

class TMinikqlProgramFieldValuePrinter : public ::google::protobuf::TextFormat::FastFieldValuePrinter {
public:
    void PrintBytes(const TProtoStringType& value, ::google::protobuf::TextFormat::BaseTextGenerator* generator) const override {
        const TString program = PrettyPrintMkqlProgram(value, /*generator->GetCurrentIndentationSize()*/ 4); // TODO: use GetCurrentIndentationSize() when we have it.
        generator->PrintLiteral("\n");
        generator->Print(program.Data(), program.Size());
    }

    void PrintString(const TProtoStringType& value, ::google::protobuf::TextFormat::BaseTextGenerator* generator) const override {
        PrintBytes(value, generator);
    }
};

class TGraphParamsPrinter : public NKikimr::TCustomizableTextFormatPrinter {
    void RegisterFieldPrinters(bool canonical) {
        if (canonical) {
            // Don't print values of these fields, but mention that they have values.
            RegisterFieldValuePrinters<NProto::TGraphParams, NKikimr::THideFieldValuePrinter>("GraphId", "SourceId", "Session");
            RegisterFieldValuePrinters<NActorsProto::TActorId, NKikimr::THideFieldValuePrinter>("RawX1", "RawX2");
            RegisterFieldValuePrinters<Yql::DqsProto::TFile, NKikimr::THideFieldValuePrinter>("ObjectId");
        }

        RegisterFieldValuePrinters<NYql::NDqProto::TProgram, TMinikqlProgramFieldValuePrinter>("Raw");
    }

public:
    explicit TGraphParamsPrinter(bool canonical) {
        SetExpandAny(true);
        RegisterFieldPrinters(canonical);
    }
};

} // namespace

TString PrettyPrintGraphParams(const NProto::TGraphParams& sourceGraphParams, bool canonical) {
    NProto::TGraphParams patchedGraphParams = sourceGraphParams;
    if (canonical) {
        for (auto& task : *patchedGraphParams.MutableTasks()) {
            task.ClearStageId();
        }
    }
    for (auto& [secureKey, tokenValue] : *patchedGraphParams.MutableSecureParams()) {
        tokenValue = "== token_value ==";
    }
    patchedGraphParams.ClearQueryPlan();
    patchedGraphParams.ClearQueryAst();
    patchedGraphParams.ClearYqlText();

    return NKikimr::TProtobufPrinterOutputWrapper(patchedGraphParams, TGraphParamsPrinter(canonical));
}

} // namespace NFq
