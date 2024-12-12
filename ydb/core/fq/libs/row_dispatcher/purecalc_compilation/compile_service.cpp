#include "compile_service.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/common/common.h>

#include <ydb/library/actors/core/hfunc.h>

#include <yql/essentials/public/purecalc/common/interface.h>

namespace NFq::NRowDispatcher {

namespace {

class TPurecalcCompileService : public NActors::TActor<TPurecalcCompileService> {
    using TBase = NActors::TActor<TPurecalcCompileService>;

public:
    TPurecalcCompileService()
        : TBase(&TPurecalcCompileService::StateFunc)
        , LogPrefix("TPurecalcCompileService: ")
    {}

    static constexpr char ActorName[] = "FQ_ROW_DISPATCHER_COMPILER";

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRowDispatcher::TEvPurecalcCompileRequest, Handle);
    )

    void Handle(TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr& ev) {
        LOG_ROW_DISPATCHER_TRACE("Got compile request with id: " << ev->Cookie);
        IProgramHolder::TPtr programHolder = std::move(ev->Get()->ProgramHolder);

        TStatus status = TStatus::Success();
        try {
            programHolder->CreateProgram(GetOrCreateFactory(ev->Get()->Settings));
        } catch (const NYql::NPureCalc::TCompileError& error) {
            status = TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Compile issues: " << error.GetIssues())
                .AddIssue(TStringBuilder() << "Final yql: " << error.GetYql())
                .AddParentIssue(TStringBuilder() << "Failed to compile purecalc program");
        } catch (...) {
            status = TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to compile purecalc program, got unexpected exception: " << CurrentExceptionMessage());
        }

        if (status.IsFail()) {
            LOG_ROW_DISPATCHER_ERROR("Compilation failed for request with id: " << ev->Cookie);
            Send(ev->Sender, new TEvRowDispatcher::TEvPurecalcCompileResponse(status.GetStatus(), status.GetErrorDescription()), 0, ev->Cookie);
        } else {
            LOG_ROW_DISPATCHER_TRACE("Compilation completed for request with id: " << ev->Cookie);
            Send(ev->Sender, new TEvRowDispatcher::TEvPurecalcCompileResponse(std::move(programHolder)), 0, ev->Cookie);
        }
    }

private:
    NYql::NPureCalc::IProgramFactoryPtr GetOrCreateFactory(const TPurecalcCompileSettings& settings) {
        const auto it = ProgramFactories.find(settings);
        if (it != ProgramFactories.end()) {
            return it->second;
        }
        return ProgramFactories.emplace(settings, NYql::NPureCalc::MakeProgramFactory(
            NYql::NPureCalc::TProgramFactoryOptions()
                .SetLLVMSettings(settings.EnabledLLVM ? "ON" : "OFF")
        )).first->second;
    }

private:
    const TString LogPrefix;

    std::map<TPurecalcCompileSettings, NYql::NPureCalc::IProgramFactoryPtr> ProgramFactories;
};

}  // namespace {

NActors::IActor* CreatePurecalcCompileService() {
    return new TPurecalcCompileService();
}

}  // namespace NFq::NRowDispatcher
