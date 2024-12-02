#include "compile_service.h"

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <ydb/library/actors/core/hfunc.h>

#include <yql/essentials/public/purecalc/common/interface.h>

namespace NFq::NRowDispatcher {

namespace {

class TPurecalcCompileService : public NActors::TActor<TPurecalcCompileService> {
    using TBase = NActors::TActor<TPurecalcCompileService>;

public:
    TPurecalcCompileService()
        : TBase(&TPurecalcCompileService::StateFunc)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRowDispatcher::TEvPurecalcCompileRequest, Handle);
    )

    void Handle(TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr& ev) {
        IProgramHolder::TPtr programHolder = std::move(ev->Get()->ProgramHolder);

        TString error;
        try {
            programHolder->CreateProgram(GetOrCreateFactory(ev->Get()->Settings));
        } catch (const NYql::NPureCalc::TCompileError& e) {
            error = TStringBuilder() << "Failed to compile purecalc filter: sql: " << e.GetYql() << ", error: " << e.GetIssues();
        } catch (...) {
            error = TStringBuilder() << "Failed to compile purecalc filter, unexpected exception: " << CurrentExceptionMessage();
        }

        if (error) {
            Send(ev->Sender, new TEvRowDispatcher::TEvPurecalcCompileResponse(error), 0, ev->Cookie);
        } else {
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
    std::map<TPurecalcCompileSettings, NYql::NPureCalc::IProgramFactoryPtr> ProgramFactories;
};

}  // namespace {

NActors::IActor* CreatePurecalcCompileService() {
    return new TPurecalcCompileService();
}

}  // namespace NFq::NRowDispatcher
