#include "common.h"

#include <util/system/mutex.h>

#include <ydb/library/yql/public/purecalc/common/interface.h>

namespace NFq {

namespace {

class TPureCalcProgramFactory : public IPureCalcProgramFactory {
public:
    TPureCalcProgramFactory() {
        CreateFactory({.EnabledLLVM = false});
        CreateFactory({.EnabledLLVM = true});
    }

    NYql::NPureCalc::IProgramFactoryPtr GetFactory(const TSettings& settings) const override {
        const auto it = ProgramFactories.find(settings);
        Y_ENSURE(it != ProgramFactories.end());
        return it->second;
    }

private:
    void CreateFactory(const TSettings& settings) {
        ProgramFactories.insert({settings, NYql::NPureCalc::MakeProgramFactory(
            NYql::NPureCalc::TProgramFactoryOptions()
                .SetLLVMSettings(settings.EnabledLLVM ? "ON" : "OFF")
        )});
    }

private:
    std::map<TSettings, NYql::NPureCalc::IProgramFactoryPtr> ProgramFactories;
};

} // anonymous namespace

IPureCalcProgramFactory::TPtr CreatePureCalcProgramFactory() {
    return MakeIntrusive<TPureCalcProgramFactory>();
}

} // namespace NFq
