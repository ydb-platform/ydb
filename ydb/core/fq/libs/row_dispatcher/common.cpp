#include "common.h"

#include <ydb/library/yql/public/purecalc/common/interface.h>

namespace NFq {

namespace {

class TPureCalcProgramFactory : public IPureCalcProgramFactory {
public:
    NYql::NPureCalc::IProgramFactoryPtr GetFactory(const TSettings& settings) override {
        const auto it = ProgramFactories.find(settings);
        if (it != ProgramFactories.end()) {
            return it->second;
        }
        return CreateFactory(settings);
    }

private:
    NYql::NPureCalc::IProgramFactoryPtr CreateFactory(const TSettings& settings) {
        return ProgramFactories.insert({settings, NYql::NPureCalc::MakeProgramFactory(
            NYql::NPureCalc::TProgramFactoryOptions()
                .SetLLVMSettings(settings.EnabledLLVM ? "ON" : "OFF")
        )}).first->second;
    }

private:
    std::map<TSettings, NYql::NPureCalc::IProgramFactoryPtr> ProgramFactories;
};

} // anonymous namespace

IPureCalcProgramFactory::TPtr CreatePureCalcProgramFactory() {
    return MakeIntrusive<TPureCalcProgramFactory>();
}

} // namespace NFq
