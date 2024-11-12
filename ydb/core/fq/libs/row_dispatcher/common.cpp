#include "common.h"

#include <util/system/mutex.h>

#include <ydb/library/yql/public/purecalc/common/interface.h>

namespace NFq {

namespace {

class TPureCalcProgramFactory : public IPureCalcProgramFactory {
public:
    NYql::NPureCalc::IProgramFactoryPtr GetFactory(const TSettings& settings) override {
        TGuard<TMutex> guard(FactoriesMutex);

        const auto it = ProgramFactories.find(settings);
        if (it != ProgramFactories.end()) {
            return it->second;
        }

        return ProgramFactories.insert({settings, NYql::NPureCalc::MakeProgramFactory(
            NYql::NPureCalc::TProgramFactoryOptions()
                .SetLLVMSettings(settings.EnabledLLVM ? "ON" : "OFF")
        )}).first->second;
    }

private:
    TMutex FactoriesMutex;
    std::map<TSettings, NYql::NPureCalc::IProgramFactoryPtr> ProgramFactories;
};

} // anonymous namespace

IPureCalcProgramFactory::TPtr CreatePureCalcProgramFactory() {
    return MakeIntrusive<TPureCalcProgramFactory>();
}

} // namespace NFq
