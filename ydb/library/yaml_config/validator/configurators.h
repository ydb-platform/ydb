#include <ydb/library/yaml_config/validator/validator_builder.h>

namespace NKikimr::NYamlConfig::NValidator {
namespace Configurators {

std::function<void(TInt64Builder&)> nonNegative();

std::function<void(TArrayBuilder&)> uniqueByPath(TString path, TString checkName = "");

std::function<void(TMapBuilder&)> mustBeEqual(TString path1, TString path2);

template<typename Builder>
std::function<void(Builder&)> combine(std::function<void(Builder&)> configurator) {
   return configurator;
}

template<typename Builder, typename ...Builders>
std::function<void(Builder&)> combine(
    std::function<void(Builder&)> configurator,
    std::function<void(Builders)>... configurators) {
    return [configurator, remain = combine(configurators...)](Builder& b) {
        b.Configure(configurator).Configure(remain);
    };
}

} // namespace Configurators

TNodeWrapper walkFrom(TNodeWrapper node, TString path);
TNodeWrapper walkFrom(TNodeWrapper node, const TVector<TString>& pathTokens);

} // namespace NKikimr::NYamlConfig::NValidator
