#include "configurators.h"

#include <ydb/library/yaml_config/validator/validator_builder.h>
#include <ydb/library/yaml_config/validator/validator_checks.h>

#include <util/string/split.h>
#include <util/generic/hash.h>

namespace NKikimr::NYamlConfig::NValidator {
namespace Configurators {

std::function<void(TInt64Builder&)> nonNegative() {
    return [](auto& b) {
        b.Min(0);
    };
}

std::function<void(TArrayBuilder&)> uniqueByPath(TString path, TString checkName) {
    checkName = (checkName == "") ? ("All array items, that located in \"" + path + "\" must be unique") : checkName;
    return [=](auto& b) {
        b.AddCheck(checkName, [=](auto& c) {
            TArrayNodeWrapper array = c.Node();
            THashMap<TString, i64> valueToIndex;

            TVector<TString> pathTokens = StringSplitter(path).Split('/');
            for (int i = 0; i < array.Length(); ++i) {
                TNodeWrapper node = array[i];
                node = walkFrom(node, pathTokens);
                if (!node.Exists()) {
                    continue;
                }
                if (!node.IsScalar()) {
                    ythrow yexception() << "Can't check uniqness of non-scalar fields";
                }
                TString value = node.Scalar();
                if (valueToIndex.contains(value)) {
                    TString i1 = ToString(valueToIndex[value]);
                    TString i2 = ToString(i);
                    c.Fail("items with indexes " + i1 + " and " + i2 + " are conflicting");
                }
                valueToIndex[value] = i;
            }
        });
    };
}

std::function<void(TMapBuilder&)> mustBeEqual(TString path1, TString path2) {
    return [=](auto& b){
        b.AddCheck("Fields with paths " + path1 + " and " + path2 +" must be equal", [=](auto& c){
            auto node1 = walkFrom(c.Node(), path1);
            auto node2 = walkFrom(c.Node(), path2);

            c.Expect(node1.Scalar() == node2.Scalar());
        });
    };
}

} // namespace Configurators

TNodeWrapper walkFrom(TNodeWrapper node, TString path) {
    return walkFrom(node, TVector<TString>(StringSplitter(path).Split('/')));
}

TNodeWrapper walkFrom(TNodeWrapper node, const TVector<TString>& pathTokens) {
    for (const TString& pathToken : pathTokens) {
        if (node.Exists()) {
            if (node.IsMap()) {
                node = node.Map()[pathToken];
            } else if (node.IsArray()) {
                auto i = TryFromString<size_t>(pathToken);
                if (!i.Defined()) {
                    ythrow yexception() << "incorrect array index";
                }
                node = node.Array()[i.GetRef()];
            } else {
                ythrow yexception() << "incorrect path";
            }
        } else {
            node = node.Map()[pathToken];
        }
    }
    return node;
}

} // namespace NKikimr::NYamlConfig::NValidator
