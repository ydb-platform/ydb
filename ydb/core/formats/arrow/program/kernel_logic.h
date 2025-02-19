#pragma once
#include "abstract.h"
#include "collection.h"

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NArrow::NSSA {

class IKernelLogic {
private:
    virtual TConclusion<bool> DoExecute(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output,
        const std::shared_ptr<TAccessorsCollection>& resources) const = 0;

public:
    virtual ~IKernelLogic() = default;

    using TFactory = NObjectFactory::TObjectFactory<IKernelLogic, TString>;

    TConclusion<bool> Execute(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output,
        const std::shared_ptr<TAccessorsCollection>& resources) const {
        if (!resources) {
            return TConclusionStatus::Fail("resources in incorrect (nullptr)");
        }
        return DoExecute(input, output, resources);
    }
};

class TGetJsonPath: public IKernelLogic {
public:
    static TString GetClassNameStatic() {
        return "JsonValue";
    }
private:
    static const inline TFactory::TRegistrator<TGetJsonPath> Registrator = TFactory::TRegistrator<TGetJsonPath>(GetClassNameStatic());

    virtual TConclusion<bool> DoExecute(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output,
        const std::shared_ptr<TAccessorsCollection>& resources) const override;

    std::shared_ptr<IChunkedArray> ExtractArray(const std::shared_ptr<IChunkedArray>& jsonAcc, const std::string_view svPath) const;

public:
};
}   // namespace NKikimr::NArrow::NSSA
