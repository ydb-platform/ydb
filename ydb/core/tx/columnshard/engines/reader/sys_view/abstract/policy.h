#pragma once
#include "filler.h"

#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/constructor.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

class ISysViewPolicy {
private:
    virtual std::unique_ptr<IScannerConstructor> DoCreateConstructor(const TScannerConstructorContext& request) const = 0;
    virtual std::shared_ptr<IMetadataFiller> DoCreateMetadataFiller() const = 0;
public:
    virtual ~ISysViewPolicy() = default;

    using TFactory = NObjectFactory::TObjectFactory<ISysViewPolicy, TString>;

    static THolder<ISysViewPolicy> BuildByPath(const TString& tablePath);

    std::shared_ptr<IMetadataFiller> CreateMetadataFiller() const {
        auto result = DoCreateMetadataFiller();
        AFL_VERIFY(!!result);
        return result;
    }
    std::unique_ptr<IScannerConstructor> CreateConstructor(const TScannerConstructorContext& request) const {
        auto result = DoCreateConstructor(request);
        AFL_VERIFY(!!result);
        return result;
    }
};

}