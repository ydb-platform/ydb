#pragma once
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract {

class ISourcesConstructor: public NCommon::ISourcesConstructor {
private:
    virtual void DoFillReadStats(TReadStats& /*stats*/) const override {
    }

public:
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract
