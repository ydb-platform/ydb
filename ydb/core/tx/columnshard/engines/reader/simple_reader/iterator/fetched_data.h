#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetched_data.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TFetchedData: public NCommon::TFetchedData {
private:
    using TBase = NCommon::TFetchedData;

public:
    using TBase::TBase;
};

class TFetchedResult: public NCommon::TFetchedResult {
private:
    using TBase = NCommon::TFetchedResult;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap
