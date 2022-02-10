#pragma once

#include "yql_dq_full_result_writer.h"

#include <util/generic/ptr.h>

#include <vector>

namespace NYql {

class IDqTaskPreprocessor : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDqTaskPreprocessor>;

    virtual ~IDqTaskPreprocessor() = default;

    virtual THashMap<TString, TString> GetTaskParams(const THashMap<TString, TString>& graphParams, const THashMap<TString, TString>& secureParams) = 0;
    virtual void Finish(bool success) = 0;
    virtual THolder<IDqFullResultWriter> CreateFullResultWriter() = 0;
};

using TDqTaskPreprocessorFactory = std::function<IDqTaskPreprocessor::TPtr()>;
using TDqTaskPreprocessorFactoryCollection = std::vector<TDqTaskPreprocessorFactory>;

} // namespace NYql
