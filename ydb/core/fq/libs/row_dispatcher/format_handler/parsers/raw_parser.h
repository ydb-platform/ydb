#pragma once

#include "parser_abstract.h"

#include <ydb/core/fq/libs/row_dispatcher/common/row_dispatcher_settings.h>

namespace NKikimr::NMiniKQL {
    class IFunctionRegistry;
} // namespace NKikimr::NMiniKQL

namespace NFq::NRowDispatcher {

TValueStatus<ITopicParser::TPtr> CreateRawParser(IParsedDataConsumer::TPtr consumer, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, const TCountersDesc& counters, std::shared_ptr<NYql::NDq::IMemoryQuotaManager> memoryQuotaManager = {});

}  // namespace NFq::NRowDispatcher
