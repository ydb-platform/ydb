#pragma once

#include <functional>

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>

namespace NKikimr::NMiniKQL {
class THolderFactory;
class TType;
}

namespace NYql::NDq {
    struct TPqMetaExtractor {
        using TPqMetaExtractorLambda = std::function<std::pair<NYql::NUdf::TUnboxedValuePod, i64>(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage&)>;

    public:
        TPqMetaExtractor(const NKikimr::NMiniKQL::THolderFactory& holderFactory, const NKikimr::NMiniKQL::TType* messageMetaDictType);
        TPqMetaExtractorLambda FindExtractorLambda(const TString& sysColumn) const;

    private:
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
        const NKikimr::NMiniKQL::TType* const MessageMetaDictType_;
    };
}
