#pragma once

#include <functional>

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h>

#include <util/generic/string.h>

namespace NYql::NDq {
    struct TPqMetaExtractor {
        using TPqMetaExtractorLambda = std::function<std::pair<NYql::NUdf::TUnboxedValuePod, i64>(const NYdb::NFederatedTopic::TReadSessionEvent::TDataReceivedEvent::TMessage&)>;

    public:
        TPqMetaExtractor();
        TPqMetaExtractorLambda FindExtractorLambda(const TString& sysColumn) const;
    };
}
