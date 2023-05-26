#pragma once

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <util/generic/string.h>

#include <map>
#include <memory>

namespace NKikimr::NKqp {

struct TKqpQueryRef {
    TKqpQueryRef(const TString& text, std::shared_ptr<std::map<TString, Ydb::Type>> parameterTypes = {})
        : Text(text)
        , ParameterTypes(parameterTypes)
    {}

    // Text is owned by TKqpQueryId
    const TString& Text;
    std::shared_ptr<std::map<TString, Ydb::Type>> ParameterTypes;
};

} // namespace NKikimr::NKqp
