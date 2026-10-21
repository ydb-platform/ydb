#pragma once

#include "public.h"

#include <variant>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

extern const std::string OperationAliasPrefix;

// This wrapper is needed for ADL in FromProto/ToProto to work.
struct TOperationIdOrAlias
{
    TOperationIdOrAlias() = default;
    TOperationIdOrAlias(TOperationId id);
    TOperationIdOrAlias(std::string alias);

    static TOperationIdOrAlias FromString(std::string operationIdOrAlias);

    std::variant<TOperationId, std::string> Payload;

    bool operator==(const TOperationIdOrAlias& other) const;

    operator size_t() const;
};

void FormatValue(TStringBuilderBase* builder, const TOperationIdOrAlias& operationIdOrAlias, TStringBuf spec);

// NB: TOperationIdOrAlias corresponds to a oneof group of fields in proto representation,
// so we use an enclosing proto message object to properly serialize or deserialize it.
template <class TProtoClass>
void FromProto(TOperationIdOrAlias* operationIdOrAlias, const TProtoClass& enclosingProtoMessage);

template <class TProtoClassPtr>
void ToProto(TProtoClassPtr enclosingProtoMessage, const TOperationIdOrAlias& operationIdOrAlias);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

#define OPERATION_ID_OR_ALIAS_INL_H_
#include "operation_id_or_alias-inl.h"
#undef OPERATION_ID_OR_ALIAS_INL_H_
