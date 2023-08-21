#ifndef OPERATION_ID_OR_ALIAS_INL_H_
#error "Direct inclusion of this file is not allowed, include operation_id_or_alias.h"
// For the sake of sane code completion.
#include "operation_id_or_alias.h"
#endif

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TProtoClass>
void FromProto(TOperationIdOrAlias* operationIdOrAlias, const TProtoClass& enclosingProtoMessage)
{
    using NYT::FromProto;

    switch (enclosingProtoMessage.operation_id_or_alias_case()) {
        case TProtoClass::OperationIdOrAliasCase::kOperationId: {
            operationIdOrAlias->Payload = FromProto<TOperationId>(enclosingProtoMessage.operation_id());
            break;
        }
        case TProtoClass::OperationIdOrAliasCase::kOperationAlias: {
            const auto& operationAlias = enclosingProtoMessage.operation_alias();
            if (!operationAlias.StartsWith(OperationAliasPrefix)) {
                THROW_ERROR_EXCEPTION("Operation alias should start with %Qv", OperationAliasPrefix)
                    << TErrorAttribute("operation_alias", operationAlias);
            }
            operationIdOrAlias->Payload = operationAlias;
            break;
        }
        case TProtoClass::OperationIdOrAliasCase::OPERATION_ID_OR_ALIAS_NOT_SET: {
            THROW_ERROR_EXCEPTION("None of operation id and operation alias is set in oneof OperationIdOrAlias proto");
        }
    }
}

template <class TProtoClassPtr>
void ToProto(TProtoClassPtr enclosingProtoMessage, const TOperationIdOrAlias& operationIdOrAlias)
{
    using NYT::ToProto;

    Visit(operationIdOrAlias.Payload,
        [&] (const TOperationId& operationId) {
            ToProto(enclosingProtoMessage->mutable_operation_id(), operationId);
        },
        [&] (const TString& alias) {
            enclosingProtoMessage->set_operation_alias(alias);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
