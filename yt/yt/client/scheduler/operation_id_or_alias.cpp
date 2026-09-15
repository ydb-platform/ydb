#include "operation_id_or_alias.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const std::string OperationAliasPrefix("*");

TOperationIdOrAlias::TOperationIdOrAlias(TOperationId id)
    : Payload(id)
{ }

TOperationIdOrAlias::TOperationIdOrAlias(std::string alias)
    : Payload(std::move(alias))
{ }

bool TOperationIdOrAlias::operator==(const TOperationIdOrAlias& other) const
{
    return Payload == other.Payload;
}

TOperationIdOrAlias TOperationIdOrAlias::FromString(std::string operationIdOrAlias)
{
    if (!operationIdOrAlias.empty() && operationIdOrAlias[0] == '*') {
        return TOperationIdOrAlias(operationIdOrAlias);
    } else {
        return TOperationIdOrAlias(TOperationId(TGuid::FromString(operationIdOrAlias)));
    }
}

void FormatValue(TStringBuilderBase* builder, const TOperationIdOrAlias& operationIdOrAlias, TStringBuf /*spec*/)
{
    Visit(operationIdOrAlias.Payload,
        [&] (const std::string& alias) {
            builder->AppendFormat("%v", alias);
        },
        [&] (const TOperationId& operationId) {
            builder->AppendFormat("%v", operationId);
        });
}

TOperationIdOrAlias::operator size_t() const
{
    size_t result = 0;
    Visit(Payload,
        [&] (const std::string& alias) {
            HashCombine(result, alias);
        },
        [&] (const TOperationId& operationId) {
            HashCombine(result, operationId);
        });
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
