#include "public.h"

#include <yt/yt/core/misc/guid.h>

#include <util/string/vector.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

TVersionedObjectId::TVersionedObjectId(TObjectId objectId)
    : ObjectId(objectId)
{ }

TVersionedObjectId::TVersionedObjectId(
    TObjectId objectId,
    TTransactionId transactionId)
    : ObjectId(objectId)
    , TransactionId(transactionId)
{ }

bool TVersionedObjectId::IsBranched() const
{
    return TransactionId.operator bool();
}

TVersionedObjectId TVersionedObjectId::FromString(TStringBuf str)
{
    TStringBuf objectToken, transactionToken;
    str.Split(':', objectToken, transactionToken);

    auto objectId = TObjectId::FromString(objectToken);
    auto transactionId =
        transactionToken.empty()
        ? NullTransactionId
        : TTransactionId::FromString(transactionToken);
    return TVersionedObjectId(objectId, transactionId);
}

void FormatValue(TStringBuilderBase* builder, const TVersionedObjectId& id, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v:%v", id.ObjectId, id.TransactionId);
}

bool operator == (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (TVersionedObjectId)) == 0;
}

bool operator < (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (TVersionedObjectId)) < 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

