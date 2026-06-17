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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

