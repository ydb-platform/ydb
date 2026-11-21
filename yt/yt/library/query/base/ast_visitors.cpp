#include "ast_visitors.h"

namespace NYT::NQueryClient::NAst {

////////////////////////////////////////////////////////////////////////////////

TReferenceHarvester::TReferenceHarvester(TColumnSet* storage)
    : Storage_(storage)
{ }

void TReferenceHarvester::OnReference(const TReferenceExpression* referenceExpr)
{
    Storage_->insert(referenceExpr->Reference.ColumnName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NAst
