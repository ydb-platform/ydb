#include "spec.h"

using namespace NYql::NPureCalc;

TArrowInputSpec::TArrowInputSpec(const TVector<NYT::TNode>& schemas)
    : Schemas_(schemas)
{
}

const TVector<NYT::TNode>& TArrowInputSpec::GetSchemas() const {
    return Schemas_;
}

TArrowOutputSpec::TArrowOutputSpec(const NYT::TNode& schema)
    : Schema_(schema)
{
}

const NYT::TNode& TArrowOutputSpec::GetSchema() const {
    return Schema_;
}
