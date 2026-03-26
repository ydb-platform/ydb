#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NTransformation {

class TCompositeTransformer: public ITransformer {
private:
    std::vector<ITransformer::TPtr> Transformers;
protected:
    virtual std::shared_ptr<arrow20::RecordBatch> DoTransform(const std::shared_ptr<arrow20::RecordBatch>& batch) const override;
    virtual TString DoDebugString() const override;
public:
};

}
