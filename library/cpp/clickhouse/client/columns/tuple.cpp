#include "tuple.h"

namespace NClickHouse {
    static TVector<TTypeRef> CollectTypes(const TVector<TColumnRef>& columns) {
        TVector<TTypeRef> types;
        for (const auto& col : columns) {
            types.push_back(col->Type());
        }
        return types;
    }

    TColumnTuple::TColumnTuple(const TVector<TColumnRef>& columns)
        : TColumn(TType::CreateTuple(CollectTypes(columns)))
        , Columns_(columns)
    {
    }

    TIntrusivePtr<TColumnTuple> TColumnTuple::Create(const TVector<TColumnRef>& columns) {
        return new TColumnTuple(columns);
    }

    size_t TColumnTuple::Size() const {
        return Columns_.empty() ? 0 : Columns_[0]->Size();
    }

    bool TColumnTuple::Load(TCodedInputStream* input, size_t rows) {
        for (auto ci = Columns_.begin(); ci != Columns_.end(); ++ci) {
            if (!(*ci)->Load(input, rows)) {
                return false;
            }
        }

        return true;
    }

    void TColumnTuple::Save(TCodedOutputStream* output) {
        for (auto ci = Columns_.begin(); ci != Columns_.end(); ++ci) {
            (*ci)->Save(output);
        }
    }

}
