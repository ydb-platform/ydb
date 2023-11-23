#include "nullable.h"

#include <util/generic/yexception.h>
#include <util/system/yassert.h>

namespace NClickHouse {
    TColumnNullable::TColumnNullable(TColumnRef nested, TColumnRef nulls)
        : TColumn(TType::CreateNullable(nested->Type()))
        , Nested_(nested)
        , Nulls_(nulls->As<TColumnUInt8>())
    {
        if (Nested_->Size() != nulls->Size()) {
            ythrow yexception() << "count of elements in nested and nulls should be the same";
        }
    }

    TIntrusivePtr<TColumnNullable> TColumnNullable::Create(TColumnRef nested) {
        return new TColumnNullable(nested, TColumnUInt8::Create());
    }

    TIntrusivePtr<TColumnNullable> TColumnNullable::Create(TColumnRef nested, TColumnRef nulls) {
        return new TColumnNullable(nested, nulls);
    }

    bool TColumnNullable::IsNull(size_t n) const {
        return Nulls_->At(n) != 0;
    }

    TColumnRef TColumnNullable::Nested() const {
        return Nested_;
    }

    void TColumnNullable::Append(TColumnRef column) {
        if (auto col = column->As<TColumnNullable>()) {
            if (!col->Nested_->Type()->IsEqual(Nested_->Type())) {
                return;
            }

            Nested_->Append(col->Nested_);
            Nulls_->Append(col->Nulls_);
        }
    }

    bool TColumnNullable::Load(TCodedInputStream* input, size_t rows) {
        if (!Nulls_->Load(input, rows)) {
            return false;
        }
        if (!Nested_->Load(input, rows)) {
            return false;
        }
        return true;
    }

    void TColumnNullable::Save(TCodedOutputStream* output) {
        Nulls_->Save(output);
        Nested_->Save(output);
    }

    size_t TColumnNullable::Size() const {
        Y_ASSERT(Nested_->Size() == Nulls_->Size());
        return Nulls_->Size();
    }

    TColumnRef TColumnNullable::Slice(size_t begin, size_t len) {
        (void)begin;
        (void)len;
        return TColumnRef();
    }

}
