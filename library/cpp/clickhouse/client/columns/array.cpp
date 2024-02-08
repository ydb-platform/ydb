#include "array.h"

#include <util/generic/yexception.h>

namespace NClickHouse {
    TColumnArray::TColumnArray(TColumnRef data)
        : TColumn(TType::CreateArray(data->Type()))
        , Data_(data)
        , Offsets_(TColumnUInt64::Create())
    {
    }

    TColumnArray::TColumnArray(TColumnRef data, TVector<ui64>&& offsets)
        : TColumn(TType::CreateArray(data->Type()))
        , Data_(data)
        , Offsets_(TColumnUInt64::Create(std::move(offsets)))
    {
    }

    TIntrusivePtr<TColumnArray> TColumnArray::Create(TColumnRef data) {
        return new TColumnArray(data);
    }

    TIntrusivePtr<TColumnArray> TColumnArray::Create(TColumnRef data, TVector<ui64>&& offsets) {
        return new TColumnArray(data, std::move(offsets));
    }

    void TColumnArray::AppendAsColumn(TColumnRef array) {
        if (!Data_->Type()->IsEqual(array->Type())) {
            ythrow yexception()
                << "can't append column of type " << array->Type()->GetName() << " "
                << "to column type " << Data_->Type()->GetName();
        }

        if (Offsets_->Size() == 0) {
            Offsets_->Append(array->Size());
        } else {
            Offsets_->Append((*Offsets_)[Offsets_->Size() - 1] + array->Size());
        }

        Data_->Append(array);
    }

    void TColumnArray::Append(TColumnRef column) {
        if (auto col = column->As<TColumnArray>()) {
            if (!col->Data_->Type()->IsEqual(Data_->Type())) {
                return;
            }

            for (size_t i = 0; i < col->Size(); ++i) {
                AppendAsColumn(col->GetAsColumn(i));
            }
        }
    }

    TColumnRef TColumnArray::GetAsColumn(size_t n) const {
        return Data_->Slice(GetOffset(n), GetSize(n));
    }

    bool TColumnArray::Load(TCodedInputStream* input, size_t rows) {
        if (!Offsets_->Load(input, rows)) {
            return false;
        }
        if (!Data_->Load(input, (*Offsets_)[rows - 1])) {
            return false;
        }
        return true;
    }

    void TColumnArray::Save(TCodedOutputStream* output) {
        Offsets_->Save(output);
        Data_->Save(output);
    }

    size_t TColumnArray::Size() const {
        return Offsets_->Size();
    }

    size_t TColumnArray::GetOffset(size_t n) const {
        return (n == 0) ? 0 : (*Offsets_)[n - 1];
    }

    size_t TColumnArray::GetSize(size_t n) const {
        return (n == 0) ? (*Offsets_)[n] : ((*Offsets_)[n] - (*Offsets_)[n - 1]);
    }

}
