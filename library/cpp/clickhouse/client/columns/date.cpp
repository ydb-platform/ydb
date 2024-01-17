#include "date.h"

namespace NClickHouse {
    TIntrusivePtr<TColumnDate> TColumnDate::Create() {
        return new TColumnDate();
    }

    TIntrusivePtr<TColumnDate> TColumnDate::Create(const TVector<TInstant>& data) {
        return new TColumnDate(data);
    }

    TColumnDate::TColumnDate()
        : TColumn(TType::CreateDate())
        , Data_(TColumnUInt16::Create())
    {
    }

    TColumnDate::TColumnDate(const TVector<TInstant>& data)
        : TColumnDate()
    {
        for (const auto& value : data) {
            Append(value);
        }
    }

    void TColumnDate::Append(const TInstant& value) {
        Data_->Append(static_cast<ui16>(value.Days()));
    }

    std::time_t TColumnDate::At(size_t n) const {
        return Data_->At(n) * 86400;
    }

    void TColumnDate::SetAt(size_t n, const TInstant& value) {
        Data_->SetAt(n, static_cast<ui16>(value.Days()));
    }

    void TColumnDate::Append(TColumnRef column) {
        if (auto col = column->As<TColumnDate>()) {
            Data_->Append(col->Data_);
        }
    }

    bool TColumnDate::Load(TCodedInputStream* input, size_t rows) {
        return Data_->Load(input, rows);
    }

    void TColumnDate::Save(TCodedOutputStream* output) {
        Data_->Save(output);
    }

    size_t TColumnDate::Size() const {
        return Data_->Size();
    }

    TColumnRef TColumnDate::Slice(size_t begin, size_t len) {
        auto col = Data_->Slice(begin, len)->As<TColumnUInt16>();
        auto result = TColumnDate::Create();

        result->Data_->Append(col);

        return result;
    }

    TColumnDateTime::TColumnDateTime()
        : TColumn(TType::CreateDateTime())
        , Data_(TColumnUInt32::Create())
    {
    }

    TColumnDateTime::TColumnDateTime(const TVector<TInstant>& data)
        : TColumnDateTime()
    {
        for (const auto& value : data) {
            Append(value);
        }
    }

    TIntrusivePtr<TColumnDateTime> TColumnDateTime::Create() {
        return new TColumnDateTime();
    }

    TIntrusivePtr<TColumnDateTime> TColumnDateTime::Create(const TVector<TInstant>& data) {
        return new TColumnDateTime(data);
    }

    void TColumnDateTime::Append(const TInstant& value) {
        Data_->Append(static_cast<ui32>(value.Seconds()));
    }

    std::time_t TColumnDateTime::At(size_t n) const {
        return Data_->At(n);
    }

    void TColumnDateTime::SetAt(size_t n, const TInstant& value) {
        Data_->SetAt(n, static_cast<ui32>(value.Seconds()));
    }

    void TColumnDateTime::Append(TColumnRef column) {
        if (auto col = column->As<TColumnDateTime>()) {
            Data_->Append(col->Data_);
        }
    }

    bool TColumnDateTime::Load(TCodedInputStream* input, size_t rows) {
        return Data_->Load(input, rows);
    }

    void TColumnDateTime::Save(TCodedOutputStream* output) {
        Data_->Save(output);
    }

    size_t TColumnDateTime::Size() const {
        return Data_->Size();
    }

    TColumnRef TColumnDateTime::Slice(size_t begin, size_t len) {
        auto col = Data_->Slice(begin, len)->As<TColumnUInt32>();
        auto result = TColumnDateTime::Create();

        result->Data_->Append(col);

        return result;
    }

}
