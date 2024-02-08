#include "string.h"
#include "utils.h"

#include <library/cpp/clickhouse/client/base/wire_format.h>

#include <util/memory/tempbuf.h>

namespace NClickHouse {
    TColumnFixedString::TColumnFixedString(size_t n)
        : TColumn(TType::CreateString(n))
        , StringSize_(n)
    {
    }

    TColumnFixedString::TColumnFixedString(size_t n, const TVector<TString>& data)
        : TColumnFixedString(n)
    {
        Data_.reserve(data.size());
        for (const auto& value : data) {
            Append(value);
        }
    }

    TIntrusivePtr<TColumnFixedString> TColumnFixedString::Create(size_t n) {
        return new TColumnFixedString(n);
    }

    TIntrusivePtr<TColumnFixedString> TColumnFixedString::Create(size_t n, const TVector<TString>& data) {
        return new TColumnFixedString(n, data);
    }

    void TColumnFixedString::Append(const TString& str) {
        Data_.push_back(str);
        Data_.back().resize(StringSize_);
    }

    const TString& TColumnFixedString::At(size_t n) const {
        return Data_.at(n);
    }

    const TString& TColumnFixedString::operator[](size_t n) const {
        return Data_[n];
    }

    void TColumnFixedString::SetAt(size_t n, const TString& value) {
        TString stringResized(value);
        stringResized.resize(StringSize_);
        Data_.at(n) = stringResized;
    }

    void TColumnFixedString::Append(TColumnRef column) {
        if (auto col = column->As<TColumnFixedString>()) {
            if (StringSize_ == col->StringSize_) {
                Data_.insert(Data_.end(), col->Data_.begin(), col->Data_.end());
            }
        }
    }

    bool TColumnFixedString::Load(TCodedInputStream* input, size_t rows) {
        for (size_t i = 0; i < rows; ++i) {
            TTempBuf s(StringSize_);

            if (!TWireFormat::ReadBytes(input, s.Data(), StringSize_)) {
                return false;
            }

            Data_.push_back(TString(s.Data(), StringSize_));
        }

        return true;
    }

    void TColumnFixedString::Save(TCodedOutputStream* output) {
        for (size_t i = 0; i < Data_.size(); ++i) {
            TWireFormat::WriteBytes(output, Data_[i].data(), StringSize_);
        }
    }

    size_t TColumnFixedString::Size() const {
        return Data_.size();
    }

    TColumnRef TColumnFixedString::Slice(size_t begin, size_t len) {
        auto result = new TColumnFixedString(StringSize_);

        if (begin < Data_.size()) {
            result->Data_ = SliceVector(Data_, begin, len);
        }

        return result;
    }

    TColumnString::TColumnString()
        : TColumn(TType::CreateString())
    {
    }

    TColumnString::TColumnString(const TVector<TString>& data)
        : TColumn(TType::CreateString())
        , Data_(data)
    {
    }

    TColumnString::TColumnString(TVector<TString>&& data)
        : TColumn(TType::CreateString())
        , Data_(std::move(data))
    {
    }

    TIntrusivePtr<TColumnString> TColumnString::Create() {
        return new TColumnString();
    }

    TIntrusivePtr<TColumnString> TColumnString::Create(const TVector<TString>& data) {
        return new TColumnString(data);
    }

    TIntrusivePtr<TColumnString> TColumnString::Create(TVector<TString>&& data) {
        return new TColumnString(std::move(data));
    }

    void TColumnString::Append(const TString& str) {
        Data_.push_back(str);
    }

    const TString& TColumnString::At(size_t n) const {
        return Data_.at(n);
    }

    const TString& TColumnString::operator[](size_t n) const {
        return Data_[n];
    }

    void TColumnString::SetAt(size_t n, const TString& value) {
        Data_.at(n) = value;
    }

    void TColumnString::Append(TColumnRef column) {
        if (auto col = column->As<TColumnString>()) {
            Data_.insert(Data_.end(), col->Data_.begin(), col->Data_.end());
        }
    }

    bool TColumnString::Load(TCodedInputStream* input, size_t rows) {
        for (size_t i = 0; i < rows; ++i) {
            TString s;

            if (!TWireFormat::ReadString(input, &s)) {
                return false;
            }

            Data_.push_back(s);
        }

        return true;
    }

    void TColumnString::Save(TCodedOutputStream* output) {
        for (auto si = Data_.begin(); si != Data_.end(); ++si) {
            TWireFormat::WriteString(output, *si);
        }
    }

    size_t TColumnString::Size() const {
        return Data_.size();
    }

    TColumnRef TColumnString::Slice(size_t begin, size_t len) {
        return new TColumnString(SliceVector(Data_, begin, len));
    }

    TColumnStringBuf::TColumnStringBuf()
        : TColumn(TType::CreateString())
    {
    }

    TColumnStringBuf::TColumnStringBuf(const TVector<TStringBuf>& data)
        : TColumn(TType::CreateString())
        , Data_(data)
    {
    }

    TColumnStringBuf::TColumnStringBuf(TVector<TStringBuf>&& data)
        : TColumn(TType::CreateString())
        , Data_(std::move(data))
    {
    }

    TIntrusivePtr<TColumnStringBuf> TColumnStringBuf::Create() {
        return new TColumnStringBuf();
    }

    TIntrusivePtr<TColumnStringBuf> TColumnStringBuf::Create(const TVector<TStringBuf>& data) {
        return new TColumnStringBuf(data);
    }

    TIntrusivePtr<TColumnStringBuf> TColumnStringBuf::Create(TVector<TStringBuf>&& data) {
        return new TColumnStringBuf(std::move(data));
    }

    void TColumnStringBuf::Append(TStringBuf str) {
        Data_.push_back(str);
    }

    const TStringBuf& TColumnStringBuf::At(size_t n) const {
        return Data_.at(n);
    }

    const TStringBuf& TColumnStringBuf::operator[](size_t n) const {
        return Data_[n];
    }

    void TColumnStringBuf::SetAt(size_t n, TStringBuf value) {
        Data_.at(n) = value;
    }

    void TColumnStringBuf::Append(TColumnRef column) {
        if (auto col = column->As<TColumnStringBuf>()) {
            Data_.insert(Data_.end(), col->Data_.begin(), col->Data_.end());
        }
    }

    bool TColumnStringBuf::Load(TCodedInputStream*, size_t) {
        ythrow yexception() << "load not implemented";
    }

    void TColumnStringBuf::Save(TCodedOutputStream* output) {
        for (auto si = Data_.begin(); si != Data_.end(); ++si) {
            TWireFormat::WriteStringBuf(output, *si);
        }
    }

    size_t TColumnStringBuf::Size() const {
        return Data_.size();
    }

    TColumnRef TColumnStringBuf::Slice(size_t begin, size_t len) {
        return new TColumnStringBuf(SliceVector(Data_, begin, len));
    }

}
