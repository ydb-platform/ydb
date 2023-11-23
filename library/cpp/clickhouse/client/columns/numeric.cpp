#include "numeric.h"

#include "utils.h"

namespace NClickHouse {
    template <typename T>
    TColumnVector<T>::TColumnVector()
        : TColumn(TType::CreateSimple<T>())
    {
    }

    template <typename T>
    TColumnVector<T>::TColumnVector(const TVector<T>& data)
        : TColumn(TType::CreateSimple<T>())
        , Data_(data)
    {
    }

    template <typename T>
    TColumnVector<T>::TColumnVector(TVector<T>&& data)
        : TColumn(TType::CreateSimple<T>())
        , Data_(std::move(data))
    {
    }

    template <typename T>
    TIntrusivePtr<TColumnVector<T>> TColumnVector<T>::Create() {
        return new TColumnVector<T>();
    }

    template <typename T>
    TIntrusivePtr<TColumnVector<T>> TColumnVector<T>::Create(const TVector<T>& data) {
        return new TColumnVector<T>(data);
    }

    template <typename T>
    TIntrusivePtr<TColumnVector<T>> TColumnVector<T>::Create(TVector<T>&& data) {
        return new TColumnVector<T>(std::move(data));
    }

    template <typename T>
    void TColumnVector<T>::Append(const T& value) {
        Data_.push_back(value);
    }

    template <typename T>
    const T& TColumnVector<T>::At(size_t n) const {
        return Data_.at(n);
    }

    template <typename T>
    const T& TColumnVector<T>::operator[](size_t n) const {
        return Data_[n];
    }

    template <typename T>
    void TColumnVector<T>::SetAt(size_t n, const T& value) {
        Data_.at(n) = value;
    }

    template <typename T>
    void TColumnVector<T>::Append(TColumnRef column) {
        if (auto col = column->As<TColumnVector<T>>()) {
            Data_.insert(Data_.end(), col->Data_.begin(), col->Data_.end());
        }
    }

    template <typename T>
    bool TColumnVector<T>::Load(TCodedInputStream* input, size_t rows) {
        Data_.resize(rows);

        return input->ReadRaw(Data_.data(), Data_.size() * sizeof(T));
    }

    template <typename T>
    void TColumnVector<T>::Save(TCodedOutputStream* output) {
        output->WriteRaw(Data_.data(), Data_.size() * sizeof(T));
    }

    template <typename T>
    size_t TColumnVector<T>::Size() const {
        return Data_.size();
    }

    template <typename T>
    TColumnRef TColumnVector<T>::Slice(size_t begin, size_t len) {
        return new TColumnVector<T>(SliceVector(Data_, begin, len));
    }

    template class TColumnVector<i8>;
    template class TColumnVector<i16>;
    template class TColumnVector<i32>;
    template class TColumnVector<i64>;

    template class TColumnVector<ui8>;
    template class TColumnVector<ui16>;
    template class TColumnVector<ui32>;
    template class TColumnVector<ui64>;

    template class TColumnVector<float>;
    template class TColumnVector<double>;

}
