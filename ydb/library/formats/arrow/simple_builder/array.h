#pragma once
#include <ydb/library/accessor/accessor.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_dict.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow::NConstruction {

class IArrayBuilder {
private:
    YDB_READONLY_DEF(TString, FieldName);
    YDB_ACCESSOR(bool, Nullable, true);
protected:
    virtual std::shared_ptr<arrow::Array> DoBuildArray(const ui32 recordsCount) const = 0;
public:
    using TPtr = std::shared_ptr<IArrayBuilder>;
    virtual ~IArrayBuilder() = default;
    std::shared_ptr<arrow::Array> BuildArray(const ui32 recordsCount) const {
        return DoBuildArray(recordsCount);
    }

    IArrayBuilder(const TString& fieldName, bool nullable = true)
        : FieldName(fieldName)
        , Nullable(nullable)
    {
    }
};

template <class TValue>
class TFillerBuilderConstructor {
public:
    using TBuilder = typename arrow::TypeTraits<TValue>::BuilderType;
    static TBuilder Construct() {
        return TBuilder();
    }
};

template <>
class TFillerBuilderConstructor<arrow::TimestampType> {
public:
    using TBuilder = arrow::TypeTraits<arrow::TimestampType>::BuilderType;
    static TBuilder Construct() {
        return arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
    }
};

template <class TFiller>
class TSimpleArrayConstructor: public IArrayBuilder {
private:
    using TBase = IArrayBuilder;
    using TSelf = TSimpleArrayConstructor<TFiller>;
    using TBuilder = typename arrow::TypeTraits<typename TFiller::TValue>::BuilderType;
    const TFiller Filler;
    ui32 ShiftValue = 0;

    TSimpleArrayConstructor(const TString& fieldName, bool nullable, const TFiller& filler, ui32 shiftValue = 0)
        : TBase(fieldName, nullable)
        , Filler(filler)
        , ShiftValue(shiftValue)
    {
    }
protected:
    virtual std::shared_ptr<arrow::Array> DoBuildArray(const ui32 recordsCount) const override {
        TBuilder fBuilder = TFillerBuilderConstructor<typename TFiller::TValue>::Construct();
        Y_ABORT_UNLESS(fBuilder.Reserve(recordsCount).ok());
        for (ui32 i = 0; i < recordsCount; ++i) {
            Y_ABORT_UNLESS(fBuilder.Append(Filler.GetValue(i + ShiftValue)).ok());
        }
        return *fBuilder.Finish();
    }


public:
    TSimpleArrayConstructor(const TString& fieldName, const TFiller& filler = TFiller(), ui32 shiftValue = 0)
        : TBase(fieldName)
        , Filler(filler)
        , ShiftValue(shiftValue)
    {
    }

    static IArrayBuilder::TPtr BuildNotNullable(const TString& fieldName, const TFiller& filler = TFiller()) {
        return std::shared_ptr<TSelf>(new TSelf(fieldName, false, filler));
    }
};

template <class TFiller>
class TBinaryArrayConstructor: public IArrayBuilder {
private:
    using TBase = IArrayBuilder;
    using TBuilder = typename arrow::TypeTraits<typename TFiller::TValue>::BuilderType;
    const TFiller Filler;
protected:
    virtual std::shared_ptr<arrow::Array> DoBuildArray(const ui32 recordsCount) const override {
        TBuilder fBuilder = TBuilder();
        std::vector<const char*> values;
        values.reserve(recordsCount);
        for (ui32 i = 0; i < recordsCount; ++i) {
            values.emplace_back(Filler.GetValueView(i));
        }
        auto addStatus = fBuilder.AppendValues(values.data(), recordsCount);
        if (!addStatus.ok()) {
            const std::string errorMessage = addStatus.ToString();
            Y_ABORT_UNLESS(false, "%s", errorMessage.data());
        }
        return *fBuilder.Finish();
    }
public:
    TBinaryArrayConstructor(const TString& fieldName, const TFiller& filler = TFiller())
        : TBase(fieldName)
        , Filler(filler) {

    }
};

template <class TFiller>
class TDictionaryArrayConstructor: public IArrayBuilder {
private:
    using TBase = IArrayBuilder;
    const TFiller Filler;
protected:
    virtual std::shared_ptr<arrow::Array> DoBuildArray(const ui32 recordsCount) const override {
        auto fBuilder = std::make_shared<arrow::DictionaryBuilder<typename TFiller::TValue>>(std::make_shared<typename TFiller::TValue>());
        Y_ABORT_UNLESS(fBuilder->Reserve(recordsCount).ok());
        for (ui32 i = 0; i < recordsCount; ++i) {
            Y_ABORT_UNLESS(fBuilder->Append(Filler.GetValue(i)).ok());
        }
        return *fBuilder->Finish();
    }
public:
    TDictionaryArrayConstructor(const TString& fieldName, const TFiller& filler = TFiller())
        : TBase(fieldName)
        , Filler(filler) {

    }
};
}
