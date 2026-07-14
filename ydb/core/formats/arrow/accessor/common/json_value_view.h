#pragma once

#include <util/generic/string.h>

#include <optional>

namespace NKikimr::NArrow::NAccessor {

// A non-owning view over either BinaryJson or string/double/bool value.
class TJsonValueView {
public:
    static TJsonValueView OfBinaryJson(const TStringBuf& blob);
    static TJsonValueView OfString(const TStringBuf& value);
    static TJsonValueView OfNumber(double value);
    static TJsonValueView OfBool(bool value);

    // Return a string representation if the value is a non-null top-level scalar, nullopt otherwise.
    std::optional<TStringBuf> GetScalarOptional() const;

    // Raw BinaryJson bytes when the value is stored as a blob.
    // nullopt for a native scalar, which has no sub-structure.
    // So result will be different for a native scalar and the same scalar wrapped in BinaryJson.
    // Expected to be used only for containers, but no checks enforce that.
    std::optional<TStringBuf> GetBinaryJsonBlobOptional() const;
private:
    static std::optional<TString> JsonNumberToString(double jsonNumber);

    enum class EKind {
        BinaryJson,
        String,
        Number,
        Bool,
    };

    explicit TJsonValueView(const EKind kind)
        : Kind(kind) {
    }

    std::optional<TStringBuf> ScalarFromBinaryJson() const;
    std::optional<TStringBuf> ProjectNumber(double value) const;
    static TStringBuf ProjectBool(bool value);

    EKind Kind;
    // One of these fields is filled, depending on Kind.
    TStringBuf Bytes;
    double Number = 0;
    bool Bool = false;

    // For string scalar lazy evaluation.
    // For numbers, need to store string representation somewhere so that we can return a TStringBuf over it.
    mutable TString ScalarHolder;
};

} // namespace NKikimr::NArrow::NAccessor
