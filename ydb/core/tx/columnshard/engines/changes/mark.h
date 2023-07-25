#pragma once

#include <ydb/core/formats/arrow/replace_key.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimr::NOlap {

class TMark {
public:
    struct TCompare {
        using is_transparent = void;

        bool operator() (const TMark& a, const TMark& b) const {
            return a < b;
        }

        bool operator() (const arrow::Scalar& a, const TMark& b) const {
            return a < b;
        }

        bool operator() (const TMark& a, const arrow::Scalar& b) const {
            return a < b;
        }
    };

    explicit TMark(const NArrow::TReplaceKey& key)
        : Border(key)
    {}

    TMark(const TMark& m) = default;
    TMark& operator = (const TMark& m) = default;

    bool operator == (const TMark& m) const {
        return Border == m.Border;
    }

    std::partial_ordering operator <=> (const TMark& m) const {
        return Border <=> m.Border;
    }

    bool operator == (const arrow::Scalar& firstKey) const {
        // TODO: avoid ToScalar()
        return NArrow::ScalarCompare(*NArrow::TReplaceKey::ToScalar(Border, 0), firstKey) == 0;
    }

    std::partial_ordering operator <=> (const arrow::Scalar& firstKey) const {
        // TODO: avoid ToScalar()
        const int cmp = NArrow::ScalarCompare(*NArrow::TReplaceKey::ToScalar(Border, 0), firstKey);
        if (cmp < 0) {
            return std::partial_ordering::less;
        } else if (cmp > 0) {
            return std::partial_ordering::greater;
        } else {
            return std::partial_ordering::equivalent;
        }
    }

    const NArrow::TReplaceKey& GetBorder() const noexcept {
        return Border;
    }

    ui64 Hash() const {
        return Border.Hash();
    }

    operator size_t () const {
        return Hash();
    }

    operator bool () const = delete;

    static TString SerializeScalar(const NArrow::TReplaceKey& key, const std::shared_ptr<arrow::Schema>& schema);
    static NArrow::TReplaceKey DeserializeScalar(const TString& key, const std::shared_ptr<arrow::Schema>& schema);

    static TString SerializeComposite(const NArrow::TReplaceKey& key, const std::shared_ptr<arrow::Schema>& schema);
    static NArrow::TReplaceKey DeserializeComposite(const TString& key, const std::shared_ptr<arrow::Schema>& schema);

    static NArrow::TReplaceKey MinBorder(const std::shared_ptr<arrow::Schema>& schema);
    static NArrow::TReplaceKey ExtendBorder(const NArrow::TReplaceKey& key, const std::shared_ptr<arrow::Schema>& schema);

    std::string ToString() const;

private:
    NArrow::TReplaceKey Border;

    static std::shared_ptr<arrow::Scalar> MinScalar(const std::shared_ptr<arrow::DataType>& type);
};

}
