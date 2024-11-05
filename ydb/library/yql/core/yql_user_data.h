#pragma once

#include <library/cpp/enumbitset/enumbitset.h>

#include <ydb/library/yql/core/file_storage/file_storage.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <functional>

namespace NYql {

// -- user files --

enum class EUserDataType {
    URL = 1, // indicates that provided user data represents URL which can be used to obtain table data
    PATH = 2, // indicates that provided user data represents file path which can be used to obtain table data
    RAW_INLINE_DATA = 3, // table data is inside the provided data block
};

enum class EUserDataBlockUsage {
    Begin,
    Path = Begin,
    Content,
    Udf,
    Library,
    PgExt,
    End,
};
typedef TEnumBitSet<EUserDataBlockUsage, static_cast<int>(EUserDataBlockUsage::Begin),
    static_cast<int>(EUserDataBlockUsage::End)> TUserDataBlockUsage;

struct TUserDataBlock {
    EUserDataType Type = EUserDataType::PATH;
    TString UrlToken;
    TString Data;
    TUserDataBlockUsage Usage;

    TFileLinkPtr FrozenFile;
    // Prefix added to all UDF module names
    TString CustomUdfPrefix = {};

    THashMap<TString, TString> Options = {};
};

class TUserDataKey {
public:
    enum class EDataType {
        FILE, UDF
    };

    static TUserDataKey File(const TString& alias) {
        return { EDataType::FILE, alias };
    }

    static TUserDataKey File(const TStringBuf& alias) {
        return { EDataType::FILE, TString(alias) };
    }

    static TUserDataKey Udf(const TString& alias) {
        return { EDataType::UDF, alias };
    }

    static TUserDataKey Udf(const TStringBuf& alias) {
        return { EDataType::UDF, TString(alias) };
    }

    inline bool IsFile() const { return Type_ == EDataType::FILE; }
    inline bool IsUdf() const { return Type_ == EDataType::UDF; }
    inline const TString& Alias() const { return Alias_; }
    inline EDataType Type() const { return Type_; }

    bool operator<(const TUserDataKey& other) const {
        return std::tie(Type_, Alias_) < std::tie(other.Type_, other.Alias_);
    }

    struct THash {
        size_t operator()(const TUserDataKey& key) {
            auto type = static_cast<size_t>(key.Type_);
            return CombineHashes(type, ComputeHash(key.Alias_));
        }
    };

    struct TEqualTo {
        bool operator()(const TUserDataKey& k1, const TUserDataKey& k2) const {
            return k1.Type_ == k2.Type_ && k1.Alias_ == k2.Alias_;
        }
    };

private:
    inline TUserDataKey(EDataType type, TString alias)
        : Type_(type)
        , Alias_(std::move(alias))
    {
    }

private:
    const EDataType Type_;
    const TString Alias_;
};

inline IOutputStream& operator<<(IOutputStream& os, const TUserDataKey& key) {
    os << "Type: " << key.Type() << ", alias: " << key.Alias();
    return os;
}

using TUserDataTable = THashMap<TUserDataKey, TUserDataBlock, TUserDataKey::THash, TUserDataKey::TEqualTo>;

using TTokenResolver = std::function<TString(const TString&, const TString&)>;

} // namespace NYql
