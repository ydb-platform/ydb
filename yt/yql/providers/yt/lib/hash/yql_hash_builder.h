#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <openssl/sha.h>

#include <utility>
#include <type_traits>

namespace NYql {

class THashBuilder {
public:
    THashBuilder();

    template <typename T, std::enable_if_t<std::is_pod<std::remove_cv_t<T>>::value>* = nullptr>
    void Update(T data) {
        Update(&data, sizeof(data));
    }
    void Update(TStringBuf data);
    void Update(const TString& data);

    TString Finish();

private:
    void Update(const void* data, size_t len);

private:
    SHA256_CTX Sha256;
};

template <class T>
static inline THashBuilder& operator<<(THashBuilder& builder, const T& t) {
    builder.Update(t);
    return builder;
}

template <class T>
static inline THashBuilder&& operator<<(THashBuilder&& builder, const T& t) {
    builder.Update(t);
    return std::move(builder);
}

}
