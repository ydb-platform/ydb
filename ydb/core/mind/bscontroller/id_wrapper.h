#pragma once
#include <concepts>
#include <compare>
#include "defs.h"
#include <util/generic/string.h>
using TString = TBasicString<char>;

class TGroupIdTag;
template <class T> 
concept IntegralType = std::is_integral<T>::value;

template <class T>
concept StringType = std::convertible_to<T, TString>;
template <typename T, typename Tag>
class TIdWrapper {
private:
    T Raw;
public:
    TIdWrapper() = default;

    ~TIdWrapper() = default;

    TIdWrapper(const T& value) : Raw(value) {};

    TIdWrapper(const TIdWrapper& other) : TIdWrapper(other.Raw) {};


    TIdWrapper& operator=(const T& value) = delete;

    void CopyToProto(NProtoBuf::Message *message, void (NProtoBuf::Message::*pfn)(T value)) {
        (message->*pfn)(*this);
    }

    T& operator+=(const T& other) {
        return this->Raw += other.Raw;
    }


    friend TIdWrapper operator+(const TIdWrapper& first, const TIdWrapper& second) Y_WARN_UNUSED_RESULT {
        return first + second;
    }

    constexpr auto operator<=>(const TIdWrapper&) const = default;

    T GetRawId(){
        return this->Raw;
    }

    friend std::hash<TIdWrapper<T, Tag>>;
    friend THash<TIdWrapper<T, Tag>>;
};

template<typename T, typename Tag>
struct std::hash<TIdWrapper<T, Tag>>{
   std::size_t operator()(const TIdWrapper<T, Tag>& id) const{
        return std::hash<T>{}(id.Raw);
   }
};

template<typename T, typename Tag>
struct THash<TIdWrapper<T, Tag>>{
   std::size_t operator()(const TIdWrapper<T, Tag>& id) const{
        return THash<T>()(id.Raw);
   }
};
