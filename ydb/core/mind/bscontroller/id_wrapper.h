#include <concepts>
#include <compare>
#include <ydb/util/generic/string.h>
using TString = TBasicString<char>;

class TGroupIdTag;
template <class T> 
concept IntegralType = std::is_integral<T>::value;

concept StringType = std::convertible_to<TString>;
template <typename T, typename Tag>
class TIdWrapper {
private:
    T Raw;
public:
    TIdWrapper() = default;

    ~TIdWrapper() = default;

    TIdWrapper(const T& value) : Raw(value) {};

    TIdWrapper(const TIdWrapper& other) : TIdWrapper(other.Raw);


    TIdWrapper& operator=(const T& value) = delete;

    void CopyToProto(NProtoBuf::Message *message, void (NProtoBuf::Message::*pfn)(T value)) {
        (message->*pfn)(*this);
    }

    T& operator+=(const T& other) {
        return this.Raw += other.Raw;
    }


    friend TBasicString operator+(const T& first, const T& other) Y_WARN_UNUSED_RESULT {
        return first + second;
    }

    constexpr auto operator<=>(const IntWrapper&) const = default;
  

};