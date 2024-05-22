#pragma once
#include "defs.h"
#include <compare>
#include <concepts>
#include <util/generic/string.h>
using TString = TBasicString<char>;

class TGroupIdTag;
template <class T>
concept IntegralType = std::is_integral<T>::value;

template <class T>
concept StringType = std::convertible_to<T, TString>;
template <typename T, typename Tag> class TIdWrapper {
private:
  T Raw;

public:
  TIdWrapper() = default;

  ~TIdWrapper() = default;

  TIdWrapper(const T &value) : Raw(value){};

  TIdWrapper(TIdWrapper &&value) = default;

  TIdWrapper(const TIdWrapper &other) = default;

  TIdWrapper &operator=(const TIdWrapper &value) = default;

  TIdWrapper &operator=(TIdWrapper &&value) = default;

  void CopyToProto(NProtoBuf::Message *message,
                   void (NProtoBuf::Message::*pfn)(T value)) {
    (message->*pfn)(*this);
  }

  TIdWrapper &operator+=(const T &other) {
    Raw += other.Raw;
    return *this;
  }

  friend TIdWrapper operator+(const TIdWrapper &first,
                              const T &second) Y_WARN_UNUSED_RESULT {
    return TIdWrapper(first->Raw + second);
  }

  TIdWrapper &operator++() {
    Raw++;
    return *this;
  }

  TIdWrapper operator++(int) {
    TIdWrapper old = *this;
    operator++();
    return old;
  }

  constexpr auto operator<=>(const TIdWrapper &) const = default;

  T GetRawId() const { return Raw; }

  friend std::hash<TIdWrapper<T, Tag>>;
  friend THash<TIdWrapper<T, Tag>>;
};

template <typename T, typename Tag> struct std::hash<TIdWrapper<T, Tag>> {
  std::size_t operator()(const TIdWrapper<T, Tag> &id) const {
    return std::hash<T>{}(id.Raw);
  }
};

template <typename T, typename Tag> struct THash<TIdWrapper<T, Tag>> {
  std::size_t operator()(const TIdWrapper<T, Tag> &id) const {
    return THash<T>()(id.Raw);
  }
};
