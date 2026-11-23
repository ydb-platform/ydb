// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/common/detail/ascii.h"
#include "opentelemetry/common/kv_properties.h"
#include "opentelemetry/common/macros.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE

namespace baggage
{

class OPENTELEMETRY_EXPORT Baggage
{
public:
  static constexpr size_t kMaxKeyValuePairs = 180;
  static constexpr size_t kMaxKeyValueSize  = 4096;
  static constexpr size_t kMaxSize          = 8192;
  static constexpr char kKeyValueSeparator  = '=';
  static constexpr char kMembersSeparator   = ',';
  static constexpr char kMetadataSeparator  = ';';

  Baggage() noexcept : kv_properties_(new common::KeyValueProperties()) {}
  Baggage(size_t size) noexcept : kv_properties_(new common::KeyValueProperties(size)) {}

  template <class T>
  Baggage(const T &keys_and_values) noexcept
      : kv_properties_(new common::KeyValueProperties(keys_and_values))
  {}

  OPENTELEMETRY_API_SINGLETON static nostd::shared_ptr<Baggage> GetDefault()
  {
    static nostd::shared_ptr<Baggage> baggage{new Baggage()};
    return baggage;
  }

  /* Get value for key in the baggage
     @returns true if key is found, false otherwise
  */
  bool GetValue(nostd::string_view key, std::string &value) const noexcept
  {
    return kv_properties_->GetValue(key, value);
  }

  /* Returns shared_ptr of new baggage object which contains new key-value pair. If key or value is
     invalid, copy of current baggage is returned
  */
  nostd::shared_ptr<Baggage> Set(const nostd::string_view &key,
                                 const nostd::string_view &value) noexcept
  {

    nostd::shared_ptr<Baggage> baggage(new Baggage(kv_properties_->Size() + 1));
    const bool valid_kv = IsValidKey(key) && IsValidValue(value) && IsValidValueMetadata(value);

    if (valid_kv)
    {
      baggage->kv_properties_->AddEntry(key, value);
    }

    // add rest of the fields.
    kv_properties_->GetAllEntries(
        [&baggage, &key, &valid_kv](nostd::string_view e_key, nostd::string_view e_value) {
          // if key or value was not valid, add all the entries. Add only remaining entries
          // otherwise.
          if (!valid_kv || key != e_key)
          {
            baggage->kv_properties_->AddEntry(e_key, e_value);
          }

          return true;
        });

    return baggage;
  }

  // @return all key-values entries by repeatedly invoking the function reference passed as argument
  // for each entry
  bool GetAllEntries(
      nostd::function_ref<bool(nostd::string_view, nostd::string_view)> callback) const noexcept
  {
    return kv_properties_->GetAllEntries(callback);
  }

  // delete key from the baggage if it exists. Returns shared_ptr of new baggage object.
  // if key does not exist, copy of current baggage is returned.
  // Validity of key is not checked as invalid keys should never be populated in baggage in the
  // first place.
  nostd::shared_ptr<Baggage> Delete(nostd::string_view key) noexcept
  {
    // keeping size of baggage same as key might not be found in it
    nostd::shared_ptr<Baggage> baggage(new Baggage(kv_properties_->Size()));
    kv_properties_->GetAllEntries(
        [&baggage, &key](nostd::string_view e_key, nostd::string_view e_value) {
          if (key != e_key)
            baggage->kv_properties_->AddEntry(e_key, e_value);
          return true;
        });
    return baggage;
  }

  // Returns shared_ptr of baggage after extracting key-value pairs from header
  static nostd::shared_ptr<Baggage> FromHeader(nostd::string_view header) noexcept
  {
    if (header.size() > kMaxSize)
    {
      // header size exceeds maximum threshold, return empty baggage
      return GetDefault();
    }

    common::KeyValueStringTokenizer kv_str_tokenizer(header);
    size_t cnt = kv_str_tokenizer.NumTokens();  // upper bound on number of kv pairs
    if (cnt > kMaxKeyValuePairs)
    {
      cnt = kMaxKeyValuePairs;
    }

    nostd::shared_ptr<Baggage> baggage(new Baggage(cnt));
    bool kv_valid{false};
    nostd::string_view key, value;

    while (kv_str_tokenizer.next(kv_valid, key, value) && baggage->kv_properties_->Size() < cnt)
    {
      if (!kv_valid || key.size() + value.size() > kMaxKeyValueSize)
      {
        continue;
      }
      // Preserve the tokenizer's edge-whitespace normalization, but reject
      // unsafe bytes before decoding or separating metadata from the value.
      if (!IsValidWireString(key) || !IsValidWireString(value))
      {
        continue;
      }

      // NOTE : metadata is kept as part of value only as it does not have any semantic meaning.
      // but, we need to extract it (else Decode on value will return error)
      nostd::string_view metadata;
      auto metadata_separator = value.find(kMetadataSeparator);
      if (metadata_separator != std::string::npos)
      {
        metadata = common::StringUtil::Substr(value, metadata_separator);
        value    = common::StringUtil::Substr(value, 0, metadata_separator);
      }

      bool err       = false;
      auto key_str   = UrlDecode(common::StringUtil::Trim(key), err);
      auto value_str = UrlDecode(common::StringUtil::Trim(value), err);

      if (err == false && IsValidKey(key_str) && IsValidValue(value_str) &&
          IsValidMetadata(metadata))
      {
        if (!metadata.empty())
        {
          value_str.append(metadata.data(), metadata.size());
        }
        baggage->kv_properties_->AddEntry(key_str, value_str);
      }
    }

    return baggage;
  }

  // Creates string from baggage object.
  std::string ToHeader() const noexcept
  {
    std::string header_s;
    bool first = true;
    kv_properties_->GetAllEntries([&](nostd::string_view key, nostd::string_view value) {
      // Constructors can populate baggage without going through FromHeader or Set.
      auto metadata_separator = value.find(kMetadataSeparator);
      if (!IsValidValueMetadata(value))
      {
        return true;
      }
      if (!first)
      {
        header_s.push_back(kMembersSeparator);
      }
      else
      {
        first = false;
      }
      header_s.append(UrlEncode(key));
      header_s.push_back(kKeyValueSeparator);

      // extracting metadata from value. We do not encode metadata
      if (metadata_separator != std::string::npos)
      {
        header_s.append(UrlEncode(common::StringUtil::Substr(value, 0, metadata_separator)));
        auto metadata = common::StringUtil::Substr(value, metadata_separator);
        header_s.append(std::string(metadata.data(), metadata.size()));
      }
      else
      {
        header_s.append(UrlEncode(value));
      }
      return true;
    });
    return header_s;
  }

private:
  static bool IsValidValueMetadata(nostd::string_view value) noexcept
  {
    auto separator = value.find(kMetadataSeparator);
    return separator == nostd::string_view::npos ||
           IsValidMetadata(common::StringUtil::Substr(value, separator));
  }

  static nostd::string_view TrimOws(nostd::string_view value) noexcept
  {
    while (!value.empty() && (value[0] == ' ' || value[0] == '\t'))
    {
      value = common::StringUtil::Substr(value, 1);
    }
    while (!value.empty() && (value[value.size() - 1] == ' ' || value[value.size() - 1] == '\t'))
    {
      value = common::StringUtil::Substr(value, 0, value.size() - 1);
    }
    return value;
  }

  static bool IsValidWireString(nostd::string_view value) noexcept
  {
    for (unsigned char ch : value)
    {
      if ((ch < 0x20 && ch != '\t') || ch > 0x7e)
      {
        return false;
      }
    }
    return true;
  }

  static bool IsToken(nostd::string_view value) noexcept
  {
    if (value.empty())
    {
      return false;
    }
    for (char ch : value)
    {
      if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
            nostd::string_view("!#$%&'*+-.^_`|~").find(ch) != nostd::string_view::npos))
      {
        return false;
      }
    }
    return true;
  }

  // Metadata is stored in wire form. Validate properties without decoding or
  // re-encoding them, so existing percent escapes retain their meaning.
  static bool IsValidMetadata(nostd::string_view metadata) noexcept
  {
    while (!metadata.empty())
    {
      if (metadata[0] != kMetadataSeparator)
      {
        return false;
      }
      metadata      = common::StringUtil::Substr(metadata, 1);
      auto end      = metadata.find(kMetadataSeparator);
      auto property = TrimOws(common::StringUtil::Substr(metadata, 0, end));
      auto equals   = property.find(kKeyValueSeparator);
      if (!IsToken(TrimOws(common::StringUtil::Substr(property, 0, equals))))
      {
        return false;
      }
      if (equals != nostd::string_view::npos)
      {
        for (unsigned char ch : TrimOws(common::StringUtil::Substr(property, equals + 1)))
        {
          // W3C baggage-octet; SP and HTAB are only allowed around properties.
          if (ch < 0x21 || ch > 0x7e || ch == '"' || ch == ',' || ch == '\\')
          {
            return false;
          }
        }
      }
      if (end == nostd::string_view::npos)
      {
        return true;
      }
      metadata = common::StringUtil::Substr(metadata, end);
    }
    return true;
  }

  static bool IsPrintableString(nostd::string_view str)
  {
    for (const auto ch : str)
    {
      if (ch < ' ' || ch > '~')
      {
        return false;
      }
    }

    return true;
  }

  static bool IsValidKey(nostd::string_view key) { return key.size() && IsPrintableString(key); }

  static bool IsValidValue(nostd::string_view value) { return IsPrintableString(value); }

  // Uri encode key value pairs before injecting into header
  // Implementation inspired from : https://golang.org/src/net/url/url.go?s=7851:7884#L264
  static std::string UrlEncode(nostd::string_view str)
  {
    std::string ret;

    for (auto c : str)
    {
      if (common::detail::IsAsciiAlnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
      {
        ret.push_back(c);
      }
      else if (c == ' ')
      {
        ret.push_back('+');
      }
      else
      {
        ret.push_back('%');
        ret.push_back(common::detail::UpperHexDigit(static_cast<unsigned char>(c) >> 4));
        ret.push_back(common::detail::UpperHexDigit(c));
      }
    }

    return ret;
  }

  // Uri decode key value pairs after extracting from header
  static std::string UrlDecode(nostd::string_view str, bool &err)
  {
    std::string ret;
    ret.reserve(str.size());

    for (size_t i = 0; i < str.size(); i++)
    {
      if (str[i] == '%')
      {
        if (i + 2 >= str.size() || !common::detail::IsAsciiHexDigit(str[i + 1]) ||
            !common::detail::IsAsciiHexDigit(str[i + 2]))
        {
          err = true;
          return "";
        }
        ret.push_back(static_cast<char>(common::detail::HexDigitValue(str[i + 1]) << 4 |
                                        common::detail::HexDigitValue(str[i + 2])));
        i += 2;
      }
      else if (str[i] == '+')
      {
        ret.push_back(' ');
      }
      // See https://www.w3.org/TR/baggage/#definition
      else if ((str[i] >= '!')      /* 21 */
               && (str[i] <= '~')   /* 7E */
               && (str[i] != '"')   /* 22 */
               && (str[i] != ',')   /* 2C */
               && (str[i] != ';')   /* 3B */
               && (str[i] != '\\')) /* 5C */
      {
        ret.push_back(str[i]);
      }
      else
      {
        err = true;
        return "";
      }
    }

    return ret;
  }

private:
  // Store entries in a C-style array to avoid using std::array or std::vector.
  nostd::unique_ptr<common::KeyValueProperties> kv_properties_;
};

}  // namespace baggage

OPENTELEMETRY_END_NAMESPACE
