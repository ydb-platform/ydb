#pragma once

///
/// @file yt/cpp/mapreduce/interface/format.h
///
/// Header containing class to work with raw [YT formats](https://ytsaurus.tech/docs/en/user-guide/storage/formats).

#include "node.h"

#include <google/protobuf/descriptor.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// @deprecated
struct TYamredDsvAttributes
{
    /// Names of key columns.
    TVector<TString> KeyColumnNames;

    /// Names of subkey columns.
    TVector<TString> SubkeyColumnNames;
};

////////////////////////////////////////////////////////////////////////////////

/// @brief Class representing YT data format.
///
/// Normally the user does not need to use it.
/// However, the class is handy for "raw" operations and table reading and writing,
/// e.g. @ref NYT::IOperationClient::RawMap and other raw operations,
/// @ref NYT::IIOClient::CreateRawReader and @ref NYT::IIOClient::CreateRawWriter.
/// Anyway, the static factory methods should be preferred to the constructor.
///
/// @see [YT doc](https://ytsaurus.tech/docs/en/user-guide/storage/formats).
struct TFormat
{
public:
    /// Format representation understandable by YT.
    TNode Config;

public:
    /// @brief Construct format from given YT format representation.
    ///
    /// @note Prefer using static factory methods (e.g. @ref NYT::TFormat::YsonBinary, @ref NYT::TFormat::YsonText, @ref NYT::TFormat::Protobuf).
    explicit TFormat(const TNode& config = TNode());

    /// @brief Create text YSON format.
    ///
    /// @see [the doc](https://ytsaurus.tech/docs/en/user-guide/storage/formats#yson)
    static TFormat YsonText();

    /// @brief Create binary YSON format.
    ///
    /// @see [the doc](https://ytsaurus.tech/docs/en/user-guide/storage/formats#yson)
    static TFormat YsonBinary();

    /// @brief Create YaMR format.
    ///
    /// @deprecated
    static TFormat YaMRLenval();

    /// @brief Create protobuf format from protobuf message descriptors.
    ///
    /// @see [the doc](https://ytsaurus.tech/docs/en/api/c++/protobuf.html).
    static TFormat Protobuf(
        const TVector<const ::google::protobuf::Descriptor*>& descriptors,
        bool withDescriptors = false);

    /// @brief Create JSON format.
    ///
    /// @see [the doc](https://ytsaurus.tech/docs/en/user-guide/storage/formats#json)
    static TFormat Json();

    /// @brief Create DSV (TSKV) format.
    ///
    /// @see [the doc](https://ytsaurus.tech/docs/en/user-guide/storage/formats.html#dsv)
    static TFormat Dsv();

    /// @brief Create protobuf format for the message specified in template parameter.
    ///
    /// `T` must be inherited from `Message`.
    ///
    /// @see [the doc](https://ytsaurus.tech/docs/en/api/c++/protobuf.html).
    template<typename T>
    static inline TFormat Protobuf(bool withDescriptors = false);

    /// @brief Is the format text YSON?
    ///
    /// @see [the doc](https://ytsaurus.tech/docs/en/user-guide/storage/formats#yson)
    bool IsTextYson() const;

    /// @brief Is the format protobuf?
    ///
    /// @see [the doc](https://ytsaurus.tech/docs/en/api/c/protobuf.html)
    bool IsProtobuf() const;

    /// @brief Is the format YaMR?
    ///
    /// @deprecated
    bool IsYamredDsv() const;

    /// @brief For YAMR format returns its attributes in structured way.
    ///
    /// @deprecated
    TYamredDsvAttributes GetYamredDsvAttributes() const;
};

////////////////////////////////////////////////////////////////////////////////

template<typename T>
TFormat TFormat::Protobuf(bool withDescriptors) {
    return TFormat::Protobuf({T::descriptor()}, withDescriptors);
}

/// @brief Create table schema from protobuf message descriptor.
///
/// @param messageDescriptor Message descriptor
/// @param keepFieldsWithoutExtension Add to schema fields without "column_name" or "key_column_name" extensions.
TTableSchema CreateTableSchema(
    const ::google::protobuf::Descriptor& messageDescriptor,
    bool keepFieldsWithoutExtension);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
