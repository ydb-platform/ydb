#pragma once

//! @file type_io.h
//!
//! Utilities for serializing and deserializing type instances.

#include "type.h"

#include <library/cpp/yson_pull/yson.h>

namespace NTi::NIo {
    /// Load type from a serialized representation.
    ///
    /// Serialization uses YSON (either binary or text). Contents are described in the [docs page].
    ///
    /// Throws `TTypeDeserializationException` if input is not valid.
    ///
    /// [docs page]: https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/type_info/docs/types_serialization.md
    ///
    /// @param factory factory that will be used to allocate new type. Technically, type will be deserialized into
    ///        a temporary pool factory and then adopted into a given one.
    /// @param reader yson pull reader that'll be used to read types.
    /// @param deduplicate use deduplication while creating new types. See `NTi::PoolFactory` function for more info.
    /// @{
    TTypePtr DeserializeYson(ITypeFactory& factory, NYsonPull::TReader& reader, bool deduplicate = true);
    TTypePtr DeserializeYson(ITypeFactory& factory, TStringBuf data, bool deduplicate = true);
    TTypePtr DeserializeYson(ITypeFactory& factory, IInputStream& input, bool deduplicate = true);
    /// @}

    /// Like `Deserialize`, but returns a raw pointer.
    /// @{
    const TType* DeserializeYsonRaw(IPoolTypeFactory& factory, NYsonPull::TReader& reader);
    const TType* DeserializeYsonRaw(IPoolTypeFactory& factory, TStringBuf data);
    const TType* DeserializeYsonRaw(IPoolTypeFactory& factory, IInputStream& input);
    /// @}

    /// Like `Deserialize`, but allows deserializing multiple types from the same reader.
    ///
    /// This function takes a reader created with `NYsonPull::EStreamType::ListFragment` mode. It reads a type,
    /// but doesn't fails if there is no `BeginStream` event. If the reader is empty, it returns nullptr.
    ///
    /// This function mirrors `SerializeMultiple`. Call it multiple times on the same reader to read multiple types.
    const TType* DeserializeYsonMultipleRaw(IPoolTypeFactory& factory, NYsonPull::TReader& reader);

    /// Serialize this type info.
    ///
    /// Serialization uses YSON (either binary or text). Contents are described in the [RFC].
    ///
    /// [RFC]: https://a.yandex-team.ru/arc/trunk/arcadia/logfeller/mvp/docs/types_serialization.md
    ///
    /// @param humanReadable use pretty textual format instead of a binary one.
    /// @param includeTags when disabled, tagged types will be removed from the result, only tagged type contents
    ///        will be dumped. For example, `Tagged<'Url', String>` will be rendered as just `String`.
    ///        This is useful if you only care about physical layout of a type and don't want to export
    ///        any semantical meaning.
    /// @{
    void SerializeYson(const TType* type, NYsonPull::IConsumer& consumer, bool includeTags = true);
    void SerializeYson(const TType* type, IOutputStream& stream, bool humanReadable = false, bool includeTags = true);
    TString SerializeYson(const TType* type, bool humanReadable = false, bool includeTags = true);
    /// @}

    /// Like `Serialize`, but allows serializing multiple types into the same consumer.
    ///
    /// This function takes a consumer created with `NYsonPull::EStreamType::ListFragment` mode. It writes type,
    /// but doesn't emit the `BeginStream` and `EndStream` commands.
    ///
    /// Call this function multiple times on the same consumer to write multiple types. Note that you must emit
    /// the `BeginStream` and `EndStream` commands to the consumer yourself.
    void SerializeYsonMultiple(const TType* type, NYsonPull::IConsumer& consumer, bool includeTags = true);

    /// Convert type to the lisp-like representation used in YQL row specs.
    ///
    /// TODO: move this code to yql/
    ///
    /// @param includeTags when disabled, tagged types will be removed from the result, only tagged type contents
    ///        will be dumped. For example, `Tagged<'Url', String>` will be rendered as just `String`.
    ///        This is useful if you only care about physical layout of a type and don't want to export
    ///        any semantic meaning.
    /// @{
    void AsYqlType(const TType* type, NYsonPull::IConsumer& consumer, bool includeTags = true);
    TString AsYqlType(const TType* type, bool includeTags = true);
    /// @}

    /// Generate a strict YQL row spec. Toplevel tags will be ignored.
    ///
    /// Throws `TApiException` if the type is not a (possibly tagged) struct.
    ///
    /// TODO: move this code to yql/
    ///
    /// @param type type that'll be converted to YQL row spec.
    /// @param consumer yson pull consumer. Attention: `OnBeginStream` and `OnEndStream` should be emitted manually
    ///        before and after calling this function.
    /// @param includeTags same as in `TType::AsYqlType`.
    /// @{
    void AsYqlRowSpec(const TType* type, NYsonPull::IConsumer& consumer, bool includeTags = true);
    TString AsYqlRowSpec(const TType* type, bool includeTags = true);
    /// @}

    /// Generate a strict YT schema (only types are exported, no index/sorting information).
    ///
    /// Throws `TApiException` if the type is not a (possibly tagged) struct.
    ///
    /// The schema is generated according to the translation rules of YQL types, i.e. container types translate
    /// to `Any`.
    ///
    /// TODO: move this code to mapreduce/yt/
    ///
    /// @param failOnEmptyStruct if true, will throw `TApiException` if called on a struct with no fields;
    ///                          if false, will emit a strict YT schema with a single column `'_yql_fake_column'`
    ///                          of type `Optional<Bool>` (this is how YQL handles empty tables).
    /// @{
    void AsYtSchema(const TType* type, NYsonPull::IConsumer& consumer, bool failOnEmptyStruct = true);
    TString AsYtSchema(const TType* type, bool failOnEmptyStruct = true);
    /// @}
}
