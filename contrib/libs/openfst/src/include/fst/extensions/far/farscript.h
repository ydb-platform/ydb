// Copyright 2005-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.
//
// Convenience file for including all of the FAR operations, or registering
// them for new arc types.

#ifndef FST_EXTENSIONS_FAR_FARSCRIPT_H_
#define FST_EXTENSIONS_FAR_FARSCRIPT_H_

#include <cstdint>
#include <string>
#include <vector>

#include <fst/extensions/far/compile-strings.h>
#include <fst/extensions/far/convert.h>
#include <fst/extensions/far/create.h>
#include <fst/extensions/far/encode.h>
#include <fst/extensions/far/equal.h>
#include <fst/extensions/far/extract.h>
#include <fst/extensions/far/far-class.h>
#include <fst/extensions/far/far.h>
#include <fst/extensions/far/info.h>
#include <fst/extensions/far/isomorphic.h>
#include <fst/extensions/far/print-strings.h>
#include <fst/extensions/far/script-impl.h>
#include <fst/script/arg-packs.h>
#include <string_view>

namespace fst {
namespace script {

// Note: it is safe to pass these strings as references because this struct is
// only used to pass them deeper in the call graph. Be sure you understand why
// this is so before using this struct for anything else!
struct FarCompileStringsArgs {
  const std::vector<std::string> &sources;
  FarWriterClass &writer;
  std::string_view fst_type;
  const int32_t generate_keys;
  const FarEntryType fet;
  const TokenType tt;
  const std::string &symbols_source;
  const std::string &unknown_symbol;
  const bool keep_symbols;
  const bool initial_symbols;
  const bool allow_negative_labels;
  const std::string &key_prefix;
  const std::string &key_suffix;
};

template <class Arc>
void CompileStrings(FarCompileStringsArgs *args) {
  FarWriter<Arc> &writer = *args->writer.GetFarWriter<Arc>();
  ::fst::CompileStrings<Arc>(
      args->sources, writer, args->fst_type, args->generate_keys, args->fet,
      args->tt, args->symbols_source, args->unknown_symbol, args->keep_symbols,
      args->initial_symbols, args->allow_negative_labels, args->key_prefix,
      args->key_suffix);
}

void CompileStrings(const std::vector<std::string> &sources,
                    FarWriterClass &writer, std::string_view fst_type,
                    int32_t generate_keys, FarEntryType fet, TokenType tt,
                    const std::string &symbols_source,
                    const std::string &unknown_symbol, bool keep_symbols,
                    bool initial_symbols, bool allow_negative_labels,
                    const std::string &key_prefix,
                    const std::string &key_suffix);

using FarConvertArgs =
    std::tuple<FarReaderClass &, FarWriterClass &, std::string_view>;

template <class Arc>
void Convert(FarConvertArgs *args) {
  FarReader<Arc> &reader = *std::get<0>(*args).GetFarReader<Arc>();
  FarWriter<Arc> &writer = *std::get<1>(*args).GetFarWriter<Arc>();
  ::fst::Convert<Arc>(reader, writer, std::get<2>(*args));
}

void Convert(FarReaderClass &reader, FarWriterClass &writer,
             std::string_view fst_type);

// Note: it is safe to pass these strings as references because this struct is
// only used to pass them deeper in the call graph. Be sure you understand why
// this is so before using this struct for anything else!
struct FarCreateArgs {
  const std::vector<std::string> &sources;
  FarWriterClass &writer;
  const int32_t generate_keys;
  const std::string &key_prefix;
  const std::string &key_suffix;
};

template <class Arc>
void Create(FarCreateArgs *args) {
  FarWriter<Arc> &writer = *args->writer.GetFarWriter<Arc>();
  ::fst::Create<Arc>(args->sources, writer, args->generate_keys,
                         args->key_prefix, args->key_suffix);
}

void Create(const std::vector<std::string> &sources, FarWriterClass &writer,
            int32_t generate_keys, const std::string &key_prefix,
            const std::string &key_suffix);

using FarDecodeArgs =
    std::tuple<FarReaderClass &, FarWriterClass &, const EncodeMapperClass &>;

template <class Arc>
void Decode(FarDecodeArgs *args) {
  FarReader<Arc> &reader = *std::get<0>(*args).GetFarReader<Arc>();
  FarWriter<Arc> &writer = *std::get<1>(*args).GetFarWriter<Arc>();
  const EncodeMapper<Arc> &mapper = *std::get<2>(*args).GetEncodeMapper<Arc>();
  Decode(reader, writer, mapper);
}

void Decode(FarReaderClass &reader, FarWriterClass &writer,
            const EncodeMapperClass &encoder);

using FarEncodeArgs =
    std::tuple<FarReaderClass &, FarWriterClass &, EncodeMapperClass *>;

template <class Arc>
void Encode(FarEncodeArgs *args) {
  FarReader<Arc> &reader = *std::get<0>(*args).GetFarReader<Arc>();
  FarWriter<Arc> &writer = *std::get<1>(*args).GetFarWriter<Arc>();
  EncodeMapper<Arc> *mapper = std::get<2>(*args)->GetEncodeMapper<Arc>();
  Encode(reader, writer, mapper);
}

void Encode(FarReaderClass &reader, FarWriterClass &writer,
            EncodeMapperClass *encoder);

using FarEqualInnerArgs = std::tuple<FarReaderClass &, FarReaderClass &, float,
                                     std::string_view, std::string_view>;

using FarEqualArgs = WithReturnValue<bool, FarEqualInnerArgs>;

template <class Arc>
void Equal(FarEqualArgs *args) {
  FarReader<Arc> &reader1 = *std::get<0>(args->args).GetFarReader<Arc>();
  FarReader<Arc> &reader2 = *std::get<1>(args->args).GetFarReader<Arc>();
  args->retval =
      ::fst::Equal<Arc>(reader1, reader2, std::get<2>(args->args),
                            std::get<3>(args->args), std::get<4>(args->args));
}

bool Equal(FarReaderClass &reader1, FarReaderClass &reader2,
           float delta = kDelta, std::string_view begin_key = "",
           std::string_view end_key = "");

using FarExtractArgs =
    std::tuple<FarReaderClass &, int32_t, const std::string &,
               const std::string &, const std::string &, const std::string &,
               const std::string &>;

template <class Arc>
void Extract(FarExtractArgs *args) {
  FarReader<Arc> &reader = *std::get<0>(*args).GetFarReader<Arc>();
  ::fst::Extract<Arc>(reader, std::get<1>(*args), std::get<2>(*args),
                          std::get<3>(*args), std::get<4>(*args),
                          std::get<5>(*args), std::get<6>(*args));
}

void Extract(FarReaderClass &reader, int32_t generate_sources,
             const std::string &keys, const std::string &key_separator,
             const std::string &range_delimiter,
             const std::string &source_prefix,
             const std::string &source_suffix);

using FarInfoArgs =
    std::tuple<const std::vector<std::string> &, const std::string &,
               const std::string &, const bool>;

template <class Arc>
void Info(FarInfoArgs *args) {
  ::fst::Info<Arc>(std::get<0>(*args), std::get<1>(*args),
                       std::get<2>(*args), std::get<3>(*args));
}

void Info(const std::vector<std::string> &sources, const std::string &arc_type,
          const std::string &begin_key, const std::string &end_key,
          const bool list_fsts);

using FarGetInfoArgs =
    std::tuple<const std::vector<std::string> &, const std::string &,
               const std::string &, const bool, FarInfoData *>;

template <class Arc>
void GetInfo(FarGetInfoArgs *args) {
  ::fst::GetInfo<Arc>(std::get<0>(*args), std::get<1>(*args),
                          std::get<2>(*args), std::get<3>(*args),
                          std::get<4>(*args));
}

void GetInfo(const std::vector<std::string> &sources,
             const std::string &arc_type, const std::string &begin_key,
             const std::string &end_key, const bool list_fsts, FarInfoData *);

using FarIsomorphicInnerArgs =
    std::tuple<FarReaderClass &, FarReaderClass &, float, std::string_view,
               std::string_view>;

using FarIsomorphicArgs = WithReturnValue<bool, FarIsomorphicInnerArgs>;

template <class Arc>
void Isomorphic(FarIsomorphicArgs *args) {
  FarReader<Arc> &reader1 = *std::get<0>(args->args).GetFarReader<Arc>();
  FarReader<Arc> &reader2 = *std::get<1>(args->args).GetFarReader<Arc>();
  args->retval = ::fst::Isomorphic<Arc>(
      reader1, reader2, std::get<2>(args->args), std::get<3>(args->args),
      std::get<4>(args->args));
}

bool Isomorphic(FarReaderClass &reader1, FarReaderClass &reader2,
                float delta = kDelta, std::string_view begin_key = "",
                std::string_view end_key = "");

struct FarPrintStringsArgs {
  FarReaderClass &reader;
  const FarEntryType entry_type;
  const TokenType token_type;
  const std::string &begin_key;
  const std::string &end_key;
  const bool print_key;
  const bool print_weight;
  const std::string &symbols_source;
  const bool initial_symbols;
  const int32_t generate_sources;
  const std::string &source_prefix;
  const std::string &source_suffix;
};

template <class Arc>
void PrintStrings(FarPrintStringsArgs *args) {
  FarReader<Arc> &reader = *args->reader.GetFarReader<Arc>();
  ::fst::PrintStrings<Arc>(reader, args->entry_type, args->token_type,
                               args->begin_key, args->end_key, args->print_key,
                               args->print_weight, args->symbols_source,
                               args->initial_symbols, args->generate_sources,
                               args->source_prefix, args->source_suffix);
}

void PrintStrings(FarReaderClass &reader, const FarEntryType entry_type,
                  const TokenType token_type, const std::string &begin_key,
                  const std::string &end_key, const bool print_key,
                  const bool print_weight, const std::string &symbols_source,
                  const bool initial_symbols, const int32_t generate_sources,
                  const std::string &source_prefix,
                  const std::string &source_suffix);

}  // namespace script
}  // namespace fst

#define REGISTER_FST_FAR_OPERATIONS(ArcType)                              \
  REGISTER_FST_OPERATION(CompileStrings, ArcType, FarCompileStringsArgs); \
  REGISTER_FST_OPERATION(Create, ArcType, FarCreateArgs);                 \
  REGISTER_FST_OPERATION(Equal, ArcType, FarEqualArgs);                   \
  REGISTER_FST_OPERATION(Extract, ArcType, FarExtractArgs);               \
  REGISTER_FST_OPERATION(Info, ArcType, FarInfoArgs);                     \
  REGISTER_FST_OPERATION(Isomorphic, ArcType, FarIsomorphicArgs);         \
  REGISTER_FST_OPERATION(PrintStrings, ArcType, FarPrintStringsArgs);     \
  REGISTER_FST_OPERATION(GetInfo, ArcType, FarGetInfoArgs)

#endif  // FST_EXTENSIONS_FAR_FARSCRIPT_H_
