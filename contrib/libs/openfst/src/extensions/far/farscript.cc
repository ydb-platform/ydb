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
// Definitions of 'scriptable' versions of FAR operations, that is,
// those that can be called with FstClass-type arguments.

#include <fst/extensions/far/farscript.h>

#include <cstdint>
#include <string>

#include <fst/extensions/far/far.h>
#include <fst/arc.h>
#include <fst/script/script-impl.h>

#define REGISTER_FST_OPERATION_4ARCS(Op, ArgPack) \
  REGISTER_FST_OPERATION_3ARCS(Op, ArgPack);      \
  REGISTER_FST_OPERATION(Op, ErrorArc, ArgPack)

namespace fst {
namespace script {

void CompileStrings(const std::vector<std::string> &sources,
                    FarWriterClass &writer, std::string_view fst_type,
                    int32_t generate_keys, FarEntryType fet, TokenType tt,
                    const std::string &symbols_source,
                    const std::string &unknown_symbol, bool keep_symbols,
                    bool initial_symbols, bool allow_negative_labels,
                    const std::string &key_prefix,
                    const std::string &key_suffix) {
  FarCompileStringsArgs args{sources,
                             writer,
                             fst_type,
                             generate_keys,
                             fet,
                             tt,
                             symbols_source,
                             unknown_symbol,
                             keep_symbols,
                             initial_symbols,
                             allow_negative_labels,
                             key_prefix,
                             key_suffix};
  Apply<Operation<FarCompileStringsArgs>>("CompileStrings", writer.ArcType(),
                                          &args);
}

REGISTER_FST_OPERATION_4ARCS(CompileStrings, FarCompileStringsArgs);

void Convert(FarReaderClass &reader, FarWriterClass &writer,
             std::string_view fst_type) {
  FarConvertArgs args{reader, writer, fst_type};
  Apply<Operation<FarConvertArgs>>("Convert", reader.ArcType(), &args);
}

REGISTER_FST_OPERATION_4ARCS(Convert, FarConvertArgs);

void Create(const std::vector<std::string> &sources, FarWriterClass &writer,
            const int32_t generate_keys, const std::string &key_prefix,
            const std::string &key_suffix) {
  FarCreateArgs args{sources, writer, generate_keys, key_prefix, key_suffix};
  Apply<Operation<FarCreateArgs>>("Create", writer.ArcType(), &args);
}

REGISTER_FST_OPERATION_4ARCS(Create, FarCreateArgs);

void Decode(FarReaderClass &reader, FarWriterClass &writer,
            const EncodeMapperClass &encoder) {
  if (!internal::ArcTypesMatch(reader, encoder, "Decode") ||
      !internal::ArcTypesMatch(writer, encoder, "Decode")) {
    return;
  }
  FarDecodeArgs args{reader, writer, encoder};
  Apply<Operation<FarDecodeArgs>>("Decode", reader.ArcType(), &args);
}

REGISTER_FST_OPERATION_4ARCS(Decode, FarDecodeArgs);

void Encode(FarReaderClass &reader, FarWriterClass &writer,
            EncodeMapperClass *encoder) {
  if (!internal::ArcTypesMatch(reader, *encoder, "Encode") ||
      !internal::ArcTypesMatch(writer, *encoder, "Encode")) {
    return;
  }
  FarEncodeArgs args{reader, writer, encoder};
  Apply<Operation<FarEncodeArgs>>("Encode", reader.ArcType(), &args);
}

REGISTER_FST_OPERATION_4ARCS(Encode, FarEncodeArgs);

bool Equal(FarReaderClass &reader1, FarReaderClass &reader2, float delta,
           std::string_view begin_key, std::string_view end_key) {
  if (!internal::ArcTypesMatch(reader1, reader2, "Equal")) return false;
  FarEqualInnerArgs args{reader1, reader2, delta, begin_key, end_key};
  FarEqualArgs args_with_retval(args);
  Apply<Operation<FarEqualArgs>>("Equal", reader1.ArcType(), &args_with_retval);
  return args_with_retval.retval;
}

REGISTER_FST_OPERATION_4ARCS(Equal, FarEqualArgs);

void Extract(FarReaderClass &reader, int32_t generate_sources,
             const std::string &keys, const std::string &key_separator,
             const std::string &range_delimiter,
             const std::string &source_prefix,
             const std::string &source_suffix) {
  FarExtractArgs args{reader,        generate_sources, keys,
                      key_separator, range_delimiter,  source_prefix,
                      source_suffix};
  Apply<Operation<FarExtractArgs>>("Extract", reader.ArcType(), &args);
}

REGISTER_FST_OPERATION_4ARCS(Extract, FarExtractArgs);

void GetInfo(const std::vector<std::string> &sources,
             const std::string &arc_type, const std::string &begin_key,
             const std::string &end_key, bool list_fsts, FarInfoData *data) {
  FarGetInfoArgs args{sources, begin_key, end_key, list_fsts, data};
  Apply<Operation<FarGetInfoArgs>>("GetInfo", arc_type, &args);
}

REGISTER_FST_OPERATION_4ARCS(GetInfo, FarGetInfoArgs);

void Info(const std::vector<std::string> &sources, const std::string &arc_type,
          const std::string &begin_key, const std::string &end_key,
          bool list_fsts) {
  FarInfoArgs args{sources, begin_key, end_key, list_fsts};
  Apply<Operation<FarInfoArgs>>("Info", arc_type, &args);
}

REGISTER_FST_OPERATION_4ARCS(Info, FarInfoArgs);

bool Isomorphic(FarReaderClass &reader1, FarReaderClass &reader2, float delta,
                std::string_view begin_key, std::string_view end_key) {
  if (!internal::ArcTypesMatch(reader1, reader2, "Equal")) return false;
  FarIsomorphicInnerArgs args{reader1, reader2, delta, begin_key, end_key};
  FarIsomorphicArgs args_with_retval(args);
  Apply<Operation<FarIsomorphicArgs>>("Isomorphic", reader1.ArcType(),
                                      &args_with_retval);
  return args_with_retval.retval;
}

REGISTER_FST_OPERATION_4ARCS(Isomorphic, FarIsomorphicArgs);

void PrintStrings(FarReaderClass &reader, const FarEntryType entry_type,
                  const TokenType token_type, const std::string &begin_key,
                  const std::string &end_key, bool print_key, bool print_weight,
                  const std::string &symbols_source, bool initial_symbols,
                  const int32_t generate_sources,
                  const std::string &source_prefix,
                  const std::string &source_suffix) {
  FarPrintStringsArgs args{reader,           entry_type,     token_type,
                           begin_key,        end_key,        print_key,
                           print_weight,     symbols_source, initial_symbols,
                           generate_sources, source_prefix,  source_suffix};
  Apply<Operation<FarPrintStringsArgs>>("PrintStrings", reader.ArcType(),
                                        &args);
}

REGISTER_FST_OPERATION_4ARCS(PrintStrings, FarPrintStringsArgs);

}  // namespace script
}  // namespace fst
