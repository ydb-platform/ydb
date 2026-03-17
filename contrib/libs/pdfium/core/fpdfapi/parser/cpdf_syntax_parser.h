// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_SYNTAX_PARSER_H_
#define CORE_FPDFAPI_PARSER_CPDF_SYNTAX_PARSER_H_

#include <stdint.h>

#include <array>
#include <memory>
#include <vector>

#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_types.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/string_pool_template.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/weak_ptr.h"

class CPDF_Dictionary;
class CPDF_IndirectObjectHolder;
class CPDF_Object;
class CPDF_ReadValidator;
class CPDF_Stream;
class IFX_SeekableReadStream;

class CPDF_SyntaxParser {
 public:
  enum class ParseType : bool { kStrict, kLoose };

  struct WordResult {
    ByteString word;
    bool is_number;
  };

  static std::unique_ptr<CPDF_SyntaxParser> CreateForTesting(
      RetainPtr<IFX_SeekableReadStream> pFileAccess,
      FX_FILESIZE HeaderOffset);

  explicit CPDF_SyntaxParser(RetainPtr<IFX_SeekableReadStream> pFileAccess);
  CPDF_SyntaxParser(RetainPtr<CPDF_ReadValidator> pValidator,
                    FX_FILESIZE HeaderOffset);
  ~CPDF_SyntaxParser();

  void SetReadBufferSize(uint32_t read_buffer_size) {
    m_ReadBufferSize = read_buffer_size;
  }

  FX_FILESIZE GetPos() const { return m_Pos; }
  void SetPos(FX_FILESIZE pos);

  RetainPtr<CPDF_Object> GetObjectBody(CPDF_IndirectObjectHolder* pObjList);
  RetainPtr<CPDF_Object> GetIndirectObject(CPDF_IndirectObjectHolder* pObjList,
                                           ParseType parse_type);

  ByteString GetKeyword();
  void ToNextLine();
  void ToNextWord();
  void RecordingToNextWord();
  bool BackwardsSearchToWord(ByteStringView word, FX_FILESIZE limit);
  FX_FILESIZE FindTag(ByteStringView tag);
  bool ReadBlock(pdfium::span<uint8_t> buffer);
  bool GetCharAt(FX_FILESIZE pos, uint8_t& ch);
  WordResult GetNextWord();
  ByteString PeekNextWord();

  RetainPtr<CPDF_ReadValidator> GetValidator() const;
  uint32_t GetDirectNum();
  bool GetNextChar(uint8_t& ch);

  // The document size may be smaller than the file size.
  // The syntax parser use position relative to document
  // offset (|m_HeaderOffset|).
  // The document size will be FileSize - "Header offset".
  // All offsets was readed from document, should not be great than document
  // size. Use it for checks instead of real file size.
  FX_FILESIZE GetDocumentSize() const;

  ByteString ReadString();
  DataVector<uint8_t> ReadHexString();

  void SetTrailerEnds(std::vector<unsigned int>* trailer_ends) {
    m_TrailerEnds = trailer_ends;
  }

 private:
  enum class WordType : bool { kWord, kNumber };

  friend class CPDF_DataAvail;
  friend class cpdf_syntax_parser_ReadHexString_Test;

  static constexpr int kParserMaxRecursionDepth = 64;
  static int s_CurrentRecursionDepth;

  bool ReadBlockAt(FX_FILESIZE read_pos);
  bool GetCharAtBackward(FX_FILESIZE pos, uint8_t* ch);
  WordType GetNextWordInternal();
  bool IsWholeWord(FX_FILESIZE startpos,
                   FX_FILESIZE limit,
                   ByteStringView tag,
                   bool checkKeyword);

  unsigned int ReadEOLMarkers(FX_FILESIZE pos);
  FX_FILESIZE FindWordPos(ByteStringView word);
  FX_FILESIZE FindStreamEndPos();
  RetainPtr<CPDF_Stream> ReadStream(RetainPtr<CPDF_Dictionary> pDict);

  bool IsPositionRead(FX_FILESIZE pos) const;

  RetainPtr<CPDF_Object> GetObjectBodyInternal(
      CPDF_IndirectObjectHolder* pObjList,
      ParseType parse_type);

  RetainPtr<CPDF_ReadValidator> m_pFileAccess;
  // The syntax parser use position relative to header offset.
  // The header contains at file start, and can follow after some stuff. We
  // ignore this stuff.
  const FX_FILESIZE m_HeaderOffset;
  const FX_FILESIZE m_FileLen;
  FX_FILESIZE m_Pos = 0;
  WeakPtr<ByteStringPool> m_pPool;
  DataVector<uint8_t> m_pFileBuf;
  FX_FILESIZE m_BufOffset = 0;
  uint32_t m_WordSize = 0;
  uint32_t m_ReadBufferSize = CPDF_Stream::kFileBufSize;
  std::array<uint8_t, 257> m_WordBuffer = {};

  // The syntax parser records traversed trailer end byte offsets here.
  UnownedPtr<std::vector<unsigned int>> m_TrailerEnds;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_SYNTAX_PARSER_H_
