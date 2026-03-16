// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_streamparser.h"

#include <ctype.h>

#include <algorithm>
#include <memory>
#include <utility>

#include "constants/stream_dict_common.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_boolean.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_null.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcodec/data_and_bytes_consumed.h"
#include "core/fxcodec/jpeg/jpegmodule.h"
#include "core/fxcodec/scanlinedecoder.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/span_util.h"
#include "core/fxge/calculate_pitch.h"

namespace {

const uint32_t kMaxNestedParsingLevel = 512;
const size_t kMaxStringLength = 32767;

const char kTrue[] = "true";
const char kFalse[] = "false";
const char kNull[] = "null";

uint32_t DecodeAllScanlines(std::unique_ptr<ScanlineDecoder> pDecoder) {
  if (!pDecoder)
    return FX_INVALID_OFFSET;

  int ncomps = pDecoder->CountComps();
  int bpc = pDecoder->GetBPC();
  int width = pDecoder->GetWidth();
  int height = pDecoder->GetHeight();
  if (width <= 0 || height <= 0)
    return FX_INVALID_OFFSET;

  std::optional<uint32_t> maybe_size =
      fxge::CalculatePitch8(bpc, ncomps, width);
  if (!maybe_size.has_value())
    return FX_INVALID_OFFSET;

  FX_SAFE_UINT32 size = maybe_size.value();
  size *= height;
  if (size.ValueOrDefault(0) == 0)
    return FX_INVALID_OFFSET;

  for (int row = 0; row < height; ++row) {
    if (pDecoder->GetScanline(row).empty())
      break;
  }
  return pDecoder->GetSrcOffset();
}

uint32_t DecodeInlineStream(pdfium::span<const uint8_t> src_span,
                            int width,
                            int height,
                            const ByteString& decoder,
                            RetainPtr<const CPDF_Dictionary> pParam,
                            uint32_t orig_size) {
  // |decoder| should not be an abbreviation.
  DCHECK(decoder != "A85");
  DCHECK(decoder != "AHx");
  DCHECK(decoder != "CCF");
  DCHECK(decoder != "DCT");
  DCHECK(decoder != "Fl");
  DCHECK(decoder != "LZW");
  DCHECK(decoder != "RL");

  if (decoder == "FlateDecode") {
    return FlateOrLZWDecode(/*use_lzw=*/false, src_span, pParam.Get(),
                            /*estimated_size=*/orig_size)
        .bytes_consumed;
  }
  if (decoder == "LZWDecode") {
    return FlateOrLZWDecode(
               /*use_lzw=*/true, src_span, pParam.Get(),
               /*estimated_size=*/0)
        .bytes_consumed;
  }
  if (decoder == "DCTDecode") {
    std::unique_ptr<ScanlineDecoder> pDecoder = JpegModule::CreateDecoder(
        src_span, width, height, 0,
        !pParam || pParam->GetIntegerFor("ColorTransform", 1));
    return DecodeAllScanlines(std::move(pDecoder));
  }
  if (decoder == "CCITTFaxDecode") {
    std::unique_ptr<ScanlineDecoder> pDecoder =
        CreateFaxDecoder(src_span, width, height, pParam.Get());
    return DecodeAllScanlines(std::move(pDecoder));
  }

  if (decoder == "ASCII85Decode") {
    return A85Decode(src_span).bytes_consumed;
  }
  if (decoder == "ASCIIHexDecode") {
    return HexDecode(src_span).bytes_consumed;
  }
  if (decoder == "RunLengthDecode") {
    return RunLengthDecode(src_span).bytes_consumed;
  }

  return FX_INVALID_OFFSET;
}

}  // namespace

CPDF_StreamParser::CPDF_StreamParser(pdfium::span<const uint8_t> span)
    : m_pBuf(span) {}

CPDF_StreamParser::CPDF_StreamParser(pdfium::span<const uint8_t> span,
                                     const WeakPtr<ByteStringPool>& pPool)
    : m_pPool(pPool), m_pBuf(span) {}

CPDF_StreamParser::~CPDF_StreamParser() = default;

RetainPtr<CPDF_Stream> CPDF_StreamParser::ReadInlineStream(
    CPDF_Document* pDoc,
    RetainPtr<CPDF_Dictionary> pDict,
    const CPDF_Object* pCSObj) {
  if (m_Pos < m_pBuf.size() && PDFCharIsWhitespace(m_pBuf[m_Pos]))
    m_Pos++;

  if (m_Pos == m_pBuf.size())
    return nullptr;

  ByteString decoder;
  RetainPtr<const CPDF_Dictionary> pParam;
  RetainPtr<const CPDF_Object> pFilter = pDict->GetDirectObjectFor("Filter");
  if (pFilter) {
    const CPDF_Array* pArray = pFilter->AsArray();
    if (pArray) {
      decoder = pArray->GetByteStringAt(0);
      RetainPtr<const CPDF_Array> pParams =
          pDict->GetArrayFor(pdfium::stream::kDecodeParms);
      if (pParams)
        pParam = pParams->GetDictAt(0);
    } else {
      decoder = pFilter->GetString();
      pParam = pDict->GetDictFor(pdfium::stream::kDecodeParms);
    }
  }
  uint32_t width = pDict->GetIntegerFor("Width");
  uint32_t height = pDict->GetIntegerFor("Height");
  uint32_t bpc = 1;
  uint32_t nComponents = 1;
  if (pCSObj) {
    RetainPtr<CPDF_ColorSpace> pCS =
        CPDF_DocPageData::FromDocument(pDoc)->GetColorSpace(pCSObj, nullptr);
    nComponents = pCS ? pCS->ComponentCount() : 3;
    bpc = pDict->GetIntegerFor("BitsPerComponent");
  }
  std::optional<uint32_t> maybe_size =
      fxge::CalculatePitch8(bpc, nComponents, width);
  if (!maybe_size.has_value())
    return nullptr;

  FX_SAFE_UINT32 size = maybe_size.value();
  size *= height;
  if (!size.IsValid())
    return nullptr;

  uint32_t dwOrigSize = size.ValueOrDie();
  DataVector<uint8_t> data;
  uint32_t dwStreamSize;
  if (decoder.IsEmpty()) {
    dwOrigSize = std::min<uint32_t>(dwOrigSize, m_pBuf.size() - m_Pos);
    auto src_span = m_pBuf.subspan(m_Pos, dwOrigSize);
    data = DataVector<uint8_t>(src_span.begin(), src_span.end());
    dwStreamSize = dwOrigSize;
    m_Pos += dwOrigSize;
  } else {
    dwStreamSize = DecodeInlineStream(m_pBuf.subspan(m_Pos), width, height,
                                      decoder, std::move(pParam), dwOrigSize);
    if (!pdfium::IsValueInRangeForNumericType<int>(dwStreamSize)) {
      return nullptr;
    }

    uint32_t dwSavePos = m_Pos;
    m_Pos += dwStreamSize;
    while (true) {
      uint32_t dwPrevPos = m_Pos;
      ElementType type = ParseNextElement();
      if (type == ElementType::kEndOfData)
        break;

      if (type != ElementType::kKeyword) {
        dwStreamSize += m_Pos - dwPrevPos;
        continue;
      }
      if (GetWord() == "EI") {
        m_Pos = dwPrevPos;
        break;
      }
      dwStreamSize += m_Pos - dwPrevPos;
    }
    m_Pos = dwSavePos;
    auto src_span = m_pBuf.subspan(m_Pos, dwStreamSize);
    data = DataVector<uint8_t>(src_span.begin(), src_span.end());
    m_Pos += dwStreamSize;
  }
  pDict->SetNewFor<CPDF_Number>("Length", static_cast<int>(dwStreamSize));
  return pdfium::MakeRetain<CPDF_Stream>(std::move(data), std::move(pDict));
}

CPDF_StreamParser::ElementType CPDF_StreamParser::ParseNextElement() {
  m_pLastObj.Reset();
  m_WordSize = 0;
  if (!PositionIsInBounds())
    return ElementType::kEndOfData;

  uint8_t ch = m_pBuf[m_Pos++];
  while (true) {
    while (PDFCharIsWhitespace(ch)) {
      if (!PositionIsInBounds())
        return ElementType::kEndOfData;

      ch = m_pBuf[m_Pos++];
    }

    if (ch != '%')
      break;

    while (true) {
      if (!PositionIsInBounds())
        return ElementType::kEndOfData;

      ch = m_pBuf[m_Pos++];
      if (PDFCharIsLineEnding(ch))
        break;
    }
  }

  if (PDFCharIsDelimiter(ch) && ch != '/') {
    m_Pos--;
    m_pLastObj = ReadNextObject(false, false, 0);
    return ElementType::kOther;
  }

  bool bIsNumber = true;
  while (true) {
    if (m_WordSize < kMaxWordLength)
      m_WordBuffer[m_WordSize++] = ch;

    if (!PDFCharIsNumeric(ch))
      bIsNumber = false;

    if (!PositionIsInBounds())
      break;

    ch = m_pBuf[m_Pos++];

    if (PDFCharIsDelimiter(ch) || PDFCharIsWhitespace(ch)) {
      m_Pos--;
      break;
    }
  }

  m_WordBuffer[m_WordSize] = 0;
  if (bIsNumber)
    return ElementType::kNumber;

  if (m_WordBuffer[0] == '/')
    return ElementType::kName;

  if (m_WordSize == 4) {
    if (GetWord() == kTrue) {
      m_pLastObj = pdfium::MakeRetain<CPDF_Boolean>(true);
      return ElementType::kOther;
    }
    if (GetWord() == kNull) {
      m_pLastObj = pdfium::MakeRetain<CPDF_Null>();
      return ElementType::kOther;
    }
  } else if (m_WordSize == 5) {
    if (GetWord() == kFalse) {
      m_pLastObj = pdfium::MakeRetain<CPDF_Boolean>(false);
      return ElementType::kOther;
    }
  }
  return ElementType::kKeyword;
}

RetainPtr<CPDF_Object> CPDF_StreamParser::ReadNextObject(
    bool bAllowNestedArray,
    bool bInArray,
    uint32_t dwRecursionLevel) {
  bool bIsNumber;
  // Must get the next word before returning to avoid infinite loops.
  GetNextWord(bIsNumber);
  if (!m_WordSize || dwRecursionLevel > kMaxNestedParsingLevel)
    return nullptr;

  if (bIsNumber) {
    m_WordBuffer[m_WordSize] = 0;
    return pdfium::MakeRetain<CPDF_Number>(GetWord());
  }

  int first_char = m_WordBuffer[0];
  if (first_char == '/') {
    ByteString name = PDF_NameDecode(GetWord().Substr(1));
    return pdfium::MakeRetain<CPDF_Name>(m_pPool, name);
  }

  if (first_char == '(') {
    return pdfium::MakeRetain<CPDF_String>(m_pPool, ReadString());
  }

  if (first_char == '<') {
    if (m_WordSize == 1) {
      return pdfium::MakeRetain<CPDF_String>(m_pPool, ReadHexString(),
                                             CPDF_String::DataType::kIsHex);
    }

    auto pDict = pdfium::MakeRetain<CPDF_Dictionary>(m_pPool);
    while (true) {
      GetNextWord(bIsNumber);
      if (m_WordSize == 2 && m_WordBuffer[0] == '>')
        break;

      if (!m_WordSize || m_WordBuffer[0] != '/')
        return nullptr;

      ByteString key = PDF_NameDecode(GetWord().Substr(1));
      RetainPtr<CPDF_Object> pObj =
          ReadNextObject(true, bInArray, dwRecursionLevel + 1);
      if (!pObj)
        return nullptr;

      pDict->SetFor(key, std::move(pObj));
    }
    return pDict;
  }

  if (first_char == '[') {
    if ((!bAllowNestedArray && bInArray))
      return nullptr;

    auto pArray = pdfium::MakeRetain<CPDF_Array>();
    while (true) {
      RetainPtr<CPDF_Object> pObj =
          ReadNextObject(bAllowNestedArray, true, dwRecursionLevel + 1);
      if (pObj) {
        pArray->Append(std::move(pObj));
        continue;
      }
      if (!m_WordSize || m_WordBuffer[0] == ']')
        break;
    }
    return pArray;
  }

  if (GetWord() == kFalse)
    return pdfium::MakeRetain<CPDF_Boolean>(false);
  if (GetWord() == kTrue)
    return pdfium::MakeRetain<CPDF_Boolean>(true);
  if (GetWord() == kNull)
    return pdfium::MakeRetain<CPDF_Null>();
  return nullptr;
}

// TODO(npm): the following methods are almost identical in cpdf_syntaxparser
void CPDF_StreamParser::GetNextWord(bool& bIsNumber) {
  m_WordSize = 0;
  bIsNumber = true;
  if (!PositionIsInBounds())
    return;

  uint8_t ch = m_pBuf[m_Pos++];
  while (true) {
    while (PDFCharIsWhitespace(ch)) {
      if (!PositionIsInBounds()) {
        return;
      }
      ch = m_pBuf[m_Pos++];
    }

    if (ch != '%')
      break;

    while (true) {
      if (!PositionIsInBounds())
        return;
      ch = m_pBuf[m_Pos++];
      if (PDFCharIsLineEnding(ch))
        break;
    }
  }

  if (PDFCharIsDelimiter(ch)) {
    bIsNumber = false;
    m_WordBuffer[m_WordSize++] = ch;
    if (ch == '/') {
      while (true) {
        if (!PositionIsInBounds())
          return;
        ch = m_pBuf[m_Pos++];
        if (!PDFCharIsOther(ch) && !PDFCharIsNumeric(ch)) {
          m_Pos--;
          return;
        }
        if (m_WordSize < kMaxWordLength)
          m_WordBuffer[m_WordSize++] = ch;
      }
    } else if (ch == '<') {
      if (!PositionIsInBounds())
        return;
      ch = m_pBuf[m_Pos++];
      if (ch == '<')
        m_WordBuffer[m_WordSize++] = ch;
      else
        m_Pos--;
    } else if (ch == '>') {
      if (!PositionIsInBounds())
        return;
      ch = m_pBuf[m_Pos++];
      if (ch == '>')
        m_WordBuffer[m_WordSize++] = ch;
      else
        m_Pos--;
    }
    return;
  }

  while (true) {
    if (m_WordSize < kMaxWordLength)
      m_WordBuffer[m_WordSize++] = ch;
    if (!PDFCharIsNumeric(ch))
      bIsNumber = false;
    if (!PositionIsInBounds())
      return;

    ch = m_pBuf[m_Pos++];
    if (PDFCharIsDelimiter(ch) || PDFCharIsWhitespace(ch)) {
      m_Pos--;
      break;
    }
  }
}

ByteString CPDF_StreamParser::ReadString() {
  if (!PositionIsInBounds())
    return ByteString();

  ByteString buf;
  int parlevel = 0;
  int status = 0;
  int iEscCode = 0;
  uint8_t ch = m_pBuf[m_Pos++];
  while (true) {
    switch (status) {
      case 0:
        if (ch == ')') {
          if (parlevel == 0) {
            return buf.First(std::min(buf.GetLength(), kMaxStringLength));
          }
          parlevel--;
          buf += ')';
        } else if (ch == '(') {
          parlevel++;
          buf += '(';
        } else if (ch == '\\') {
          status = 1;
        } else {
          buf += static_cast<char>(ch);
        }
        break;
      case 1:
        if (FXSYS_IsOctalDigit(ch)) {
          iEscCode = FXSYS_DecimalCharToInt(static_cast<char>(ch));
          status = 2;
          break;
        }
        if (ch == '\r') {
          status = 4;
          break;
        }
        if (ch == '\n') {
          // Do nothing.
        } else if (ch == 'n') {
          buf += '\n';
        } else if (ch == 'r') {
          buf += '\r';
        } else if (ch == 't') {
          buf += '\t';
        } else if (ch == 'b') {
          buf += '\b';
        } else if (ch == 'f') {
          buf += '\f';
        } else {
          buf += static_cast<char>(ch);
        }
        status = 0;
        break;
      case 2:
        if (FXSYS_IsOctalDigit(ch)) {
          iEscCode =
              iEscCode * 8 + FXSYS_DecimalCharToInt(static_cast<char>(ch));
          status = 3;
        } else {
          buf += static_cast<char>(iEscCode);
          status = 0;
          continue;
        }
        break;
      case 3:
        if (FXSYS_IsOctalDigit(ch)) {
          iEscCode =
              iEscCode * 8 + FXSYS_DecimalCharToInt(static_cast<char>(ch));
          buf += static_cast<char>(iEscCode);
          status = 0;
        } else {
          buf += static_cast<char>(iEscCode);
          status = 0;
          continue;
        }
        break;
      case 4:
        status = 0;
        if (ch != '\n')
          continue;
        break;
    }
    if (!PositionIsInBounds())
      return buf.First(std::min(buf.GetLength(), kMaxStringLength));

    ch = m_pBuf[m_Pos++];
  }
}

DataVector<uint8_t> CPDF_StreamParser::ReadHexString() {
  if (!PositionIsInBounds()) {
    return DataVector<uint8_t>();
  }

  // TODO(thestig): Deduplicate CPDF_SyntaxParser::ReadHexString()?
  DataVector<uint8_t> buf;
  bool bFirst = true;
  uint8_t code = 0;
  while (PositionIsInBounds()) {
    uint8_t ch = m_pBuf[m_Pos++];
    if (ch == '>')
      break;

    if (!isxdigit(ch))
      continue;

    int val = FXSYS_HexCharToInt(ch);
    if (bFirst) {
      code = val * 16;
    } else {
      code += val;
      buf.push_back(code);
    }
    bFirst = !bFirst;
  }
  if (!bFirst) {
    buf.push_back(code);
  }

  if (buf.size() > kMaxStringLength) {
    buf.resize(kMaxStringLength);
  }
  return buf;
}

bool CPDF_StreamParser::PositionIsInBounds() const {
  return m_Pos < m_pBuf.size();
}
