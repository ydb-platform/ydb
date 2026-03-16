// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/fpdf_parser_decode.h"

#include <ctype.h>
#include <limits.h>
#include <stddef.h>

#include <algorithm>
#include <array>
#include <utility>

#include "build/build_config.h"
#include "constants/stream_dict_common.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcodec/data_and_bytes_consumed.h"
#include "core/fxcodec/fax/faxmodule.h"
#include "core/fxcodec/flate/flatemodule.h"
#include "core/fxcodec/scanlinedecoder.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxcrt/utf16.h"

namespace {

const uint32_t kMaxStreamSize = 20 * 1024 * 1024;

bool CheckFlateDecodeParams(int Colors, int BitsPerComponent, int Columns) {
  if (Colors < 0 || BitsPerComponent < 0 || Columns < 0)
    return false;

  FX_SAFE_INT32 check = Columns;
  check *= Colors;
  check *= BitsPerComponent;
  if (!check.IsValid())
    return false;

  return check.ValueOrDie() <= INT_MAX - 7;
}

uint8_t GetA85Result(uint32_t res, size_t i) {
  return static_cast<uint8_t>(res >> (3 - i) * 8);
}

}  // namespace

const std::array<uint16_t, 256> kPDFDocEncoding = {
    0x0000, 0x0001, 0x0002, 0x0003, 0x0004, 0x0005, 0x0006, 0x0007, 0x0008,
    0x0009, 0x000a, 0x000b, 0x000c, 0x000d, 0x000e, 0x000f, 0x0010, 0x0011,
    0x0012, 0x0013, 0x0014, 0x0015, 0x0016, 0x0017, 0x02d8, 0x02c7, 0x02c6,
    0x02d9, 0x02dd, 0x02db, 0x02da, 0x02dc, 0x0020, 0x0021, 0x0022, 0x0023,
    0x0024, 0x0025, 0x0026, 0x0027, 0x0028, 0x0029, 0x002a, 0x002b, 0x002c,
    0x002d, 0x002e, 0x002f, 0x0030, 0x0031, 0x0032, 0x0033, 0x0034, 0x0035,
    0x0036, 0x0037, 0x0038, 0x0039, 0x003a, 0x003b, 0x003c, 0x003d, 0x003e,
    0x003f, 0x0040, 0x0041, 0x0042, 0x0043, 0x0044, 0x0045, 0x0046, 0x0047,
    0x0048, 0x0049, 0x004a, 0x004b, 0x004c, 0x004d, 0x004e, 0x004f, 0x0050,
    0x0051, 0x0052, 0x0053, 0x0054, 0x0055, 0x0056, 0x0057, 0x0058, 0x0059,
    0x005a, 0x005b, 0x005c, 0x005d, 0x005e, 0x005f, 0x0060, 0x0061, 0x0062,
    0x0063, 0x0064, 0x0065, 0x0066, 0x0067, 0x0068, 0x0069, 0x006a, 0x006b,
    0x006c, 0x006d, 0x006e, 0x006f, 0x0070, 0x0071, 0x0072, 0x0073, 0x0074,
    0x0075, 0x0076, 0x0077, 0x0078, 0x0079, 0x007a, 0x007b, 0x007c, 0x007d,
    0x007e, 0x0000, 0x2022, 0x2020, 0x2021, 0x2026, 0x2014, 0x2013, 0x0192,
    0x2044, 0x2039, 0x203a, 0x2212, 0x2030, 0x201e, 0x201c, 0x201d, 0x2018,
    0x2019, 0x201a, 0x2122, 0xfb01, 0xfb02, 0x0141, 0x0152, 0x0160, 0x0178,
    0x017d, 0x0131, 0x0142, 0x0153, 0x0161, 0x017e, 0x0000, 0x20ac, 0x00a1,
    0x00a2, 0x00a3, 0x00a4, 0x00a5, 0x00a6, 0x00a7, 0x00a8, 0x00a9, 0x00aa,
    0x00ab, 0x00ac, 0x0000, 0x00ae, 0x00af, 0x00b0, 0x00b1, 0x00b2, 0x00b3,
    0x00b4, 0x00b5, 0x00b6, 0x00b7, 0x00b8, 0x00b9, 0x00ba, 0x00bb, 0x00bc,
    0x00bd, 0x00be, 0x00bf, 0x00c0, 0x00c1, 0x00c2, 0x00c3, 0x00c4, 0x00c5,
    0x00c6, 0x00c7, 0x00c8, 0x00c9, 0x00ca, 0x00cb, 0x00cc, 0x00cd, 0x00ce,
    0x00cf, 0x00d0, 0x00d1, 0x00d2, 0x00d3, 0x00d4, 0x00d5, 0x00d6, 0x00d7,
    0x00d8, 0x00d9, 0x00da, 0x00db, 0x00dc, 0x00dd, 0x00de, 0x00df, 0x00e0,
    0x00e1, 0x00e2, 0x00e3, 0x00e4, 0x00e5, 0x00e6, 0x00e7, 0x00e8, 0x00e9,
    0x00ea, 0x00eb, 0x00ec, 0x00ed, 0x00ee, 0x00ef, 0x00f0, 0x00f1, 0x00f2,
    0x00f3, 0x00f4, 0x00f5, 0x00f6, 0x00f7, 0x00f8, 0x00f9, 0x00fa, 0x00fb,
    0x00fc, 0x00fd, 0x00fe, 0x00ff};

bool ValidateDecoderPipeline(const CPDF_Array* pDecoders) {
  size_t count = pDecoders->size();
  if (count == 0)
    return true;

  for (size_t i = 0; i < count; ++i) {
    RetainPtr<const CPDF_Object> object = pDecoders->GetDirectObjectAt(i);
    if (!object || !object->IsName()) {
      return false;
    }
  }

  if (count == 1)
    return true;

  // TODO(thestig): Consolidate all the places that use these filter names.
  static const char kValidDecoders[][16] = {
      "FlateDecode",    "Fl",  "LZWDecode",       "LZW", "ASCII85Decode", "A85",
      "ASCIIHexDecode", "AHx", "RunLengthDecode", "RL"};
  for (size_t i = 0; i < count - 1; ++i) {
    if (!pdfium::Contains(kValidDecoders, pDecoders->GetByteStringAt(i)))
      return false;
  }
  return true;
}

DataAndBytesConsumed A85Decode(pdfium::span<const uint8_t> src_span) {
  if (src_span.empty()) {
    return {DataVector<uint8_t>(), 0u};
  }

  // Count legal characters and zeros.
  uint32_t zcount = 0;
  uint32_t pos = 0;
  while (pos < src_span.size()) {
    uint8_t ch = src_span[pos];
    if (ch == 'z') {
      zcount++;
    } else if ((ch < '!' || ch > 'u') && !PDFCharIsLineEnding(ch) &&
               ch != ' ' && ch != '\t') {
      break;
    }
    pos++;
  }
  // No content to decode.
  if (pos == 0) {
    return {DataVector<uint8_t>(), 0u};
  }

  // Count the space needed to contain non-zero characters. The encoding ratio
  // of Ascii85 is 4:5.
  uint32_t space_for_non_zeroes = (pos - zcount) / 5 * 4 + 4;
  FX_SAFE_UINT32 size = zcount;
  size *= 4;
  size += space_for_non_zeroes;
  if (!size.IsValid()) {
    return {DataVector<uint8_t>(), FX_INVALID_OFFSET};
  }

  DataVector<uint8_t> dest_buf(size.ValueOrDie());
  pdfium::span<uint8_t> dest_span(dest_buf);
  size_t state = 0;
  uint32_t res = 0;
  pos = 0;
  while (pos < src_span.size()) {
    uint8_t ch = src_span[pos++];
    if (PDFCharIsLineEnding(ch) || ch == ' ' || ch == '\t') {
      continue;
    }

    if (ch == 'z') {
      fxcrt::Fill(dest_span.first(4), 0);
      dest_span = dest_span.subspan(4);
      state = 0;
      res = 0;
      continue;
    }

    // Check for the end or illegal character.
    if (ch < '!' || ch > 'u') {
      break;
    }

    res = res * 85 + ch - 33;
    if (state < 4) {
      ++state;
      continue;
    }

    for (size_t i = 0; i < 4; ++i) {
      dest_span.front() = GetA85Result(res, i);
      dest_span = dest_span.subspan(1);
    }
    state = 0;
    res = 0;
  }
  // Handle partial group.
  if (state) {
    for (size_t i = state; i < 5; ++i) {
      res = res * 85 + 84;
    }
    for (size_t i = 0; i < state - 1; ++i) {
      dest_span.front() = GetA85Result(res, i);
      dest_span = dest_span.subspan(1);
    }
  }
  if (pos < src_span.size() && src_span[pos] == '>') {
    ++pos;
  }
  dest_buf.resize(dest_buf.size() - dest_span.size());
  return {std::move(dest_buf), pos};
}

DataAndBytesConsumed HexDecode(pdfium::span<const uint8_t> src_span) {
  if (src_span.empty()) {
    return {DataVector<uint8_t>(), 0u};
  }

  uint32_t i = 0;
  // Find the end of data.
  while (i < src_span.size() && src_span[i] != '>') {
    ++i;
  }

  DataVector<uint8_t> dest_buf(i / 2 + 1);
  pdfium::span<uint8_t> dest_span(dest_buf);
  bool is_first = true;
  for (i = 0; i < src_span.size(); ++i) {
    uint8_t ch = src_span[i];
    if (PDFCharIsLineEnding(ch) || ch == ' ' || ch == '\t') {
      continue;
    }

    if (ch == '>') {
      ++i;
      break;
    }
    if (!isxdigit(ch)) {
      continue;
    }

    int digit = FXSYS_HexCharToInt(ch);
    if (is_first) {
      dest_span.front() = digit * 16;
    } else {
      dest_span.front() += digit;
      dest_span = dest_span.subspan(1);
    }
    is_first = !is_first;
  }
  size_t dest_size = dest_buf.size() - dest_span.size();
  if (!is_first) {
    ++dest_size;
  }
  dest_buf.resize(dest_size);
  return {std::move(dest_buf), i};
}

DataAndBytesConsumed RunLengthDecode(pdfium::span<const uint8_t> src_span) {
  uint32_t dest_size = 0;
  size_t i = 0;
  while (i < src_span.size()) {
    if (src_span[i] == 128)
      break;

    uint32_t old = dest_size;
    if (src_span[i] < 128) {
      dest_size += src_span[i] + 1;
      if (dest_size < old) {
        return {DataVector<uint8_t>(), FX_INVALID_OFFSET};
      }
      i += src_span[i] + 2;
    } else {
      dest_size += 257 - src_span[i];
      if (dest_size < old) {
        return {DataVector<uint8_t>(), FX_INVALID_OFFSET};
      }
      i += 2;
    }
  }
  if (dest_size >= kMaxStreamSize) {
    return {DataVector<uint8_t>(), FX_INVALID_OFFSET};
  }

  DataVector<uint8_t> dest_buf(dest_size);
  auto dest_span = pdfium::make_span(dest_buf);
  i = 0;
  int dest_count = 0;
  while (i < src_span.size()) {
    if (src_span[i] == 128)
      break;

    if (src_span[i] < 128) {
      uint32_t copy_len = src_span[i] + 1;
      uint32_t buf_left = src_span.size() - i - 1;
      if (buf_left < copy_len) {
        uint32_t delta = copy_len - buf_left;
        copy_len = buf_left;
        fxcrt::Fill(dest_span.subspan(dest_count + copy_len, delta), 0);
      }
      auto copy_span = src_span.subspan(i + 1, copy_len);
      fxcrt::Copy(copy_span, dest_span.subspan(dest_count));
      dest_count += src_span[i] + 1;
      i += src_span[i] + 2;
    } else {
      const uint8_t fill = i + 1 < src_span.size() ? src_span[i + 1] : 0;
      const size_t fill_size = 257 - src_span[i];
      fxcrt::Fill(dest_span.subspan(dest_count, fill_size), fill);
      dest_count += fill_size;
      i += 2;
    }
  }
  return {std::move(dest_buf),
          pdfium::checked_cast<uint32_t>(std::min(i + 1, src_span.size()))};
}

std::unique_ptr<ScanlineDecoder> CreateFaxDecoder(
    pdfium::span<const uint8_t> src_span,
    int width,
    int height,
    const CPDF_Dictionary* pParams) {
  int K = 0;
  bool EndOfLine = false;
  bool ByteAlign = false;
  bool BlackIs1 = false;
  int Columns = 1728;
  int Rows = 0;
  if (pParams) {
    K = pParams->GetIntegerFor("K");
    EndOfLine = !!pParams->GetIntegerFor("EndOfLine");
    ByteAlign = !!pParams->GetIntegerFor("EncodedByteAlign");
    BlackIs1 = !!pParams->GetIntegerFor("BlackIs1");
    Columns = pParams->GetIntegerFor("Columns", 1728);
    Rows = pParams->GetIntegerFor("Rows");
    if (Rows > USHRT_MAX)
      Rows = 0;
  }
  return FaxModule::CreateDecoder(src_span, width, height, K, EndOfLine,
                                  ByteAlign, BlackIs1, Columns, Rows);
}

std::unique_ptr<ScanlineDecoder> CreateFlateDecoder(
    pdfium::span<const uint8_t> src_span,
    int width,
    int height,
    int nComps,
    int bpc,
    const CPDF_Dictionary* pParams) {
  int predictor = 0;
  int Colors = 0;
  int BitsPerComponent = 0;
  int Columns = 0;
  if (pParams) {
    predictor = pParams->GetIntegerFor("Predictor");
    Colors = pParams->GetIntegerFor("Colors", 1);
    BitsPerComponent = pParams->GetIntegerFor("BitsPerComponent", 8);
    Columns = pParams->GetIntegerFor("Columns", 1);
    if (!CheckFlateDecodeParams(Colors, BitsPerComponent, Columns))
      return nullptr;
  }
  return FlateModule::CreateDecoder(src_span, width, height, nComps, bpc,
                                    predictor, Colors, BitsPerComponent,
                                    Columns);
}

DataAndBytesConsumed FlateOrLZWDecode(bool use_lzw,
                                      pdfium::span<const uint8_t> src_span,
                                      const CPDF_Dictionary* pParams,
                                      uint32_t estimated_size) {
  int predictor = 0;
  int Colors = 0;
  int BitsPerComponent = 0;
  int Columns = 0;
  bool bEarlyChange = true;
  if (pParams) {
    predictor = pParams->GetIntegerFor("Predictor");
    bEarlyChange = !!pParams->GetIntegerFor("EarlyChange", 1);
    Colors = pParams->GetIntegerFor("Colors", 1);
    BitsPerComponent = pParams->GetIntegerFor("BitsPerComponent", 8);
    Columns = pParams->GetIntegerFor("Columns", 1);
    if (!CheckFlateDecodeParams(Colors, BitsPerComponent, Columns))
      return {DataVector<uint8_t>(), FX_INVALID_OFFSET};
  }
  return FlateModule::FlateOrLZWDecode(use_lzw, src_span, bEarlyChange,
                                       predictor, Colors, BitsPerComponent,
                                       Columns, estimated_size);
}

std::optional<DecoderArray> GetDecoderArray(
    RetainPtr<const CPDF_Dictionary> pDict) {
  RetainPtr<const CPDF_Object> pFilter = pDict->GetDirectObjectFor("Filter");
  if (!pFilter)
    return DecoderArray();

  if (!pFilter->IsArray() && !pFilter->IsName())
    return std::nullopt;

  RetainPtr<const CPDF_Object> pParams =
      pDict->GetDirectObjectFor(pdfium::stream::kDecodeParms);

  DecoderArray decoder_array;
  if (const CPDF_Array* pDecoders = pFilter->AsArray()) {
    if (!ValidateDecoderPipeline(pDecoders))
      return std::nullopt;

    RetainPtr<const CPDF_Array> pParamsArray = ToArray(pParams);
    for (size_t i = 0; i < pDecoders->size(); ++i) {
      decoder_array.emplace_back(
          pDecoders->GetByteStringAt(i),
          pParamsArray ? pParamsArray->GetDictAt(i) : nullptr);
    }
  } else {
    DCHECK(pFilter->IsName());
    decoder_array.emplace_back(pFilter->GetString(),
                               pParams ? pParams->GetDict() : nullptr);
  }

  return decoder_array;
}

PDFDataDecodeResult::PDFDataDecodeResult() = default;

PDFDataDecodeResult::PDFDataDecodeResult(
    DataVector<uint8_t> data,
    ByteString image_encoding,
    RetainPtr<const CPDF_Dictionary> image_params)
    : data(std::move(data)),
      image_encoding(std::move(image_encoding)),
      image_params(std::move(image_params)) {}

PDFDataDecodeResult::PDFDataDecodeResult(PDFDataDecodeResult&& that) noexcept =
    default;

PDFDataDecodeResult& PDFDataDecodeResult::operator=(
    PDFDataDecodeResult&& that) noexcept = default;

PDFDataDecodeResult::~PDFDataDecodeResult() = default;

std::optional<PDFDataDecodeResult> PDF_DataDecode(
    pdfium::span<const uint8_t> src_span,
    uint32_t last_estimated_size,
    bool bImageAcc,
    const DecoderArray& decoder_array) {
  PDFDataDecodeResult result;
  // May be changed to point to `result.data` in the for-loop below. So put it
  // below `result` and let it get destroyed first.
  pdfium::span<const uint8_t> last_span = src_span;
  const size_t nSize = decoder_array.size();
  for (size_t i = 0; i < nSize; ++i) {
    int estimated_size = i == nSize - 1 ? last_estimated_size : 0;
    ByteString decoder = decoder_array[i].first;
    RetainPtr<const CPDF_Dictionary> pParam =
        ToDictionary(decoder_array[i].second);
    DataVector<uint8_t> new_buf;
    uint32_t bytes_consumed = FX_INVALID_OFFSET;
    if (decoder == "Crypt")
      continue;
    if (decoder == "FlateDecode" || decoder == "Fl") {
      if (bImageAcc && i == nSize - 1) {
        result.image_encoding = "FlateDecode";
        result.image_params = std::move(pParam);
        return result;
      }
      DataAndBytesConsumed decode_result = FlateOrLZWDecode(
          /*use_lzw=*/false, last_span, pParam, estimated_size);
      new_buf = std::move(decode_result.data);
      bytes_consumed = decode_result.bytes_consumed;
    } else if (decoder == "LZWDecode" || decoder == "LZW") {
      DataAndBytesConsumed decode_result =
          FlateOrLZWDecode(/*use_lzw=*/true, last_span, pParam, estimated_size);
      new_buf = std::move(decode_result.data);
      bytes_consumed = decode_result.bytes_consumed;
    } else if (decoder == "ASCII85Decode" || decoder == "A85") {
      DataAndBytesConsumed decode_result = A85Decode(last_span);
      new_buf = std::move(decode_result.data);
      bytes_consumed = decode_result.bytes_consumed;
    } else if (decoder == "ASCIIHexDecode" || decoder == "AHx") {
      DataAndBytesConsumed decode_result = HexDecode(last_span);
      new_buf = std::move(decode_result.data);
      bytes_consumed = decode_result.bytes_consumed;
    } else if (decoder == "RunLengthDecode" || decoder == "RL") {
      if (bImageAcc && i == nSize - 1) {
        result.image_encoding = "RunLengthDecode";
        result.image_params = std::move(pParam);
        return result;
      }
      DataAndBytesConsumed decode_result = RunLengthDecode(last_span);
      new_buf = std::move(decode_result.data);
      bytes_consumed = decode_result.bytes_consumed;
    } else {
      // If we get here, assume it's an image decoder.
      if (decoder == "DCT") {
        decoder = "DCTDecode";
      } else if (decoder == "CCF") {
        decoder = "CCITTFaxDecode";
      }
      result.image_encoding = std::move(decoder);
      result.image_params = std::move(pParam);
      return result;
    }
    if (bytes_consumed == FX_INVALID_OFFSET) {
      return std::nullopt;
    }

    last_span = pdfium::make_span(new_buf);
    result.data = std::move(new_buf);
  }

  result.image_encoding.clear();
  result.image_params = nullptr;
  return result;
}

static size_t StripLanguageCodes(pdfium::span<wchar_t> s, size_t n) {
  size_t dest_pos = 0;
  for (size_t i = 0; i < n; ++i) {
    // 0x001B is a begin/end marker for language metadata region that
    // should not be in the decoded text.
    if (s[i] == 0x001B) {
      for (++i; i < n && s[i] != 0x001B; ++i) {
        // No for-loop body. The loop searches for the terminating 0x001B.
      }
      continue;
    }
    s[dest_pos++] = s[i];
  }
  return dest_pos;
}

WideString PDF_DecodeText(pdfium::span<const uint8_t> span) {
  size_t dest_pos = 0;
  WideString result;
  if (span.size() >= 2 && ((span[0] == 0xfe && span[1] == 0xff) ||
                           (span[0] == 0xff && span[1] == 0xfe))) {
    if (span[0] == 0xfe) {
      result = WideString::FromUTF16BE(span.subspan(2));
    } else {
      result = WideString::FromUTF16LE(span.subspan(2));
    }
    pdfium::span<wchar_t> dest_buf = result.GetBuffer(result.GetLength());
    dest_pos = StripLanguageCodes(dest_buf, result.GetLength());
  } else if (span.size() >= 3 && span[0] == 0xef && span[1] == 0xbb &&
             span[2] == 0xbf) {
    result = WideString::FromUTF8(ByteStringView(span.subspan(3)));
    pdfium::span<wchar_t> dest_buf = result.GetBuffer(result.GetLength());
    dest_pos = StripLanguageCodes(dest_buf, result.GetLength());
  } else {
    pdfium::span<wchar_t> dest_buf = result.GetBuffer(span.size());
    for (size_t i = 0; i < span.size(); ++i)
      dest_buf[i] = kPDFDocEncoding[span[i]];
    dest_pos = span.size();
  }
  result.ReleaseBuffer(dest_pos);
  return result;
}

ByteString PDF_EncodeText(WideStringView str) {
  size_t i = 0;
  size_t len = str.GetLength();
  ByteString result;
  {
    pdfium::span<char> dest_buf = result.GetBuffer(len);
    for (i = 0; i < len; ++i) {
      int code;
      for (code = 0; code < 256; ++code) {
        if (kPDFDocEncoding[code] == str[i])
          break;
      }
      if (code == 256)
        break;

      dest_buf[i] = code;
    }
  }
  result.ReleaseBuffer(i);
  if (i == len)
    return result;

  if (len > INT_MAX / 2 - 1) {
    result.ReleaseBuffer(0);
    return result;
  }

  size_t dest_index = 0;
  {
    std::u16string utf16 = FX_UTF16Encode(str);
    // 2 bytes required per UTF-16 code unit.
    pdfium::span<uint8_t> dest_buf =
        pdfium::as_writable_bytes(result.GetBuffer(utf16.size() * 2 + 2));

    dest_buf[dest_index++] = 0xfe;
    dest_buf[dest_index++] = 0xff;
    for (char16_t code_unit : utf16) {
      dest_buf[dest_index++] = code_unit >> 8;
      dest_buf[dest_index++] = static_cast<uint8_t>(code_unit);
    }
  }
  result.ReleaseBuffer(dest_index);
  return result;
}

ByteString PDF_EncodeString(ByteStringView src) {
  ByteString result;
  result.Reserve(src.GetLength() + 2);
  result += '(';
  for (size_t i = 0; i < src.GetLength(); ++i) {
    uint8_t ch = src[i];
    if (ch == 0x0a) {
      result += "\\n";
      continue;
    }
    if (ch == 0x0d) {
      result += "\\r";
      continue;
    }
    if (ch == ')' || ch == '\\' || ch == '(')
      result += '\\';
    result += static_cast<char>(ch);
  }
  result += ')';
  return result;
}

ByteString PDF_HexEncodeString(ByteStringView src) {
  ByteString result;
  result.Reserve(2 * src.GetLength() + 2);
  result += '<';
  for (size_t i = 0; i < src.GetLength(); ++i) {
    char buf[2];
    FXSYS_IntToTwoHexChars(src[i], buf);
    result += buf[0];
    result += buf[1];
  }
  result += '>';
  return result;
}
