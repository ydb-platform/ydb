// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/fpdf_parser_utility.h"

#include <ostream>
#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_boolean.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_stream.h"

// Indexed by 8-bit character code, contains either:
//   'W' - for whitespace: NUL, TAB, CR, LF, FF, SPACE, 0x80, 0xff
//   'N' - for numeric: 0123456789+-.
//   'D' - for delimiter: %()/<>[]{}
//   'R' - otherwise.
const char kPDFCharTypes[256] = {
    // NUL  SOH  STX  ETX  EOT  ENQ  ACK  BEL  BS   HT   LF   VT   FF   CR   SO
    // SI
    'W', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'W', 'W', 'R', 'W', 'W', 'R',
    'R',

    // DLE  DC1  DC2  DC3  DC4  NAK  SYN  ETB  CAN  EM   SUB  ESC  FS   GS   RS
    // US
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R',

    // SP    !    "    #    $    %    &    '    (    )    *    +    ,    -    .
    // /
    'W', 'R', 'R', 'R', 'R', 'D', 'R', 'R', 'D', 'D', 'R', 'N', 'R', 'N', 'N',
    'D',

    // 0    1    2    3    4    5    6    7    8    9    :    ;    <    =    > ?
    'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'R', 'R', 'D', 'R', 'D',
    'R',

    // @    A    B    C    D    E    F    G    H    I    J    K    L    M    N O
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R',

    // P    Q    R    S    T    U    V    W    X    Y    Z    [    \    ]    ^ _
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'D', 'R', 'D', 'R',
    'R',

    // `    a    b    c    d    e    f    g    h    i    j    k    l    m    n o
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R',

    // p    q    r    s    t    u    v    w    x    y    z    {    |    }    ~
    // DEL
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'D', 'R', 'D', 'R',
    'R',

    'W', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R',
    'R', 'R', 'R', 'R', 'R', 'R', 'R', 'W'};

std::optional<FX_FILESIZE> GetHeaderOffset(
    const RetainPtr<IFX_SeekableReadStream>& pFile) {
  static constexpr size_t kBufSize = 4;
  uint8_t buf[kBufSize];
  for (FX_FILESIZE offset = 0; offset <= 1024; ++offset) {
    if (!pFile->ReadBlockAtOffset(buf, offset))
      return std::nullopt;

    if (memcmp(buf, "%PDF", 4) == 0)
      return offset;
  }
  return std::nullopt;
}

ByteString PDF_NameDecode(ByteStringView orig) {
  size_t src_size = orig.GetLength();
  size_t out_index = 0;
  ByteString result;
  {
    // Span's lifetime must end before ReleaseBuffer() below.
    pdfium::span<char> pDest = result.GetBuffer(src_size);
    for (size_t i = 0; i < src_size; i++) {
      if (orig[i] == '#' && i + 2 < src_size) {
        pDest[out_index++] = FXSYS_HexCharToInt(orig[i + 1]) * 16 +
                             FXSYS_HexCharToInt(orig[i + 2]);
        i += 2;
      } else {
        pDest[out_index++] = orig[i];
      }
    }
  }
  result.ReleaseBuffer(out_index);
  return result;
}

ByteString PDF_NameEncode(const ByteString& orig) {
  pdfium::span<const uint8_t> src_span = orig.unsigned_span();
  size_t dest_len = 0;
  for (const auto ch : src_span) {
    if (ch >= 0x80 || PDFCharIsWhitespace(ch) || ch == '#' ||
        PDFCharIsDelimiter(ch)) {
      dest_len += 3;
    } else {
      dest_len++;
    }
  }
  if (dest_len == src_span.size()) {
    return orig;
  }
  ByteString res;
  {
    // Span's lifetime must end before ReleaseBuffer() below.
    pdfium::span<char> dest_buf = res.GetBuffer(dest_len);
    dest_len = 0;
    for (const auto ch : src_span) {
      if (ch >= 0x80 || PDFCharIsWhitespace(ch) || ch == '#' ||
          PDFCharIsDelimiter(ch)) {
        dest_buf[dest_len++] = '#';
        FXSYS_IntToTwoHexChars(ch, &dest_buf[dest_len]);
        dest_len += 2;
        continue;
      }
      dest_buf[dest_len++] = ch;
    }
  }
  res.ReleaseBuffer(dest_len);
  return res;
}

std::vector<float> ReadArrayElementsToVector(const CPDF_Array* pArray,
                                             size_t nCount) {
  DCHECK(pArray);
  DCHECK(pArray->size() >= nCount);
  std::vector<float> ret(nCount);
  for (size_t i = 0; i < nCount; ++i)
    ret[i] = pArray->GetFloatAt(i);
  return ret;
}

bool ValidateDictType(const CPDF_Dictionary* dict, ByteStringView type) {
  DCHECK(!type.IsEmpty());
  return dict && dict->GetNameFor("Type") == type;
}

bool ValidateDictAllResourcesOfType(const CPDF_Dictionary* dict,
                                    ByteStringView type) {
  if (!dict)
    return false;

  CPDF_DictionaryLocker locker(dict);
  for (const auto& it : locker) {
    RetainPtr<const CPDF_Dictionary> entry =
        ToDictionary(it.second->GetDirect());
    if (!ValidateDictType(entry.Get(), type))
      return false;
  }
  return true;
}

bool ValidateFontResourceDict(const CPDF_Dictionary* dict) {
  return ValidateDictAllResourcesOfType(dict, "Font");
}

bool ValidateDictOptionalType(const CPDF_Dictionary* dict,
                              ByteStringView type) {
  DCHECK(!type.IsEmpty());
  return dict && (!dict->KeyExist("Type") || dict->GetNameFor("Type") == type);
}

std::ostream& operator<<(std::ostream& buf, const CPDF_Object* pObj) {
  if (!pObj) {
    buf << " null";
    return buf;
  }
  switch (pObj->GetType()) {
    case CPDF_Object::kNullobj:
      buf << " null";
      break;
    case CPDF_Object::kBoolean:
    case CPDF_Object::kNumber:
      buf << " " << pObj->GetString();
      break;
    case CPDF_Object::kString:
      buf << pObj->AsString()->EncodeString();
      break;
    case CPDF_Object::kName: {
      ByteString str = pObj->GetString();
      buf << "/" << PDF_NameEncode(str);
      break;
    }
    case CPDF_Object::kReference: {
      buf << " " << pObj->AsReference()->GetRefObjNum() << " 0 R ";
      break;
    }
    case CPDF_Object::kArray: {
      const CPDF_Array* p = pObj->AsArray();
      buf << "[";
      for (size_t i = 0; i < p->size(); i++) {
        RetainPtr<const CPDF_Object> pElement = p->GetObjectAt(i);
        if (!pElement->IsInline()) {
          buf << " " << pElement->GetObjNum() << " 0 R";
        } else {
          buf << pElement.Get();
        }
      }
      buf << "]";
      break;
    }
    case CPDF_Object::kDictionary: {
      CPDF_DictionaryLocker locker(pObj->AsDictionary());
      buf << "<<";
      for (const auto& it : locker) {
        const ByteString& key = it.first;
        const RetainPtr<CPDF_Object>& pValue = it.second;
        buf << "/" << PDF_NameEncode(key);
        if (!pValue->IsInline()) {
          buf << " " << pValue->GetObjNum() << " 0 R ";
        } else {
          buf << pValue;
        }
      }
      buf << ">>";
      break;
    }
    case CPDF_Object::kStream: {
      RetainPtr<const CPDF_Stream> p(pObj->AsStream());
      buf << p->GetDict().Get() << "stream\r\n";
      auto pAcc = pdfium::MakeRetain<CPDF_StreamAcc>(std::move(p));
      pAcc->LoadAllDataRaw();
      auto span = pdfium::as_chars(pAcc->GetSpan());
      buf.write(span.data(), span.size());
      buf << "\r\nendstream";
      break;
    }
  }
  return buf;
}
