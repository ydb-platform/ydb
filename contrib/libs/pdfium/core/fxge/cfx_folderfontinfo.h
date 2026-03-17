// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_FOLDERFONTINFO_H_
#define CORE_FXGE_CFX_FOLDERFONTINFO_H_

#include <map>
#include <memory>
#include <vector>

#include "core/fxcrt/fx_codepage_forward.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/cfx_fontmapper.h"
#include "core/fxge/systemfontinfo_iface.h"

#define CHARSET_FLAG_ANSI (1 << 0)
#define CHARSET_FLAG_SYMBOL (1 << 1)
#define CHARSET_FLAG_SHIFTJIS (1 << 2)
#define CHARSET_FLAG_BIG5 (1 << 3)
#define CHARSET_FLAG_GB (1 << 4)
#define CHARSET_FLAG_KOREAN (1 << 5)

class CFX_FolderFontInfo : public SystemFontInfoIface {
 public:
  CFX_FolderFontInfo();
  ~CFX_FolderFontInfo() override;

  void AddPath(const ByteString& path);

  // SystemFontInfoIface:
  bool EnumFontList(CFX_FontMapper* pMapper) override;
  void* MapFont(int weight,
                bool bItalic,
                FX_Charset charset,
                int pitch_family,
                const ByteString& face) override;
  void* GetFont(const ByteString& face) override;
  size_t GetFontData(void* hFont,
                     uint32_t table,
                     pdfium::span<uint8_t> buffer) override;
  void DeleteFont(void* hFont) override;
  bool GetFaceName(void* hFont, ByteString* name) override;
  bool GetFontCharset(void* hFont, FX_Charset* charset) override;

 protected:
  friend class CFX_FolderFontInfoTest;

  class FontFaceInfo {
   public:
    static constexpr int32_t kSimilarityScoreMax = 68;

    FontFaceInfo(ByteString filePath,
                 ByteString faceName,
                 ByteString fontTables,
                 uint32_t fontOffset,
                 uint32_t fileSize);

    bool IsEligibleForFindFont(uint32_t flag, FX_Charset charset) const;
    int32_t SimilarityScore(int weight,
                            bool italic,
                            int pitch_family,
                            bool exact_match_bonus) const;

    const ByteString m_FilePath;
    const ByteString m_FaceName;
    const ByteString m_FontTables;
    const uint32_t m_FontOffset;
    const uint32_t m_FileSize;
    uint32_t m_Styles = 0;
    uint32_t m_Charsets = 0;
  };

  void ScanPath(const ByteString& path);
  void ScanFile(const ByteString& path);
  void ReportFace(const ByteString& path,
                  FILE* pFile,
                  FX_FILESIZE filesize,
                  uint32_t offset);
  void* GetSubstFont(const ByteString& face);
  void* FindFont(int weight,
                 bool bItalic,
                 FX_Charset charset,
                 int pitch_family,
                 const ByteString& family,
                 bool bMatchName);

  std::map<ByteString, std::unique_ptr<FontFaceInfo>> m_FontList;
  std::vector<ByteString> m_PathList;
  UnownedPtr<CFX_FontMapper> m_pMapper;
};

#endif  // CORE_FXGE_CFX_FOLDERFONTINFO_H_
