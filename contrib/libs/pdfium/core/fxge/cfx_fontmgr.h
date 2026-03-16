// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_FONTMGR_H_
#define CORE_FXGE_CFX_FONTMGR_H_

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <map>
#include <memory>
#include <tuple>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fixed_size_data_vector.h"
#include "core/fxcrt/observed_ptr.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxge/cfx_face.h"
#include "core/fxge/freetype/fx_freetype.h"

class CFX_FontMapper;

class CFX_FontMgr {
 public:
  class FontDesc final : public Retainable, public Observable {
   public:
    CONSTRUCT_VIA_MAKE_RETAIN;

    pdfium::span<const uint8_t> FontData() const { return m_pFontData; }
    void SetFace(size_t index, CFX_Face* face);
    CFX_Face* GetFace(size_t index) const;

   private:
    explicit FontDesc(FixedSizeDataVector<uint8_t> data);
    ~FontDesc() override;

    const FixedSizeDataVector<uint8_t> m_pFontData;
    std::array<ObservedPtr<CFX_Face>, 16> m_TTCFaces;
  };

  // `index` must be less than `CFX_FontMapper::kNumStandardFonts`.
  static pdfium::span<const uint8_t> GetStandardFont(size_t index);
  static pdfium::span<const uint8_t> GetGenericSansFont();
  static pdfium::span<const uint8_t> GetGenericSerifFont();

  CFX_FontMgr();
  ~CFX_FontMgr();

  RetainPtr<FontDesc> GetCachedFontDesc(const ByteString& face_name,
                                        int weight,
                                        bool bItalic);
  RetainPtr<FontDesc> AddCachedFontDesc(const ByteString& face_name,
                                        int weight,
                                        bool bItalic,
                                        FixedSizeDataVector<uint8_t> data);

  RetainPtr<FontDesc> GetCachedTTCFontDesc(size_t ttc_size, uint32_t checksum);
  RetainPtr<FontDesc> AddCachedTTCFontDesc(size_t ttc_size,
                                           uint32_t checksum,
                                           FixedSizeDataVector<uint8_t> data);

  RetainPtr<CFX_Face> NewFixedFace(RetainPtr<FontDesc> pDesc,
                                   pdfium::span<const uint8_t> span,
                                   size_t face_index);

  // Always present.
  CFX_FontMapper* GetBuiltinMapper() const { return m_pBuiltinMapper.get(); }

  FXFT_LibraryRec* GetFTLibrary() const { return m_FTLibrary.get(); }
  bool FTLibrarySupportsHinting() const { return m_FTLibrarySupportsHinting; }

 private:
  bool FreeTypeVersionSupportsHinting() const;
  bool SetLcdFilterMode() const;

  // Must come before |m_pBuiltinMapper| and |m_FaceMap|.
  ScopedFXFTLibraryRec const m_FTLibrary;
  std::unique_ptr<CFX_FontMapper> m_pBuiltinMapper;
  std::map<std::tuple<ByteString, int, bool>, ObservedPtr<FontDesc>> m_FaceMap;
  std::map<std::tuple<size_t, uint32_t>, ObservedPtr<FontDesc>> m_TTCFaceMap;
  const bool m_FTLibrarySupportsHinting;
};

#endif  // CORE_FXGE_CFX_FONTMGR_H_
