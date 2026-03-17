// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_CLIPPATH_H_
#define CORE_FPDFAPI_PAGE_CPDF_CLIPPATH_H_

#include <memory>
#include <utility>
#include <vector>

#include "core/fpdfapi/page/cpdf_path.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/shared_copy_on_write.h"
#include "core/fxge/cfx_fillrenderoptions.h"

class CPDF_TextObject;

class CPDF_ClipPath {
 public:
  CPDF_ClipPath();
  CPDF_ClipPath(const CPDF_ClipPath& that);
  CPDF_ClipPath& operator=(const CPDF_ClipPath& that);
  ~CPDF_ClipPath();

  void Emplace() { m_Ref.Emplace(); }
  void SetNull() { m_Ref.SetNull(); }

  bool HasRef() const { return !!m_Ref; }
  bool operator==(const CPDF_ClipPath& that) const {
    return m_Ref == that.m_Ref;
  }
  bool operator!=(const CPDF_ClipPath& that) const { return !(*this == that); }

  size_t GetPathCount() const;
  CPDF_Path GetPath(size_t i) const;
  CFX_FillRenderOptions::FillType GetClipType(size_t i) const;
  size_t GetTextCount() const;
  CPDF_TextObject* GetText(size_t i) const;
  CFX_FloatRect GetClipBox() const;
  void AppendPath(CPDF_Path path, CFX_FillRenderOptions::FillType type);
  void AppendPathWithAutoMerge(CPDF_Path path,
                               CFX_FillRenderOptions::FillType type);
  void AppendTexts(std::vector<std::unique_ptr<CPDF_TextObject>>* pTexts);
  void CopyClipPath(const CPDF_ClipPath& that);
  void Transform(const CFX_Matrix& matrix);

 private:
  class PathData final : public Retainable {
   public:
    CONSTRUCT_VIA_MAKE_RETAIN;

    RetainPtr<PathData> Clone() const;

    using PathAndTypeData =
        std::pair<CPDF_Path, CFX_FillRenderOptions::FillType>;

    std::vector<PathAndTypeData> m_PathAndTypeList;
    std::vector<std::unique_ptr<CPDF_TextObject>> m_TextList;

   private:
    PathData();
    PathData(const PathData& that);
    ~PathData() override;
  };

  SharedCopyOnWrite<PathData> m_Ref;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_CLIPPATH_H_
