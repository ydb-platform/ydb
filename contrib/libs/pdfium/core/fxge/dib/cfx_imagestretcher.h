// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_DIB_CFX_IMAGESTRETCHER_H_
#define CORE_FXGE_DIB_CFX_IMAGESTRETCHER_H_

#include <memory>

#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/dib/fx_dib.h"
#include "core/fxge/dib/scanlinecomposer_iface.h"

class CFX_DIBBase;
class CStretchEngine;
class PauseIndicatorIface;

class CFX_ImageStretcher {
 public:
  CFX_ImageStretcher(ScanlineComposerIface* pDest,
                     RetainPtr<const CFX_DIBBase> source,
                     int dest_width,
                     int dest_height,
                     const FX_RECT& bitmap_rect,
                     const FXDIB_ResampleOptions& options);
  ~CFX_ImageStretcher();

  bool Start();
  bool Continue(PauseIndicatorIface* pPause);

  RetainPtr<const CFX_DIBBase> source();

 private:
  bool StartStretch();
  bool ContinueStretch(PauseIndicatorIface* pPause);

  UnownedPtr<ScanlineComposerIface> const m_pDest;
  RetainPtr<const CFX_DIBBase> const m_pSource;
  std::unique_ptr<CStretchEngine> m_pStretchEngine;
  const FXDIB_ResampleOptions m_ResampleOptions;
  const int m_DestWidth;
  const int m_DestHeight;
  const FX_RECT m_ClipRect;
  const FXDIB_Format m_DestFormat;
};

#endif  // CORE_FXGE_DIB_CFX_IMAGESTRETCHER_H_
