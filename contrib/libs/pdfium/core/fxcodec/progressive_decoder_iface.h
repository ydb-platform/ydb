// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_PROGRESSIVE_DECODER_IFACE_H_
#define CORE_FXCODEC_PROGRESSIVE_DECODER_IFACE_H_

#include "core/fxcrt/fx_types.h"
#include "core/fxcrt/retain_ptr.h"

#ifndef PDF_ENABLE_XFA
#error "XFA Only"
#endif

class CFX_CodecMemory;

namespace fxcodec {

class ProgressiveDecoderIface {
 public:
  class Context {
   public:
    virtual ~Context() = default;
  };

  virtual ~ProgressiveDecoderIface() = default;

  // Returns the number of unprocessed bytes remaining in the input buffer.
  virtual FX_FILESIZE GetAvailInput(Context* pContext) const = 0;

  // Provides a new input buffer to the codec. Returns true on success.
  virtual bool Input(Context* pContext,
                     RetainPtr<CFX_CodecMemory> codec_memory) = 0;
};

}  // namespace fxcodec

using fxcodec::ProgressiveDecoderIface;

#endif  // CORE_FXCODEC_PROGRESSIVE_DECODER_IFACE_H_
