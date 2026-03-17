// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_PAUSEINDICATOR_IFACE_H_
#define CORE_FXCRT_PAUSEINDICATOR_IFACE_H_

class PauseIndicatorIface {
 public:
  virtual ~PauseIndicatorIface() = default;
  virtual bool NeedToPauseNow() = 0;
};

#endif  // CORE_FXCRT_PAUSEINDICATOR_IFACE_H_
