// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CFX_TIMER_H_
#define CORE_FXCRT_CFX_TIMER_H_

#include <stdint.h>

#include "core/fxcrt/observed_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CFX_Timer {
 public:
  // HandlerIface is implemented by upper layers that actually perform
  // the system-dependent actions of scheduling and triggering timers.
  class HandlerIface : public Observable {
   public:
    static constexpr int32_t kInvalidTimerID = 0;
    using TimerCallback = void (*)(int32_t idEvent);

    virtual ~HandlerIface() = default;

    virtual int32_t SetTimer(int32_t uElapse, TimerCallback lpTimerFunc) = 0;
    virtual void KillTimer(int32_t nTimerID) = 0;
  };

  // CallbackIface is implemented by layers that want to perform a
  // specific action on timer expiry.
  class CallbackIface {
   public:
    virtual ~CallbackIface() = default;
    virtual void OnTimerFired() = 0;
  };

  static void InitializeGlobals();
  static void DestroyGlobals();

  CFX_Timer(HandlerIface* pHandlerIface,
            CallbackIface* pCallbackIface,
            int32_t nInterval);
  ~CFX_Timer();

  bool HasValidID() const {
    return m_nTimerID != HandlerIface::kInvalidTimerID;
  }

 private:
  static void TimerProc(int32_t idEvent);

  int32_t m_nTimerID = HandlerIface::kInvalidTimerID;
  ObservedPtr<HandlerIface> m_pHandlerIface;
  UnownedPtr<CallbackIface> const m_pCallbackIface;
};

#endif  // CORE_FXCRT_CFX_TIMER_H_
