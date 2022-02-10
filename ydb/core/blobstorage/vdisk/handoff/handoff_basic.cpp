#include "handoff_basic.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_syncneighbors.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/base/appdata.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    // FIXME: Small blobs we should put to the buffer, but for large blobs we can save their id and read it later.
    //        This will allow us to handle spikes gracefully.

    namespace NHandoff {

        ////////////////////////////////////////////////////////////////////////////
        // TCounters
        ////////////////////////////////////////////////////////////////////////////
        TCounters &TCounters::operator += (const TCounters &s) {
            LocalHandoffSendRightAway += s.LocalHandoffSendRightAway;
            LocalHandoffPostpone += s.LocalHandoffPostpone;
            LocalHandoffDiscard += s.LocalHandoffDiscard;

            ReplyOKResult += s.ReplyOKResult;
            ReplyInvalidVDisk += s.ReplyInvalidVDisk;
            ReplyBadStatusResult += s.ReplyBadStatusResult;
            ReplyOrphanOKResult += s.ReplyOrphanOKResult;
            ReplyCookieMismatch += s.ReplyCookieMismatch;

            ReplyUndelivered += s.ReplyUndelivered;
            WakeupsAlreadyGood += s.WakeupsAlreadyGood;
            WakeupsStillBad += s.WakeupsStillBad;

            StateGoodToBadTransition += s.StateGoodToBadTransition;
            StateBadToGoodTransition += s.StateBadToGoodTransition;

            return *this;
        }

        void TCounters::OutputHtml(IOutputStream &str) const {
            str << "\n";
            HTML(str) {
                DIV_CLASS(".narrow-line70") {
                    SMALL() {
                        SMALL() {
                            HTML_OUTPUT_PARAM(str, LocalHandoffSendRightAway);
                            HTML_OUTPUT_PARAM(str, LocalHandoffPostpone);
                            HTML_OUTPUT_PARAM(str, LocalHandoffDiscard);
                            HTML_OUTPUT_PARAM(str, ReplyOKResult);
                            HTML_OUTPUT_PARAM(str, ReplyInvalidVDisk);
                            HTML_OUTPUT_PARAM(str, ReplyBadStatusResult);
                            HTML_OUTPUT_PARAM(str, ReplyOrphanOKResult);
                            HTML_OUTPUT_PARAM(str, ReplyCookieMismatch);
                            HTML_OUTPUT_PARAM(str, ReplyUndelivered);
                            HTML_OUTPUT_PARAM(str, WakeupsAlreadyGood);
                            HTML_OUTPUT_PARAM(str, WakeupsStillBad);
                            HTML_OUTPUT_PARAM(str, StateGoodToBadTransition);
                            HTML_OUTPUT_PARAM(str, StateBadToGoodTransition);
                        }
                    }
                }
            }
            str << "\n";
        }

        ////////////////////////////////////////////////////////////////////////////
        // TProxyState
        ////////////////////////////////////////////////////////////////////////////
        TProxyState::TProxyState()
            : Monitor(0)
            , EarlyDiscardCounter(0)
            , SelfVDiskID()
            , TargetVDiskID()
            , ProxyID()
            , MaxBytes(0)
            , Initialized(false)
        {}

        TProxyState::TProxyState(IInputStream &) {
            Y_FAIL("Not supported");
        }

        void TProxyState::Init(const TVDiskID &selfVDiskID, const TVDiskID &targetVDiskID, ui32 maxElems, ui32 maxBytes) {
            ui64 monitor = (ui64)maxElems;
            monitor <<= 32u;
            monitor |= (ui64)maxBytes;
            AtomicSet(Monitor, monitor);
            AtomicSet(EarlyDiscardCounter, 0);
            SelfVDiskID = selfVDiskID;
            TargetVDiskID = targetVDiskID;
            MaxBytes = maxBytes;
            Initialized = true;
        }

        bool TProxyState::Restore(const TActorContext &ctx,
                                  const TLogoBlobID &id,
                                  ui64 fullDataSize,
                                  TRope&& data) {
            Y_VERIFY(Initialized, "Restore(%p): SelfVDiskID# %s TargetVDiskID# %s id# %s",
                   this, SelfVDiskID.ToString().data(), TargetVDiskID.ToString().data(), id.ToString().data());
            ui32 byteSize = TEvLocalHandoff::ByteSize(data.GetSize());
            Y_VERIFY_DEBUG(byteSize < MaxBytes);

            while (true) {
                ui64 monitor = AtomicGet(Monitor);
                ui64 elems = monitor >> 32u;
                ui64 bytes = (monitor << 32u) >> 32u;
                if (elems > 0 && bytes > byteSize) {
                    elems--;
                    bytes -= byteSize;
                    ui64 newMonitor = (elems << 32u) | bytes;
                    bool done = AtomicCas<ui64>((ui64 *)&Monitor, newMonitor, monitor);
                    if (done) {
                        // send message
                        ctx.Send(ProxyID, new TEvLocalHandoff(id, fullDataSize, data.ConvertToString()));
                        return true;
                    }
                } else {
                    AtomicIncrement(EarlyDiscardCounter);
                    return false;
                }
            }
        }

        void TProxyState::FreeElement(ui32 byteSize) {
            Y_VERIFY_DEBUG(Initialized);
            bool done = false;
            while (!done) {
                ui64 monitor = AtomicGet(Monitor);
                ui64 elems = monitor >> 32u;
                ui64 bytes = (monitor << 32u) >> 32u;
                elems++;
                bytes += byteSize;
                ui64 newMonitor = (elems << 32u) | bytes;
                done = AtomicCas<ui64>((ui64 *)&Monitor, newMonitor, monitor);
            }
        }

        TString TProxyState::ToString() const {
            return Sprintf("MaxBytes# %" PRIu32 " Initialized# %s", MaxBytes, (Initialized ? "true" : "false"));
        }


        ////////////////////////////////////////////////////////////////////////////
        // TPrivateProxyState
        ////////////////////////////////////////////////////////////////////////////
        void TPrivateProxyState::OutputHtml(IOutputStream &str) const {
            str << "\n";
            HTML(str) {
                DIV_CLASS(".narrow-line70") {
                    SMALL() {
                        SMALL() {
                            HTML_OUTPUT_PARAM(str, WaitQueueSize);
                            HTML_OUTPUT_PARAM(str, WaitQueueByteSize);
                            HTML_OUTPUT_PARAM(str, InFlightQueueSize);
                            HTML_OUTPUT_PARAM(str, InFlightQueueByteSize);
                            HTML_OUTPUT_PARAM(str, CookieCounter);
                            HTML_OUTPUT_TIME_PARAM(str, LastSendTime);
                            HTML_OUTPUT_PARAM(str, WaitQueueSize);
                            HTML_OUTPUT_PARAM(str, WakeupCounter);
                        }
                    }
                }
            }
            str << "\n";
        }

    } // NHandoff

} // NKikimr

