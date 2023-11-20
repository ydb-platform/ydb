#pragma once

#include "util_basics.h"
#include "flat_scan_lead.h"
#include "flat_row_eggs.h"
#include "flat_row_scheme.h"

namespace NKikimr {
namespace NTable {
    class TRowState;
    class TSpent;

    /*_ IScan, the generic scan interface. Scan life cycle starts with
        invocation of Prepare(..) method after which it has to progress
        with IDriver::Touch(..).

        Once rows iterator invalidated Seek(..) method is called getting
        next position for seeking followed by a series of Feed(..)
        with rows. Thus the next call after Prepare(..) is always Seek(, 0).

        If Seek call succeeded and return EScan::Feed, then Feed called to pass
        data to reader. When range supplied by Seek completed then Exhausted method is
        called. Default implementation of Exhausted method return EScan::Reset,
        thus Seek method is called again with incrementing seq parameter. Also
        Feed method may return EScan::Reset indicating that current range is
        completed and next one should be set up in Seek method.

        IScan may express its desire of futher IDriver env behaviour
        with EScan codes where applicable.

        At the end IDriver calls Finish() once requesting a product. After
        this IScan is left on its own and impl. have to take care of all
        owned resources and self object in particular. It is allowed for
        IScan to return self as an IDestructable product.

        Limitations and caveats:

            1. Usage of IDriver object is not valid in contexts of any call
               of IScan iface including the initial Prepare(..).

            2. IScan has to maintain its own functional state until Finish(..)
               is called.

            3. Host env may stop progress with Finish(..) not triggered by
               IScan desires. In that case IScan may continue to work alone
               taking care of owned resources.

            4. Seek(..) will progress to Feed(..) on next wake if ::Sleep
                was requested and TLead object is left in valid state on exit
                from Seek(..). Otherwise Seek(..) will be recalled with the
                same state later.

            5. All IScan calls may be spread over time with undefined
                delays ruled by IDriver host environment. Thus IScan should
                not rely on any particular delays.

            6. It is unspecified by this inteface how IDriver activity has
                to be synchronized with IScan, left to host env design.
     */

    enum class EScan {
        Feed    = 0,    /* Feed rows from current iterator state    */
        Sleep   = 1,    /* Suspend execution until explicit resume  */
        Reset   = 3,    /* Reconfigure iterator with new settings   */
        Final   = 8,    /* Impl. isn't needed in more rows or seeks */
    };

    enum class EAbort {
        None    = 0,    /* Regular process termination              */
        Lost    = 1,    /* Owner entity is lost, no way for product */
        Term    = 2,    /* Explicit process termination by owner    */
        Host    = 3,    /* Terminated due to execution env. error   */
    };

    class IDriver {
    public:
        virtual void Touch(EScan) noexcept = 0;
    };


    class IScan : public IDestructable {
    public:
        using TRow = NTable::TRowState;
        using IDriver = NTable::IDriver;
        using TScheme = NTable::TRowScheme;
        using TLead = NTable::TLead;
        using TSpent = NTable::TSpent;
        using EScan = NTable::EScan;
        using EAbort = NTable::EAbort;

        struct TConf {
            TConf() = default;
            TConf(bool noe) : NoErased(noe) { }

            bool NoErased   = true; /* Skip ERowOp::Erase'ed rows */

            /* External blobs below this bytes edge will be materialized
                and passed as a regular ELargeObj::Inline values in row TCells,
                otherwise internal references will be expanded to TGlobId
                unit (ELargeObj::GlobId) suitable for reads from BS.
            */

            ui32 LargeEdge = Max<ui32>();

            // Scan can override default read ahead settings
            ui64 ReadAheadLo = Max<ui64>();
            ui64 ReadAheadHi = Max<ui64>();
        };

        struct TInitialState {
            EScan Scan;     /* Initial scan state           */
            TConf Conf;     /* Scan conveyer configuration  */
        };

        virtual TInitialState Prepare(IDriver*, TIntrusiveConstPtr<TScheme>) noexcept = 0;
        virtual EScan Seek(TLead&, ui64 seq) noexcept = 0;
        virtual EScan Feed(TArrayRef<const TCell>, const TRow&) noexcept = 0;
        virtual TAutoPtr<IDestructable> Finish(EAbort) noexcept = 0;
        virtual void Describe(IOutputStream&) const noexcept = 0;

        /**
         * Called on page faults during iteration
         *
         * The default is to return EScan::Feed, to keep trying to fetch data
         * until the next row is available or iteration is exhausted.
         */
        virtual EScan PageFault() noexcept {
            return EScan::Feed;
        }

        /**
         * Called when iteration is exhausted
         *
         * The default is to return EScan::Reset, causing another Seek for
         * compatibility and making it possible to iterate multiple times.
         */
        virtual EScan Exhausted() noexcept {
            return EScan::Reset;
        }
    };


    class IVersionScan : public IScan {
    private:
        EScan Feed(TArrayRef<const TCell>, const TRow&) noexcept override final {
            Y_ABORT("Unexpected unversioned call");
        }

    public:
        virtual EScan BeginKey(TArrayRef<const TCell>) noexcept = 0;
        virtual EScan BeginDeltas() noexcept = 0;
        virtual EScan Feed(const TRow&, ui64) noexcept = 0;
        virtual EScan EndDeltas() noexcept = 0;
        virtual EScan Feed(const TRow&, TRowVersion&) noexcept = 0;
        virtual EScan EndKey() noexcept = 0;
    };

}
}
