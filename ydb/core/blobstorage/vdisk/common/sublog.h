#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TSublog
    // We use TSublog for logging activities inside some class (actor).
    // In case of an error we can see how we came out to this situation.
    ////////////////////////////////////////////////////////////////////////////
    template <class TStream = TStringStream>
    class TSublog {
    public:
        // Creates a Sublog. Every line is prefixed with prefix
        TSublog(bool prefixWithTime = false, const TString &prefix = {})
            : PrefixWithTime(prefixWithTime)
            , Prefix(prefix)
        {}

        // Start a new line of the log
        // USAGE:
        // Sublog.Log() << "My component got a message " << message << "\n";
        IOutputStream &Log() const {
            ++Recs;
            if (PrefixWithTime) {
                Out << TAppData::TimeProvider->Now().ToStringLocalUpToSeconds() << " ";
            }
            Out << Prefix;
            return Out;
        }

        TString Get() const {
            return Out.Str();
        }

    private:
        const bool PrefixWithTime;
        const TString Prefix;
        mutable TStream Out;
        mutable size_t Recs = 0;
    };


    ////////////////////////////////////////////////////////////////////////
    // TEvSublogLine
    // We may want to log some activities inside a component that is
    // represented as a number of actors that have common actor-ancestor.
    // For instance, VDisk Guid Recovery process is started from Syncer
    // actor, some new actors are borned and died and we want to track
    // what happened in one place. In this case we can generate log messages
    // (i.e. TEvSublogLine) and send it to common ancestor Syncer. Syncer
    // can show the full log in Http introspection.
    ////////////////////////////////////////////////////////////////////////
    struct TEvSublogLine :
        public TEventLocal<TEvSublogLine, TEvBlobStorage::EvSublogLine>
    {
        TStringStream Stream;

        const TString &GetLine() const {
            return Stream.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////
    // TSublogLineHolder
    ////////////////////////////////////////////////////////////////////////
    class TSublogLineHolder {
    public:
        TSublogLineHolder(const TActorId &aid, const TActorContext &ctx)
            : Aid(aid)
            , Ctx(ctx)
            , Ev(std::make_unique<TEvSublogLine>())
        {}

        ~TSublogLineHolder() {
            Ctx.Send(Aid, Ev.release());
        }

        IOutputStream &GetStream() {
            return Ev->Stream;
        }
    private:
        TActorId Aid;
        const TActorContext &Ctx;
        std::unique_ptr<TEvSublogLine> Ev;
    };

} // NKikimr


// SUBLOGLINE is used for generating TEvSublogLine events
// EX:
//
// SUBLOGLINE(aid, ctx, {
//     stream << "Hello, world " << 5;
// });

#define SUBLOGLINE(aid, ctx, userCode)      \
{                                           \
    TSublogLineHolder holder(aid, ctx);     \
    auto &stream = holder.GetStream();      \
    userCode                                \
}
