#pragma once

#include <util/generic/vector.h>
#include <util/generic/string.h>

#include <ydb/library/actors/util/rc_buf.h>
#include <ydb/library/actors/util/rope.h>

namespace NKikimr::NSchemeShard {

// Ordered wire fragments composing one pipe-event message body, in the exact
// byte order SerializeToArcadiaStream would emit them.
struct TPipeMessageFragments {
    TVector<TRcBuf> Items;

    TPipeMessageFragments() = default;
};

// Make a fragment: takes ownership of the serialized part by move
// (rvalue-only: an lvalue argument will not compile rather than silently copy).
inline TRcBuf MakePipeMessageFragment(TString&& data) {
    return TRcBuf(std::move(data));
}

// TOperation::TPreSerializedMessage holds, until the first materialization,
// only the fragment list.
//
// The mechanism is built around common parts: a large serialized part (a
// topic-level config, a multi-megabyte cdc-stream bootstrap) is serialized
// once and every message holds only a TRcBuf handle to it, instead of each
// message owning its own copy. A handle copy is a refcount bump of the
// payload holder (an explicit refcount — rc_buf.h keeps a TString "always
// behind a shared TObjectHolder" — so the sharing holds under any TString
// flavor), and the payload is immutable here.
//
// The main user is TConfigureParts::ProgressState (any subop configuring
// shards — a table, an index, a topic, a cdc stream): one pass stages one
// pipe message per shard, all sharing the same common parts; the per-shard
// owned bytes are only the config delta and the record tail. The mechanics
// are generic though: any subop state can stage deferred pipe messages the
// same way (BindMsgToPipe, DoBindMsg, RestartPipeTx).
//
// Memory across a subop state's lifetime:
//
// - TConfigureParts::ProgressState, BindMsgToPipe: the message parks as the
//   fragment list — its own small bytes plus refcounted handles to the
//   common parts, which exist once per pass.
// - TSideEffects::ApplyOnComplete, DoBindMsg: Materialize() chains the
//   fragments, in order, into an NActors::TRope — exactly the message body
//   bytes. Each fragment becomes a rope chunk referencing the same payload
//   holder — the common parts are still not copied; built once at the send,
//   fragments dropped afterwards.
// - Parked in PipeBindedMessages until the ack: the materialized rope — the
//   message's own bytes plus one rope chunk (a payload handle + a byte
//   range) per fragment; the common parts are still one shared buffer each.
// - Pipe re-attach: RestartPipeTx resends the stored entries as is (its
//   Materialize() is idempotent for a deferred entry).

}
