#pragma once

#include <yql/essentials/utils/strong_alias.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/system/types.h>

namespace NKikimr::NMiniKQL {

// Node ids are assigned independently by each peer (monotonic counter, scoped
// to that peer's own node table). There is no fixed/well-known node: every
// callable, including the first one a caller needs, is obtained by sending
// ResolveFunction -- this lets many functions from the same UDF module share
// one worker (and thus one node table), which matters whenever a Resource
// value produced by one bridged function needs to be usable as an argument
// to another.
using TBridgeNodeId = NYql::TStrongAlias<class TBridgeNodeIdTag, ui64>;

// A node reference on the wire is always (namespace, node id), never a bare
// node id: since each side assigns node ids independently, a bare id is
// ambiguous -- it could mean "my own id N" or "your id N", and those refer to
// completely different objects. The namespace says whose table the id lives
// in. 0 is reserved for the host/graph side; every worker (one per bridged
// module) gets its own distinct, nonzero id, assigned once when its channel
// is first created (see TComputationContext::GetOrCreateBridgeChannel). A
// side decoding a reference just compares the namespace against its own: a
// match means "look this id up in my own table and hand back the real
// value"; anything else means "build a proxy". Critically, a proxy always
// remembers the *original* (namespace, node id) it was built from, so
// forwarding it back into a call re-sends that same pair rather than
// wrapping it in a fresh one -- this is what makes a proxy passed back to
// its own owner resolve directly to the real object in one hop, instead of
// chaining through an intermediate, undereferenceable proxy-of-a-proxy.
using TBridgeNamespaceId = NYql::TStrongAlias<class TBridgeNamespaceIdTag, ui64>;
inline constexpr TBridgeNamespaceId HostBridgeNamespace(0);

// Must stay dense (no gaps, nothing reordered/removed): ReadCommand below
// validates an incoming byte by range-checking it against Max, not by
// matching each individual value.
enum class EBridgeCommand: ui8 {
    Run,
    MakeIterator,
    NextIterator,
    ListLength,
    ListEstimatedLength,
    ListHasItems,
    MakeDictIterator,
    NextDictIterator,
    MakeKeysIterator,
    MakePayloadsIterator,
    DictLength,
    DictHasItems,
    DictContains,
    DictLookup,
    UnrefNode,
    ResolveFunction,
    Max = ResolveFunction,
};

// Every command sent by one peer is answered by exactly one frame from the
// other peer before the sender's SendRequest() call returns -- see
// TBridgeChannel::SendRequest in mkql_bridge.h. Because execution is strictly
// synchronous and nested (never concurrent), whichever frame a peer reads
// right after issuing a request is unambiguously either the answer to that
// request, or a nested request the peer must itself serve first.
//
// Must stay dense (no gaps, nothing reordered/removed): ReadFrameHeader below
// validates an incoming byte by range-checking it against Max, not by
// matching each individual value.
enum class EBridgeFrameKind: ui8 {
    Request,
    Response,
    Error,
    // Same payload as Error (a TString message), but raised because the
    // remote side's UdfTerminate/MKQLTerminate fired (NKikimr::NMiniKQL::
    // TTerminateException) rather than some other exception -- the receiver
    // re-raises via MKQLTerminate itself instead of throwing TBridgeException,
    // so a UDF's deliberate terminate call is classified the same way whether
    // or not it happened to run across the bridge (see mkql_terminator.h).
    TerminateError,
    Max = TerminateError,
};

struct TBridgeException: public yexception {
};

// Frame layout on the wire (both the in-process pipe and the out-of-process
// binary's stdin/stdout use the exact same framing):
//   EBridgeFrameKind kind
//   if kind == Request:  EBridgeCommand command, then command-specific payload
//   if kind == Response: command-specific payload (the shape is known
//                        statically by whichever SendRequest() call is
//                        awaiting it)
//   if kind == Error/TerminateError: TString message
void WriteFrameHeader(IOutputStream& out, EBridgeFrameKind kind);
void WriteRequestHeader(IOutputStream& out, EBridgeCommand command);
EBridgeFrameKind ReadFrameHeader(IInputStream& in);
EBridgeCommand ReadCommand(IInputStream& in);
TString ReadErrorMessage(IInputStream& in);
void WriteErrorMessage(IOutputStream& out, const TString& message);

void WriteNodeId(IOutputStream& out, TBridgeNodeId nodeId);
TBridgeNodeId ReadNodeId(IInputStream& in);

void WriteBool(IOutputStream& out, bool value);
bool ReadBool(IInputStream& in);

void WriteBytes(IOutputStream& out, TStringBuf bytes);
TString ReadBytes(IInputStream& in);

void WriteUi32(IOutputStream& out, ui32 value);
ui32 ReadUi32(IInputStream& in);

void WriteUi64(IOutputStream& out, ui64 value);
ui64 ReadUi64(IInputStream& in);

} // namespace NKikimr::NMiniKQL
