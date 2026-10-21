#include "mkql_bridge_protocol.h"

namespace NKikimr::NMiniKQL {

namespace {

// Deliberately not util/ysaveload.h: its generic Save/Load pair reacts to a
// short read (e.g. the peer process dying mid-frame) with an unrecoverable
// fatal panic instead of a normal, catchable exception -- exactly the wrong
// behavior for a wire protocol where "the other side just died" is an
// expected, recoverable-as-an-error condition (see TBridgeChannel::
// ServeForever/WaitForResponse). IInputStream::LoadOrFail throws a plain
// yexception on a short read instead, which every caller here already
// expects to propagate as an ordinary Bridge error.
template <typename T>
void WritePod(IOutputStream& out, const T& value) {
    out.Write(&value, sizeof(value));
}

template <typename T>
T ReadPod(IInputStream& in) {
    T value;
    in.LoadOrFail(&value, sizeof(value));
    return value;
}

} // namespace

void WriteFrameHeader(IOutputStream& out, EBridgeFrameKind kind) {
    WritePod(out, static_cast<ui8>(kind));
}

void WriteRequestHeader(IOutputStream& out, EBridgeCommand command) {
    WriteFrameHeader(out, EBridgeFrameKind::Request);
    WritePod(out, static_cast<ui8>(command));
}

EBridgeFrameKind ReadFrameHeader(IInputStream& in) {
    const auto kind = ReadPod<ui8>(in);
    if (kind > static_cast<ui8>(EBridgeFrameKind::Max)) {
        ythrow TBridgeException() << "Bridge protocol: unexpected frame kind " << static_cast<int>(kind);
    }
    return static_cast<EBridgeFrameKind>(kind);
}

EBridgeCommand ReadCommand(IInputStream& in) {
    const auto command = ReadPod<ui8>(in);
    if (command > static_cast<ui8>(EBridgeCommand::Max)) {
        ythrow TBridgeException() << "Bridge protocol: unexpected command " << static_cast<int>(command);
    }
    return static_cast<EBridgeCommand>(command);
}

void WriteBytes(IOutputStream& out, TStringBuf bytes) {
    WritePod(out, static_cast<ui64>(bytes.size()));
    out.Write(bytes.data(), bytes.size());
}

TString ReadBytes(IInputStream& in) {
    const auto size = ReadPod<ui64>(in);
    TString result;
    result.ReserveAndResize(size);
    in.LoadOrFail(result.begin(), size);
    return result;
}

TString ReadErrorMessage(IInputStream& in) {
    return ReadBytes(in);
}

void WriteErrorMessage(IOutputStream& out, const TString& message) {
    WriteBytes(out, message);
}

void WriteNodeId(IOutputStream& out, TBridgeNodeId nodeId) {
    WritePod(out, nodeId);
}

TBridgeNodeId ReadNodeId(IInputStream& in) {
    return ReadPod<TBridgeNodeId>(in);
}

void WriteBool(IOutputStream& out, bool value) {
    WritePod(out, static_cast<ui8>(value ? 1 : 0));
}

bool ReadBool(IInputStream& in) {
    return ReadPod<ui8>(in) != 0;
}

void WriteUi32(IOutputStream& out, ui32 value) {
    WritePod(out, value);
}

ui32 ReadUi32(IInputStream& in) {
    return ReadPod<ui32>(in);
}

void WriteUi64(IOutputStream& out, ui64 value) {
    WritePod(out, value);
}

ui64 ReadUi64(IInputStream& in) {
    return ReadPod<ui64>(in);
}

} // namespace NKikimr::NMiniKQL
