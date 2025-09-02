#include "datashard_repl_offsets_server.h"
#include "datashard_impl.h"

namespace NKikimr::NDataShard {

namespace {
    // We will try to split source offsets into 8MB messages even if window is larger
    constexpr ui64 ResultBytesPerMessage = 8 * 1024 * 1024;
}

void TReplicationSourceOffsetsServer::Unlink() {
    if (Self) {
        auto* ds = std::exchange(Self, nullptr);
        ds->ReplicationSourceOffsetsServer = nullptr;
    }
}

void TReplicationSourceOffsetsServer::StateWork(STFUNC_SIG) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(TEvDataShard::TEvGetReplicationSourceOffsets, Handle);
        hFunc(TEvDataShard::TEvReplicationSourceOffsetsAck, Handle);
        hFunc(TEvDataShard::TEvReplicationSourceOffsetsCancel, Handle);
    }
}

void TReplicationSourceOffsetsServer::Handle(TEvDataShard::TEvGetReplicationSourceOffsets::TPtr& ev) {
    const auto* msg = ev->Get();

    TReadId readId(ev->Sender, msg->Record.GetReadId());
    auto& state = Reads[readId];
    state.PathId.OwnerId = msg->Record.GetTableId().GetOwnerId();
    state.PathId.LocalPathId = msg->Record.GetTableId().GetTableId();
    state.Cookie = ev->Cookie;

    if (msg->Record.HasWindowSize()) {
        state.WindowSize = msg->Record.GetWindowSize();
    }

    state.NextSourceId = msg->Record.GetFromSourceId();
    state.NextSplitKeyId = msg->Record.GetFromSplitKeyId();

    if (readId.ActorId.NodeId() != SelfId().NodeId()) {
        Y_ENSURE(ev->InterconnectSession);
        state.InterconnectSession = ev->InterconnectSession;
        auto& nodeState = Nodes[readId.ActorId.NodeId()];
        auto& sessionState = Sessions[ev->InterconnectSession];
        if (!sessionState.NodeId) {
            // Make sure we subscribe to a new session
            sessionState.NodeId = readId.ActorId.NodeId();
            Send(ev->InterconnectSession, new TEvents::TEvSubscribe, IEventHandle::FlagTrackDelivery);
            nodeState.Sessions.insert(ev->InterconnectSession);
        }
        sessionState.Reads.insert(readId);
    }

    ProcessRead(readId, state);
}

void TReplicationSourceOffsetsServer::ProcessRead(const TReadId& readId, TReadState& state) {
    if (state.Finished || state.WaitingNode || state.InFlightTotal >= state.WindowSize) {
        // Wait until some data is acked
        return;
    }

    TSessionState* sessionState = nullptr;
    TNodeState* nodeState = nullptr;
    if (state.InterconnectSession) {
        sessionState = &Sessions.at(state.InterconnectSession);
        nodeState = &Nodes.at(sessionState->NodeId);

        // Wait until some data is acked first
        if (nodeState->InFlightTotal >= nodeState->WindowSize) {
            state.WaitingNodeIt = nodeState->WaitingReads.emplace(nodeState->WaitingReads.end(), readId);
            state.WaitingNode = true;
            return;
        }
    }

    auto res = MakeHolder<TEvDataShard::TEvReplicationSourceOffsets>(readId.ReadId, ++state.LastSeqNo);
    ui64 resSize = 0;

    auto itTable = Self->ReplicatedTables.find(state.PathId);
    if (itTable != Self->ReplicatedTables.end()) {
        auto& table = itTable->second;
        auto itSource = table.SourceById.lower_bound(state.NextSourceId);
        while (itSource != table.SourceById.end()) {
            auto& source = itSource->second;
            auto itSplitKey = source.OffsetBySplitKeyId.lower_bound(state.NextSplitKeyId);
            while (itSplitKey != source.OffsetBySplitKeyId.end()) {
                if (resSize >= ResultBytesPerMessage || resSize >= state.WindowSize - state.InFlightTotal ||
                    nodeState && resSize >= nodeState->WindowSize - nodeState->InFlightTotal)
                {
                    // We have to flush current result
                    res->Record.SetResultSize(resSize);
                    auto& entry = state.InFlight.emplace_back();
                    entry.SeqNo = state.LastSeqNo;
                    entry.Bytes = resSize;
                    state.InFlightTotal += resSize;
                    if (nodeState) {
                        nodeState->InFlightTotal += resSize;
                    }
                    state.NextSourceId = itSource->first;
                    state.NextSplitKeyId = itSplitKey->first;
                    SendViaSession(state.InterconnectSession, readId.ActorId, res.Release(), 0, state.Cookie);
                    if (state.InFlightTotal >= state.WindowSize) {
                        return;
                    }
                    if (nodeState && nodeState->InFlightTotal >= nodeState->WindowSize) {
                        state.WaitingNodeIt = nodeState->WaitingReads.emplace(nodeState->WaitingReads.end(), readId);
                        state.WaitingNode = true;
                        return;
                    }
                    // Start a new chunk immediately
                    res = MakeHolder<TEvDataShard::TEvReplicationSourceOffsets>(readId.ReadId, ++state.LastSeqNo);
                    resSize = 0;
                }

                auto* p = res->Record.AddResult();
                p->SetSourceId(itSource->first);
                p->SetSplitKeyId(itSplitKey->first);
                p->SetSource(source.Name);
                const auto& buffer = itSplitKey->second.SplitKey.GetBuffer();
                if (buffer) {
                    p->SetSplitKey(buffer);
                }
                if (itSplitKey->second.MaxOffset >= 0) {
                    p->SetMaxOffset(itSplitKey->second.MaxOffset);
                }
                resSize += source.CalcStatBytes(itSplitKey->second);
                ++itSplitKey;
            }
            ++itSource;
        }
    }

    res->Record.SetResultSize(resSize);
    res->Record.SetEndOfStream(true);
    auto& entry = state.InFlight.emplace_back();
    entry.SeqNo = state.LastSeqNo;
    entry.Bytes = resSize;
    state.InFlightTotal += resSize;
    if (nodeState) {
        nodeState->InFlightTotal += resSize;
    }
    state.Finished = true;
    SendViaSession(state.InterconnectSession, readId.ActorId, res.Release(), 0, state.Cookie);
}

void TReplicationSourceOffsetsServer::ProcessNode(TNodeState& node) {
    while (node.InFlightTotal < node.WindowSize && !node.WaitingReads.empty()) {
        auto waitingRead = node.WaitingReads.front();
        auto& waitingState = Reads.at(waitingRead);
        Y_ENSURE(waitingState.WaitingNode);
        node.WaitingReads.pop_front();
        waitingState.WaitingNode = false;
        ProcessRead(waitingRead, waitingState);
    }
}

void TReplicationSourceOffsetsServer::Handle(TEvDataShard::TEvReplicationSourceOffsetsAck::TPtr& ev) {
    const auto* msg = ev->Get();
    TReadId readId(ev->Sender, msg->Record.GetReadId());

    auto it = Reads.find(readId);
    if (it == Reads.end()) {
        return;
    }

    auto& state = it->second;
    const ui64 ackSeqNo = msg->Record.GetAckSeqNo();

    TSessionState* sessionState = nullptr;
    TNodeState* nodeState = nullptr;
    if (state.InterconnectSession) {
        sessionState = &Sessions.at(state.InterconnectSession);
        nodeState = &Nodes.at(sessionState->NodeId);
    }

    while (!state.InFlight.empty()) {
        auto& next = state.InFlight.front();
        if (ackSeqNo < next.SeqNo) {
            break;
        }
        state.InFlightTotal -= next.Bytes;
        if (nodeState) {
            nodeState->InFlightTotal -= next.Bytes;
        }
        state.InFlight.pop_front();
    }

    if (!state.Finished) {
        ProcessRead(readId, state);
    } else if (state.InFlight.empty()) {
        // Forget this read
        Y_ENSURE(!state.WaitingNode);
        if (sessionState) {
            sessionState->Reads.erase(readId);
        }
        Reads.erase(it);
    }

    // We may have freed space in the node and need to wake up waiters
    if (nodeState) {
        ProcessNode(*nodeState);
    }
}

void TReplicationSourceOffsetsServer::Handle(TEvDataShard::TEvReplicationSourceOffsetsCancel::TPtr& ev) {
    const auto* msg = ev->Get();
    TReadId readId(ev->Sender, msg->Record.GetReadId());

    auto it = Reads.find(readId);
    if (it == Reads.end()) {
        return;
    }

    auto& state = it->second;

    TSessionState* sessionState = nullptr;
    TNodeState* nodeState = nullptr;
    if (state.InterconnectSession) {
        sessionState = &Sessions.at(state.InterconnectSession);
        nodeState = &Nodes.at(sessionState->NodeId);
    }

    if (nodeState) {
        if (state.WaitingNode) {
            nodeState->WaitingReads.erase(state.WaitingNodeIt);
            state.WaitingNode = false;
        }
        nodeState->InFlightTotal -= state.InFlightTotal;
    }

    Y_ENSURE(!state.WaitingNode);
    if (sessionState) {
        sessionState->Reads.erase(readId);
    }
    Reads.erase(it);

    // We may have freed space in the node and need to wake up waiters
    if (nodeState) {
        ProcessNode(*nodeState);
    }
}

void TReplicationSourceOffsetsServer::SendViaSession(const TActorId& sessionId, const TActorId& target, IEventBase* event, ui32 flags, ui64 cookie) {
    NDataShard::SendViaSession(sessionId, target, SelfId(), event, flags, cookie);
}

void TReplicationSourceOffsetsServer::Handle(TEvents::TEvUndelivered::TPtr& ev) {
    // We only use FlagTrackDelivery with interconnect sessions
    NodeDisconnected(ev->Sender);
}

void TReplicationSourceOffsetsServer::Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    NodeConnected(ev->Sender);
}

void TReplicationSourceOffsetsServer::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    NodeDisconnected(ev->Sender);
}

void TReplicationSourceOffsetsServer::NodeConnected(const TActorId& sessionId) {
    auto itSession = Sessions.find(sessionId);
    if (itSession == Sessions.end()) {
        return;
    }

    itSession->second.Subscribed = true;
}

void TReplicationSourceOffsetsServer::NodeDisconnected(const TActorId& sessionId) {
    auto itSession = Sessions.find(sessionId);
    if (itSession == Sessions.end()) {
        return;
    }

    auto& session = itSession->second;
    auto& node = Nodes.at(session.NodeId);
    node.Sessions.erase(sessionId);

    for (const auto& readId : session.Reads) {
        auto& state = Reads.at(readId);
        if (state.WaitingNode) {
            node.WaitingReads.erase(state.WaitingNodeIt);
            state.WaitingNode = false;
        }
        node.InFlightTotal -= state.InFlightTotal;
        Reads.erase(readId);
    }
    session.Reads.clear();

    if (node.Sessions.empty()) {
        // We no longer need this node
        Y_ENSURE(node.WaitingReads.empty());
        Nodes.erase(session.NodeId);
    } else {
        ProcessNode(node);
    }

    Sessions.erase(itSession);
}

void TDataShard::HandleByReplicationSourceOffsetsServer(STATEFN_SIG) {
    if (!ReplicationSourceOffsetsServer) {
        ReplicationSourceOffsetsServer = new TReplicationSourceOffsetsServer(this);
        RegisterWithSameMailbox(ReplicationSourceOffsetsServer);
    }

    InvokeOtherActor(
        *ReplicationSourceOffsetsServer,
        &TReplicationSourceOffsetsServer::Receive,
        ev);
}

} // namespace NKikimr::NDataShard
