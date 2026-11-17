// Exhaust every per-peer durable write order and post-quorum cancellation prefix.
machine GuidPublicationProof {
  var topology: ETopology;
  var committed: set[int];
  var reported: bool;
  start state Checking {
    entry {
      announce eScenarioStarted;
      topology = Block4Plus2;
      if ($) { topology = Mirror3dc; }
      new TGuidPublication((Storage = this, Notify = this, Owner = 0,
        Info = (State = RemoteFinal, Guid = 1), Topology = topology));
    }
    on eGuidWriteRequest do (req: TGuidWriteRequest) {
      assert req.Owner == 0 && req.Info.State == RemoteFinal && req.Info.Guid == 1;
      assert !(req.Peer in committed);
      committed += (req.Peer);
      committed = CanonicalDisks(committed, DiskCount(topology));
      send req.Source, eGuidWriteAck, (Peer = req.Peer, Success = true);
    }
    on TEvVDiskGuidWritten do (acks: set[int]) {
      assert !reported && HasGroupQuorum(topology, acks);
      assert HasGroupQuorum(topology, committed);
      reported = true;
      announce eScenarioFinished;
    }
  }
}

test tcProofGuidPublication [main=GuidPublicationProof]:
  assert ScenarioCompletes in { GuidPublicationProof, TGuidPublication };
