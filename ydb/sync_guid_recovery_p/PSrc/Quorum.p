fun DiskCount(topology: ETopology): int {
  if (topology == Mirror3dc) { return 9; }
  return 8;
}

// PEx serializes set insertion order. Keep membership-only disk sets canonical.
fun CanonicalDisks(disks: set[int], count: int): set[int] {
  var disk: int;
  var result: set[int];
  while (disk < count) {
    if (disk in disks) { result += (disk); }
    disk = disk + 1;
  }
  return result;
}

// TQuorumCheckerOrdinary / TQuorumCheckerMirror3dc::CheckQuorumForGroupDomains.
// One VDisk per fail domain; mirror3dc physical realms are {0,1,2}, {3,4,5}, {6,7,8}.
fun HasGroupQuorum(topology: ETopology, replies: set[int]): bool {
  var realm: int;
  var domain: int;
  var missing: int;
  var num1: int;
  var num2: int;
  if (topology == Block4Plus2) { return sizeof(replies) >= 6; }
  while (realm < 3) {
    domain = 0;
    missing = 0;
    while (domain < 3) {
      if (!(3 * realm + domain in replies)) { missing = missing + 1; }
      domain = domain + 1;
    }
    if (missing >= 1) { num1 = num1 + 1; }
    if (missing >= 2) { num2 = num2 + 1; }
    realm = realm + 1;
  }
  return num1 <= 2 && num2 <= 1;
}

// CheckQuorumForFullSync in NORMAL, all realms INCLUDED, E=LAYOUT_111_22:
// the missing domains must not contain 111 or 22. This equals the group predicate.
// The caller excludes the recovering VDisk from sources.
fun HasFullSyncQuorum(topology: ETopology, sources: set[int]): bool {
  return HasGroupQuorum(topology, sources);
}
