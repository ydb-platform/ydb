#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Trie0001 blob builder and a Python lookup that mirrors binary_trie.h.

Layout (trieBits=1), matching NBinaryTrie::LookupTrie / LookupWithString:

  header (16): "Trie0001" | ui32 dictSize | ui8 trieBits=1 | ui8 flags=0 | pad[2]
  trie region (dictSize): nodes of 16 bytes
      ui32 child[0], ui32 child[1]   — byte offsets inside the trie region
      ui64 payload                    — Lookup() return value
  string table (optional tail): records of ui32 length + bytes
      LookupWithString treats the payload as an absolute offset into the blob.

The high bit of a child slot (0x80000000) marks a prefix match on that node.
Longest match wins: the walker overwrites payloadPos as it descends.
"""

from __future__ import annotations

import argparse
import ipaddress
import struct
import sys

HEADER_SIZE = 16
SIGNATURE = b"Trie0001"
TRIE_BITS = 1
NODE_SIZE = 16  # 2 * ui32 children + ui64 payload
MATCH = 0x80000000
CHILD_MASK = 0x7FFFFFFF

# Shown as-is by LookupWithString in the readable demo query.
WELL_KNOWN = [
    ("8.8.8.0", 24, "AS15169 | Google LLC | 8.8.8.0/24"),
    ("8.8.4.0", 24, "AS15169 | Google LLC | 8.8.4.0/24"),
    ("1.1.1.0", 24, "AS13335 | Cloudflare | 1.1.1.0/24"),
    ("1.0.0.0", 24, "AS13335 | Cloudflare | 1.0.0.0/24"),
    ("9.9.9.0", 24, "AS19281 | Quad9 | 9.9.9.0/24"),
    ("208.67.222.0", 24, "AS36692 | OpenDNS | 208.67.222.0/24"),
    ("4.2.2.0", 24, "AS3356 | Level3 | 4.2.2.0/24"),
    ("203.0.113.0", 24, "AS64500 | TEST-NET-3 | 203.0.113.0/24"),
    ("198.51.100.0", 24, "AS64500 | TEST-NET-2 | 198.51.100.0/24"),
    ("192.0.2.0", 24, "AS64500 | TEST-NET-1 | 192.0.2.0/24"),
    ("10.0.0.0", 8, "RFC1918 | Private | 10.0.0.0/8"),
    ("172.16.0.0", 12, "RFC1918 | Private | 172.16.0.0/12"),
    ("192.168.0.0", 16, "RFC1918 | Private | 192.168.0.0/16"),
    ("100.64.0.0", 10, "RFC6598 | Shared CGNAT | 100.64.0.0/10"),
]

# Distinct addresses used as UDF arguments. Different literals are required:
# YQL collapses identical Apply(Udf, ...) into one call.
DEMO_LOOKUP_IPS = [
    "8.8.8.8",
    "8.8.4.4",
    "1.1.1.1",
    "9.9.9.9",
    "208.67.222.222",
    "4.2.2.2",
    "203.0.113.7",
    "198.51.100.42",
    "192.0.2.1",
    "10.1.2.3",
    "172.16.5.6",
    "192.168.1.1",
    "100.64.0.1",
    "185.60.216.35",  # typically a miss against WELL_KNOWN
    "8.8.8.1",
    "1.0.0.1",
]


def ipv4_bytes(addr: str) -> bytes:
    return ipaddress.IPv4Address(addr).packed


def ipv4_hex(addr: str) -> str:
    return ipv4_bytes(addr).hex()


def _u32(buf: bytes, off: int) -> int:
    return struct.unpack_from("<I", buf, off)[0]


def _u64(buf: bytes, off: int) -> int:
    return struct.unpack_from("<Q", buf, off)[0]


class TrieBuilder:
    def __init__(self) -> None:
        self._nodes = bytearray(NODE_SIZE)  # root at offset 0

    def _alloc(self) -> int:
        off = len(self._nodes)
        self._nodes.extend(b"\x00" * NODE_SIZE)
        return off

    def _child(self, head: int, bit: int) -> int:
        return _u32(self._nodes, head + bit * 4)

    def _set_child(self, head: int, bit: int, value: int) -> None:
        struct.pack_into("<I", self._nodes, head + bit * 4, value)

    def _set_payload(self, head: int, value: int) -> None:
        struct.pack_into("<Q", self._nodes, head + 8, value)

    def insert_bits(self, bits: int, length: int, payload: int) -> None:
        """Insert a big-endian bit string of `length` bits from `bits`."""
        if length < 0 or length > 32:
            raise ValueError("prefix length must be 0..32, got %s" % length)
        head = 0
        for i in range(length):
            bit = (bits >> (length - 1 - i)) & 1
            slot = self._child(head, bit)
            nxt = slot & CHILD_MASK
            if nxt == 0:
                nxt = self._alloc()
                self._set_child(head, bit, nxt | (slot & MATCH))
            head = nxt
        self._set_child(head, 0, self._child(head, 0) | MATCH)
        self._set_payload(head, payload)

    def insert_ipv4(self, network: str, prefixlen: int, payload: int) -> None:
        packed = ipv4_bytes(network)
        bits = int.from_bytes(packed, "big") >> (32 - prefixlen) if prefixlen else 0
        self.insert_bits(bits, prefixlen, payload)

    def dumps(self) -> bytes:
        dict_size = len(self._nodes)
        header = SIGNATURE + struct.pack("<I", dict_size) + bytes([TRIE_BITS, 0, 0, 0])
        assert len(header) == HEADER_SIZE
        return header + bytes(self._nodes)


def build_acl_blob(entries: list[tuple[str, int, str]]) -> bytes:
    """entries: (network, prefixlen, label). Payload is the offset of the label record."""
    labels = [label.encode("utf-8") for _, _, label in entries]
    # First pass: placeholders, then patch payloads after the string table is laid out.
    builder = TrieBuilder()
    for network, prefixlen, _ in entries:
        builder.insert_ipv4(network, prefixlen, 0)

    trie_blob = builder.dumps()
    dict_size = _u32(trie_blob, 8)
    # Records live after header+trie. LookupWithString reads from the start of the blob.
    cursor = HEADER_SIZE + dict_size
    records: list[tuple[int, bytes]] = []
    tail = bytearray()
    for label in labels:
        records.append((cursor, label))
        tail.extend(struct.pack("<I", len(label)))
        tail.extend(label)
        cursor += 4 + len(label)

    # Patch each node's payload. Re-insert with real offsets: same paths, new payloads.
    builder = TrieBuilder()
    for (network, prefixlen, _), (offset, _) in zip(entries, records):
        builder.insert_ipv4(network, prefixlen, offset)
    return builder.dumps() + bytes(tail)


def lookup(blob: bytes, haystack: bytes) -> int:
    """Mirror of NBinaryTrie::LookupTrie. Returns payload ui64, or -1."""
    if len(blob) < HEADER_SIZE:
        raise ValueError("corrupt trie: too small")
    if blob[:8] != SIGNATURE:
        raise ValueError("corrupt trie: invalid signature")
    dict_size = _u32(blob, 8)
    trie_bits = blob[12]
    flags = blob[13]
    if (flags & 1) != 0:
        raise ValueError("incompatible trie flags=%s" % flags)
    if trie_bits != 1:
        raise ValueError("this helper only walks trieBits=1, got %s" % trie_bits)
    if len(blob) - HEADER_SIZE < dict_size:
        raise ValueError("corrupt trie: truncated")
    trie = blob[HEADER_SIZE:]
    trie_size = 1 << trie_bits
    if dict_size < trie_size * 4:
        raise ValueError("corrupt trie: smaller than one node")

    def read_trie(head: int, bit: int) -> int:
        return _u32(trie, head + bit * 4)

    payload_pos = 0
    head = 0
    if read_trie(head, 0) & MATCH:
        payload_pos = head + trie_size * 4  # j=0

    stop = False
    for byte in haystack:
        i = 8
        while i:
            i -= trie_bits
            bit = (byte >> i) & (trie_size - 1)
            nxt = read_trie(head, bit) & CHILD_MASK
            if nxt > dict_size - trie_size * 4:
                raise ValueError("corrupt trie: child out of range")
            if nxt == 0:
                stop = True
                break
            head = nxt
            for j in range(trie_bits - 1, -1, -1):
                if read_trie(head, j) & MATCH:
                    payload_pos = head + trie_size * 4 + j * 8
        if stop:
            break

    if payload_pos == 0:
        return -1
    if payload_pos + 8 > len(blob) - HEADER_SIZE:
        raise ValueError("corrupt trie: payload out of range")
    return _u64(trie, payload_pos)


def lookup_string(blob: bytes, haystack: bytes) -> str | None:
    """Mirror of Trie::LookupWithString."""
    offset = lookup(blob, haystack)
    if offset < 0:
        return None
    if offset > len(blob) - 4:
        raise ValueError("payload offset out of range: %s" % offset)
    size = _u32(blob, offset)
    start = offset + 4
    if start + size > len(blob):
        raise ValueError("payload string truncated")
    return blob[start:start + size].decode("utf-8")


def _self_test() -> None:
    # Fixture from query.sql / binary_trie_ut.cpp. Hit is Ip::FromString("8000::").
    fixture = bytes.fromhex(
        "5472696530303031"
        "20000000"
        "01000000"
        "00000000"
        "10000000"
        "00000000"
        "00000000"
        "00000080"
        "00000000"
        "0a00000000000000"
    )
    hit = bytes.fromhex("80000000000000000000000000000000")
    miss = bytes(16)
    assert lookup(fixture, hit) == 10, lookup(fixture, hit)
    assert lookup(fixture, miss) == -1, lookup(fixture, miss)

    entries = [
        ("8.8.8.0", 24, "AS15169 | Google LLC | 8.8.8.0/24"),
        ("10.0.0.0", 8, "RFC1918 | Private | 10.0.0.0/8"),
        ("10.1.0.0", 16, "ACME | corp-vpn | 10.1.0.0/16"),
        ("203.0.113.0", 24, "AS64500 | TEST-NET-3 | 203.0.113.0/24"),
    ]
    blob = build_acl_blob(entries)
    assert lookup_string(blob, ipv4_bytes("8.8.8.8")) == entries[0][2]
    assert lookup_string(blob, ipv4_bytes("8.8.8.1")) == entries[0][2]
    assert lookup_string(blob, ipv4_bytes("8.8.4.4")) is None
    # Longest match: 10.1.2.3 is inside both 10/8 and 10.1/16.
    assert lookup_string(blob, ipv4_bytes("10.1.2.3")) == entries[2][2]
    assert lookup_string(blob, ipv4_bytes("10.9.9.9")) == entries[1][2]
    assert lookup_string(blob, ipv4_bytes("203.0.113.7")) == entries[3][2]
    assert lookup_string(blob, ipv4_bytes("1.2.3.4")) is None

    # Rebuilding the fixture shape: one-bit prefix 1 → payload 10, 16-byte haystack.
    builder = TrieBuilder()
    builder.insert_bits(1, 1, 10)
    rebuilt = builder.dumps()
    assert lookup(rebuilt, hit) == 10
    assert lookup(rebuilt, miss) == -1
    print("self-test ok: fixture + ipv4 longest-match, blob=%s bytes" % len(blob))


def main() -> int:
    parser = argparse.ArgumentParser(description="Trie0001 builder / lookup helper")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return 0
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
