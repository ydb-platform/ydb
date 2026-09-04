#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Load demo tables: 10k IPv4 keys and 10 Trie dictionaries (1..10 MiB).

  ip_addr  (id, ip, addr)     — addresses to look up
  ip_dict  (id, size_mb, acl) — one Trie0001 blob per row, padded to N MiB

The measured queries do not JOIN: each run picks one dictionary by id and
scans ip_addr. The 1 MiB working trie is built once and padded; Lookup still
walks the same prefixes (WELL_KNOWN + filler).
"""

from __future__ import annotations

import argparse
import ipaddress
import os
import random
import sys
import time

from trie_blob import WELL_KNOWN, build_acl_blob, ipv4_bytes, lookup_string

MIB = 1024 * 1024


def make_dictionary(target_bytes: int) -> bytes:
    entries = list(WELL_KNOWN)
    rng = random.Random(1)
    seen = {(n, p) for n, p, _ in entries}
    while len(entries) < 4000:
        prefixlen = rng.choice((16, 20, 24, 24, 24, 28, 32))
        addr_int = rng.randrange(0, 2**32)
        if prefixlen:
            addr_int = (addr_int >> (32 - prefixlen)) << (32 - prefixlen)
        network = str(ipaddress.IPv4Address(addr_int))
        key = (network, prefixlen)
        if key in seen:
            continue
        seen.add(key)
        entries.append((network, prefixlen, "AS%s | net | %s/%s" % (64000 + rng.randrange(999), network, prefixlen)))
    blob = build_acl_blob(entries)
    google = lookup_string(blob, ipv4_bytes("8.8.8.8"))
    if google is None or "Google" not in google:
        raise RuntimeError("dictionary failed self-check for 8.8.8.8: %r" % google)
    if len(blob) > target_bytes:
        raise RuntimeError("trie is %s bytes, larger than target %s" % (len(blob), target_bytes))
    return blob + b"\x00" * (target_bytes - len(blob))


def make_addresses(n: int) -> list[dict]:
    rows = []
    # Put well-known hits first so the readable query has something to show.
    seeds = [
        "8.8.8.8", "8.8.4.4", "1.1.1.1", "9.9.9.9",
        "208.67.222.222", "4.2.2.2", "203.0.113.7", "198.51.100.42",
        "192.0.2.1", "10.1.2.3", "172.16.5.6", "192.168.1.1",
        "100.64.0.1", "185.60.216.35",
    ]
    rng = random.Random(7)
    seen = set(seeds)
    ips = list(seeds)
    i = 0
    while len(ips) < n:
        candidate = str(ipaddress.IPv4Address(rng.randrange(0, 2**32)))
        if candidate in seen:
            continue
        seen.add(candidate)
        ips.append(candidate)
        i += 1
    for idx, ip in enumerate(ips[:n], start=1):
        rows.append({"id": idx, "ip": ip, "addr": ipv4_bytes(ip)})
    return rows


def _ydb():
    try:
        import ydb  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            "the ydb Python package is required (pip install ydb). Original: %s" % exc
        ) from exc
    return ydb


def upload(endpoint: str, database: str, addr_table: str, dict_table: str,
           addresses: list[dict], dictionaries: list[dict], batch: int) -> None:
    ydb = _ydb()
    addr_path = database.rstrip("/") + "/" + addr_table
    dict_path = database.rstrip("/") + "/" + dict_table
    config = ydb.DriverConfig(endpoint=endpoint, database=database)
    with ydb.Driver(config) as driver:
        driver.wait(timeout=30, fail_fast=True)
        with ydb.QuerySessionPool(driver, size=1) as pool:
            for table in (addr_table, dict_table):
                pool.execute_with_retries("DROP TABLE IF EXISTS `%s`;" % table)
            pool.execute_with_retries(
                "CREATE TABLE `%s` ("
                "    id Uint64 NOT NULL,"
                "    ip Utf8,"
                "    addr String,"
                "    PRIMARY KEY (id)"
                ");" % addr_table
            )
            pool.execute_with_retries(
                "CREATE TABLE `%s` ("
                "    id Uint64 NOT NULL,"
                "    size_mb Uint64,"
                "    acl String,"
                "    PRIMARY KEY (id)"
                ");" % dict_table
            )
        addr_cols = ydb.BulkUpsertColumns()
        addr_cols.add_column("id", ydb.PrimitiveType.Uint64)
        addr_cols.add_column("ip", ydb.PrimitiveType.Utf8)
        addr_cols.add_column("addr", ydb.PrimitiveType.String)
        for i in range(0, len(addresses), batch):
            chunk = addresses[i:i + batch]
            driver.table_client.bulk_upsert(addr_path, chunk, addr_cols)
            print("  addresses %s..%s" % (chunk[0]["id"], chunk[-1]["id"]), flush=True)
        dict_cols = ydb.BulkUpsertColumns()
        dict_cols.add_column("id", ydb.PrimitiveType.Uint64)
        dict_cols.add_column("size_mb", ydb.PrimitiveType.Uint64)
        dict_cols.add_column("acl", ydb.PrimitiveType.String)
        for row in dictionaries:
            driver.table_client.bulk_upsert(dict_path, [row], dict_cols)
            print("  dictionary id=%s %s MiB" % (row["id"], row["size_mb"]), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Load ip_addr + ip_dict demo tables")
    parser.add_argument("--endpoint", default=os.environ.get("ENDPOINT", "grpc://localhost:2146"))
    parser.add_argument("--database", default=os.environ.get("DB", "/Root/test"))
    parser.add_argument("--addr-table", default="ip_addr")
    parser.add_argument("--dict-table", default="ip_dict")
    parser.add_argument("--addresses", type=int, default=10_000)
    parser.add_argument("--dicts", type=int, default=10, help="dictionaries of 1..N MiB")
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("building 1 MiB trie core + %s padded dictionaries" % args.dicts, flush=True)
    t0 = time.time()
    core = make_dictionary(1 * MIB)
    dictionaries = []
    for i in range(1, args.dicts + 1):
        target = i * MIB
        blob = core + b"\x00" * (target - len(core)) if i > 1 else core
        dictionaries.append({"id": i, "size_mb": i, "acl": blob})
        print("  dict id=%s size=%s bytes" % (i, len(blob)), flush=True)
    addresses = make_addresses(args.addresses)
    print("built %s addresses, %s dicts in %.2fs"
          % (len(addresses), len(dictionaries), time.time() - t0), flush=True)

    if args.dry_run:
        print("  LookupWithString(8.8.8.8) = %s"
              % lookup_string(core, ipv4_bytes("8.8.8.8")))
        return 0

    upload(args.endpoint, args.database, args.addr_table, args.dict_table,
           addresses, dictionaries, args.batch)
    print("done: %s/%s  %s/%s"
          % (args.database, args.addr_table, args.database, args.dict_table))
    return 0


if __name__ == "__main__":
    sys.exit(main())
