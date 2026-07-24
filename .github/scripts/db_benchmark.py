#!/usr/bin/env python3
"""
Compare YDB vs PostgreSQL (pgvector) on the same datasets
(sparse, yfcc-10M, text2image-10M), using ANN vector indexes.

For each backend it
measures index build time, and then sweeps a search-effort parameter measuring
QPS (queries run concurrently across --threads worker connections) and recall@k.

Backends
--------
PostgreSQL: pgvector HNSW.
  - dense datasets   -> column type `vector(d)`,    ops vector_ip_ops / vector_l2_ops
  - sparse dataset   -> column type `sparsevec(d)`, ops sparsevec_ip_ops  (needs pgvector >= 0.7)
  - search effort knob: `SET hnsw.ef_search = <ef>` (this is what --ef_search sweeps)

YDB: `vector_kmeans_tree` index + Knn:: UDF.
  - dense vectors stored as a binary string via Knn::ToBinaryStringFloat
  - YDB has no native sparse type, so the sparse dataset is *densified* to d-dim
    float32 before loading (per the chosen option; this is large/slow at 30109-d).
  - query-time quality/perf knob: PRAGMA ydb.KMeansTreeSearchTopSize (the
    kmeans_tree analogue of HNSW ef_search). The --ef_search values are swept as
    this top-size. Build-time recall is also governed by --ydb-clusters/--ydb-levels.

NOTE: this is a *raw vector ANN* comparison. The yfcc filtered-track metadata
filters are not applied (same scope as benchmark.py).

Setup
-----
    pip install "psycopg[binary]" pgvector ydb numpy scipy tqdm
    # PostgreSQL with the pgvector extension available
    # YDB reachable (local: `ydb -e grpc://localhost:2136 -d /local ...`)

Servers (the script starts and stops them; no running instance assumed)
-----------------------------------------------------------------------
YDB: by default the script DOWNLOADS a prebuilt `ydbd` for --ydb-version and
brings up a local single-node cluster the same way the official `start.sh ram`
does — start a storage node, init blobstorage, register database /Root/test, and
start a database (tenant) node — then stops it. No YDB source tree / test
harness needed; only the `ydb` Python SDK (pip install ydb) for readiness +
queries. Database is /Root/test. Binaries come from the public bucket the YDB
compatibility tests use:
    https://storage.yandexcloud.net/ydb-builds/<version>/release/ydbd
Overrides: --ydb-binary <local ydbd>, --ydb-endpoint <connect to a running one>,
or --ydb-start-cmd <custom launch command>.

    python db_benchmark.py --backend ydb --dataset text2image-10M       # downloads ydbd
    python db_benchmark.py --backend ydb --dataset yfcc-10M --ydb-version stable-26-1-1
    python db_benchmark.py --backend ydb --ydb-binary /path/to/ydbd     # use a local build

PostgreSQL: by default the script BUILDS PostgreSQL + pgvector from source for
--pg-version (cached under --pg-cache-dir; needs gcc + make) and launches it,
then stops it. No prebuilt PostgreSQL distribution bundles pgvector, and the
headless prebuilt binaries ship no server headers to compile it against, so a
one-time cached source build is the reliable route.
Overrides: --pg-binary <local PG bin dir with pgvector>, --pg-dsn <connect to a
running instance>.

    python db_benchmark.py --backend postgres --dataset text2image-10M   # builds PG+pgvector
    python db_benchmark.py --backend both --dataset all \
        --pg-version 16.4 --ydb-version stable-26-2-1

Servers are stopped (and temp data dirs removed) on success, error, or Ctrl-C.

Usage
-----
    python db_benchmark.py --backend both --dataset text2image-10M
    python db_benchmark.py --backend postgres --dataset all --n_base 100000
    python db_benchmark.py --backend ydb --dataset yfcc-10M \
        --ydb-endpoint grpc://localhost:2136 --ydb-database /local
"""

import argparse
import atexit
import json
import os
import shlex
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import traceback
import psycopg
import ydb
from ydb.table import IndexStatus
import tarfile
from scipy.sparse import csr_matrix, hstack
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Dataset constants and file I/O
# ---------------------------------------------------------------------------
_DATA_BASEDIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
_SPARSE_DIR = os.path.join(_DATA_BASEDIR, "sparse")
_BASE_FILE = os.path.join(_SPARSE_DIR, "base_full.csr")
_QUERY_FILE = os.path.join(_SPARSE_DIR, "queries.dev.csr")

ALL_DATASETS = ["sparse", "yfcc-10M", "text2image-10M"]


def _xbin_mmap(fname, dtype, maxn=-1):
    """Memory-map a competition-format .bin file (uint32 header: n, d)."""
    n, d = map(int, np.fromfile(fname, dtype="uint32", count=2))
    assert os.stat(fname).st_size == 8 + n * d * np.dtype(dtype).itemsize, (
        f"{fname}: expected {8 + n * d * np.dtype(dtype).itemsize} bytes, "
        f"got {os.stat(fname).st_size}"
    )
    if maxn > 0:
        n = min(n, maxn)
    return np.memmap(fname, dtype=dtype, mode="r", offset=8, shape=(n, d))


class _Text2Image10M:
    """text2image-10M: 200-dim float32, inner-product metric."""
    nb = 10_000_000
    d = 200
    nq = 100_000
    dtype = "float32"
    _basedir = os.path.join(_DATA_BASEDIR, "text2image1B")
    # BillionScale datasets are stored with a crop suffix when nb < 1B.
    _ds_fn = "base.1B.fbin.crop_nb_10000000"
    _qs_fn = "query.public.100K.fbin"

    def distance(self):
        return "ip"

    def get_data_in_range(self, start, end):
        return _xbin_mmap(os.path.join(self._basedir, self._ds_fn),
                          dtype=self.dtype, maxn=self.nb)[start:end]

    def get_queries(self):
        return np.ascontiguousarray(
            _xbin_mmap(os.path.join(self._basedir, self._qs_fn), dtype=self.dtype))


class _YFCC100MDataset:
    """yfcc-10M: 192-dim uint8, euclidean metric, with optional filter metadata."""
    nb = 10_000_000
    d = 192
    nq = 100_000
    dtype = "uint8"
    _basedir = os.path.join(_DATA_BASEDIR, "yfcc100M")
    _ds_fn = "base.10M.u8bin"
    _qs_fn = "query.public.100K.u8bin"
    _ds_metadata_fn = "base.metadata.10M.spmat"
    _qs_metadata_fn = "query.metadata.public.100K.spmat"

    def distance(self):
        return "euclidean"

    def get_data_in_range(self, start, end):
        return _xbin_mmap(os.path.join(self._basedir, self._ds_fn),
                          dtype=self.dtype, maxn=self.nb)[start:end]

    def get_queries(self):
        return np.ascontiguousarray(
            _xbin_mmap(os.path.join(self._basedir, self._qs_fn), dtype=self.dtype))

    def get_dataset_metadata(self):
        # .spmat uses the same binary layout as .csr: int64 header, int64 indptr,
        # int32 indices, float32 data — readable by read_sparse_matrix().
        return read_sparse_matrix(os.path.join(self._basedir, self._ds_metadata_fn))

    def get_queries_metadata(self):
        return read_sparse_matrix(os.path.join(self._basedir, self._qs_metadata_fn))


_DENSE_DATASETS = {
    "yfcc-10M": _YFCC100MDataset,
    "text2image-10M": _Text2Image10M,
}


# ---------------------------------------------------------------------------
# Sparse I/O
# ---------------------------------------------------------------------------
def read_sparse_matrix(fname, max_rows=0):
    """Read a CSR matrix from the .csr binary format used by the sparse dataset."""
    with open(fname, "rb") as f:
        nrow, ncol, nnz = np.fromfile(f, dtype="int64", count=3)
        indptr = np.fromfile(f, dtype="int64", count=nrow + 1)
        if max_rows > 0 and max_rows < nrow:
            nnz_subset = int(indptr[max_rows])
            indptr = indptr[: max_rows + 1]
            nrow = max_rows
        else:
            nnz_subset = nnz
        indices = np.fromfile(f, dtype="int32", count=nnz_subset)
        data = np.fromfile(f, dtype="float32", count=nnz_subset)
    return csr_matrix((data, indices, indptr), shape=(nrow, ncol))


def rescale_inplace(sparse_mat, name=""):
    """Scale a sparse matrix's values into a normal float32 range, in place.

    SPLADE weights are stored as denormal float32 (~1e-41); inner products of
    two such values underflow to 0, making every result random.  Max-inner-
    product ranking is invariant to independent positive scalings of base and
    query vectors.
    """
    if sparse_mat.nnz == 0:
        return sparse_mat
    max_abs = np.abs(sparse_mat.data).max()
    if max_abs > 0 and not (1e-3 <= max_abs <= 1e3):
        scale = np.float64(1.0) / np.float64(max_abs)
        sparse_mat.data = (sparse_mat.data.astype(np.float64) * scale).astype("float32")
        print(f"Rescaled {name} values by {scale:.3e} "
              f"(max abs {max_abs:.3e} -> {np.abs(sparse_mat.data).max():.3e})")
    return sparse_mat


# ---------------------------------------------------------------------------
# Ground truth
# ---------------------------------------------------------------------------
def recompute_ground_truth_sparse(base_sparse, queries_sparse, k=10):
    print("Recomputing ground truth (sparse, brute-force)...")
    nq = queries_sparse.shape[0]
    gt_ids = np.zeros((nq, k), dtype="int32")
    base_csc = base_sparse.tocsc()
    for i in tqdm(range(nq), desc="Ground truth"):
        scores = base_csc.dot(queries_sparse.getrow(i).T).toarray().ravel()
        top_k = np.argpartition(scores, -k)[-k:]
        gt_ids[i] = top_k
    return gt_ids


def recompute_ground_truth_dense(base, queries, metric, k=10, batch=2048):
    print(f"Recomputing ground truth (dense {metric}, brute-force)...")
    base = np.ascontiguousarray(base, dtype="float32")
    nq = queries.shape[0]
    gt_ids = np.zeros((nq, k), dtype="int32")
    base_sqnorm = (base ** 2).sum(axis=1) if metric == "euclidean" else None
    for i in tqdm(range(0, nq, batch), desc="Ground truth"):
        qb = np.ascontiguousarray(queries[i : i + batch], dtype="float32")
        dots = qb @ base.T
        if metric == "ip":
            idx = np.argpartition(-dots, k - 1, axis=1)[:, :k]
        else:
            d2 = base_sqnorm[None, :] - 2.0 * dots
            idx = np.argpartition(d2, k - 1, axis=1)[:, :k]
        gt_ids[i : i + qb.shape[0]] = idx.astype("int32")
    return gt_ids


def compute_recall(gt_ids, result_ids):
    nq, k = result_ids.shape
    recalls = []
    for i in range(nq):
        gt_set = set(gt_ids[i].tolist())
        if not gt_set:
            continue
        recalls.append(len(gt_set & set(result_ids[i].tolist())) / min(k, len(gt_set)))
    return float(np.mean(recalls)) if recalls else 0.0


# ---------------------------------------------------------------------------
# Dataset loading
# ---------------------------------------------------------------------------
def load_sparse(n_base, n_queries):
    print("Loading sparse base vectors...")
    base_sparse = read_sparse_matrix(_BASE_FILE, max_rows=n_base)
    n_base = base_sparse.shape[0]
    print(f"Base: {base_sparse.shape}, nnz={base_sparse.nnz}, "
          f"avg nnz/row={base_sparse.nnz / n_base:.1f}")

    print("Loading sparse queries...")
    queries_sparse = read_sparse_matrix(_QUERY_FILE, max_rows=n_queries)
    nq = queries_sparse.shape[0]
    print(f"Queries: {queries_sparse.shape}, nnz={queries_sparse.nnz}, "
          f"avg nnz/row={queries_sparse.nnz / nq:.1f}")

    if queries_sparse.shape[1] < base_sparse.shape[1]:
        pad = base_sparse.shape[1] - queries_sparse.shape[1]
        queries_sparse = hstack([queries_sparse, csr_matrix((nq, pad))]).tocsr()
    elif queries_sparse.shape[1] > base_sparse.shape[1]:
        pad = queries_sparse.shape[1] - base_sparse.shape[1]
        base_sparse = hstack([base_sparse, csr_matrix((n_base, pad))]).tocsr()

    rescale_inplace(base_sparse, name="base")
    rescale_inplace(queries_sparse, name="queries")
    print(f"Aligned dimension: {base_sparse.shape[1]}")

    gt_ids = recompute_ground_truth_sparse(base_sparse, queries_sparse, k=10)
    return {"kind": "sparse", "metric": "ip", "base_sparse": base_sparse,
            "queries_sparse": queries_sparse, "gt_ids": gt_ids,
            "n_base": n_base, "nq": nq, "dim": base_sparse.shape[1]}


def load_dense(name, n_base, n_queries, k):
    if name not in _DENSE_DATASETS:
        raise KeyError(f"unknown dataset '{name}'")
    ds = _DENSE_DATASETS[name]()
    metric = ds.distance()

    n_base = ds.nb if n_base <= 0 else min(n_base, ds.nb)
    print(f"Loading {name} base vectors (first {n_base} of {ds.nb})...")
    base = np.ascontiguousarray(ds.get_data_in_range(0, n_base), dtype="float32")

    queries = np.ascontiguousarray(ds.get_queries(), dtype="float32")
    if n_queries > 0:
        queries = queries[:n_queries]
    nq = queries.shape[0]
    dim = base.shape[1]
    print(f"Base: {base.shape} ({ds.dtype}->float32), Queries: {queries.shape}, "
          f"metric={metric}")

    gt_ids = recompute_ground_truth_dense(base, queries, metric, k=k)
    return {"kind": "dense", "metric": metric, "base_dense": base, "queries_dense": queries,
            "gt_ids": gt_ids, "n_base": n_base, "nq": nq, "dim": dim}


def load_dataset(name, n_base, n_queries, k):
    if name == "sparse":
        return load_sparse(n_base, n_queries)
    return load_dense(name, n_base, n_queries, k)


# ---------------------------------------------------------------------------
# Vector literal helpers
# ---------------------------------------------------------------------------
def dense_vec_literal(vec):
    """pgvector dense literal: [v1,v2,...]"""
    return "[" + ",".join(f"{x:.7g}" for x in vec) + "]"


def sparsevec_literal(indices, values, dim):
    """pgvector sparsevec literal: {i1:v1,i2:v2}/dim  (indices are 1-based)."""
    body = ",".join(f"{int(i) + 1}:{float(v):.7g}" for i, v in zip(indices, values))
    return "{" + body + "}/" + str(dim)


def iter_base_literals(data, sparse_for_pg):
    """Yield (id, literal) for every base row, in the right pgvector text form."""
    if data["kind"] == "sparse" and sparse_for_pg:
        m = data["base_sparse"]
        dim = data["dim"]
        for i in range(m.shape[0]):
            lo, hi = m.indptr[i], m.indptr[i + 1]
            yield i, sparsevec_literal(m.indices[lo:hi], m.data[lo:hi], dim)
    else:
        base = base_as_dense(data)
        for i in range(base.shape[0]):
            yield i, dense_vec_literal(base[i])


def query_literals(data, sparse_for_pg):
    """Precompute query literals (out of the timed region)."""
    if data["kind"] == "sparse" and sparse_for_pg:
        m = data["queries_sparse"]
        dim = data["dim"]
        out = []
        for i in range(m.shape[0]):
            lo, hi = m.indptr[i], m.indptr[i + 1]
            out.append(sparsevec_literal(m.indices[lo:hi], m.data[lo:hi], dim))
        return out
    q = queries_as_dense(data)
    return [dense_vec_literal(q[i]) for i in range(q.shape[0])]


def base_as_dense(data):
    """Dense float32 base array (densifies sparse if needed)."""
    if data["kind"] == "dense":
        return data["base_dense"]
    return np.ascontiguousarray(data["base_sparse"].toarray(), dtype="float32")


def queries_as_dense(data):
    if data["kind"] == "dense":
        return data["queries_dense"]
    return np.ascontiguousarray(data["queries_sparse"].toarray(), dtype="float32")


def _load_yfcc_filter(data, k):
    """Augment an already-loaded yfcc-10M data dict with per-label filter metadata (in-place).

    Adds:
      data["base_labels"]      int32 (n_base,)  — first vocab attr per base vector, -1 if none
      data["query_labels"]     int32 (nq,)      — first vocab attr per query, -1 if none
      data["filtered_gt_ids"]  int32 (nq, k)    — exact k-NN restricted to matching-label base rows

    Silently skips if the metadata files have not been downloaded yet.
    """
    try:
        ds = _YFCC100MDataset()
        base_meta = ds.get_dataset_metadata()   # CSR (nb_total, vocab)
        query_meta = ds.get_queries_metadata()  # CSR (nq_total, vocab)
    except Exception as e:
        print(f"  [filter] yfcc metadata unavailable ({e}); using unfiltered index")
        return

    n_base = data["n_base"]
    nq = data["nq"]

    def _first_attr(csr, n):
        """First non-zero column index per row (up to row n), -1 if the row is empty."""
        labels = np.full(n, -1, dtype="int32")
        row_lens = np.diff(csr.indptr[:n + 1])
        has = np.where(row_lens > 0)[0]
        if has.size:
            labels[has] = csr.indices[csr.indptr[has]]
        return labels

    base_labels = _first_attr(base_meta, n_base)
    query_labels = _first_attr(query_meta, nq)

    # Filtered ground truth: for each query label, brute-force k-NN over matching base rows.
    base_dense = data["base_dense"]
    queries_dense = data["queries_dense"]
    metric = data["metric"]
    filtered_gt = np.full((nq, k), -1, dtype="int32")

    print("  [filter] computing filtered ground truth for yfcc-10M...")
    for lbl in np.unique(query_labels[query_labels >= 0]):
        q_mask = query_labels == lbl
        b_mask = base_labels == lbl
        if not np.any(b_mask):
            continue
        q_sub = queries_dense[q_mask]
        b_sub = base_dense[b_mask]
        b_ids = np.where(b_mask)[0].astype("int32")
        n_ret = min(k, len(b_ids))
        if metric == "euclidean":
            scores = (-2.0 * (q_sub @ b_sub.T)
                      + (b_sub ** 2).sum(1)
                      + (q_sub ** 2).sum(1)[:, None])
            idx = np.argpartition(scores, n_ret - 1, axis=1)[:, :n_ret]
        else:
            scores = q_sub @ b_sub.T
            idx = np.argpartition(-scores, n_ret - 1, axis=1)[:, :n_ret]
        for i, qi in enumerate(np.where(q_mask)[0]):
            filtered_gt[qi, :n_ret] = b_ids[idx[i]]

    data["base_labels"] = base_labels
    data["query_labels"] = query_labels
    data["filtered_gt_ids"] = filtered_gt
    n_filtered = int(np.sum(query_labels >= 0))
    print(f"  [filter] {n_filtered}/{nq} queries have label filters")


# ---------------------------------------------------------------------------
# Dataset download (no external dependencies beyond stdlib + numpy)
# ---------------------------------------------------------------------------

def _stream_download(url, dst, label, max_bytes=None):
    """Download url to dst via a .part temp file.

    When max_bytes is set, sends a Range request so the server only transmits
    the first max_bytes bytes (standard HTTP/1.1 range; honoured by most CDNs).
    Falls back gracefully if the server ignores the Range header.
    """
    os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
    tmp = dst + ".part"
    headers = {"Range": f"bytes=0-{max_bytes - 1}"} if max_bytes else {}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as resp:
        total = int(resp.headers.get("Content-Length") or max_bytes or 0)
        downloaded = 0
        t0 = time.time()
        with open(tmp, "wb") as f:
            while True:
                want = min(1 << 20, max_bytes - downloaded) if max_bytes else 1 << 20
                chunk = resp.read(want)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                elapsed = max(time.time() - t0, 1e-9)
                speed = downloaded / elapsed / 1e6
                pct = downloaded / total * 100 if total else 0
                print(f"\r  [{label}] {downloaded/1e6:,.0f} / {total/1e6:,.0f} MB "
                      f"({pct:.0f}%,  {speed:.0f} MB/s)", end="", flush=True)
                if max_bytes and downloaded >= max_bytes:
                    break
    print()
    os.replace(tmp, dst)


def _gunzip_inplace(path, label):
    """Decompress a .gz file to path[:-3] and delete the compressed original."""
    import gzip
    out = path[:-3]
    print(f"  [{label}] decompressing {os.path.basename(path)}...")
    with gzip.open(path, "rb") as src, open(out, "wb") as dst_f:
        shutil.copyfileobj(src, dst_f)
    os.remove(path)
    return out


def _already_have(path, expected_bytes=None):
    if not os.path.exists(path):
        return False
    if expected_bytes is not None and os.path.getsize(path) != expected_bytes:
        return False
    return True


def _download_sparse(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://storage.googleapis.com/ann-challenge-sparse-vectors/csr/"
    for stem in ("base_full", "queries.dev"):
        dst = os.path.join(out_dir, f"{stem}.csr")
        if _already_have(dst):
            print(f"  [sparse] {stem}.csr already present, skipping")
            continue
        gz = dst + ".gz"
        _stream_download(base_url + f"{stem}.csr.gz", gz, "sparse")
        _gunzip_inplace(gz, "sparse")
    print("  [sparse] done")


def _download_yfcc(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/yfcc100M/"
    files = [
        ("base.10M.u8bin",                   8 + 10_000_000 * 192),
        ("query.public.100K.u8bin",          8 + 100_000 * 192),
        ("base.metadata.10M.spmat",          None),
        ("query.metadata.public.100K.spmat", None),
    ]
    for fname, expected_bytes in files:
        dst = os.path.join(out_dir, fname)
        if _already_have(dst, expected_bytes):
            print(f"  [yfcc] {fname} already present, skipping")
            continue
        _stream_download(base_url + fname, dst, "yfcc")
    print("  [yfcc] done")


def _download_text2image(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://storage.yandexcloud.net/yandex-research/ann-datasets/T2I/"

    qs_dst = os.path.join(out_dir, "query.public.100K.fbin")
    qs_bytes = 8 + 100_000 * 200 * 4
    if _already_have(qs_dst, qs_bytes):
        print("  [t2i] query.public.100K.fbin already present, skipping")
    else:
        _stream_download(base_url + "query.public.100K.fbin", qs_dst, "t2i")

    crop_bytes = 8 + 10_000_000 * 200 * 4  # 8_000_000_008
    crop_dst = os.path.join(out_dir, "base.1B.fbin.crop_nb_10000000")
    if _already_have(crop_dst, crop_bytes):
        print("  [t2i] base.1B.fbin.crop_nb_10000000 already present, skipping")
    else:
        print(f"  [t2i] downloading first 10M rows of base.1B.fbin ({crop_bytes / 1e9:.1f} GB)...")
        _stream_download(base_url + "base.1B.fbin", crop_dst, "t2i", max_bytes=crop_bytes)
        # The full file has n=1_000_000_000 in its header; patch to n=10_000_000.
        hdr = np.memmap(crop_dst, dtype="uint32", mode="r+", shape=2)
        assert int(hdr[1]) == 200, f"unexpected dim {hdr[1]} in base.1B.fbin header"
        hdr[0] = 10_000_000
        hdr.flush()
        del hdr
    print("  [t2i] done")


def download_dataset(name):
    """Download all required files for a dataset to its standard data/ subdirectory."""
    if name == "sparse":
        _download_sparse(os.path.join(_DATA_BASEDIR, "sparse"))
    elif name in ("yfcc-10M", "yfcc100M"):
        _download_yfcc(os.path.join(_DATA_BASEDIR, "yfcc100M"))
    elif name in ("text2image-10M", "text2image1B"):
        _download_text2image(os.path.join(_DATA_BASEDIR, "text2image1B"))
    else:
        raise ValueError(
            f"unknown dataset '{name}'; choose: sparse, yfcc-10M, text2image-10M"
        )


def threaded_run(nq, threads, fn, executor=None):
    """Run fn(i) for i in [0,nq) across `threads` workers; return wall seconds.

    If `executor` is given it is reused (and left open) — callers that keep a
    per-thread resource (e.g. a DB connection) MUST reuse a single executor so
    the worker threads, and thus their connections, are recycled across calls
    instead of leaking a fresh batch every sweep.
    """
    t0 = time.perf_counter()
    if executor is not None:
        list(executor.map(fn, range(nq)))
    else:
        with ThreadPoolExecutor(max_workers=threads) as ex:
            list(ex.map(fn, range(nq)))
    return time.perf_counter() - t0


# ===========================================================================
# PostgreSQL / pgvector backend
# ===========================================================================
class PostgresBackend:
    name = "postgres"

    # metric -> (operator, dense ops, sparse ops). <#> is negative inner product
    # (ASC order => max inner product); <-> is L2.
    METRIC = {
        "ip": ("<#>", "vector_ip_ops", "sparsevec_ip_ops"),
        "euclidean": ("<->", "vector_l2_ops", "sparsevec_l2_ops"),
    }

    def __init__(self, dsn, table="ann_items"):
        self.dsn = dsn
        self.table = table
        self._local = threading.local()
        self._op = None
        self._coltype = None
        self._conns = []                 # every opened connection, for cleanup
        self._conns_lock = threading.Lock()
        self._executor = None            # reused across search sweeps

    def _conn(self):
        """One connection per worker thread (DB clients release the GIL on I/O)."""
        c = getattr(self._local, "conn", None)
        if c is None:
            c = psycopg.connect(self.dsn, autocommit=True)
            self._local.conn = c
            with self._conns_lock:
                self._conns.append(c)
        return c

    def _pool(self, threads):
        """A single long-lived executor so worker threads (and their thread-local
        connections) are recycled across every ef_search sweep. Creating a fresh
        pool per sweep would open `threads` new connections each time and quickly
        exhaust the server's max_connections."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=threads)
        return self._executor

    def build(self, data, M, ef_construction, threads):
        sparse = data["kind"] == "sparse"
        dim = data["dim"]
        op, dense_ops, sparse_ops = self.METRIC[data["metric"]]
        self._op = op
        self._coltype = f"sparsevec({dim})" if sparse else f"vector({dim})"
        ops = sparse_ops if sparse else dense_ops

        conn = self._conn()
        self._ensure_vector_extension(conn)
        conn.execute(f"DROP TABLE IF EXISTS {self.table}")
        conn.execute(f"CREATE TABLE {self.table} (id bigint PRIMARY KEY, embedding {self._coltype})")

        print(f"  [pg] loading {data['n_base']} rows via COPY...")
        with conn.cursor() as cur:
            with cur.copy(f"COPY {self.table} (id, embedding) FROM STDIN") as copy:
                for i, lit in iter_base_literals(data, sparse_for_pg=True):
                    copy.write_row((i, lit))

        print(f"  [pg] building HNSW index (m={M}, ef_construction={ef_construction})...")
        conn.execute("SET maintenance_work_mem = '2GB'")
        conn.execute(f"SET max_parallel_maintenance_workers = {max(threads - 1, 0)}")
        t0 = time.perf_counter()
        conn.execute(f"CREATE INDEX ON {self.table} USING hnsw (embedding {ops}) "
                     f"WITH (m = {M}, ef_construction = {ef_construction})")
        return time.perf_counter() - t0

    @staticmethod
    def _ensure_vector_extension(conn):
        """Make sure the pgvector extension is usable on the *server* we connect to.

        This must work against a remote/managed PostgreSQL where we have no
        filesystem access: we probe entirely over SQL. If pgvector is already
        enabled, we're done. Otherwise we try to enable it, and if that fails we
        translate the server-side error into an actionable message instead of a
        cryptic `could not open extension control file ...` traceback.
        """
        # Already installed in this database? Nothing to do.
        row = conn.execute(
            "SELECT 1 FROM pg_extension WHERE extname = 'vector'"
        ).fetchone()
        if row:
            return

        # Is the extension available to be created on this server?
        available = conn.execute(
            "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'"
        ).fetchone()
        if not available:
            raise RuntimeError(
                "the pgvector extension is not installed on the PostgreSQL "
                "server behind --pg-dsn.\n"
                "  This machine cannot install it for you: --pg-dsn may point at "
                "a remote/managed server we have no filesystem access to.\n"
                "  Install pgvector on the target server (e.g. `apt install "
                "postgresql-<ver>-pgvector`, `CREATE EXTENSION vector` on a "
                "managed service that ships it, or build from "
                "https://github.com/pgvector/pgvector), then re-run.\n"
                "  Alternatively, omit --pg-dsn to let this script build and "
                "launch a local PostgreSQL+pgvector automatically."
            )

        # Available but not yet enabled: enable it (needs privileges).
        try:
            conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        except psycopg.errors.InsufficientPrivilege as e:
            raise RuntimeError(
                "pgvector is available on the server but the --pg-dsn role lacks "
                "privileges to run `CREATE EXTENSION vector`.\n"
                "  Ask a superuser to run `CREATE EXTENSION vector` in the target "
                f"database, then re-run. (server said: {e})"
            ) from e

    def search_params(self, ef_search_values):
        return list(ef_search_values)  # pgvector sweeps hnsw.ef_search

    def search(self, q_literals, k, ef, threads):
        sql = (f"SELECT id FROM {self.table} "
               f"ORDER BY embedding {self._op} %s::{self._coltype} LIMIT {k}")
        nq = len(q_literals)
        result_ids = np.full((nq, k), -1, dtype="int32")

        def one(i):
            conn = self._conn()
            conn.execute(f"SET hnsw.ef_search = {ef}")  # cheap; ensures it's set per worker
            with conn.cursor() as cur:
                cur.execute(sql, (q_literals[i],))
                rows = cur.fetchall()
            for j, (rid,) in enumerate(rows[:k]):
                result_ids[i, j] = rid

        qt = threaded_run(nq, threads, one, self._pool(threads))
        return result_ids, qt

    def close(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
        with self._conns_lock:
            conns, self._conns = self._conns, []
        for c in conns:
            try:
                c.close()
            except Exception:
                pass


# ===========================================================================
# YDB backend (vector_kmeans_tree)
# ===========================================================================
class YDBBackend:
    name = "ydb"

    # metric -> (WITH option, Knn function, ORDER direction)
    METRIC = {
        "ip": ("similarity=inner_product", "Knn::InnerProductSimilarity", "DESC"),
        "euclidean": ("distance=euclidean", "Knn::EuclideanDistance", "ASC"),
    }

    def __init__(self, endpoint, database, table="ann_items", clusters=64, levels=2):
        self.endpoint = endpoint
        self.database = database
        self.table = table
        self.clusters = clusters
        self.levels = levels
        self._ydb = None
        self._driver = None
        self._pool = None
        self._metric = None
        self._use_filter = False
        self._query_labels = None

    def _connect(self):
        if self._driver is None:
            self._ydb = ydb
            cfg = self._ydb.DriverConfig(
                self.endpoint, self.database,
                credentials=_ydb_credentials(self._ydb),
            )
            self._driver = self._ydb.Driver(cfg)
            try:
                self._driver.wait(timeout=15)
            except Exception as e:
                raise RuntimeError(
                    f"YDB driver failed to connect to {self.endpoint} "
                    f"(database={self.database}): {e!r}"
                ) from e
            self._pool = self._ydb.SessionPool(self._driver)
        return self._pool

    def _exec_scheme(self, yql):
        """DDL (CREATE/DROP/ALTER TABLE) — must be a scheme op, not a data query."""
        pool = self._connect()
        return pool.retry_operation_sync(lambda s: s.execute_scheme(yql))

    def _exec_data(self, yql, params=None):
        """DML (UPSERT/SELECT). Prepared so the DECLARE types drive parameter binding."""
        pool = self._connect()

        def op(session):
            tx = session.transaction(self._ydb.SerializableReadWrite())
            if params:
                return tx.execute(session.prepare(yql), params, commit_tx=True)
            return tx.execute(yql, commit_tx=True)

        return pool.retry_operation_sync(op)

    def build(self, data, M, ef_construction, threads):
        self._connect()
        dim = data["dim"]
        with_opt, _fn, _dir = self.METRIC[data["metric"]]
        self._metric = data["metric"]
        self._use_filter = data.get("base_labels") is not None
        self._query_labels = data.get("query_labels")

        # (Re)create the table.  With filter: add a label Int32 column so the
        # covering index can use it as a prefix for filtered vector search.
        try:
            self._exec_scheme(f"DROP TABLE `{self.table}`")
        except Exception:
            pass
        if self._use_filter:
            self._exec_scheme(
                f"CREATE TABLE `{self.table}` "
                f"(id Uint64, label Int32, embedding String, PRIMARY KEY (id))")
        else:
            self._exec_scheme(
                f"CREATE TABLE `{self.table}` (id Uint64, embedding String, PRIMARY KEY (id))")

        base = base_as_dense(data)  # densifies sparse per the chosen option
        if data["kind"] == "sparse":
            print(f"  [ydb] WARNING: densified sparse vectors to {dim}-dim float "
                  f"({base.shape[0] * dim * 4 / 1e9:.1f} GB) — large and slow.")

        # Load via BulkUpsert of the binary vector serialized client-side.  This
        # avoids the (very slow) pure-Python protobuf encoding of List<Float> and
        # the server-side Knn::ToBinaryStringFloat call: at dim=30109 the old path
        # spent ~all its time building one protobuf Value per float (billions of
        # them).  The Knn FloatVector layout is just the little-endian float32
        # bytes followed by a 1-byte format tag (EFormat::FloatVector == 1, see
        # ydb/library/yql/udfs/common/knn/knn-defines.h), which numpy produces at
        # C speed.  BulkUpsert also skips the transaction/query machinery.
        print(f"  [ydb] loading {base.shape[0]} rows via BulkUpsert...")
        FLOAT_VECTOR_TAG = b"\x01"
        pt = self._ydb.PrimitiveType
        columns = self._ydb.BulkUpsertColumns().add_column("id", pt.Uint64)
        if self._use_filter:
            columns = columns.add_column("label", pt.Int32)
        columns = columns.add_column("embedding", pt.String)
        table_path = f"{self.database}/{self.table}"

        # Serialized row = dim*4 bytes + 1 tag byte.  Keep each BulkUpsert request
        # well under YDB's 64 MB gRPC limit.
        row_bytes = dim * 4 + 1
        batch = max(1, min(2000, 48_000_000 // row_bytes))
        base_labels = data.get("base_labels")
        pbar = tqdm(total=base.shape[0], unit="rows", desc="  [ydb]")
        for start in range(0, base.shape[0], batch):
            end = min(start + batch, base.shape[0])
            # C-speed little-endian float32 serialization for the whole slice.
            blob = np.ascontiguousarray(base[start:end], dtype="<f4")
            if self._use_filter:
                rows = [{"id": start + i, "label": int(base_labels[start + i]),
                         "embedding": blob[i].tobytes() + FLOAT_VECTOR_TAG}
                        for i in range(end - start)]
            else:
                rows = [{"id": start + i,
                         "embedding": blob[i].tobytes() + FLOAT_VECTOR_TAG}
                        for i in range(end - start)]
            self._driver.table_client.bulk_upsert(table_path, rows, columns)
            pbar.update(end - start)
        pbar.close()

        # Add the ANN index and time its build.
        # Filtered: ON (label, embedding) partitions the index by label so that
        # WHERE label = $l at query time only traverses that label's subtree.
        if self._use_filter:
            idx_cols = "ON (label, embedding)"
            print(f"  [ydb] building filtered vector_kmeans_tree index "
                  f"(clusters={self.clusters}, levels={self.levels})...")
        else:
            idx_cols = "ON (embedding)"
            print(f"  [ydb] building vector_kmeans_tree index "
                  f"(clusters={self.clusters}, levels={self.levels})...")
        t0 = time.perf_counter()
        self._exec_scheme(
            f"ALTER TABLE `{self.table}` ADD INDEX vidx GLOBAL USING vector_kmeans_tree "
            f"{idx_cols} WITH ({with_opt}, vector_type=\"float\", "
            f"vector_dimension={dim}, clusters={self.clusters}, levels={self.levels});"
        )
        self._wait_index_ready()
        return time.perf_counter() - t0

    def _wait_index_ready(self, timeout=900):
        """Block until the async vector index build finishes (status BUILDING -> READY)."""
        deadline = time.time() + timeout
        path = f"{self.database}/{self.table}"
        while time.time() < deadline:
            desc = self._driver.table_client.session().create().describe_table(path)
            idx = [i for i in desc.indexes if i.name == "vidx"]
            if idx:
                status = getattr(idx[0], "status", None)
                # status is an IntEnum (READY=1, BUILDING=2); None on SDKs that omit it.
                if status is None or status == IndexStatus.READY:
                    return
            time.sleep(2)
        print("  [ydb] WARNING: index build wait timed out; querying anyway.")

    def search_params(self, ef_search_values):
        # kmeans_tree's query-time quality/perf knob (analogous to HNSW ef_search):
        # PRAGMA ydb.KMeansTreeSearchTopSize. Sweep the same values.
        return list(ef_search_values)

    def search(self, queries_dense, k, top_size, threads):
        _opt, fn, direction = self.METRIC[self._metric]
        pragma = (f'PRAGMA ydb.KMeansTreeSearchTopSize = "{top_size}";\n'
                  if top_size is not None else "")
        nq = len(queries_dense)
        result_ids = np.full((nq, k), -1, dtype="int32")

        if self._use_filter and self._query_labels is not None:
            yql = (
                pragma +
                "DECLARE $q AS List<Float>;\n"
                "DECLARE $label AS Int32;\n"
                "$t = Untag(Knn::ToBinaryStringFloat($q), \"FloatVector\");\n"
                f"SELECT id FROM `{self.table}` VIEW vidx "
                f"WHERE label = $label "
                f"ORDER BY {fn}(embedding, $t) {direction} LIMIT {k};"
            )
            query_labels = self._query_labels

            def one(i):
                res = self._exec_data(
                    yql, {"$q": queries_dense[i].tolist(), "$label": int(query_labels[i])})
                for j, row in enumerate(res[0].rows[:k]):
                    result_ids[i, j] = row["id"]
        else:
            yql = (
                pragma +
                "DECLARE $q AS List<Float>;\n"
                "$t = Untag(Knn::ToBinaryStringFloat($q), \"FloatVector\");\n"
                f"SELECT id FROM `{self.table}` VIEW vidx "
                f"ORDER BY {fn}(embedding, $t) {direction} LIMIT {k};"
            )

            def one(i):
                res = self._exec_data(yql, {"$q": queries_dense[i].tolist()})
                for j, row in enumerate(res[0].rows[:k]):
                    result_ids[i, j] = row["id"]

        qt = threaded_run(nq, threads, one)
        return result_ids, qt

    def close(self):
        if self._pool is not None:
            self._pool.stop()
        if self._driver is not None:
            self._driver.stop()


# ===========================================================================
# Server lifecycle (start a DB from a custom binary, stop it after)
# ===========================================================================
def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_tcp(host, port, timeout, proc=None):
    """Block until host:port accepts a connection, or the process dies / we time out.

    Uses create_connection so it resolves and tries both IPv4 and IPv6 (ydbd often
    binds IPv6).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"server process exited early (code {proc.returncode}); check its log")
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.3)
    raise TimeoutError(f"{host}:{port} not ready after {timeout}s")


def _http_download(url, dst, label, approx_gb=None):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    tmp = dst + ".part"
    size_note = f" (~{approx_gb} GB)" if approx_gb else ""
    print(f"  [{label}] downloading {url}{size_note}")
    last = [0.0]

    def progress(block, bsize, total):
        done = block * bsize
        now = time.time()
        if now - last[0] > 2 or (total > 0 and done >= total):
            pct = (done / total * 100) if total > 0 else 0
            print(f"\r  [{label}]   {done / 1e6:.0f} MB ({pct:.0f}%)", end="", flush=True)
            last[0] = now

    urllib.request.urlretrieve(url, tmp, reporthook=progress)
    print()
    os.replace(tmp, dst)
    return dst


def _run_logged(cmd, cwd, logf, env=None):
    logf.write(f"\n$ {' '.join(cmd)}  (cwd={cwd})\n".encode())
    logf.flush()
    r = subprocess.run(cmd, cwd=cwd, env=env, stdout=logf, stderr=subprocess.STDOUT)
    if r.returncode != 0:
        raise RuntimeError(f"command failed ({' '.join(cmd[:3])}...); see build log")


def provision_postgres(pg_version, pgvector_version, cache_dir):
    """Build PostgreSQL + pgvector from source into a cached prefix; return its bin dir.

    No prebuilt PostgreSQL distribution bundles pgvector, and the headless
    prebuilt binaries (zonky) ship no server headers to compile it against, so we
    do a one-time source build (needs gcc + make) and cache the result.
    """
    cache = os.path.expanduser(cache_dir)
    prefix = os.path.join(cache, f"pg-{pg_version}-pgvector-{pgvector_version}")
    bindir = os.path.join(prefix, "bin")
    have_pg = os.path.exists(os.path.join(bindir, "postgres"))
    have_vec = bool(os.path.exists(prefix) and
                    any(f == "vector.control"
                        for _r, _d, fs in os.walk(prefix) for f in fs)) if have_pg else False
    if have_pg and have_vec:
        print(f"  [pg] using cached build {bindir}")
        return bindir

    build_dir = tempfile.mkdtemp(prefix="annbench_pgbuild_")
    log_path = os.path.join(cache, f"build-pg-{pg_version}.log")
    os.makedirs(cache, exist_ok=True)
    print(f"  [pg] building PostgreSQL {pg_version} + pgvector {pgvector_version} "
          f"(one-time, logs -> {log_path})")
    with open(log_path, "wb") as logf:
        # PostgreSQL
        pg_tar = os.path.join(build_dir, "pg.tar.bz2")
        _http_download(f"https://ftp.postgresql.org/pub/source/v{pg_version}/"
                       f"postgresql-{pg_version}.tar.bz2", pg_tar, "pg")
        with tarfile.open(pg_tar) as t:
            t.extractall(build_dir)
        pg_src = os.path.join(build_dir, f"postgresql-{pg_version}")
        print("  [pg] configure + make (this takes a few minutes)...")
        _run_logged(["./configure", f"--prefix={prefix}",
                     "--without-readline", "--without-icu", "--without-zlib"], pg_src, logf)
        _run_logged(["make", "-j", str(os.cpu_count() or 4)], pg_src, logf)
        _run_logged(["make", "install"], pg_src, logf)

        # pgvector, built against the PostgreSQL we just installed
        pv_tar = os.path.join(build_dir, "pgvector.tar.gz")
        _http_download(f"https://codeload.github.com/pgvector/pgvector/tar.gz/"
                       f"refs/tags/v{pgvector_version}", pv_tar, "pgvector")
        with tarfile.open(pv_tar) as t:
            t.extractall(build_dir)
        pv_src = os.path.join(build_dir, f"pgvector-{pgvector_version}")
        pg_config = os.path.join(bindir, "pg_config")
        print("  [pg] building pgvector...")
        _run_logged(["make", f"PG_CONFIG={pg_config}"], pv_src, logf)
        _run_logged(["make", "install", f"PG_CONFIG={pg_config}"], pv_src, logf)

    shutil.rmtree(build_dir, ignore_errors=True)
    print(f"  [pg] built {bindir}")
    return bindir


class ManagedPostgres:
    """initdb into a temp dir, run `postgres` on a port, tear it all down.

    `binary` may be the PostgreSQL bin directory or any binary inside it
    (initdb/pg_ctl/postgres); initdb and postgres are resolved from that dir.
    pgvector must already be installed into this PostgreSQL build.
    """

    def __init__(self, binary, port=0, datadir=None, keep=False, max_connections=100):
        bindir = binary if os.path.isdir(binary) else os.path.dirname(os.path.abspath(binary))
        self.initdb = os.path.join(bindir, "initdb")
        self.postgres = os.path.join(bindir, "postgres")
        self.port = port or _free_port()
        self._datadir = datadir
        self._created = datadir is None
        self.keep = keep
        self.max_connections = max_connections
        self.proc = None
        self.log = None
        self._stopped = False

    def start(self):
        self.datadir = self._datadir or tempfile.mkdtemp(prefix="annbench_pg_")
        print(f"  [pg] initdb -> {self.datadir}")
        subprocess.run([self.initdb, "-D", self.datadir, "-U", "postgres", "-A", "trust", "--no-sync"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.log = open(os.path.join(self.datadir, "postgres.log"), "wb")
        self.proc = subprocess.Popen(
            [self.postgres, "-D", self.datadir, "-p", str(self.port), "-k", self.datadir,
             "-c", "listen_addresses=127.0.0.1",
             "-c", f"max_connections={self.max_connections}"],
            stdout=self.log, stderr=self.log)
        _wait_for_tcp("127.0.0.1", self.port, 60, self.proc)
        dsn = f"postgresql://postgres@127.0.0.1:{self.port}/postgres"
        self._wait_accepts(dsn)
        print(f"  [pg] ready at {dsn}")
        return dsn

    def _wait_accepts(self, dsn, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                psycopg.connect(dsn).close()
                return
            except Exception:
                time.sleep(0.3)
        raise TimeoutError("postgres opened its port but never accepted connections")

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if self.log:
            self.log.close()
        if self._created and not self.keep:
            shutil.rmtree(self.datadir, ignore_errors=True)
        print("  [pg] stopped")


def _ydb_credentials(ydb):
    """Anonymous for a local cluster; honor env credentials only if one is set.

    (credentials_from_env_variables() otherwise falls back to the cloud metadata
    server, which hangs/errors on a local box.)"""
    for var in ("YDB_TOKEN", "YDB_ACCESS_TOKEN_CREDENTIALS",
                "YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS", "YDB_METADATA_CREDENTIALS",
                "YDB_ANONYMOUS_CREDENTIALS"):
        if os.environ.get(var):
            return ydb.credentials_from_env_variables()
    return ydb.AnonymousCredentials()


def download_ydbd(version, cache_dir):
    """Download a prebuilt `ydbd` binary for `version` (e.g. stable-26-2-1) and cache it.

    Binaries live in the public (anonymous) Yandex Cloud S3 bucket `ydb-builds`,
    same source the YDB compatibility tests use:
        https://storage.yandexcloud.net/ydb-builds/<version>/release/ydbd
    """
    url = f"https://storage.yandexcloud.net/ydb-builds/{version}/release/ydbd"
    dst = os.path.join(os.path.expanduser(cache_dir), version, "ydbd")
    if os.path.exists(dst) and os.path.getsize(dst) > 100 * 1024 * 1024:
        print(f"  [ydb] using cached binary {dst}")
        return dst
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    tmp = dst + ".part"
    print(f"  [ydb] downloading ydbd {version} (~2.6 GB) from {url}")
    last = [0.0]

    def progress(block, bsize, total):
        done = block * bsize
        now = time.time()
        if now - last[0] > 2 or done >= total:
            pct = (done / total * 100) if total > 0 else 0
            print(f"\r  [ydb]   {done / 1e9:.2f} GB ({pct:.0f}%)", end="", flush=True)
            last[0] = now

    urllib.request.urlretrieve(url, tmp, reporthook=progress)
    print()
    os.chmod(tmp, os.stat(tmp).st_mode | stat.S_IEXEC)
    os.replace(tmp, dst)
    print(f"  [ydb] saved {dst}")
    return dst


# Legacy static single-node config (in-memory SectorMap). This is the config the
# official local-cluster `start.sh ram` uses: it defines a domain storage pool
# ("ssd") and a channel profile, which is what lets user tables be created.
# {ic_port} is the storage node's interconnect port.
_YDB_RAM_CONFIG = """\
static_erasure: none
host_configs:
- drive:
  - path: SectorMap:1:64
    type: SSD
  host_config_id: 1
hosts:
- host: localhost
  host_config_id: 1
  port: {ic_port}
  walle_location: {{body: 1, data_center: '1', rack: '1'}}
domains_config:
  domain:
  - name: Root
    storage_pool_types:
    - kind: ssd
      pool_config:
        box_id: 1
        erasure_species: none
        kind: ssd
        pdisk_filter:
        - property:
          - type: SSD
        vdisk_kind: Default
  state_storage:
  - ring:
      node: [1]
      nto_select: 1
    ssid: 1
blob_storage_config:
  service_set:
    groups:
    - erasure_species: none
      rings:
      - fail_domains:
        - vdisk_locations:
          - node_id: 1
            path: SectorMap:1:64
            pdisk_category: SSD
channel_profile_config:
  profile:
  - channel:
    - {{erasure_species: none, pdisk_category: 1, storage_pool_kind: ssd}}
    - {{erasure_species: none, pdisk_category: 1, storage_pool_kind: ssd}}
    - {{erasure_species: none, pdisk_category: 1, storage_pool_kind: ssd}}
    profile_id: 0
table_service_config:
  sql_version: 1
grpc_config:
  host: 127.0.0.1
"""

_YDB_DEFAULT_DB = "/Root/test"


class ManagedYDB:
    """Run a single-node YDB and stop it after the test.

    Does NOT expect a running instance: it downloads a prebuilt `ydbd` for
    --ydb-version (cached) and brings up a local cluster the same way the
    official `start.sh ram` does — a storage node, blobstorage init, register
    database /Root/test, then a database (tenant) node. Database is /Root/test.

    Overrides:
      * --ydb-binary     use a local ydbd instead of downloading
      * --ydb-start-cmd  custom launch command (then we just wait on --ydb-grpc-port)
    """

    def __init__(self, version, binary=None, database=_YDB_DEFAULT_DB, cache_dir="~/.cache/ann_ydbd",
                 start_cmd=None, grpc_port=2136, workdir=None, keep=False, **_ignored):
        self.version = version
        self.binary = binary
        self.database = "/" + database.lstrip("/")
        self.cache_dir = cache_dir
        self.start_cmd = start_cmd
        self.grpc_port = grpc_port or _free_port()
        self._workdir = workdir
        self._created = workdir is None
        self.keep = keep
        self.procs = []   # [storage_node, db_node]
        self.logs = []
        self._stopped = False

    def _resolve_binary(self):
        if self.binary:
            print(f"  [ydb] using binary {self.binary}")
            return self.binary
        return download_ydbd(self.version, self.cache_dir)

    def _spawn(self, cmd, log_name):
        log = open(os.path.join(self.workdir, log_name), "wb")
        self.logs.append(log)
        proc = subprocess.Popen(cmd, stdout=log, stderr=log, cwd=self.workdir)
        self.procs.append(proc)
        return proc

    def _ydbd_admin(self, binary, *args, timeout=120):
        r = subprocess.run([binary, "-s", f"grpc://localhost:{self.grpc_port}", *args],
                           cwd=self.workdir, capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            raise RuntimeError(f"ydbd {' '.join(args[:3])} failed: {r.stdout}\n{r.stderr}")
        return r

    def start(self):
        binary = self._resolve_binary()
        self.workdir = self._workdir or tempfile.mkdtemp(prefix="annbench_ydb_")
        endpoint = f"grpc://localhost:{self.grpc_port}"

        # Custom-command override: just launch and wait for the gRPC port.
        if self.start_cmd:
            proc = self._spawn(shlex.split(self.start_cmd), "ydbd.log")
            _wait_for_tcp("localhost", self.grpc_port, 120, proc)
            self._wait_ready(endpoint, self.database)
            print(f"  [ydb] ready at {endpoint} (database {self.database})")
            return endpoint, self.database

        # Official local-cluster recipe (mirrors start.sh ram).
        self.database = _YDB_DEFAULT_DB
        cfg_dir = os.path.join(self.workdir, "config")
        os.makedirs(cfg_dir, exist_ok=True)
        cfg = os.path.join(cfg_dir, "ram.yaml")
        ic_port = _free_port()
        with open(cfg, "w") as f:
            f.write(_YDB_RAM_CONFIG.format(ic_port=ic_port))

        print(f"  [ydb] starting storage node (grpc {self.grpc_port})...")
        storage = self._spawn(
            [binary, "server", "--yaml-config", cfg, "--node", "1",
             "--grpc-port", str(self.grpc_port), "--ic-port", str(ic_port),
             "--mon-port", str(_free_port())], "storage.log")
        _wait_for_tcp("localhost", self.grpc_port, 120, storage)

        print("  [ydb] initializing storage...")
        self._ydbd_admin(binary, "admin", "blobstorage", "config", "init", "--yaml-file", cfg)
        print(f"  [ydb] registering database {self.database}...")
        self._ydbd_admin(binary, "admin", "database", self.database, "create", "ssd:1")

        print("  [ydb] starting database node...")
        self._spawn(
            [binary, "server", "--yaml-config", cfg, "--tenant", self.database,
             "--node-broker", f"localhost:{self.grpc_port}",
             "--grpc-port", str(_free_port()), "--ic-port", str(_free_port()),
             "--mon-port", str(_free_port())], "database.log")

        self._wait_ready(endpoint, self.database)
        print(f"  [ydb] ready at {endpoint} (database {self.database})")
        return endpoint, self.database

    def _wait_ready(self, endpoint, database, timeout=120):
        """Wait until the database accepts SDK connections."""
        time.sleep(25)  # SDK unavailable here; give the cluster time
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            for p in self.procs:
                if p.poll() is not None:
                    raise RuntimeError(f"a ydbd node exited early (code {p.returncode}); check logs")
            try:
                driver = ydb.Driver(ydb.DriverConfig(
                    endpoint, database, credentials=_ydb_credentials(ydb)))
                driver.wait(timeout=5)
                driver.stop()
                return
            except Exception as e:
                last = e
                time.sleep(2)
        raise RuntimeError(f"YDB not ready after {timeout}s: {last}")

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        for p in reversed(self.procs):   # database node first, then storage
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    p.kill()
        for log in self.logs:
            log.close()
        if self._created and not self.keep and getattr(self, "workdir", None):
            shutil.rmtree(self.workdir, ignore_errors=True)
        print("  [ydb] stopped")


# ===========================================================================
# Runner
# ===========================================================================
def run_backend(backend, data, args):
    """Build + sweep one backend on one already-loaded dataset. Returns rows."""
    print(f"\n{'='*70}\n{backend.name} :: {data.get('name', '')} "
          f"(metric={data['metric']}, n={data['n_base']}, nq={data['nq']}, dim={data['dim']})\n{'='*70}")
    build_time = backend.build(data, args.M, args.ef_construction, args.threads)
    print(f"  build time: {build_time:.2f}s")

    # Precompute query inputs outside the timed region.
    if backend.name == "postgres":
        q_input = query_literals(data, sparse_for_pg=True)
    else:
        q_input = queries_as_dense(data)

    ef_values = [int(x) for x in args.ef_search.split(",")]
    # Use filtered GT when the YDB backend built a filtered covering index.
    using_filter = getattr(backend, "_use_filter", False)
    gt_ids = data.get("filtered_gt_ids") if using_filter and "filtered_gt_ids" in data else data["gt_ids"]
    filter_tag = " (filtered)" if using_filter else ""
    rows = []
    for param in backend.search_params(ef_values):
        result_ids, qt = backend.search(q_input, args.k, param, args.threads)
        recall = compute_recall(gt_ids, result_ids)
        qps = data["nq"] / qt
        label = "-" if param is None else str(param)
        print(f"  ef_search={label:>5}: query={qt:.2f}s, QPS={qps:.1f}, "
              f"recall@{args.k}={recall:.4f}{filter_tag}")
        rows.append({"backend": backend.name, "dataset": data.get("name", ""),
                     "ef_search": label, "build_time_s": round(build_time, 2),
                     "qps": round(qps, 1), "recall_at_k": round(recall, 4),
                     "filtered": using_filter})
    return rows


def make_backends(args):
    backends = []
    if args.backend in ("postgres", "both"):
        backends.append(PostgresBackend(args.pg_dsn, table=args.table))
    if args.backend in ("ydb", "both"):
        backends.append(YDBBackend(args.ydb_endpoint, args.ydb_database, table=args.table,
                                   clusters=args.ydb_clusters, levels=args.ydb_levels))
    return backends


def main():
    p = argparse.ArgumentParser(description="Compare YDB vs PostgreSQL (pgvector) on ANN datasets")
    p.add_argument("--dataset", default="all", choices=["all"] + ALL_DATASETS)
    p.add_argument("--backend", default="both", choices=["postgres", "ydb", "both"])
    p.add_argument("--n_base", type=int, default=50000, help="Base vectors (0 = all). Default 50000")
    p.add_argument("--n_queries", type=int, default=1000, help="Queries (0 = all). Default 1000")
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--M", type=int, default=16, help="pgvector HNSW m")
    p.add_argument("--ef_construction", type=int, default=200)
    p.add_argument("--ef_search", default="50,100,200,400", help="pgvector hnsw.ef_search sweep")
    p.add_argument("--threads", type=int, default=os.cpu_count())
    p.add_argument("--table", default="ann_items")
    # PostgreSQL. By default PG is built (PG + pgvector) and launched; set
    # --pg-dsn to instead connect to an already-running instance.
    p.add_argument("--pg-dsn", default=os.environ.get("PG_DSN"),
                   help="connect to a running PostgreSQL at this DSN instead of launching one")
    # YDB. By default YDB is downloaded + launched (not assumed running). Set
    # --ydb-endpoint to instead connect to an already-running instance.
    p.add_argument("--ydb-endpoint", default=os.environ.get("YDB_ENDPOINT"),
                   help="connect to a running YDB at this endpoint instead of launching one")
    p.add_argument("--ydb-database", default=os.environ.get("YDB_DATABASE", "/Root/test"))
    p.add_argument("--ydb-clusters", type=int, default=64)
    p.add_argument("--ydb-levels", type=int, default=2)
    # Managed YDB: download a prebuilt ydbd (or use --ydb-binary) and launch it.
    p.add_argument("--ydb-version", default="stable-26-2-1",
                   help="prebuilt ydbd version to download (e.g. stable-26-1-1, prestable-26-3)")
    p.add_argument("--ydb-cache-dir", default="~/.cache/ann_ydbd",
                   help="where downloaded ydbd binaries are cached")
    p.add_argument("--ydb-binary", help="use this local ydbd instead of downloading")
    p.add_argument("--ydb-start-cmd", help="custom command to start a ready YDB on --ydb-grpc-port "
                                           "(fallback if the YDB harness is not importable)")
    p.add_argument("--ydb-grpc-port", type=int, default=2136, help="port for --ydb-start-cmd path")
    p.add_argument("--ydb-workdir", help="working dir for the --ydb-start-cmd path")
    # Managed PostgreSQL: build PG+pgvector from source (cached) or use a local build.
    p.add_argument("--pg-version", default="16.4",
                   help="PostgreSQL version to build from source (PG + pgvector)")
    p.add_argument("--pgvector-version", default="0.8.0", help="pgvector version to build")
    p.add_argument("--pg-cache-dir", default="~/.cache/ann_pg",
                   help="where the built PostgreSQL+pgvector prefix is cached")
    p.add_argument("--pg-binary", help="use this local PostgreSQL bin dir (or a binary in it) "
                                       "instead of building; pgvector must already be installed")
    p.add_argument("--pg-port", type=int, default=0, help="port for the managed PG (0 = auto)")
    p.add_argument("--pg-datadir", help="data dir for managed PG (default: temp, removed on exit)")
    p.add_argument("--keep-data", action="store_true", help="keep managed servers' data dirs")
    p.add_argument("--download", nargs="*", metavar="DATASET",
                   help="download dataset files and exit "
                        "(sparse, yfcc-10M, text2image-10M; omit names for all)")
    args = p.parse_args()

    if args.download is not None:
        targets = args.download if args.download else ALL_DATASETS
        for name in targets:
            print(f"\n=== Downloading {name} ===")
            download_dataset(name)
        sys.exit(0)

    # Start managed DB servers (from custom binaries) if requested; ensure they
    # are stopped on normal exit, errors, and Ctrl-C.
    managed = []

    def shutdown():
        for m in reversed(managed):
            try:
                m.stop()
            except Exception as e:
                print(f"!! error stopping {type(m).__name__}: {e}")

    atexit.register(shutdown)

    use_pg = args.backend in ("postgres", "both")
    use_ydb = args.backend in ("ydb", "both")

    all_rows = []
    try:
        if use_pg and not args.pg_dsn:
            # Default: build (or reuse a local) PG+pgvector and launch it.
            print("Starting managed PostgreSQL...")
            bindir = (args.pg_binary if args.pg_binary
                      else provision_postgres(args.pg_version, args.pgvector_version,
                                              args.pg_cache_dir))
            # Headroom over --threads: each search worker holds a connection,
            # plus superuser-reserved slots, autovacuum workers and our own
            # bookkeeping connection.
            mp = ManagedPostgres(bindir, args.pg_port, args.pg_datadir, args.keep_data,
                                 max_connections=max(args.threads + 16, 100))
            managed.append(mp)
            args.pg_dsn = mp.start()
        if use_ydb and not args.ydb_endpoint:
            # Default: download + launch YDB ourselves (do not assume one is running).
            print("Starting managed YDB...")
            my = ManagedYDB(args.ydb_version, args.ydb_binary, args.ydb_database, args.ydb_cache_dir,
                            args.ydb_start_cmd, args.ydb_grpc_port, args.ydb_workdir, args.keep_data)
            managed.append(my)
            args.ydb_endpoint, args.ydb_database = my.start()

        all_rows = run_all(args)
    except Exception as e:
        print(f"\n!! aborting: {e!r}")
        traceback.print_exc()
    finally:
        shutdown()

    if not all_rows:
        sys.exit(1)


def run_all(args):
    datasets = ALL_DATASETS if args.dataset == "all" else [args.dataset]
    backends = make_backends(args)

    all_rows = []
    for name in datasets:
        print(f"\n{'#'*70}\n# DATASET: {name}\n{'#'*70}")
        try:
            data = load_dataset(name, args.n_base, args.n_queries, args.k)
            data["name"] = name
        except Exception as e:
            print(f"!! Skipping dataset '{name}': {e}")
            continue
        if name == "yfcc-10M":
            _load_yfcc_filter(data, args.k)
        for backend in backends:
            try:
                all_rows += run_backend(backend, data, args)
            except Exception as e:
                print(f"!! {backend.name} failed on '{name}': {e!r}")
                traceback.print_exc()
            finally:
                backend.close()
        # Recreate backends so each dataset gets a fresh connection/state.
        backends = make_backends(args)

    # Summary
    print(f"\n{'='*78}\n{'YDB vs PostgreSQL — SUMMARY':^78}\n{'='*78}")
    print(f"{'Dataset':<16}{'Backend':<10}{'ef_search':>10}{'Build(s)':>10}{'QPS':>10}{'Recall@k':>10}")
    for r in all_rows:
        print(f"{r['dataset']:<16}{r['backend']:<10}{r['ef_search']:>10}{r['build_time_s']:>10.2f}"
              f"{r['qps']:>10.1f}{r['recall_at_k']:>10.4f}")
    print("=" * 78)

    with open("db_benchmark_results.json", "w") as f:
        json.dump({"params": vars(args), "results": all_rows}, f, indent=2)
    print("\nResults saved to db_benchmark_results.json")
    return all_rows


if __name__ == "__main__":
    main()

