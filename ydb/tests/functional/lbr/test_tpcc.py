import json
from contextlib import ExitStack
import os
from pathlib import Path

import yatest.common

import ydb.tests.olap.load.lib.tpcc as tpcc
from ydb.tests.functional.lbr.lib.lbr import LbrCollector, parse_numeric_parameter, validate_training_settings
from ydb.tests.functional.lbr.lib.conversion import LbrConversion
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
from ydb.tests.library.common.types import Erasure


class TestTpccLocalLbr(tpcc.TpccSuiteBase, FunctionalTestBase):
    STATIC_NODES = 3
    DYNAMIC_NODES = 3

    compaction_mode = tpcc.CompactionMode.LEGACY
    threads = 0
    warmup_s = 600

    @classmethod
    def setup_class(cls):
        binary = Path(yatest.common.get_param("tpc-lbr-ydbd", "")).expanduser()
        cli = Path(yatest.common.get_param("ydb-cli", "")).expanduser()
        for name, path in (("ydbd", binary), ("CLI", cli)):
            if not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK):
                raise ValueError(f"Pass an absolute path to the prebuilt baseline {name}: {path}")
        binary = binary.resolve()
        pdisk_directory = yatest.common.get_param("tpc-lbr-pdisk-directory", "")
        if pdisk_directory:
            directory = Path(pdisk_directory)
            if not directory.is_absolute() or not directory.is_dir() or not os.access(directory, os.W_OK):
                raise ValueError(f"PDisks directory must be an existing writable absolute path: {directory}")
        pdisk_size_gib = parse_numeric_parameter(
            "tpc-lbr-pdisk-size-gib", yatest.common.get_param("tpc-lbr-pdisk-size-gib", "256"), int
        )
        if pdisk_size_gib <= 0:
            raise ValueError("PDisks size must be positive")
        cls.warehouses = parse_numeric_parameter(
            "tpc-lbr-warehouses", yatest.common.get_param("tpc-lbr-warehouses", "1000"), int
        )
        cls.time_s = parse_numeric_parameter(
            "tpc-lbr-seconds", yatest.common.get_param("tpc-lbr-seconds", "1800"), float
        )
        cls.warmup_s = parse_numeric_parameter(
            "tpc-lbr-warmup-seconds", yatest.common.get_param("tpc-lbr-warmup-seconds", "600"), float
        )
        if cls.warehouses < 1 or cls.time_s <= 0 or cls.warmup_s < cls.warehouses // 10 + 1:
            raise ValueError("Use positive warehouses/time and warmup >= warehouses // 10 + 1 seconds")
        cls.phase_interval_s = parse_numeric_parameter(
            "tpc-lbr-phase-interval-seconds", yatest.common.get_param("tpc-lbr-phase-interval-seconds", "60"), float
        )
        cls.mmap_pages = parse_numeric_parameter(
            "tpc-lbr-mmap-pages", yatest.common.get_param("tpc-lbr-mmap-pages", "64"), int
        )
        cls.record_seconds = parse_numeric_parameter(
            "tpc-lbr-record-seconds", yatest.common.get_param("tpc-lbr-record-seconds", "30"), float
        )
        cls.snapshots = parse_numeric_parameter(
            "tpc-lbr-snapshots", yatest.common.get_param("tpc-lbr-snapshots", "3"), int
        )
        cls.perf = yatest.common.get_param("tpc-lbr-perf", "perf")
        cls.profile_dir = Path(yatest.common.test_output_path("lbr"))
        validate_training_settings(
            window_seconds=cls.time_s,
            record_seconds=cls.record_seconds,
            snapshots=cls.snapshots,
            start_delay_seconds=cls.warmup_s,
            interval_seconds=cls.phase_interval_s,
            mmap_pages=cls.mmap_pages,
        )
        cls.profile_dir.mkdir(parents=True, exist_ok=False)
        metadata = {
            "ydbd": str(binary),
            "cli": str(cli),
            "static_nodes": cls.STATIC_NODES,
            "dynamic_nodes": cls.DYNAMIC_NODES,
            "erasure": "mirror-3-dc",
            "in_memory_pdisks": False,
            "pdisk_directory": pdisk_directory,
            "pdisk_size_gib": pdisk_size_gib,
            "expected_slot_count": 4,
            "storage_pool_erasure": "mirror-3-dc",
            "warehouses": cls.warehouses,
            "seconds": cls.time_s,
            "warmup_seconds": cls.warmup_s,
            "phases": ["import", "compaction", "workload"],
            "phase_interval_seconds": cls.phase_interval_s,
            "mmap_pages": cls.mmap_pages,
            "tx_mode": str(cls.tx_mode),
            "compaction": str(cls.compaction_mode),
        }
        for name, path in (("ydbd", binary), ("cli", cli)):
            command = [str(path), "-V"] if name == "ydbd" else [str(path), "version", "--semantic"]
            result = yatest.common.execute(command, check_exit_code=False)
            metadata[f"{name}_version_exit_code"] = result.exit_code
            (cls.profile_dir.parent / f"{name}-version.txt").write_bytes(result.std_out + result.std_err)
        (cls.profile_dir.parent / "lbr-run.json").write_text(json.dumps(metadata, indent=2))
        cls.conversion = LbrConversion(
            binary,
            cls.profile_dir.parent / "profiles",
            llvm_dir=yatest.common.get_param("tpc-lbr-llvm-directory", ""),
            perf=cls.perf,
        )
        try:
            cls.setup_cluster_ext(
                static_nodes=cls.STATIC_NODES,
                dynamic_nodes=cls.DYNAMIC_NODES,
                erasure=Erasure.MIRROR_3_DC,
                use_in_memory_pdisks=False,
                binary_paths=[str(binary)],
                pdisk_store_path=pdisk_directory or None,
                pdisk_size=pdisk_size_gib * 1024**3,
                static_pdisk_config={"expected_slot_count": 4},
                storage_pool_erasure=Erasure.MIRROR_3_DC,
            )
            super().setup_class()
        except BaseException:
            try:
                if cls.cluster is not None:
                    cls.cluster.stop()
            finally:
                cls.conversion.abort()
            raise

    @classmethod
    def _server_pids(cls):
        pids = [node.pid for node in cls.cluster.nodes.values()]
        pids += [slot.pid for slot in cls.cluster.slots.values()]
        expected = cls.STATIC_NODES + cls.DYNAMIC_NODES
        if len(pids) != expected or len(set(pids)) != expected:
            raise RuntimeError(f"Expected {expected} distinct server PIDs, got {pids}")
        return pids

    @classmethod
    def _tpcc_profile_phase(cls, phase):
        return LbrCollector(
            cls._server_pids(),
            cls.profile_dir / phase,
            None,
            record_seconds=cls.record_seconds,
            perf=cls.perf,
            interval_seconds=cls.phase_interval_s,
            mmap_pages=cls.mmap_pages,
            phase=phase,
            conversion=cls.conversion,
        )

    def test(self):
        with self.conversion.finish_on_exit():
            self._collector = LbrCollector(
                self._server_pids(),
                self.profile_dir / "workload",
                self.time_s,
                self.record_seconds,
                self.snapshots,
                self.perf,
                start_delay_seconds=self.warmup_s,
                mmap_pages=self.mmap_pages,
                phase="workload",
                conversion=self.conversion,
            )
            with self._collector.ensure_started():
                with ExitStack() as self._lbr_stack:
                    super().test()

    def _signal_tpcc_load_started(self):
        super()._signal_tpcc_load_started()
        self._lbr_stack.enter_context(self._collector)

    @classmethod
    def do_teardown_class(cls):
        try:
            cls.cluster.stop()
        finally:
            cls.conversion.abort()
