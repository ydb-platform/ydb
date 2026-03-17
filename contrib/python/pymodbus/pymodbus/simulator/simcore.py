"""Simulator data model implementation."""
from __future__ import annotations

from .simdata import SimData, SimDevice


class SimCore:
    """Datastore for the simulator/server."""

    def __init__(self) -> None:
        """Build datastore."""
        self.devices: dict[int, SimDevice] = {}

    @classmethod
    def build_block(cls, _block: list[SimData]) -> tuple[int, int, int] | None:
        """Build registers for device."""
        return None

    @classmethod
    def build_config(cls, devices: list[SimDevice]) -> SimCore:
        """Build devices/registers ready for server/simulator."""
        core = SimCore()
        for dev in devices:
            if dev.id in core.devices:
                raise TypeError(f"device id {dev.id} defined multiple times.")
            block_coil = block_direct = block_holding = block_input = block_shared = None
            for cfg_block, _block in (
                (dev.block_coil, block_coil),
                (dev.block_direct, block_direct),
                (dev.block_holding, block_holding),
                (dev.block_input, block_input),
                (dev.block_shared, block_shared)
            ):
                if cfg_block:
                    cls.build_block(cfg_block)

            #core.devices[dev.id] = (
            #    block_coil,
            #    block_direct,
            #    block_holding,
            #    block_input,
            #    block_shared
            #)
        return core
