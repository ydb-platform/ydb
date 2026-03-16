from __future__ import annotations

from schwifty import checksum


@checksum.register("BE")
class DefaultAlgorithm(checksum.ISO7064_mod97_10):
    name = "default"

    def pre_process(self, components: list[str]) -> int:
        # By default the empty checksum "00" is appended to the components, which is not the case
        # for Belgium.
        return super().pre_process(components) // 100

    def post_process(self, r: int) -> int:
        return r if r != 0 else 97
