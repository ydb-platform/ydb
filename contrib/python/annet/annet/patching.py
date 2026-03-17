from annet import rulebook
from annet.annlib.patching import (  # pylint: disable=unused-import  # pylint: disable=unused-import
    AclError,
    AclNotExclusiveError,
    PatchTree,
    apply_acl,
    apply_diff_rb,
    make_diff,
    make_patch,
    make_pre,
    strip_unchanged,
)
from annet.annlib.patching import Orderer as BaseOrderer


class Orderer(BaseOrderer):
    @classmethod
    def from_hw(cls, hw):
        return cls(
            rulebook.get_rulebook(hw)["ordering"],
            hw.vendor,
        )
