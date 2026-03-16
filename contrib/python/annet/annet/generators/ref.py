from .partial import PartialGenerator


class RefGenerator(PartialGenerator):
    def __init__(self, storage, groups=None):
        super().__init__(storage)
        self.groups = groups

    def ref(self, device):
        if hasattr(self, f"ref_{device.hw.vendor}"):
            return getattr(self, f"ref_{device.hw.vendor}")(device)
        return ""

    def with_groups(self, groups):
        return type(self)(self.storage, groups)
