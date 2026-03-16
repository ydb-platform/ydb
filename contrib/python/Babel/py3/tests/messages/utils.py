from __future__ import annotations

CUSTOM_EXTRACTOR_COOKIE = "custom extractor was here"


def custom_extractor(fileobj, keywords, comment_tags, options):
    if "treat" not in options:
        raise RuntimeError(f"The custom extractor refuses to run without a delicious treat; got {options!r}")
    return [(1, next(iter(keywords)), (CUSTOM_EXTRACTOR_COOKIE,), [])]


class Distribution:  # subset of distutils.dist.Distribution
    def __init__(self, attrs: dict) -> None:
        self.attrs = attrs

    def get_name(self) -> str:
        return self.attrs['name']

    def get_version(self) -> str:
        return self.attrs['version']

    @property
    def packages(self) -> list[str]:
        return self.attrs['packages']
