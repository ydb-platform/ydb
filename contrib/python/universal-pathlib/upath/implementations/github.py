"""
GitHub file system implementation
"""

import upath.core


class GitHubPath(upath.core.UPath):
    """
    GitHubPath supporting the fsspec.GitHubFileSystem
    """

    @property
    def path(self) -> str:
        pth = super().path
        if pth == ".":
            return ""
        return pth

    def iterdir(self):
        if self.is_file():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()
