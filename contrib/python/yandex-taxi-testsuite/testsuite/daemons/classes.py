import subprocess
import uuid


class DaemonInstance:
    process: subprocess.Popen | None

    def __init__(self, owner, process) -> None:
        self.id = uuid.uuid4().hex
        self._owner = owner
        self.process = process

    async def aclose(self) -> None:
        await self._owner.__aexit__(None, None, None)
