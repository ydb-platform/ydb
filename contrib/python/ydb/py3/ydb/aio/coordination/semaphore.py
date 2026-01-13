from ... import StatusCode, issues

from ..._grpc.grpcwrapper.ydb_coordination import (
    AcquireSemaphore,
    ReleaseSemaphore,
    UpdateSemaphore,
    DescribeSemaphore,
    CreateSemaphore,
)
from ..._grpc.grpcwrapper.ydb_coordination_public_types import (
    DescribeLockResult,
)


class CoordinationSemaphore:
    def __init__(self, session, name: str, limit: int):
        self._session = session
        self._name = name

        self._limit = limit
        self._timeout_millis = session._timeout_millis

    async def acquire(self, count: int = 1):
        await self._create_if_not_exists()
        resp = await self._try_acquire(count)

        if resp.status != StatusCode.SUCCESS:
            raise issues.Error(f"Failed to acquire lock {self._name}: {resp.status}")

        return self

    async def release(self):
        req = ReleaseSemaphore(
            req_id=await self._session.next_req_id(),
            name=self._name,
        )
        try:
            await self._session._reconnector.send_and_wait(req)
        except Exception:
            pass

    async def describe(self) -> DescribeLockResult:
        req = DescribeSemaphore(
            req_id=await self._session.next_req_id(),
            name=self._name,
            include_owners=True,
            include_waiters=True,
            watch_data=False,
            watch_owners=False,
        )
        resp = await self._session._reconnector.send_and_wait(req)
        return DescribeLockResult.from_proto(resp)

    async def update(self, new_data: bytes) -> None:
        req = UpdateSemaphore(
            req_id=await self._session.next_req_id(),
            name=self._name,
            data=new_data,
        )
        resp = await self._session._reconnector.send_and_wait(req)

        if resp.status != StatusCode.SUCCESS:
            raise issues.Error(f"Failed to update lock {self._name}: {resp.status}")

    async def close(self):
        await self.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()

    async def _try_acquire(self, count: int):
        req = AcquireSemaphore(
            req_id=await self._session.next_req_id(),
            name=self._name,
            count=count,
            ephemeral=False,
            timeout_millis=self._timeout_millis,
        )
        return await self._session._reconnector.send_and_wait(req)

    async def _create_if_not_exists(self):
        req = CreateSemaphore(
            req_id=await self._session.next_req_id(),
            name=self._name,
            limit=self._limit,
            data=b"",
        )
        resp = await self._session._reconnector.send_and_wait(req)

        if resp.status not in (
            StatusCode.SUCCESS,
            StatusCode.ALREADY_EXISTS,
        ):
            raise issues.Error(f"Failed to create lock {self._name}: {resp.status}")
