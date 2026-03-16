import pytest

from zeep import AsyncClient

import yatest.common as yc


@pytest.mark.requests
@pytest.mark.asyncio
async def test_context_manager():
    async with AsyncClient(yc.test_source_path("wsdl_files/soap.wsdl")) as async_client:
        assert async_client
