# import asyncio
# import os

# import aiomisc
import pytest

# class UnavailableDbServer(aiomisc.service.TCPServer):
#     async def handle_client(
#             self,
#             reader: asyncio.StreamReader,
#             writer: asyncio.StreamWriter
#     ):
#         while await reader.read(65534):
#             pass
#         writer.close()
#         await writer.wait_closed()


@pytest.fixture
def db_server_port(aiomisc_unused_port_factory) -> int:
    return aiomisc_unused_port_factory()


# @pytest.fixture
# def services(db_server_port, localhost):
#     return [UnavailableDbServer(port=db_server_port, address=localhost)]


@pytest.fixture
def pg_dsn() -> str:
    return "postgresql://test:test@master:5432/test"
