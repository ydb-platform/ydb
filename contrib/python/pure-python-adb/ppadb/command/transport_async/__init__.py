import logging
import re
import time


class TransportAsync:
    async def transport(self, connection):
        cmd = "host:transport:{}".format(self.serial)
        await connection.send(cmd)

        return connection

    async def shell(self, cmd, timeout=None):
        conn = await self.create_connection(timeout=timeout)

        cmd = "shell:{}".format(cmd)
        await conn.send(cmd)

        result = await conn.read_all()
        await conn.close()
        return result.decode('utf-8')

    async def sync(self):
        conn = await self.create_connection()

        cmd = "sync:"
        await conn.send(cmd)

        return conn

    async def screencap(self):
        async with await self.create_connection() as conn:
            cmd = "shell:/system/bin/screencap -p"
            await conn.send(cmd)
            result = await conn.read_all()

        if result and len(result) > 5 and result[5] == 0x0d:
            return result.replace(b'\r\n', b'\n')
        else:
            return result
