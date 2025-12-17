# This patch may be dropped after python 3.13 upver

find aiohttp -type f -exec sed --in-place 's|loop or asyncio.get_running_loop|loop or asyncio.get_event_loop|g' '{}' ';'

