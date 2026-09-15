# This patch may be dropped after python 3.13 upver

if [ "$(uname)" = "Darwin" ]; then
    SED=gsed
else
    SED=sed
fi

find aiohttp -type f -exec ${SED} --in-place 's|loop or asyncio.get_running_loop|loop or asyncio.get_event_loop|g' '{}' ';'

