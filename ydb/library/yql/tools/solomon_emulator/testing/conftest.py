try:
    from ydb.library.yql.tools.solomon_emulator.testing.solomon_runner import solomon_emulator
except ImportError:
    solomon_emulator = None

assert solomon_emulator is solomon_emulator
