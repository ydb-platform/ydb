import pathlib
import typing

JsonAny = typing.Union[int, float, str, list, dict]
JsonAnyOptional = typing.Optional[JsonAny]

FuncRetVal = typing.TypeVar('FuncRetVal')
MaybeAsyncResult = typing.Union[FuncRetVal, typing.Awaitable[FuncRetVal]]

FixtureType = typing.TypeVar('FixtureType')
YieldFixture = typing.Generator[FixtureType, None, None]
AsyncYieldFixture = typing.AsyncGenerator[FixtureType, None]

PathOrStr = typing.Union[str, pathlib.Path]
