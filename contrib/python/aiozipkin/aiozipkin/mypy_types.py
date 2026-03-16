import asyncio
from typing import Mapping, Optional


Headers = Mapping[str, str]
OptStr = Optional[str]
OptTs = Optional[float]
OptInt = Optional[int]
OptBool = Optional[bool]
OptLoop = Optional[asyncio.AbstractEventLoop]
