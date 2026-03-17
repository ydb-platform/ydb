from ..phonemetadata import NumberFormat

from .alt_format_255 import PHONE_ALT_FORMAT_255
from .alt_format_27 import PHONE_ALT_FORMAT_27
from .alt_format_30 import PHONE_ALT_FORMAT_30
from .alt_format_31 import PHONE_ALT_FORMAT_31
from .alt_format_34 import PHONE_ALT_FORMAT_34
from .alt_format_350 import PHONE_ALT_FORMAT_350
from .alt_format_351 import PHONE_ALT_FORMAT_351
from .alt_format_352 import PHONE_ALT_FORMAT_352
from .alt_format_358 import PHONE_ALT_FORMAT_358
from .alt_format_359 import PHONE_ALT_FORMAT_359
from .alt_format_36 import PHONE_ALT_FORMAT_36
from .alt_format_372 import PHONE_ALT_FORMAT_372
from .alt_format_373 import PHONE_ALT_FORMAT_373
from .alt_format_380 import PHONE_ALT_FORMAT_380
from .alt_format_381 import PHONE_ALT_FORMAT_381
from .alt_format_385 import PHONE_ALT_FORMAT_385
from .alt_format_39 import PHONE_ALT_FORMAT_39
from .alt_format_43 import PHONE_ALT_FORMAT_43
from .alt_format_44 import PHONE_ALT_FORMAT_44
from .alt_format_49 import PHONE_ALT_FORMAT_49
from .alt_format_505 import PHONE_ALT_FORMAT_505
from .alt_format_506 import PHONE_ALT_FORMAT_506
from .alt_format_52 import PHONE_ALT_FORMAT_52
from .alt_format_54 import PHONE_ALT_FORMAT_54
from .alt_format_55 import PHONE_ALT_FORMAT_55
from .alt_format_58 import PHONE_ALT_FORMAT_58
from .alt_format_595 import PHONE_ALT_FORMAT_595
from .alt_format_61 import PHONE_ALT_FORMAT_61
from .alt_format_62 import PHONE_ALT_FORMAT_62
from .alt_format_64 import PHONE_ALT_FORMAT_64
from .alt_format_66 import PHONE_ALT_FORMAT_66
from .alt_format_675 import PHONE_ALT_FORMAT_675
from .alt_format_676 import PHONE_ALT_FORMAT_676
from .alt_format_679 import PHONE_ALT_FORMAT_679
from .alt_format_7 import PHONE_ALT_FORMAT_7
from .alt_format_81 import PHONE_ALT_FORMAT_81
from .alt_format_84 import PHONE_ALT_FORMAT_84
from .alt_format_855 import PHONE_ALT_FORMAT_855
from .alt_format_856 import PHONE_ALT_FORMAT_856
from .alt_format_90 import PHONE_ALT_FORMAT_90
from .alt_format_91 import PHONE_ALT_FORMAT_91
from .alt_format_94 import PHONE_ALT_FORMAT_94
from .alt_format_95 import PHONE_ALT_FORMAT_95
from .alt_format_971 import PHONE_ALT_FORMAT_971
from .alt_format_972 import PHONE_ALT_FORMAT_972
from .alt_format_995 import PHONE_ALT_FORMAT_995
_AVAILABLE_REGION_CODES: list[str]
_AVAILABLE_NONGEO_COUNTRY_CODES: list[int]

def _load_region(code: str | int) -> None: ...

_ALT_NUMBER_FORMATS: dict[int, list[NumberFormat]]
_COUNTRY_CODE_TO_REGION_CODE: dict[int, tuple[str, ...]]
