import pickle
from functools import partial
from typing import Union, Callable, Optional, List, Dict

from .. import uwsgi
from ..typehints import Strint
from ..utils import decode, decode_deep, listify

__offloaded_functions: Dict[str, Callable] = {}


TypeMuleFarm = Union[Strint, 'Mule', 'Farm']


def _get_farms() -> List[str]:
    return decode_deep(listify(uwsgi.opt.get(b'farm', [])))


def _mule_messages_hook(message: bytes):
    # Processes mule messages, tries to decode it.
    try:
        print(Mule.get_current_id())
        loaded = pickle.loads(message)

    except pickle.UnpicklingError:
        return

    else:
        if not isinstance(loaded, tuple):
            return

        return __offloaded_functions[loaded[1]](*loaded[2], **loaded[3])


uwsgi.mule_msg_hook = _mule_messages_hook


def __offload(func_name: str, mule_or_farm: TypeMuleFarm, *args, **kwargs) -> bool:
    # Sends a message to a mule/farm, instructing it
    # to run a function using given arguments,
    target = Mule if isinstance(mule_or_farm, int) else Farm
    return target(mule_or_farm).send(pickle.dumps(
        (
            'ucfg_off',
            func_name,
            args,
            kwargs,
        )
    ))


def mule_offload(mule_or_farm: TypeMuleFarm = None) -> Callable:
    """Decorator. Use to offload function execution to a mule or a farm.

    :param mule_or_farm: If not set, offloads to a first mule.

    """
    if isinstance(mule_or_farm, Mule):
        target = mule_or_farm.id

    elif isinstance(mule_or_farm, Farm):
        target = mule_or_farm.name

    else:
        target = mule_or_farm

    target = target or 1

    def mule_offload_(func):
        func_name = func.__name__
        __offloaded_functions[func_name] = func
        return partial(__offload, func_name, target)
        
    return mule_offload_


class Mule:
    """Represents uWSGI Mule.

    .. note:: Register mules before using this. E.g.:
        ``section.workers.set_mules_params(mules=3)``

    """
    __slots__ = ['id']

    def __init__(self, id: int):
        """
        :param id: Mule ID. Enumeration starts with 1.

        """
        self.id = id

    def __str__(self):
        return str(self.id)

    def offload(self) -> Callable:
        """Decorator. Allows to offload function execution on this mule.

        .. code-block:: python

            first_mule = Mule(1)

            @first_mule.offload()
            def for_mule(*args, **kwargs):
                # This function will be offloaded to and handled by mule 1.
                ...

        """
        return mule_offload(self)

    @classmethod
    def get_current_id(cls) -> int:
        """Returns current mule ID. Returns 0 if not a mule."""
        return uwsgi.mule_id()

    @classmethod
    def get_current(cls) -> Optional['Mule']:
        """Returns current mule object or None if not a mule."""
        mule_id = cls.get_current_id()

        if not mule_id:
            return None

        return Mule(mule_id)

    @classmethod
    def get_message(
            cls,
            *,
            signals: bool = True,
            farms: bool = False,
            buffer_size: int = 65536,
            timeout: int = -1
    ) -> str:
        """Block until a mule message is received and return it.

        This can be called from multiple threads in the same programmed mule.

        :param signals: Whether to manage signals.

        :param farms: Whether to manage farms.

        :param buffer_size:

        :param timeout: Seconds.

        :raises ValueError: If not in a mule.

        """
        return decode(uwsgi.mule_get_msg(signals, farms, buffer_size, timeout))

    def send(self, message: Union[str, bytes]) -> bool:
        """Sends a message to a mule(s)/farm.

        :param message:

        :raises ValueError: If no mules, or mule ID or farm name is not recognized.

        """
        return uwsgi.mule_msg(message, self.id)


class Farm:
    """Represents uWSGI Mule Farm.

    .. note:: Register farms before using this. E.g.:
        ``section.workers.set_mules_params(farms=section.workers.mule_farm('myfarm', 2))``

    """
    __slots__ = ['name', 'mules']

    def __init__(self, name: str, *, mules: List[int] = None):
        """
        :param name: Mule farm name.
        :param mules: Attached mules.

        """
        self.name = name
        self.mules = tuple(Mule(mule_id) for mule_id in mules or [])

    def __str__(self):
        return f"{self.name}: {', '.join(map(str, self.mules))}"

    @classmethod
    def get_farms(cls) -> List['Farm']:
        """Returns a list of registered farm objects.

        .. code-block:: python

            farms = Farm.get_farms()
            first_farm = farms[0]
            first_farm_first_mule = first_farm.mules[0]

        """
        return [Farm._from_spec(farm_spec) for farm_spec in _get_farms()]

    @classmethod
    def _from_spec(cls, spec: str) -> 'Farm':
        name, _, mules = spec.partition(':')
        return Farm(name=name, mules=[int(mule_id) for mule_id in mules.split(',')])

    def offload(self) -> Callable:
        """Decorator. Allows to offload function execution on mules of this farm.

        .. code-block:: python

            first_mule = Farm('myfarm')

            @first_mule.offload()
            def for_mule(*args, **kwargs):
                # This function will be offloaded to farm `myfarm` and handled by any mule from that farm.
                ...

        """
        return mule_offload(self)

    @property
    def is_mine(self) -> bool:
        """Returns flag indicating whether the current mule belongs to this farm."""
        return uwsgi.in_farm(self.name)

    @classmethod
    def get_message(cls) -> str:
        """Reads a mule farm message.

         * http://uwsgi.readthedocs.io/en/latest/Embed.html

         :raises ValueError: If not in a mule

         """
        return decode(uwsgi.farm_get_msg())

    def send(self, message: Union[str, bytes]):
        """Sends a message to the given farm.

        :param message:

        """
        return uwsgi.farm_msg(self.name, message)
