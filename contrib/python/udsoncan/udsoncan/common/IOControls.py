__all__ = [
    'IOMasks',
    'IOValues'
]

from typing import Dict, Any, Tuple


class IOMasks:
    """
    Allow to specify a list of masks for a :ref:`InputOutputControlByIdentifier<InputOutputControlByIdentifier>` composite codec.

    Example : IOMasks(mask1,mask2, mask3=True, mask4=False)

    :param args: Masks to set to True
    :param kwargs: Masks and their values
    """
    maskdict: Dict[str, bool]

    def __init__(self, *args: str, **kwargs: bool):
        for k in kwargs:
            if not isinstance(kwargs[k], bool):
                raise ValueError('mask value must be a boolean value')

        for k in args:
            if not isinstance(k, str):
                raise ValueError('Mask name must be a valid string')

        self.maskdict = dict()
        for k in args:
            self.maskdict[k] = True

        for k in kwargs:
            if not isinstance(kwargs[k], bool):
                raise ValueError('Mask value must be True or False')
            self.maskdict[k] = kwargs[k]

    def get_dict(self):
        return self.maskdict


# Used for IO Control service. Allows comprehensive one-liner.
class IOValues:
    """
    This class saves a function argument so they can be passed to a callback function.

    :param args: Arguments
    :param kwargs: Named arguments
    """

    args: Tuple
    kwargs: Dict[str, Any]

    def __init__(self, *args: Any, **kwargs: Any):
        self.args = args
        self.kwargs = kwargs
