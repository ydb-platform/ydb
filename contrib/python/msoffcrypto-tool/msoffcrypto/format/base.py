import abc

# For 2 and 3 compatibility
# https://stackoverflow.com/questions/35673474/
ABC = abc.ABCMeta("ABC", (object,), {"__slots__": ()})


class BaseOfficeFile(ABC):
    def __init__(self):
        pass

    @abc.abstractmethod
    def load_key(self):
        pass

    @abc.abstractmethod
    def decrypt(self, outfile):
        pass

    @abc.abstractmethod
    def is_encrypted(self) -> bool:
        pass
