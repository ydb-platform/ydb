# A signal group.


class SignalGroup:
    """A CAN signal group. Signal groups are used to define a group of
    signals within a message, e.g. to define that the signals of a
    group have to be updated in common.

    """

    def __init__(self,
                 name: str,
                 repetitions: int = 1,
                 signal_names: list[str] | None = None) -> None:
        self._name = name
        self._repetitions = repetitions
        self._signal_names = signal_names or []

    @property
    def name(self):
        """The signal group name as a string.

        """

        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def repetitions(self):
        """The signal group repetitions.

        """

        return self._repetitions

    @repetitions.setter
    def repetitions(self, value):
        self._repetitions = value

    @property
    def signal_names(self):
        """The signal names in the signal group

        """

        return self._signal_names

    @signal_names.setter
    def signal_names(self, value):
        self._signal_names = value

    def __repr__(self):
        return f"signal_group('{self._name}', {self._repetitions}, {self._signal_names})"
