class AutosarDatabaseSpecifics:
    """This class collects the AUTOSAR specific information of a system

    """
    def __init__(self,
                 arxml_version):
        self._arxml_version = arxml_version

    @property
    def arxml_version(self):
        """The used version of ARXML file format

        Note that due to technical reasons we always return version
        "4.0.0" for AUTOSAR 4.X.
        """
        return self._arxml_version
