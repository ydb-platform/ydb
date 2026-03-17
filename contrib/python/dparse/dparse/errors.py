class UnknownDependencyFileError(Exception):
    """

    """
    def __init__(self, message="Unknown File type to parse"):
        self.message = message
        super().__init__(self.message)


class MalformedDependencyFileError(Exception):

    def __init__(self, message="The dependency file is malformed. {info}",
                 info=""):
        self.message = message.format(info=info)
        super().__init__(self.message)
