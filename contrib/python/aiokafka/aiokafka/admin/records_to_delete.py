class RecordsToDelete:
    """A class for deleting records on existing topics.
    Arguments:
        before_offset (int):
            delete all the records before the given offset
    """

    def __init__(
        self,
        before_offset,
    ):
        self.before_offset = before_offset
