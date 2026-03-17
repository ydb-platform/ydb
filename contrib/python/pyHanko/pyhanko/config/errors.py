class ConfigurationError(ValueError):
    """Signal configuration errors."""

    def __init__(self, msg: str):
        self.msg = msg
        super().__init__(msg)
