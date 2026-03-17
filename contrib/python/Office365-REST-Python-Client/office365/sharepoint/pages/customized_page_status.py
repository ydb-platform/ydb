class CustomizedPageStatus:
    """Specifies the customization (ghost) status of the SPFile."""

    None_ = 0
    """The page was never cached."""

    Uncustomized = 1
    """The page is cached and has not been customized"""

    Customized = 2
    """The page was cached but has been customized"""
