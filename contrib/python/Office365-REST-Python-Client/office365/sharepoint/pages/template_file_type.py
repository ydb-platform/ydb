class TemplateFileType:
    """Specifies the type of ghosted file template to use"""

    def __init__(self):
        pass

    StandardPage = 0
    """A standard page uses the default view template. Value is 0."""

    WikiPage = 1
    """A Wiki page uses the default Wiki template. Value is 1."""

    FormPage = 2
    """A form page uses the default form template. Value is 2."""

    ClientSidePage = 3
    """A client side page uses the default client side page template."""
