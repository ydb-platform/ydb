class Sort:
    """
    Used with request options (RequestOptions) where you can filter and sort on
    the results returned from the server.

    Parameters
    ----------
    field : str
        Sets the field to sort on. The fields are defined in the RequestOption class.

    direction : str
        The direction to sort, either ascending (Asc) or descending (Desc). The
        options are defined in the RequestOptions.Direction class.
    """

    def __init__(self, field, direction):
        self.field = field
        self.direction = direction

    def __str__(self):
        return f"{self.field}:{self.direction}"
