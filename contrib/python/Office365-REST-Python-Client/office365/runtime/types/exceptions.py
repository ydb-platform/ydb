from office365.runtime.client_object import ClientObject


class NotFoundException(Exception):
    def __init__(self, entity=None, query=None):
        # type: (ClientObject, str) -> None
        self.entity = entity
        self.query = query

    def __str__(self):
        if self.entity is None:
            message = "Entity not found"
        else:
            message = "{0} not found".format(self.entity.entity_type_name)

        if self.query is not None:
            message = "{0} for query: {1}".format(message, self.query)
        return message
