class ResourceReference(object):

    def __init__(self, id_, tag_name):
        self.id = id_
        self.tag_name = tag_name

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def tag_name(self):
        return self._tag_name

    @tag_name.setter
    def tag_name(self, value):
        self._tag_name = value
