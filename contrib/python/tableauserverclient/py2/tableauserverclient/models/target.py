"""Target class meant to abstract mappings to other objects"""


class Target():
    def __init__(self, id_, target_type):
        self.id = id_
        self.type = target_type

    def __repr__(self):
        return "<Target#{id}, {type}>".format(**self.__dict__)
