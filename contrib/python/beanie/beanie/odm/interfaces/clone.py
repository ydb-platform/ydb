from copy import deepcopy


class CloneInterface:
    def clone(self):
        return deepcopy(self)
