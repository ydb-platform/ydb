class User(object):
    def __init__(self, server, name, user_id, real_name, tz, email):
        self.tz = tz
        self.name = name
        self.real_name = real_name
        self.server = server
        self.id = user_id
        self.email = email

    def __eq__(self, compare_str):
        if compare_str in (self.id, self.name):
            return True
        else:
            return False

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        data = ""
        for key in list(self.__dict__.keys()):
            if key != "server":
                data += "{0} : {1}\n".format(key, str(self.__dict__[key])[:40])
        return data

    def __repr__(self):
        return self.__str__()
