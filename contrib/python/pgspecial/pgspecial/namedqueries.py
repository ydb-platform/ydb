# -*- coding: utf-8 -*-
class NamedQueries(object):

    section_name = "named queries"

    usage = """Named Queries are a way to save frequently used queries
with a short name. Think of them as favorites.
Examples:

    # Save a new named query.
    > \\ns simple select * from abc where a is not Null;

    # List all named queries.
    > \\n
    +--------+----------------------------------------+
    | Name   | Query                                  |
    |--------+----------------------------------------|
    | simple | SELECT * FROM xyzb where a is not null |
    +--------+----------------------------------------+

    # Run a named query.
    > \\n simple
    +-----+
    |   a |
    |-----|
    |  50 |
    +-----+

    # Delete a named query.
    > \\nd simple
    simple: Deleted
"""

    # Class-level variable, for convenience to use as a singleton.
    instance = None

    def __init__(self, config):
        self.config = config

    @classmethod
    def from_config(cls, config):
        return NamedQueries(config)

    def list(self):
        return self.config.get(self.section_name, [])

    def get(self, name):
        return self.config.get(self.section_name, {}).get(name, None)

    def save(self, name, query):
        if self.section_name not in self.config:
            self.config[self.section_name] = {}
        self.config[self.section_name][name] = query
        self.config.write()

    def delete(self, name):
        try:
            del self.config[self.section_name][name]
        except KeyError:
            return "%s: Not Found." % name
        self.config.write()
        return "%s: Deleted" % name
