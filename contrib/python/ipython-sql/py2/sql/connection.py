import os
import traceback

import sqlalchemy


class ConnectionError(Exception):
    pass


def rough_dict_get(dct, sought, default=None):
    """
    Like dct.get(sought), but any key containing sought will do.

    If there is a `@` in sought, seek each piece separately.
    This lets `me@server` match `me:***@myserver/db`
    """

    sought = sought.split("@")
    for (key, val) in dct.items():
        if not any(s.lower() not in key.lower() for s in sought):
            return val
    return default


class Connection(object):
    current = None
    connections = {}

    @classmethod
    def tell_format(cls):
        return """Connection info needed in SQLAlchemy format, example:
               postgresql://username:password@hostname/dbname
               or an existing connection: %s""" % str(
            cls.connections.keys()
        )

    def __init__(self, connect_str=None, connect_args={}, creator=None):
        try:
            if creator:
                engine = sqlalchemy.create_engine(
                    connect_str, connect_args=connect_args, creator=creator
                )
            else:
                engine = sqlalchemy.create_engine(
                    connect_str, connect_args=connect_args
                )
        except Exception as ex:  # TODO: bare except; but what's an ArgumentError?
            print(traceback.format_exc())
            print(self.tell_format())
            raise
        self.url = engine.url
        self.dialect = engine.url.get_dialect()
        self.name = self.assign_name(engine)
        self.internal_connection = engine.connect()
        self.connections[repr(self.url)] = self
        self.connect_args = connect_args
        Connection.current = self

    @classmethod
    def set(cls, descriptor, displaycon, connect_args={}, creator=None):
        """Sets the current database connection"""

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                existing = rough_dict_get(cls.connections, descriptor)
            # http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#custom-dbapi-connect-arguments
            cls.current = existing or Connection(descriptor, connect_args, creator)
        else:

            if cls.connections:
                if displaycon:
                    print(cls.connection_list())
            else:
                if os.getenv("DATABASE_URL"):
                    cls.current = Connection(
                        os.getenv("DATABASE_URL"), connect_args, creator
                    )
                else:
                    raise ConnectionError(
                        "Environment variable $DATABASE_URL not set, and no connect string given."
                    )
        return cls.current

    @classmethod
    def assign_name(cls, engine):
        name = "%s@%s" % (engine.url.username or "", engine.url.database)
        return name

    @classmethod
    def connection_list(cls):
        result = []
        for key in sorted(cls.connections):
            engine_url = cls.connections[
                key
            ].url  # type: sqlalchemy.engine.url.URL
            if cls.connections[key] == cls.current:
                template = " * {}"
            else:
                template = "   {}"
            result.append(template.format(engine_url.__repr__()))
        return "\n".join(result)

    @classmethod
    def close(cls, descriptor):
        if isinstance(descriptor, Connection):
            conn = descriptor
        else:
            conn = cls.connections.get(descriptor) or cls.connections.get(
                descriptor.lower()
            )
        if not conn:
            raise Exception(
                "Could not close connection because it was not found amongst these: %s"
                % str(cls.connections.keys())
            )
        cls.connections.pop(str(conn.url))
        conn.internal_connection.close()
