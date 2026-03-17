class EnvironmentVariable:
    """A CAN environment variable.

    """

    def __init__(self,
                 name,
                 env_type,
                 minimum,
                 maximum,
                 unit,
                 initial_value,
                 env_id,
                 access_type,
                 access_node,
                 comment,
                 dbc_specifics=None):
        self._name = name
        self._env_type = env_type
        self._minimum = minimum
        self._maximum = maximum
        self._unit = unit
        self._initial_value = initial_value
        self._env_id = env_id
        self._access_type = access_type
        self._access_node = access_node
        self._comment = comment
        self._dbc = dbc_specifics

    @property
    def name(self):
        """The environment variable name as a string.

        """

        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def env_type(self):
        """The environment variable type value.

        """

        return self._env_type

    @env_type.setter
    def env_type(self, value):
        self._env_type = value

    @property
    def minimum(self):
        """The minimum value of the environment variable.

        """

        return self._minimum

    @minimum.setter
    def minimum(self, value):
        self._minimum = value

    @property
    def maximum(self):
        """The maximum value of the environment variable.

        """

        return self._maximum

    @maximum.setter
    def maximum(self, value):
        self._maximum = value

    @property
    def unit(self):
        """ The units in which the environment variable is expressed as a string.

        """

        return self._unit

    @unit.setter
    def unit(self, value):
        self._unit = value

    @property
    def initial_value(self):
        """The initial value of the environment variable.

        """

        return self._initial_value

    @initial_value.setter
    def initial_value(self, value):
        self._initial_value = value

    @property
    def env_id(self):
        """The id value of the environment variable.

        """

        return self._env_id

    @env_id.setter
    def env_id(self, value):
        self._env_id = value

    @property
    def access_type(self):
        """The environment variable access type as a string.

        """

        return self._access_type

    @access_type.setter
    def access_type(self, value):
        self._access_type = value

    @property
    def access_node(self):
        """The environment variable access node as a string.

        """

        return self._access_node

    @access_node.setter
    def access_node(self, value):
        self._access_node = value

    @property
    def comment(self):
        """The environment variable comment, or ``None`` if unavailable.

        """

        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    @property
    def dbc(self):
        """An object containing dbc specific properties like e.g. attributes.

        """

        return self._dbc

    @dbc.setter
    def dbc(self, value):
        self._dbc = value

    def __repr__(self):
        return "environment_variable('{}', {}, {}, {}, '{}', {}, {}, '{}', '{}', {})".format(
            self._name,
            self._env_type,
            self._minimum,
            self._maximum,
            self._unit,
            self._initial_value,
            self._env_id,
            self._access_type,
            self._access_node,
            "'" + self._comment + "'" if self._comment is not None else None)
