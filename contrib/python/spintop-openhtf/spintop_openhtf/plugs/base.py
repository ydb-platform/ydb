import logging

from spintop_openhtf import conf, plugs

DEFAULT_FORMAT = '%(asctime)-15s: %(message)s'

class UnboundPlug(object):
    """ A generic interface base class that allows intelligent creation of OpenHTF plugs
    without limiting its usage to OpenHTF.

    Attributes:
        logger:
            If inside an OpenHTF plug, this is the OpenHTF provided logger. If not, it is 
            a logger with the object ID as name.
    """
    def __init__(self):
        super().__init__()
        if not hasattr(self, 'logger'):
            # Check conditionnaly because openhtf BasePlug will raise an
            # exception if we define self.logger.
            self.logger = logging.getLogger(str(id(self)))

    def tearDown(self):
        """Tear down the plug instance. This is part of the OpenHTF Plug contract"""
        self.close()
        self.logger.close_log()

    def open(self, *args, **kwargs):
        """Abstract method: Open resources related to this interface."""
        raise NotImplementedError()

    def close(self):
        """Abstract method: Close resources related to this interface."""
        raise NotImplementedError()

    def log_to_stream(self, stream=None, **kwargs):
        """Create and add to :attr:`logger` a StreamHandler that streams to :obj:`stream`"""
        handler = logging.StreamHandler(stream)
        self._add_handler(handler, **kwargs)

    def log_to_filename(self, filename, **kwargs):
        """Create and add to :attr:`logger` a FileHandler that logs to :obj:`filename`."""
        handler = logging.FileHandler(filename)
        self._add_handler(handler, **kwargs)

    def _add_handler(self, handler, level=logging.DEBUG, format=DEFAULT_FORMAT):
        handler.setLevel(level)
        if format:
            handler.setFormatter(logging.Formatter(format))
        self.logger.addHandler(handler)

    def close_log(self):
        """Close all :attr:`logger` handlers."""
        for handler in self.logger.handlers:
            handler.close()

    @classmethod
    def as_plug(cls, name, **kwargs_values):
        """ Create a bound plug that will retrieve values from conf or the passed values here.

        Take SSHInterface for example. 
        
        .. highlight:: python
        .. code-block:: python

            from spintop_openhtf.plugs import from_conf
            from spintop_openhtf.plugs.ssh import SSHInterface

            MySSHInterface = SSHInterface.as_plug(
                'MySSHInterface', # The name of this plug as it will appear in logs
                addr=from_conf( # from_conf will retrieve the conf value named like this.
                    'my_ssh_addr',
                    description="The addr of my device."
                ), 
                username='x', # Always the same
                password='y'  # Always the same
            )
        """
        return plug_factory(cls, name, **kwargs_values)


def plug_factory(plug_cls, name, **kwargs_values):

    for key, value in kwargs_values.items():
        if isinstance(value, from_conf):
            value.declare()
            
    def resolve(value):
        if isinstance(value, from_conf):
            return value.resolve()
        else:
            return value


    class _AnonymousPlug(plugs.BasePlug, plug_cls):
        def __init__(self):
            kwargs = {key: resolve(value) for key, value in kwargs_values.items()}
            super().__init__(**kwargs)

    _AnonymousPlug.__name__ = name
    return _AnonymousPlug

class from_conf(object):
    def __init__(self, conf_name, description=None):
        self.conf_name = conf_name
        self.description = description
    
    def resolve(self):
        return conf[self.conf_name]

    def declare(self):
        conf.declare(self.conf_name, description=self.description)