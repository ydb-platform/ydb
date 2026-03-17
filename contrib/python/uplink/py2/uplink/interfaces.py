class AnnotationMeta(type):
    def __call__(cls, *args, **kwargs):
        if cls._can_be_static and cls._is_static_call(*args, **kwargs):
            self = super(AnnotationMeta, cls).__call__()
            self(args[0])
            return args[0]
        else:
            return super(AnnotationMeta, cls).__call__(*args, **kwargs)


class _Annotation(object):
    _can_be_static = False

    def modify_request_definition(self, request_definition_builder):
        pass

    @classmethod
    def _is_static_call(cls, *args, **kwargs):
        try:
            is_builder = isinstance(args[0], RequestDefinitionBuilder)
        except IndexError:
            return False
        else:
            return is_builder and not (kwargs or args[1:])


Annotation = AnnotationMeta("Annotation", (_Annotation,), {})


class AnnotationHandlerBuilder(object):
    __listener = None

    @property
    def listener(self):
        return self.__listener

    @listener.setter
    def listener(self, listener):
        self.__listener = listener

    def add_annotation(self, annotation, *args, **kwargs):
        if self.__listener is not None:
            self.__listener(annotation)

    def is_done(self):
        return True

    def build(self):
        raise NotImplementedError


class AnnotationHandler(object):
    @property
    def annotations(self):
        raise NotImplementedError


class UriDefinitionBuilder(object):
    @property
    def is_static(self):
        raise NotImplementedError

    @property
    def is_dynamic(self):
        raise NotImplementedError

    @is_dynamic.setter
    def is_dynamic(self, is_dynamic):
        raise NotImplementedError

    def add_variable(self, name):
        raise NotImplementedError

    @property
    def remaining_variables(self):
        raise NotImplementedError

    def build(self):
        raise NotImplementedError


class RequestDefinitionBuilder(object):
    @property
    def method(self):
        raise NotImplementedError

    @property
    def uri(self):
        raise NotImplementedError

    @property
    def argument_handler_builder(self):
        raise NotImplementedError

    @property
    def method_handler_builder(self):
        raise NotImplementedError

    def update_wrapper(self, wrapper):
        raise NotImplementedError

    def build(self):
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError


class RequestDefinition(object):
    def make_converter_registry(self, converters):
        raise NotImplementedError

    def define_request(self, request_builder, func_args, func_kwargs):
        raise NotImplementedError


class CallBuilder(object):
    @property
    def client(self):
        raise NotImplementedError

    @property
    def base_url(self):
        raise NotImplementedError

    @property
    def converters(self):
        raise NotImplementedError

    @property
    def hooks(self):
        raise NotImplementedError

    def add_hook(self, hook, *more_hooks):
        raise NotImplementedError

    @property
    def auth(self):
        raise NotImplementedError

    def build(self, definition):
        raise NotImplementedError


class Auth(object):
    def __call__(self, request_builder):
        raise NotImplementedError


class Consumer(object):
    @property
    def session(self):
        raise NotImplementedError
