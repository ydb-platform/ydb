@auto_pickle(False)
cdef class Json5DecoderException(Json5Exception):
    '''
    Base class of any exception thrown by the parser.
    '''
    def __init__(self, message=None, result=None, *args):
        super().__init__(message, result, *args)

    @property
    def result(self):
        '''Deserialized data up until now.'''
        return self.args[1]


@final
@auto_pickle(False)
cdef class Json5NestingTooDeep(Json5DecoderException):
    '''
    The maximum nesting level on the input data was exceeded.
    '''


@final
@auto_pickle(False)
cdef class Json5EOF(Json5DecoderException):
    '''
    The input ended prematurely.
    '''


@final
@auto_pickle(False)
cdef class Json5IllegalCharacter(Json5DecoderException):
    '''
    An unexpected character was encountered.
    '''
    def __init__(self, message=None, result=None, character=None, *args):
        super().__init__(message, result, character, *args)

    @property
    def character(self):
        '''
        Illegal character.
        '''
        return self.args[2]


@final
@auto_pickle(False)
cdef class Json5ExtraData(Json5DecoderException):
    '''
    The input contained extranous data.
    '''
    def __init__(self, message=None, result=None, character=None, *args):
        super().__init__(message, result, character, *args)

    @property
    def character(self):
        '''
        Extranous character.
        '''
        return self.args[2]


@final
@auto_pickle(False)
cdef class Json5IllegalType(Json5DecoderException):
    '''
    The user supplied callback function returned illegal data.
    '''
    def __init__(self, message=None, result=None, value=None, *args):
        super().__init__(message, result, value, *args)

    @property
    def value(self):
        '''
        Value that caused the problem.
        '''
        return self.args[2]


@final
@auto_pickle(False)
cdef class _DecoderException(Exception):
    cdef object cls
    cdef object msg
    cdef object extra
    cdef object result

    def __cinit__(self, cls, msg, extra, result):
        self.cls = cls
        self.msg = msg
        self.extra = extra
        self.result = result
