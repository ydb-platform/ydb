@auto_pickle(False)
cdef class Json5EncoderException(Json5Exception):
    '''
    Base class of any exception thrown by the serializer.
    '''


@auto_pickle(False)
cdef class Json5UnstringifiableType(Json5EncoderException):
    '''
    The encoder was not able to stringify the input, or it was told not to by the supplied ``Options``.
    '''
    def __init__(self, message=None, unstringifiable=None):
        super().__init__(message, unstringifiable)

    @property
    def unstringifiable(self):
        '''
        The value that caused the problem.
        '''
        return self.args[1]
