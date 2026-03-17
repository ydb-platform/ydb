@auto_pickle(False)
cdef class Json5Exception(Exception):
    '''
    Base class of any exception thrown by PyJSON5.
    '''
    def __init__(self, message=None, *args):
        super().__init__(message, *args)

    @property
    def message(self):
        '''Human readable error description'''
        return self.args[0]
