class Spec(object):
    '''API specifications holder'''
    pass


class Info(object):
    '''The object provides metadata about the API.'''
    pass


class Component(object):
    '''
    Holds a set of reusable objects.

    All objects defined within the components object will have no effect on the API
    unless they are explicitly referenced from properties outside the components object.
    '''
