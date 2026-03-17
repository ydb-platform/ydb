from django.http import HttpRequest


# In a separate file to avoid circular imports...
class Bundle(object):
    """
    A small container for instances and converted data for the
    ``dehydrate/hydrate`` cycle.

    Necessary because the ``dehydrate/hydrate`` cycle needs to access data at
    different points.
    """
    def __init__(self,
                 obj=None,
                 data=None,
                 request=None,
                 related_obj=None,
                 related_name=None,
                 objects_saved=None,
                 related_objects_to_save=None,
                 via_uri=False,
                 ):
        self.obj = obj
        self.data = data or {}
        self.request = request or HttpRequest()
        self.related_obj = related_obj
        self.related_name = related_name
        self.errors = {}
        self.objects_saved = objects_saved or set()
        self.related_objects_to_save = related_objects_to_save or {}
        self.via_uri = via_uri

    def __repr__(self):
        repr_string = "<Bundle for obj: '%r' and with data: '%r'>"
        return repr_string % (self.obj, self.data)
