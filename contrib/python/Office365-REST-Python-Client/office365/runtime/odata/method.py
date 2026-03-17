class ODataMethod(object):
    def __init__(
        self, name=None, is_beta=None, parameters=None, return_type_full_name=None
    ):
        self.Name = name
        self.Parameters = parameters
        self.IsBeta = is_beta
        self.ReturnTypeFullName = return_type_full_name
