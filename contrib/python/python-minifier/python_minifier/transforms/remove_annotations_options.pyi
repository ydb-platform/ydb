class RemoveAnnotationsOptions:
    remove_variable_annotations: bool
    remove_return_annotations: bool
    remove_argument_annotations: bool
    remove_class_attribute_annotations: bool

    def __init__(
        self,
        remove_variable_annotations: bool = ...,
        remove_return_annotations: bool = ...,
        remove_argument_annotations: bool = ...,
        remove_class_attribute_annotations: bool = ...
    ):
        ...

    def __nonzero__(self) -> bool: ...

    def __bool__(self) -> bool: ...
