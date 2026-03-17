class RemoveAnnotationsOptions(object):
    """
    Options for the RemoveAnnotations transform

    This can be passed to the minify function as the remove_annotations argument

    :param remove_variable_annotations: Remove variable annotations
    :type remove_variable_annotations: bool
    :param remove_return_annotations: Remove return annotations
    :type remove_return_annotations: bool
    :param remove_argument_annotations: Remove argument annotations
    :type remove_argument_annotations: bool
    :param remove_class_attribute_annotations: Remove class attribute annotations
    :type remove_class_attribute_annotations: bool
    """

    remove_variable_annotations = True
    remove_return_annotations = True
    remove_argument_annotations = True
    remove_class_attribute_annotations = False

    def __init__(self, remove_variable_annotations=True, remove_return_annotations=True, remove_argument_annotations=True, remove_class_attribute_annotations=False):
        self.remove_variable_annotations = remove_variable_annotations
        self.remove_return_annotations = remove_return_annotations
        self.remove_argument_annotations = remove_argument_annotations
        self.remove_class_attribute_annotations = remove_class_attribute_annotations

    def __repr__(self):
        return 'RemoveAnnotationsOptions(remove_variable_annotations=%r, remove_return_annotations=%r, remove_argument_annotations=%r, remove_class_attribute_annotations=%r)' % (
            self.remove_variable_annotations, self.remove_return_annotations, self.remove_argument_annotations, self.remove_class_attribute_annotations
        )

    def __nonzero__(self):
        return any((self.remove_variable_annotations, self.remove_return_annotations, self.remove_argument_annotations, self.remove_class_attribute_annotations))

    def __bool__(self):
        return self.__nonzero__()
