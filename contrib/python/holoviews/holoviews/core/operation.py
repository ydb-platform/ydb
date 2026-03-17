"""Operations manipulate Elements, HoloMaps and Layouts, typically for
the purposes of analysis or visualization.

"""
import param

from . import Dataset, util
from .dimension import ViewableElement
from .element import Element
from .layout import Layout
from .options import Store
from .overlay import NdOverlay, Overlay
from .spaces import Callable, HoloMap


class Operation(param.ParameterizedFunction):
    """An Operation process an Element or HoloMap at the level of
    individual elements or overlays. If a holomap is passed in as input,
    a processed holomap is returned as output where the individual
    elements have been transformed accordingly. An Operation may turn
    overlays in new elements or vice versa.

    An Operation can be set to be dynamic, which will return a
    DynamicMap with a callback that will apply the operation
    dynamically. An Operation may also supply a list of Stream classes
    on a streams parameter, which can allow dynamic control over the
    parameters on the operation.

    """

    group = param.String(default='Operation', doc="""
       The group string used to identify the output of the
       Operation. By default this should match the operation name.""")

    dynamic = param.Selector(default='default',
                                   objects=['default', True, False], doc="""
       Whether the operation should be applied dynamically when a
       specific frame is requested, specified as a Boolean. If set to
       'default' the mode will be determined based on the input type,
       i.e. if the data is a DynamicMap it will stay dynamic.""")

    input_ranges = param.ClassSelector(default={}, allow_None=True,
                                       class_=(dict, tuple), doc="""
       Ranges to be used for input normalization (if applicable) in a
       format appropriate for the Normalization.ranges parameter.

       By default, no normalization is applied. If key-wise
       normalization is required, a 2-tuple may be supplied where the
       first component is a Normalization.ranges list and the second
       component is Normalization.keys. """)

    link_inputs = param.Boolean(default=False, doc="""
       If the operation is dynamic, whether or not linked streams
       should be transferred from the operation inputs for backends
       that support linked streams.

       For example if an operation is applied to a DynamicMap with an
       RangeXY, this switch determines whether the corresponding
       visualization should update this stream with range changes
       originating from the newly generated axes.""")

    streams = param.ClassSelector(default=[], class_=(dict, list), doc="""
        List of streams that are applied if dynamic=True, allowing
        for dynamic interaction with the plot.""")

    # Hooks to allow external libraries to extend existing operations.
    # Preprocessor hooks should accept the operation and input element
    # and return a dictionary of data which will be made available to
    # the postprocessor hooks as kwargs in addition to the operation
    # and processed element
    _preprocess_hooks = []
    _postprocess_hooks = []
    _allow_extra_keywords=False

    # Whether to apply operation per element (will be enabled by default in 2.0)
    _per_element = False

    # Flag indicating whether to automatically propagate the .dataset property from
    # the input of the operation to the result
    _propagate_dataset = True

    # Options to transfer from the input element to the transformed element
    _transfer_options = []

    @classmethod
    def search(cls, element, pattern):
        """Helper method that returns a list of elements that match the
        given path pattern of form {type}.{group}.{label}.

        The input may be a Layout, an Overlay type or a single
        Element.

        """
        if isinstance(element, Layout):
            return [el for cell in element for el in cls.search(cell, pattern)]
        if isinstance(element, (NdOverlay, Overlay)):
            return [el for el in element if el.matches(pattern)]
        elif isinstance(element, Element):
            return [element] if element.matches(pattern) else []


    @classmethod
    def get_overlay_label(cls, overlay, default_label=''):
        """Returns a label if all the elements of an overlay agree on a
        consistent label, otherwise returns the default label.

        """
        if all(el.label==overlay.get(0).label for el in overlay):
            return overlay.get(0).label
        else:
            return default_label


    @classmethod
    def get_overlay_bounds(cls, overlay):
        """Returns the extents if all the elements of an overlay agree on
        a consistent extents, otherwise raises an exception.

        """
        if all(el.bounds==overlay.get(0).bounds for el in overlay):
            return overlay.get(0).bounds
        else:
            raise ValueError("Extents across the overlay are inconsistent")


    def _apply(self, element, key=None):
        """Applies the operation to the element, executing any pre- and
        post-processor hooks if defined.

        """
        kwargs = {}
        for hook in self._preprocess_hooks:
            kwargs.update(hook(self, element))

        element_pipeline = getattr(element, '_pipeline', None)

        if hasattr(element, '_in_method'):
            in_method = element._in_method
            if not in_method:
                element._in_method = True
        ret = self._process(element, key)
        if hasattr(element, '_in_method') and not in_method:
            element._in_method = in_method

        if self._transfer_options:
            for backend in Store.loaded_backends():
                Store.transfer_options(
                    element, ret, backend, self._transfer_options, level=1
                )

        for hook in self._postprocess_hooks:
            ret = hook(self, ret, **kwargs)

        if (self._propagate_dataset and isinstance(ret, Dataset)
            and isinstance(element, Dataset) and not in_method):
            ret._dataset = element.dataset.clone()
            ret._pipeline = element_pipeline.instance(
                operations=[*element_pipeline.operations, self.instance(**self.p)],
            )
            ret._transforms = element._transforms
        return ret


    def _process(self, element, key=None):
        """Process a single input element and outputs new single element
        or overlay. If a HoloMap is passed into an Operation, the
        individual components are processed sequentially with the
        corresponding key passed as the optional key argument.

        """
        return element


    def process_element(self, element, key, **params):
        """The process_element method allows a single element to be
        operated on given an externally supplied key.

        """
        if self._per_element and not isinstance(element, Element):
            return element.clone({k: self.process_element(el, key, **params)
                                  for k, el in element.items()})
        if hasattr(self, 'p'):
            if self._allow_extra_keywords:
                extras = self.p._extract_extra_keywords(params)
                self.p._extra_keywords.update(extras)
                params = {k: v for k, v in params.items() if k not in self.p._extra_keywords}
            self.p.update(params)
            self.p._check_params(params)
        else:
            self.p = param.ParamOverrides(self, params,
                                          allow_extra_keywords=self._allow_extra_keywords)
        return self._apply(element, key)


    def __call__(self, element, **kwargs):
        params = dict(kwargs)
        for k, v in kwargs.items():
            if util.is_param_method(v, has_deps=True):
                params[k] = v()
            elif isinstance(v, param.Parameter) and isinstance(v.owner, param.Parameterized):
                params[k] = getattr(v.owner, v.name)
        self.p = param.ParamOverrides(self, params,
                                      allow_extra_keywords=self._allow_extra_keywords)
        if not self.p.dynamic:
            kwargs['dynamic'] = False
            if isinstance(element, HoloMap):
                # Backwards compatibility for key argument
                return element.clone([(k, self._apply(el, key=k))
                                      for k, el in element.items()])
            elif ((self._per_element and isinstance(element, Element)) or
                  (not self._per_element and isinstance(element, ViewableElement))):
                return self._apply(element)
        elif 'streams' not in kwargs:
            kwargs['streams'] = self.p.streams
        kwargs['per_element'] = self._per_element
        kwargs['link_dataset'] = self._propagate_dataset
        kwargs['link_inputs'] = self.p.link_inputs
        return element.apply(self, **kwargs)



class OperationCallable(Callable):
    """OperationCallable allows wrapping an Operation and the objects it is
    processing to allow traversing the operations applied on a
    DynamicMap.

    """

    operation = param.ClassSelector(class_=Operation, doc="""
        The Operation being wrapped into an OperationCallable.""")

    def __init__(self, callable, **kwargs):
        if 'operation' not in kwargs:
            raise ValueError('An OperationCallable must have an operation specified')
        super().__init__(callable, **kwargs)
