from ...core.options import Store
from ...core.overlay import NdOverlay, Overlay
from ...selection import OverlaySelectionDisplay


class PlotlyOverlaySelectionDisplay(OverlaySelectionDisplay):
    """Overlay selection display subclass for use with plotly backend

    """

    def _build_element_layer(self, element, layer_color, layer_alpha, **opts):
        backend_options = Store.options(backend='plotly')
        style_options = backend_options[(type(element).name,)]['style']
        allowed = style_options.allowed_keywords

        if 'selectedpoints' in allowed:
            shared_opts = dict(selectedpoints=False)
        else:
            shared_opts = {}

        merged_opts = dict(shared_opts)

        if 'opacity' in allowed:
            merged_opts['opacity'] = layer_alpha
        elif 'alpha' in allowed:
            merged_opts['alpha'] = layer_alpha

        if layer_color is not None:
            # set color
            merged_opts.update(self._get_color_kwarg(layer_color))
        else:
            # Keep current color (including color from cycle)
            for color_prop in self.color_props:
                current_color = element.opts.get(group="style")[0].get(color_prop, None)
                if current_color:
                    merged_opts.update({color_prop: current_color})

        for opt in ('cmap', 'colorbar'):
            if opt in opts and opt in allowed:
                merged_opts[opt] = opts[opt]

        filtered = {k: v for k, v in merged_opts.items() if k in allowed}
        return element.opts(clone=True, backend='plotly', **filtered)

    def _style_region_element(self, region_element, unselected_color):
        from ..util import linear_gradient
        backend_options = Store.options(backend="plotly")

        el2_name = None
        if isinstance(region_element, NdOverlay):
            el1_name = type(region_element.last).name
        elif isinstance(region_element, Overlay):
            el1_name = type(region_element.get(0)).name
            el2_name = type(region_element.get(1)).name
        else:
            el1_name = type(region_element).name
        style_options = backend_options[(el1_name,)]['style']
        allowed_keywords = style_options.allowed_keywords
        options = {}

        if el1_name != "Histogram":
            # Darken unselected color
            if unselected_color:
                region_color = linear_gradient(unselected_color, "#000000", 9)[3]
            if "Span" in el1_name:
                unselected_color = unselected_color or "#e6e9ec"
                region_color = linear_gradient(unselected_color, "#000000", 9)[1]
            if "line_width" in allowed_keywords:
                options["line_width"] = 1
        else:
            # Darken unselected color slightly
            unselected_color = unselected_color or "#e6e9ec"
            region_color = linear_gradient(unselected_color, "#000000", 9)[1]

        if "color" in allowed_keywords and unselected_color:
            options["color"] = region_color
        elif "line_color" in allowed_keywords and unselected_color:
            options["line_color"] = region_color

        if "selectedpoints" in allowed_keywords:
            options["selectedpoints"] = False

        region = region_element.opts(el1_name, clone=True, backend='plotly', **options)
        if el2_name and el2_name == 'Path':
            region = region.opts(el2_name, backend='plotly', line_color='black')
        return region
