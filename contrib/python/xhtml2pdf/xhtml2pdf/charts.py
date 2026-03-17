from reportlab.graphics.charts.barcharts import HorizontalBarChart, VerticalBarChart
from reportlab.graphics.charts.doughnut import Doughnut
from reportlab.graphics.charts.linecharts import HorizontalLineChart
from reportlab.graphics.charts.piecharts import LegendedPie, Pie
from reportlab.graphics.widgets.markers import makeMarker

from xhtml2pdf.util import getColor


def set_properties(obj, data, prop_map):
    for key, fnc in prop_map:
        if key in data:
            try:
                value = fnc(data[key])

                if value is not None:
                    obj.__setattr__(key, value)
            except Exception:
                continue


class Props:
    def __init__(self, instance) -> None:
        self.prop_map = [
            ("x", int),
            ("y", int),
            ("width", int),
            ("height", int),
            ("data", lambda x: x),
            ("labels", instance.assign_labels),
        ]
        self.prop_map_title = [("x", int), ("y", int), ("_text", str)]
        self.prop_map_legend = [
            ("x", int),
            ("y", int),
            ("deltax", int),
            ("alignment", str),
            ("boxAnchor", str),
            ("fontSize", int),
            ("strokeWidth", int),
            ("dy", int),
            ("dx", int),
            ("dxTextSpace", int),
            ("deltay", int),
            ("columnMaximum", int),
            ("variColumn", int),
            ("deltax", int),
            ("fontName", str),
            ("colorNamePairs", list),
        ]
        self.prop_map_legend1 = [("x", int), ("y", int)]
        self.prop_map_bars = [("strokeWidth", int)]
        self.prop_map_barLabels = [("nudge", int), ("fontSize", int), ("fontName", str)]
        self.prop_map_categoryAxis = [
            ("visibleTicks", int),
            ("strokeWidth", int),
            ("tickShift", int),
            ("labelAxisMode", str),
        ]
        self.prop_map_categoryAxis_labels = [
            ("angle", int),
            ("dy", int),
            ("fontSize", int),
            ("boxAnchor", str),
            ("fontName", str),
            ("textAnchor", str),
        ]
        self.prop_map_slices = [
            ("strokeWidth", int),
            ("labelRadius", float),
            ("poput", int),
            ("fontName", str),
            ("fontSize", int),
            ("strokeDashArray", str),
        ]

    @staticmethod
    def add_prop(prop_map, data):
        prop_map += data


class BaseChart:
    def set_legend(self, data, legend, props=None):
        if props is None:
            props = Props(self)
        set_properties(legend, data, props.prop_map_legend)
        return legend

    def load_data_legend(self, data, legend):
        legend.colorNamePairs = []
        color = self.get_colors()

        for x, obj in enumerate(data["data"]):
            if isinstance(obj, list):
                for y, value in enumerate(obj):
                    if color:
                        if data["type"] == "doughnut":
                            legend.colorNamePairs.append(
                                (color[x], (data["labels"][y], " ", str(value)))
                            )
                        else:
                            legend.colorNamePairs.append(
                                (color[y], (data["labels"][y], " ", str(value)))
                            )
            elif color:
                legend.colorNamePairs.append(
                    (color[x], (data["labels"][x], " ", str(obj)))
                )

    def set_title_properties(self, data, title, props=None):
        if props is None:
            props = Props(self)
        set_properties(title, data, props.prop_map_title)
        return title

    def set_properties(self, data, props=None):
        if props is None:
            props = Props(self)
        set_properties(self, data, props.prop_map)

    @staticmethod
    def get_colors():
        return []


class BaseBarChart(BaseChart):
    def __init__(self) -> None:
        super().__init__()

    def set_properties(self, data, props=None):
        props = Props(self)
        props.add_prop(props.prop_map, [("barWidth", str)])
        props.add_prop(props.prop_map, [("barSpacing", str)])
        props.add_prop(props.prop_map, [("barLabelFormat", str)])
        props.add_prop(props.prop_map, [("strokeColor", getColor)])
        props.add_prop(props.prop_map, [("groupSpacing", int)])
        super().set_properties(data, props=props)

        if "bars" in data:
            self.set_bars(data["bars"], props=props)

        if "barLabels" in data:
            self.set_barLabels(data["barLabels"], props=props)

        if "categoryAxis" in data:
            self.set_categoryAxis(data["categoryAxis"], props=props)

            if "labels" in data["categoryAxis"]:
                self.set_categoryAxis_labels(
                    data["categoryAxis"]["labels"], props=props
                )

    def assign_labels(self, labels):
        self.categoryAxis.categoryNames = labels

    def set_bars(self, data, props=None):
        if props is None:
            props = Props(self)
        props.add_prop(props.prop_map_bars, [("strokeColor", getColor)])
        set_properties(self.bars, data, props.prop_map_bars)

    def set_barLabels(self, data, props=None):
        if props is None:
            props = Props(self)
        set_properties(self.barLabels, data, props.prop_map_barLabels)

    def set_categoryAxis(self, data, props=None):
        if props is None:
            props = Props(self)
        props.add_prop(props.prop_map_categoryAxis, [("strokeColor", getColor)])
        set_properties(self.categoryAxis, data, props.prop_map_categoryAxis)

    def set_categoryAxis_labels(self, data, props=None):
        if props is None:
            props = Props(self)
        props.add_prop(props.prop_map_categoryAxis_labels, [("fillColor", getColor)])
        set_properties(
            self.categoryAxis.labels, data, props.prop_map_categoryAxis_labels
        )


class HorizontalBar(HorizontalBarChart, BaseBarChart):
    pass


class VerticalBar(VerticalBarChart, BaseBarChart):
    pass


class HorizontalLine(HorizontalLineChart, BaseChart):
    def __init__(self) -> None:
        super().__init__()

    def assign_labels(self, labels):
        self.categoryAxis.categoryNames = labels

    def set_properties(self, data, props=None):
        props = Props(self)
        props.add_prop(props.prop_map, [("fillColor", getColor)])
        props.add_prop(props.prop_map, [("lineLabelFormat", str)])
        props.add_prop(props.prop_map, [("strokeColor", int)])
        props.add_prop(props.prop_map, [("joinedLines", int)])
        props.add_prop(props.prop_map, [("marker", self.fill_marker)])
        super().set_properties(data, props=props)

    def fill_marker(self, fill_type):
        for x in range(len(self.data)):
            self.lines[x].symbol = makeMarker(fill_type)

    @staticmethod
    def get_colors():
        return []


class PieChart(Pie, BaseChart):
    def __init__(self) -> None:
        super().__init__()

    def set_properties(self, data, props=None):
        props = Props(self)
        props.add_prop(props.prop_map, [("sideLabels", int)])
        props.add_prop(props.prop_map, [("simpleLabels", int)])
        props.add_prop(props.prop_map, [("sideLabelsOffset", int)])
        props.add_prop(props.prop_map, [("startAngle", int)])
        props.add_prop(props.prop_map, [("orderMode", str)])
        props.add_prop(props.prop_map, [("direction", str)])
        super().set_properties(data, props=props)

        if "slices" in data:
            self.set_slices(data["slices"], props=props)

    def assign_labels(self, labels):
        self.labels = labels

    def set_slices(self, data, props=None):
        if props is None:
            props = Props(self)
        props.add_prop(props.prop_map_slices, [("strokeColor", getColor)])
        props.add_prop(props.prop_map_slices, [("fillColor", getColor)])
        set_properties(self.slices, data, props.prop_map_slices)

    def get_colors(self):
        colors_list = []
        for x, _obj in enumerate(self.data):
            colors_list.append(self.slices[x].fillColor)
        return colors_list


class LegendedPieChart(LegendedPie, BaseChart):
    def __init__(self) -> None:
        super().__init__()
        self.legend1.x = 350
        self.legend1.y = 150

    def set_properties(self, data, props=None):
        props = Props(self)
        props.add_prop(props.prop_map, [("legend_data", list)])
        super().set_properties(data, props=props)

        if "legend1" in data:
            self.set_legend1(self.legend1, data["legend1"], props=props)

    def set_legend1(self, obj, data, props=None):
        if props is None:
            props = Props(self)
        set_properties(obj, data, props.prop_map_legend1)

    def assign_labels(self, labels):
        self.legend_names = labels


class DoughnutChart(Doughnut, BaseChart):
    def __init__(self) -> None:
        super().__init__()

    def assign_labels(self, labels):
        self.labels = labels

    def get_colors(self):
        colors = []
        for x, _obj in enumerate(self.data):
            colors.append(self.slices[x].fillColor)
        return colors
