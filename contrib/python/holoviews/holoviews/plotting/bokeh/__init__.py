import sys

import numpy as np
from bokeh.palettes import all_palettes
from param import concrete_descendents

from ...core import (
    AdjointLayout,
    GridMatrix,
    GridSpace,
    Layout,
    NdLayout,
    NdOverlay,
    Overlay,
    Store,
    config,
)
from ...core.options import Cycle, Options, Palette
from ...element import (
    HSV,
    RGB,
    Area,
    Arrow,
    Bars,
    Bivariate,
    Bounds,
    Box,
    BoxWhisker,
    Chord,
    Contours,
    Curve,
    Dendrogram,
    Distribution,
    Div,
    EdgePaths,
    Ellipse,
    ErrorBars,
    Graph,
    HeatMap,
    HexTiles,
    Histogram,
    HLine,
    HLines,
    HSpan,
    HSpans,
    Image,
    ImageStack,
    ItemTable,
    Labels,
    Nodes,
    Path,
    Points,
    Polygons,
    QuadMesh,
    Raster,
    Rectangles,
    Sankey,
    Scatter,
    Segments,
    Slope,
    Spikes,
    Spline,
    Spread,
    Table,
    Text,
    Tiles,
    TriMesh,
    VectorField,
    Violin,
    VLine,
    VLines,
    VSpan,
    VSpans,
)
from ..plot import PlotSelector
from ..util import fire
from .annotation import (
    ArrowPlot,
    BoxAnnotationPlot,
    DivPlot,
    HLinesAnnotationPlot,
    HSpansAnnotationPlot,
    LabelsPlot,
    LineAnnotationPlot,
    SlopePlot,
    SplinePlot,
    TextPlot,
    VLinesAnnotationPlot,
    VSpansAnnotationPlot,
)
from .callbacks import Callback  # noqa (API import)
from .chart import (
    AreaPlot,
    BarPlot,
    CurvePlot,
    ErrorPlot,
    HistogramPlot,
    PointPlot,
    SideHistogramPlot,
    SideSpikesPlot,
    SpikesPlot,
    SpreadPlot,
    VectorFieldPlot,
)
from .element import ElementPlot, OverlayPlot
from .geometry import RectanglesPlot, SegmentPlot
from .graphs import ChordPlot, GraphPlot, NodePlot, TriMeshPlot
from .heatmap import HeatMapPlot, RadialHeatMapPlot
from .hex_tiles import HexTilesPlot
from .links import LinkCallback  # noqa (API import)
from .path import ContourPlot, DendrogramPlot, PathPlot, PolygonPlot
from .plot import AdjointLayoutPlot, GridPlot, LayoutPlot
from .raster import HSVPlot, ImageStackPlot, QuadMeshPlot, RasterPlot, RGBPlot
from .renderer import BokehRenderer
from .sankey import SankeyPlot
from .stats import BivariatePlot, BoxWhiskerPlot, DistributionPlot, ViolinPlot
from .tabular import TablePlot
from .tiles import TilePlot
from .util import BOKEH_VERSION  # noqa (API import)

Store.renderers['bokeh'] = BokehRenderer.instance()

if len(Store.renderers) == 1:
    Store.set_current_backend('bokeh')

associations = {Overlay: OverlayPlot,
                NdOverlay: OverlayPlot,
                GridSpace: GridPlot,
                GridMatrix: GridPlot,
                AdjointLayout: AdjointLayoutPlot,
                Layout: LayoutPlot,
                NdLayout: LayoutPlot,

                # Charts
                Curve: CurvePlot,
                Bars: BarPlot,
                Points: PointPlot,
                Scatter: PointPlot,
                ErrorBars: ErrorPlot,
                Spread: SpreadPlot,
                Spikes: SpikesPlot,
                Area: AreaPlot,
                VectorField: VectorFieldPlot,
                Histogram: HistogramPlot,

                # Rasters
                Image: RasterPlot,
                RGB: RGBPlot,
                HSV: HSVPlot,
                Raster: RasterPlot,
                HeatMap: PlotSelector(HeatMapPlot.is_radial,
                                      {True: RadialHeatMapPlot,
                                       False: HeatMapPlot},
                                      True),
                QuadMesh: QuadMeshPlot,
                ImageStack: ImageStackPlot,

                # Paths
                Path: PathPlot,
                Contours: ContourPlot,
                Box:      PathPlot,
                Bounds:   PathPlot,
                Ellipse:  PathPlot,
                Polygons: PolygonPlot,
                Dendrogram: DendrogramPlot,

                # Geometry
                Rectangles:    RectanglesPlot,
                Segments: SegmentPlot,

                # Annotations
                VLines: VLinesAnnotationPlot,
                HLines: HLinesAnnotationPlot,
                HSpans: HSpansAnnotationPlot,
                VSpans: VSpansAnnotationPlot,
                HLine: LineAnnotationPlot,
                VLine: LineAnnotationPlot,
                HSpan: BoxAnnotationPlot,
                VSpan: BoxAnnotationPlot,
                Slope: SlopePlot,
                Text: TextPlot,
                Labels: LabelsPlot,
                Spline: SplinePlot,
                Arrow: ArrowPlot,
                Div: DivPlot,
                Tiles: TilePlot,

                # Graph Elements
                Graph: GraphPlot,
                Chord: ChordPlot,
                Nodes: NodePlot,
                EdgePaths: PathPlot,
                TriMesh: TriMeshPlot,
                Sankey: SankeyPlot,

                # Tabular
                Table: TablePlot,
                ItemTable: TablePlot,

                # Statistics
                Distribution: DistributionPlot,
                Bivariate: BivariatePlot,
                BoxWhisker: BoxWhiskerPlot,
                Violin: ViolinPlot,
                HexTiles: HexTilesPlot}


Store.register(associations, 'bokeh')

if config.no_padding:
    for plot in concrete_descendents(ElementPlot).values():
        plot.padding = 0

# Raster types, Path types and VectorField should have frames
for framedcls in [VectorFieldPlot, ContourPlot, PathPlot, PolygonPlot,
                  RasterPlot, RGBPlot, HSVPlot, QuadMeshPlot, HeatMapPlot]:
    framedcls.show_frame = True

AdjointLayoutPlot.registry[Histogram] = SideHistogramPlot
AdjointLayoutPlot.registry[Spikes] = SideSpikesPlot

point_size = np.sqrt(6) # Matches matplotlib default

# Register bokeh.palettes with Palette and Cycle
def colormap_generator(palette):
    # Epsilon ensures float precision doesn't cause issues (#4911)
    epsilon = sys.float_info.epsilon*10
    return lambda value: palette[int(value*(len(palette)-1)+epsilon)]

Palette.colormaps.update({name: colormap_generator(p[max(p.keys())])
                          for name, p in all_palettes.items()})

Cycle.default_cycles.update({name: p[max(p.keys())] for name, p in all_palettes.items()
                             if max(p.keys()) < 256})

dflt_cmap = config.default_cmap
all_palettes['fire'] = {len(fire): fire}

options = Store.options(backend='bokeh')

# Charts
options.Curve = Options('style', color=Cycle(), line_width=2)
options.BoxWhisker = Options('style', box_fill_color=Cycle(), whisker_color='black',
                             box_line_color='black', outlier_color='black')
options.Scatter = Options('style', color=Cycle(), size=point_size, cmap=dflt_cmap)
options.Points = Options('style', color=Cycle(), size=point_size, cmap=dflt_cmap, radius_dimension="min")
options.Points = Options('plot', show_frame=True)
options.Histogram = Options('style', line_color='black', color=Cycle(), muted_alpha=0.2)
options.ErrorBars = Options('style', color='black')
options.Spread = Options('style', color=Cycle(), alpha=0.6, line_color='black', muted_alpha=0.2)
options.Bars = Options('style', color=Cycle(), line_color='black', bar_width=0.8, muted_alpha=0.2)

options.Spikes = Options('style', color='black', cmap=dflt_cmap, muted_alpha=0.2)
options.Area = Options('style', color=Cycle(), alpha=1, line_color='black', muted_alpha=0.2)
options.VectorField = Options('style', color='black', muted_alpha=0.2)

# Paths
options.Contours = Options('plot', show_legend=True)
options.Contours = Options('style', color=Cycle(), cmap=dflt_cmap)
options.Path = Options('style', color=Cycle(), cmap=dflt_cmap)
options.Box = Options('style', color='black')
options.Bounds = Options('style', color='black')
options.Ellipse = Options('style', color='black')
options.Polygons = Options('style', color=Cycle(), line_color='black',
                           cmap=dflt_cmap)
options.Rectangles = Options('style', cmap=dflt_cmap)
options.Segments = Options('style', cmap=dflt_cmap)

# Geometries
options.Rectangles = Options('style', line_color='black')

# Rasters
options.Image = Options('style', cmap=config.default_gridded_cmap)
options.Raster = Options('style', cmap=config.default_gridded_cmap)
options.QuadMesh = Options('style', cmap=config.default_gridded_cmap, line_alpha=0)
options.HeatMap = Options('style', cmap=config.default_heatmap_cmap, annular_line_alpha=0,
                          xmarks_line_color="#FFFFFF", xmarks_line_width=3,
                          ymarks_line_color="#FFFFFF", ymarks_line_width=3)

# Annotations
options.HLine = Options('style', color=Cycle(), line_width=3, alpha=1)
options.VLine = Options('style', color=Cycle(), line_width=3, alpha=1)
options.Slope = Options('style', color=Cycle(), line_width=3, alpha=1)
options.VSpan = Options('style', color=Cycle(), alpha=0.5)
options.HSpan = Options('style', color=Cycle(), alpha=0.5)
options.Arrow = Options('style', arrow_size=10)
options.Labels = Options('style', text_align='center', text_baseline='middle')
options.HLines = Options('style', color=Cycle(), line_width=3, alpha=1, muted_alpha=0.2)
options.VLines = Options('style', color=Cycle(), line_width=3, alpha=1, muted_alpha=0.2)
options.VSpans = Options('style', color=Cycle(), alpha=0.5, muted_alpha=0.2)
options.HSpans = Options('style', color=Cycle(), alpha=0.5, muted_alpha=0.2)
options.Labels = Options('style', text_color=Cycle(), text_align='center', text_baseline='middle')

# Graphs
options.Graph = Options(
    'style', node_size=15, node_color=Cycle(), node_line_color='black',
    node_nonselection_fill_color=Cycle(), node_hover_line_color='black',
    node_hover_fill_color='limegreen', node_nonselection_alpha=0.2,
    edge_nonselection_alpha=0.2, node_nonselection_line_color='black',
    edge_color='black', edge_line_width=2, edge_nonselection_line_color='black',
    edge_hover_line_color='limegreen'
)
options.TriMesh = Options(
    'style', node_size=5, node_line_color='black', node_color='white',
    edge_line_color='black', node_hover_fill_color='limegreen',
    edge_line_width=1, edge_hover_line_color='limegreen',
    edge_nonselection_alpha=0.2, edge_nonselection_line_color='black',
    node_nonselection_alpha=0.2, cmap=dflt_cmap
)
options.TriMesh = Options('plot', tools=[])
options.Chord = Options('style', node_size=15, node_color=Cycle(),
                        node_line_color='black',
                        node_selection_fill_color='limegreen',
                        node_nonselection_fill_color=Cycle(),
                        node_hover_line_color='black',
                        node_nonselection_line_color='black',
                        node_selection_line_color='black',
                        node_hover_fill_color='limegreen',
                        node_nonselection_alpha=0.2,
                        edge_nonselection_alpha=0.1,
                        edge_line_color='black', edge_line_width=1,
                        edge_nonselection_line_color='black',
                        edge_hover_line_color='limegreen',
                        edge_selection_line_color='limegreen',
                        label_text_font_size='8pt')
options.Chord = Options('plot', xaxis=None, yaxis=None)
options.Nodes = Options('style', line_color='black', color=Cycle(),
                        size=20, nonselection_fill_color=Cycle(),
                        selection_fill_color='limegreen',
                        hover_fill_color='indianred')
options.Nodes = Options('plot', tools=['hover', 'tap'])
options.EdgePaths = Options('style', color='black', nonselection_alpha=0.2,
                            line_width=2, selection_color='limegreen',
                            hover_line_color='indianred')
options.EdgePaths = Options('plot', tools=['hover', 'tap'])
options.Sankey = Options(
    'plot', xaxis=None, yaxis=None, inspection_policy='edges',
    selection_policy='nodes', show_frame=False, width=1000, height=600
)
options.Sankey = Options(
    'style', node_nonselection_alpha=0.2, node_size=10, edge_nonselection_alpha=0.2,
    edge_fill_alpha=0.6, label_text_font_size='8pt', cmap='Category20',
    node_line_color='black', node_selection_line_color='black', node_hover_alpha=1,
    edge_hover_alpha=0.9
)


# Define composite defaults
options.GridMatrix = Options('plot', shared_xaxis=True, shared_yaxis=True,
                             xaxis=None, yaxis=None)

options.Overlay = Options('style', click_policy='mute')
options.NdOverlay = Options('style', click_policy='mute')
options.Curve = Options('style', muted_alpha=0.2)
options.Path = Options('style', muted_alpha=0.2)
options.Dendrogram = Options('style', muted_alpha=0.2, line_color="black")
options.Dendrogram = Options('plot',
    xaxis=None,
    yaxis=None,
    show_grid=False,
    show_title=False,
    show_frame=False,
    border=0,
    default_tools=[],
)
options.Scatter = Options('style', muted_alpha=0.2)
options.Points = Options('style', muted_alpha=0.2)
options.Polygons = Options('style', muted_alpha=0.2)

# Statistics
options.Distribution = Options(
    'style', color=Cycle(), line_color='black', fill_alpha=0.5,
    muted_alpha=0.2
)
options.Violin = Options(
    'style', violin_fill_color=Cycle(), violin_line_color='black',
    violin_fill_alpha=0.5, stats_color='black', box_color='black',
    median_color='white', cmap='Category10'
)
options.HexTiles = Options('style', muted_alpha=0.2)
