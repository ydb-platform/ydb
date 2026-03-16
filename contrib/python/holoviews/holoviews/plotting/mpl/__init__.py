import os
from contextlib import suppress

from colorcet import kbc, register_cmap
from matplotlib import rc_params_from_file
from matplotlib.colors import LinearSegmentedColormap, ListedColormap
from param import concrete_descendents

from ...core import Collator, GridMatrix, Layout, config
from ...core.options import Cycle, Options, Palette
from ...core.overlay import NdOverlay, Overlay
from ...element import *
from ..plot import PlotSelector
from ..util import fire_colors
from .annotation import *
from .chart import *
from .chart3d import *
from .element import ElementPlot
from .geometry import *
from .graphs import *
from .heatmap import *
from .hex_tiles import *
from .path import *
from .plot import *
from .raster import *
from .renderer import MPLRenderer
from .sankey import *
from .stats import *
from .tabular import *
from .util import MPL_VERSION

with suppress(ImportError):
    from pandas.plotting import register_matplotlib_converters
    register_matplotlib_converters()


def set_style(key):
    """Select a style by name, e.g. set_style('default'). To revert to the
    previous style use the key 'unset' or False.

    """
    if key is None:
        return
    elif not key or key in ['unset', 'backup']:
        if 'backup' in styles:
            plt.rcParams.update(styles['backup'])
        else:
            raise Exception('No style backed up to restore')
    elif key not in styles:
        raise KeyError('%r not in available styles.')
    else:
        path = os.path.join(os.path.dirname(__file__), styles[key])
        new_style = rc_params_from_file(path, use_default_template=False)
        styles['backup'] = dict(plt.rcParams)

        plt.rcParams.update(new_style)


# Define matplotlib based style cycles and Palettes
def get_color_cycle():
    if MPL_VERSION >= (1, 5, 0):
        cyl = mpl.rcParams['axes.prop_cycle']
        # matplotlib 1.5 verifies that axes.prop_cycle *is* a cycler
        # but no guarantee that there's a `color` key.
        # so users could have a custom rcParams w/ no color...
        try:
            return [x['color'] for x in cyl]
        except KeyError:
            pass  # just return axes.color style below
    return mpl.rcParams['axes.color_cycle']


styles = {'default': './default.mplstyle',
          'default>1.5': './default1.5.mplstyle'}

# Define Palettes and cycles from matplotlib colormaps
Palette.colormaps.update({cm: plt.get_cmap(cm) for cm in plt.cm.datad
                          if not ('spectral' in cm or 'Vega' in cm)})
listed_cmaps = [cm for cm in Palette.colormaps.values() if isinstance(cm, ListedColormap)]
Cycle.default_cycles.update({cm.name: list(cm.colors) for cm in listed_cmaps})

style_aliases = {'edgecolor': ['ec', 'ecolor'], 'facecolor': ['fc'],
                 'linewidth': ['lw'], 'edgecolors': ['ec', 'edgecolor'],
                 'size': ['s'], 'color': ['c'], 'markeredgecolor': ['mec'],
                 'markeredgewidth': ['mew'], 'markerfacecolor': ['mfc'],
                 'markersize': ['ms']}

Store.renderers['matplotlib'] = MPLRenderer.instance()

if len(Store.renderers) == 1:
    Store.set_current_backend('matplotlib')

# Defines a wrapper around GridPlot and RasterGridPlot
# switching to RasterGridPlot if the plot only contains
# Raster Elements
BasicGridPlot = GridPlot
def grid_selector(grid):
    raster_fn = lambda x: True if isinstance(x, Raster) else False
    all_raster = all(grid.traverse(raster_fn, [Element]))
    return 'RasterGridPlot' if all_raster else 'GridPlot'

GridPlot = PlotSelector(grid_selector,
                        plot_classes=[('GridPlot', BasicGridPlot),
                                      ('RasterGridPlot', RasterGridPlot)])

# Register default Elements
Store.register({Curve: CurvePlot,
                Scatter: PointPlot,
                Bars: BarPlot,
                Histogram: HistogramPlot,
                Points: PointPlot,
                VectorField: VectorFieldPlot,
                ErrorBars: ErrorPlot,
                Spread: SpreadPlot,
                Spikes: SpikesPlot,
                BoxWhisker: BoxPlot,
                Area: AreaPlot,

                # General plots
                GridSpace: GridPlot,
                GridMatrix: GridPlot,
                NdLayout: LayoutPlot,
                Layout: LayoutPlot,
                AdjointLayout: AdjointLayoutPlot,

                # Element plots
                NdOverlay: OverlayPlot,
                Overlay: OverlayPlot,

                # Chart 3D
                Surface: SurfacePlot,
                TriSurface: TriSurfacePlot,
                Scatter3D: Scatter3DPlot,
                Path3D: Path3DPlot,

                # Tabular plots
                ItemTable: TablePlot,
                Table: TablePlot,
                Collator: TablePlot,

                # Raster plots
                QuadMesh: QuadMeshPlot,
                Raster: RasterPlot,
                HeatMap: PlotSelector(HeatMapPlot.is_radial,
                                      {True: RadialHeatMapPlot,
                                       False: HeatMapPlot},
                                      True),
                Image: RasterPlot,
                ImageStack: RGBPlot,
                RGB: RGBPlot,
                HSV: RGBPlot,

                # Graph Elements
                Graph: GraphPlot,
                TriMesh: TriMeshPlot,
                Chord: ChordPlot,
                Nodes: PointPlot,
                EdgePaths: PathPlot,
                Sankey: SankeyPlot,

                # Annotation plots
                VLines: VLinesAnnotationPlot,
                HLines: HLinesAnnotationPlot,
                HSpans: HSpansAnnotationPlot,
                VSpans: VSpansAnnotationPlot,
                VLine: VLinePlot,
                HLine: HLinePlot,
                VSpan: VSpanPlot,
                HSpan: HSpanPlot,
                Slope: SlopePlot,
                Arrow: ArrowPlot,
                Spline: SplinePlot,
                Text: TextPlot,
                Labels: LabelsPlot,

                # Path plots
                Contours: ContourPlot,
                Path:     PathPlot,
                Box:      PathPlot,
                Bounds:   PathPlot,
                Dendrogram: PathPlot,
                Ellipse:  PathPlot,
                Polygons: PolygonPlot,

                # Geometry plots
                Rectangles: RectanglesPlot,
                Segments: SegmentPlot,

                # Statistics elements
                Distribution: DistributionPlot,
                Bivariate: BivariatePlot,
                Violin: ViolinPlot,
                HexTiles: HexTilesPlot},
               'matplotlib', style_aliases=style_aliases)


MPLPlot.sideplots.update({Histogram: SideHistogramPlot,
                          Area: SideAreaPlot,
                          GridSpace: GridPlot,
                          Spikes: SideSpikesPlot,
                          BoxWhisker: SideBoxPlot})

if config.no_padding:
    for plot in concrete_descendents(ElementPlot).values():
        plot.padding = 0

# Raster types, Path types and VectorField should have frames
for framedcls in [VectorFieldPlot, ContourPlot, PathPlot, RasterPlot,
                  QuadMeshPlot, HeatMapPlot, PolygonPlot]:
    framedcls.show_frame = True

fire_cmap   = LinearSegmentedColormap.from_list("fire",   fire_colors, N=len(fire_colors))
fire_r_cmap = LinearSegmentedColormap.from_list("fire_r", list(reversed(fire_colors)),
                                                N=len(fire_colors))
register_cmap("fire", cmap=fire_cmap)
register_cmap("fire_r", cmap=fire_r_cmap)

register_cmap('kbc_r',
              cmap=LinearSegmentedColormap.from_list('kbc_r',
                                                     list(reversed(kbc)), N=len(kbc)))

options = Store.options(backend='matplotlib')
dflt_cmap = config.default_cmap

# Default option definitions
# Note: *No*short aliases here! e.g. use 'facecolor' instead of 'fc'

# Charts
options.Curve = Options('style', color=Cycle(), linewidth=2)
options.Scatter = Options('style', color=Cycle(), marker='o', cmap=dflt_cmap)
options.Points = Options('plot', show_frame=True)
options.ErrorBars = Options('style', edgecolor='k')
options.Spread = Options('style', facecolor=Cycle(), alpha=0.6, edgecolor='k', linewidth=0.5)
options.Bars = Options('style', edgecolor='k', color=Cycle())
options.Histogram = Options('style', edgecolor='k', facecolor=Cycle())
options.Points = Options('style', color=Cycle(), marker='o', cmap=dflt_cmap)
options.Scatter3D = Options('style', c=Cycle(), marker='o')
options.Scatter3D = Options('plot', fig_size=150)
options.Path3D = Options('plot', fig_size=150)
options.Surface = Options('plot', fig_size=150)
options.Surface = Options('style', cmap='fire')
options.Spikes = Options('style', color='black', cmap=dflt_cmap)
options.Area = Options('style', facecolor=Cycle(), edgecolor='black')
options.BoxWhisker = Options('style', boxprops=dict(color='k', linewidth=1.5),
                             whiskerprops=dict(color='k', linewidth=1.5))

# Geometries
options.Rectangles = Options('style', edgecolor='black')

# Rasters
options.Image = Options('style', cmap=config.default_gridded_cmap, interpolation='nearest')
options.Raster = Options('style', cmap=config.default_gridded_cmap, interpolation='nearest')
options.QuadMesh = Options('style', cmap=config.default_gridded_cmap)
options.HeatMap = Options('style', cmap=config.default_heatmap_cmap, edgecolors='white',
                          annular_edgecolors='white', annular_linewidth=0.5,
                          xmarks_edgecolor='white', xmarks_linewidth=3,
                          ymarks_edgecolor='white', ymarks_linewidth=3,
                          linewidths=0)
options.HeatMap = Options('plot', show_values=True)
options.RGB = Options('style', interpolation='nearest')
# Composites
options.Layout = Options('plot', sublabel_format='{Alpha}')
options.GridMatrix = Options('plot', fig_size=160, shared_xaxis=True,
                             shared_yaxis=True, xaxis=None, yaxis=None)

# Annotations
options.VLine = Options('style', color=Cycle())
options.HLine = Options('style', color=Cycle())
options.Slope = Options('style', color=Cycle())
options.VSpan = Options('style', alpha=0.5, facecolor=Cycle())
options.HSpan = Options('style', alpha=0.5, facecolor=Cycle())
options.Spline = Options('style', edgecolor=Cycle())
options.HLines = Options('style', color=Cycle())
options.VLines = Options('style', color=Cycle())
options.VSpans = Options('style', alpha=0.5, facecolor=Cycle())
options.HSpans = Options('style', alpha=0.5, facecolor=Cycle())
options.Labels = Options('style', color=Cycle())

options.Arrow = Options('style', color='k', linewidth=2, textsize=13)
# Paths
options.Contours = Options('style', color=Cycle(), cmap=dflt_cmap)
options.Contours = Options('plot', show_legend=True)
options.Path = Options('style', color=Cycle(), cmap=dflt_cmap)
options.Dendrogram = Options('style', color="black")
options.Dendrogram = Options('plot', xaxis=None, yaxis=None)
options.Polygons = Options('style', facecolor=Cycle(), edgecolor='black',
                           cmap=dflt_cmap)
options.Rectangles = Options('style', cmap=dflt_cmap)
options.Segments = Options('style', cmap=dflt_cmap)
options.Box = Options('style', color='black')
options.Bounds = Options('style', color='black')
options.Ellipse = Options('style', color='black')

# Interface
options.TimeSeries = Options('style', color=Cycle())

# Graphs
options.Graph = Options('style', node_edgecolors='black', node_facecolors=Cycle(),
                        edge_color='black', node_size=15)
options.TriMesh = Options('style', node_edgecolors='black', node_facecolors='white',
                          edge_color='black', node_size=5, edge_linewidth=1, cmap=dflt_cmap)
options.Chord = Options('style', node_edgecolors='black', node_facecolors=Cycle(),
                        edge_color='black', node_size=10, edge_linewidth=0.5)
options.Chord = Options('plot', xaxis=None, yaxis=None)
options.Nodes = Options('style', edgecolors='black', facecolors=Cycle(),
                        marker='o', s=20**2)
options.EdgePaths = Options('style', color='black')
options.Sankey = Options('plot', xaxis=None, yaxis=None, fig_size=400,
                         aspect=1.6, show_frame=False)
options.Sankey = Options('style', edge_color='grey', node_edgecolors='black',
                         edge_alpha=0.6, node_size=6)

# Statistics
options.Distribution = Options('style', facecolor=Cycle(), edgecolor='black',
                               alpha=0.5)
options.Distribution = Options('plot', show_legend=True)
options.Violin = Options('style', facecolors=Cycle(), showextrema=False, alpha=0.7)
