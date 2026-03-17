"""
An example demonstrating how to put together a cross-selector app based
on the Auto MPG dataset.
"""
import panel as pn
import panel.widgets as pnw
from bokeh.sampledata.autompg import autompg

import holoviews as hv

df = autompg.copy()

ORIGINS = ['North America', 'Europe', 'Asia']

# data cleanup
df.origin = [ORIGINS[x-1] for x in df.origin]

df['mfr'] = [x.split()[0] for x in df.name]
df.loc[df.mfr=='chevy', 'mfr'] = 'chevrolet'
df.loc[df.mfr=='chevroelt', 'mfr'] = 'chevrolet'
df.loc[df.mfr=='maxda', 'mfr'] = 'mazda'
df.loc[df.mfr=='mercedes-benz', 'mfr'] = 'mercedes'
df.loc[df.mfr=='toyouta', 'mfr'] = 'toyota'
df.loc[df.mfr=='vokswagen', 'mfr'] = 'volkswagen'
df.loc[df.mfr=='vw', 'mfr'] = 'volkswagen'
del df['name']

columns = sorted(df.columns)
discrete = [x for x in columns if df[x].dtype == object]
continuous = [x for x in columns if x not in discrete]
quantileable = [x for x in continuous if len(df[x].unique()) > 20]

x = pnw.Select(name='X-Axis', value='mpg', options=quantileable)
y = pnw.Select(name='Y-Axis', value='hp', options=quantileable)
size = pnw.Select(name='Size', value='None', options=['None', *quantileable])
color = pnw.Select(name='Color', value='None', options=['None', *quantileable])

@pn.depends(x.param.value, y.param.value, color.param.value, size.param.value)
def create_figure(x, y, color, size):
    opts = dict(cmap='rainbow', width=800, height=600, line_color='black')
    if color != 'None':
        opts['color'] = color
    if size != 'None':
        opts['size'] = hv.dim(size).norm()*20
    return hv.Points(df, [x, y], label=f"{x.title()} vs {y.title()}").opts(**opts)

widgets = pn.WidgetBox(x, y, color, size, width=200)

pn.Row(widgets, create_figure).servable('Cross-selector')
