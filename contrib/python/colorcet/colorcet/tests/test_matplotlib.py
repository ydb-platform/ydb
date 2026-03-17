import pytest
import colorcet as cc
from packaging.version import Version

pytest.importorskip('matplotlib')

def test_matplotlib_colormaps_available():
    assert len(cc.cm.items()) == 420
    assert len(cc.cm_n.items()) == 68


@pytest.mark.mpl_image_compare
def test_matplotlib():
    import numpy as np
    import matplotlib.pyplot as plt
    xs, _ = np.meshgrid(np.linspace(0, 1, 80), np.linspace(0, 1, 10))
    fig = plt.imshow(xs, cmap=cc.cm.colorwheel).get_figure()
    return fig


@pytest.mark.mpl_image_compare
def test_matplotlib_glasbey():
    import numpy as np
    import matplotlib.pyplot as plt
    xs, _ = np.meshgrid(np.linspace(0, 1, 256), np.linspace(0, 1, 10))
    fig = plt.imshow(xs, cmap=cc.cm.glasbey).get_figure()
    return fig

@pytest.mark.mpl_image_compare
def test_matplotlib_default_colormap_plot_blues():
    hv = pytest.importorskip('holoviews')
    hv.extension('matplotlib')
    from colorcet.plotting import swatch
    fig = hv.render(swatch('blues'), backend='matplotlib')
    return fig


@pytest.mark.mpl_image_compare
def test_matplotlib_default_colormap_plot_kbc():
    hv = pytest.importorskip('holoviews')
    hv.extension('matplotlib')
    from colorcet.plotting import swatch
    fig = hv.render(swatch('kbc'), backend='matplotlib')
    return fig

@pytest.mark.parametrize('k,v', list(cc.cm.items()))
def test_get_cm(k, v):
    import matplotlib as mpl

    if Version(mpl.__version__) < Version("3.5"):
        import matplotlib.cm as mcm
        assert mcm.get_cmap('cet_' + k) is v
    else:
        from matplotlib import colormaps
        assert colormaps['cet_' + k] == v


def test_register_cmap():
    import matplotlib as mpl
    if Version(mpl.__version__) < Version("3.5"):
        return

    cmap0 = cc.ListedColormap([[0, 0, 0], [1, 1, 1]])
    cmap1 = cc.ListedColormap([[0, 0, 0], [1, 1, 1]])
    cmap2 = cc.ListedColormap([[1, 1, 1], [0, 0, 0]])

    name = "test_long_random_name_just_to_be_sure"
    cc.register_cmap(name, cmap0)

    # Same values as before should pass
    cc.register_cmap(name, cmap1)

    # Not same values should raise an Error
    msg = 'A colormap named "{}" is already registered'.format(name)
    with pytest.raises(ValueError, match=msg):
        cc.register_cmap(name, cmap2)
