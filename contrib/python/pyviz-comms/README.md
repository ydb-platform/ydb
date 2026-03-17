# pyviz_comms

![Github Actions Status](https://github.com/holoviz/pyviz_comms/workflows/tests/badge.svg)

Offers a simple bidirectional communication architecture between Python and JavaScript,
with support for Jupyter comms in both the classic notebook and Jupyterlab.
Available for use by any [PyViz](https://pyviz.org) tool, but currently primarily used by
[HoloViz](https://holoviz.org) tools.

There are two installable components in this repository: a Python
component used by various HoloViz tools and an extension to enable
Jupyterlab support. For JupyterLab 3.0 and above the extension is automatically
bundled with the `pyviz_comms` Python package.

## Installing the Jupyterlab extension

Jupyterlab users will need to install the Jupyterlab pyviz extension. Starting with JupyterLab 3.0 and above the extension will be automatically installed when installing `pyviz_comms` with `pip` using:

```bash
pip install pyviz_comms
```

or using `conda` with:

```bash
conda install -c pyviz pyviz_comms
```

For older versions of JupyterLab you must separately install:

```bash
jupyter labextension install @pyviz/jupyterlab_pyviz
```

## Compatibility

The [Holoviz](https://github.com/holoviz/holoviz) libraries are generally version independent of
[JupyterLab](https://github.com/jupyterlab/jupyterlab) and the `jupyterlab_pyviz` extension
has been supported since holoviews 1.10.0 and the first release of `pyviz_comms`.

Our goal is that `jupyterlab_pyviz` minor releases (using the [SemVer](https://semver.org/) pattern) are
made to follow JupyterLab minor release bumps and micro releases are for new `jupyterlab_pyviz` features
or bug fix releases. We've been previously inconsistent with having the extension release minor version bumps
track that of JupyterLab, so users seeking to find extension releases that are compatible with their JupyterLab
installation may refer to the below table.

###### Compatible JupyterLab and jupyterlab_pyviz versions

| JupyterLab | jupyterlab_pyviz |
| ---------- | ---------------- |
| 0.33.x     | 0.6.0            |
| 0.34.x     | 0.6.1-0.6.2      |
| 0.35.x     | 0.6.3-0.7.2      |
| 1.0.x      | 0.8.0            |
| 2.0.x      | 0.9.0-1.0.3      |
| 3.x        | 2.0              |
| 4.x        | 3.0              |

## Developing the Jupyterlab extension

Note: You will need NodeJS to build the extension package.

The `jlpm` command is JupyterLab's pinned version of
[yarn](https://yarnpkg.com/) that is installed with JupyterLab. You may use
`yarn` or `npm` in lieu of `jlpm` below.

```bash
# Clone the repo to your local environment
# Change directory to the holoviz_jlab directory
# Install package in development mode
pip install -e .
# Link your development version of the extension with JupyterLab
jupyter labextension develop . --overwrite
# Rebuild extension Typescript source after making changes
jlpm run build
```

You can watch the source directory and run JupyterLab at the same time in different terminals to watch for changes in the extension's source and automatically rebuild the extension.

```bash
# Watch the source directory in one terminal, automatically rebuilding when needed
jlpm run watch
# Run JupyterLab in another terminal
jupyter lab
```

With the watch command running, every saved change will immediately be built locally and available in your running JupyterLab. Refresh JupyterLab to load the change in your browser (you may need to wait several seconds for the extension to be rebuilt).

By default, the `jlpm run build` command generates the source maps for this extension to make it easier to debug using the browser dev tools. To also generate source maps for the JupyterLab core extensions, you can run the following command:

```bash
jupyter lab build --minimize=False
```
