# geojson-pydantic

<p align="center">
  <em> <a href="https://docs.pydantic.dev/latest/" target="_blank">Pydantic</a> models for GeoJSON.</em>
</p>
<p align="center">
  <a href="https://github.com/developmentseed/geojson-pydantic/actions?query=workflow%3ACI" target="_blank">
      <img src="https://github.com/developmentseed/geojson-pydantic/workflows/CI/badge.svg" alt="Test">
  </a>
  <a href="https://codecov.io/gh/developmentseed/geojson-pydantic" target="_blank">
      <img src="https://codecov.io/gh/developmentseed/geojson-pydantic/branch/main/graph/badge.svg" alt="Coverage">
  </a>
  <a href="https://pypi.org/project/geojson-pydantic" target="_blank">
      <img src="https://img.shields.io/pypi/v/geojson-pydantic?color=%2334D058&label=pypi%20package" alt="Package version">
  </a>
  <a href="https://pypistats.org/packages/geojson-pydantic" target="_blank">
      <img src="https://img.shields.io/pypi/dm/geojson-pydantic.svg" alt="Downloads">
  </a>
  <a href="https://github.com/developmentseed/geojson-pydantic/blob/main/LICENSE" target="_blank">
      <img src="https://img.shields.io/github/license/developmentseed/geojson-pydantic.svg" alt="License">
  </a>
  <a href="https://anaconda.org/conda-forge/geojson-pydantic" target="_blank">
      <img src="https://anaconda.org/conda-forge/geojson-pydantic/badges/version.svg" alt="Conda">
  </a>
</p>

---

**Documentation**: <a href="https://developmentseed.org/geojson-pydantic/" target="_blank">https://developmentseed.org/geojson-pydantic/</a>

**Source Code**: <a href="https://github.com/developmentseed/geojson-pydantic" target="_blank">https://github.com/developmentseed/geojson-pydantic</a>

---

## Description

`geojson_pydantic` provides a suite of Pydantic models matching the [GeoJSON specification rfc7946](https://datatracker.ietf.org/doc/html/rfc7946). Those models can be used for creating or validating geojson data.

## Install

```bash
$ python -m pip install -U pip
$ python -m pip install geojson-pydantic
```

Or install from source:

```bash
$ python -m pip install -U pip
$ python -m pip install git+https://github.com/developmentseed/geojson-pydantic.git
```

Install with conda from [`conda-forge`](https://anaconda.org/conda-forge/geojson-pydantic):

```bash
$ conda install -c conda-forge geojson-pydantic
```

## Contributing

See [CONTRIBUTING.md](https://github.com/developmentseed/geojson-pydantic/blob/main/CONTRIBUTING.md).

## Changes

See [CHANGES.md](https://github.com/developmentseed/geojson-pydantic/blob/main/CHANGELOG.md).

## Authors

Initial implementation by @geospatial-jeff; taken liberally from https://github.com/arturo-ai/stac-pydantic/

See [contributors](hhttps://github.com/developmentseed/geojson-pydantic/graphs/contributors) for a listing of individual contributors.

## License

See [LICENSE](https://github.com/developmentseed/geojson-pydantic/blob/main/LICENSE)
