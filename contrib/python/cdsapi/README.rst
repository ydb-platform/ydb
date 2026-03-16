cdsapi
------

For a more detailed description on how to use the cdsapi, please visit: https://cds.climate.copernicus.eu/how-to-api


Install
-------

Install via `pip` with::

    $ pip install cdsapi


Configure
---------

Get your Personal Access Token from your profile on the CDS portal at the address: https://cds.climate.copernicus.eu/profile
and write it into the configuration file, so it looks like::

    $ cat ~/.cdsapirc
    url: https://cds.climate.copernicus.eu/api
    key: <PERSONAL-ACCESS-TOKEN>

Remember to agree to the Terms and Conditions of every dataset that you intend to download.


Test
----

Perform a small test retrieve of ERA5 data::

    $ python
    >>> import cdsapi
    >>> cds = cdsapi.Client()
    >>> cds.retrieve('reanalysis-era5-pressure-levels', {
               "variable": "temperature",
               "pressure_level": "1000",
               "product_type": "reanalysis",
               "date": "2017-12-01/2017-12-31",
               "time": "12:00",
               "format": "grib"
           }, 'download.grib')
    >>>


License
-------

Copyright 2018 - 2019 European Centre for Medium-Range Weather Forecasts (ECMWF)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

In applying this licence, ECMWF does not waive the privileges and immunities
granted to it by virtue of its status as an intergovernmental organisation nor
does it submit to any jurisdiction.
