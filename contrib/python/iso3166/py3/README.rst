============================================================
 python-iso3166 - Standalone ISO 3166-1 country definitions
============================================================

:Authors:
        Mike Spindel
:Version: 2.1.1


ISO 3166-1 defines two-letter, three-letter, and three-digit country
codes.  `python-iso3166` is a self-contained module that converts
between these codes and the corresponding country name.


Installation
============

::

  $ pip install iso3166


Usage
=====

Country details
---------------

::

  >>> from iso3166 import countries
  >>>
  >>> countries.get('us')
  Country(name='United States', alpha2='US', alpha3='USA', numeric='840')
  >>> countries.get('ala')
  Country(name='Åland Islands', alpha2='AX', alpha3='ALA', numeric='248')
  >>> countries.get(8)
  Country(name='Albania', alpha2='AL', alpha3='ALB', numeric='008')


Country lists and indexes
-------------------------

::

  >>> from iso3166 import countries

  >>> for c in countries:
         print(c)
  >>> Country(name='Afghanistan', alpha2='AF', alpha3='AFG', numeric='004')
  Country(name='Åland Islands', alpha2='AX', alpha3='ALA', numeric='248')
  Country(name='Albania', alpha2='AL', alpha3='ALB', numeric='008')
  Country(name='Algeria', alpha2='DZ', alpha3='DZA', numeric='012')

::

  >>> import iso3166

  >>> iso3166.countries_by_name
  >>> {'AFGHANISTAN': Country(name='Afghanistan', alpha2='AF', alpha3='AFG', numeric='004'),
  'ALBANIA': Country(name='Albania', alpha2='AL', alpha3='ALB', numeric='008'),
  'ALGERIA': Country(name='Algeria', alpha2='DZ', alpha3='DZA', numeric='012'),
  ...

  >>> iso3166.countries_by_numeric
  >>> {'004': Country(name='Afghanistan', alpha2='AF', alpha3='AFG', numeric='004'),
  '008': Country(name='Albania', alpha2='AL', alpha3='ALB', numeric='008'),
  '010': Country(name='Antarctica', alpha2='AQ', alpha3='ATA', numeric='010'),
  ...

  >>> iso3166.countries_by_alpha2
  >>> {'AD': Country(name='Andorra', alpha2='AD', alpha3='AND', numeric='020'),
  'AE': Country(name='United Arab Emirates', alpha2='AE', alpha3='ARE', numeric='784'),
  'AF': Country(name='Afghanistan', alpha2='AF', alpha3='AFG', numeric='004'),
  ...

  >>> iso3166.countries_by_alpha3
  >>> {'ABW': Country(name='Aruba', alpha2='AW', alpha3='ABW', numeric='533'),
  'AFG': Country(name='Afghanistan', alpha2='AF', alpha3='AFG', numeric='004'),
  'AGO': Country(name='Angola', alpha2='AO', alpha3='AGO', numeric='024'),
  ...
