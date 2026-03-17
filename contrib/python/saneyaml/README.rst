========
saneyaml
========

This micro library is a PyYaml wrapper with sane behaviour to read and
write readable YAML safely, typically when used with configuration files.

With saneyaml you can dump readable and clean YAML and load safely any YAML
preserving ordering and avoiding surprises of type conversions by loading
everything except booleans as strings.

Optionally you can check for duplicated map keys when loading YAML.

Works with Python 3. Requires PyYAML 5.x or higher.

license: apache-2.0
homepage_url: https://github.com/aboutcode-org/saneyaml

Usage::

    pip install saneyaml

    >>> from  saneyaml import load
    >>> from  saneyaml import dump
    >>> a=load('''version: 3.0.0.dev6
    ...
    ... description: |
    ...     AboutCode Toolkit is a tool to process ABOUT files. An ABOUT file
    ...     provides a way to document a software component.
    ... ''')
    >>> a
    dict([
        (u'version', u'3.0.0.dev6'),
        (u'description', u'AboutCode Toolkit is a tool to process ABOUT files. '
        'An ABOUT file\nprovides a way to document a software component.\n')])

    >>> pprint(a.items())
    [(u'version', u'3.0.0.dev6'),
     (u'description',
      u'AboutCode Toolkit is a tool to process ABOUT files. An ABOUT file\nprovides a way to document a software component.\n')]
    >>> print(dump(a))
    version: 3.0.0.dev6
    description: |
      AboutCode Toolkit is a tool to process ABOUT files. An ABOUT file
      provides a way to document a software component.
