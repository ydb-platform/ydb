==========
django-ace
==========


Usage
=====

::

    from django import forms
    from django_ace import AceWidget

    class EditorForm(forms.Form):
        text = forms.CharField(widget=AceWidget(width=None, height=None))

Syntax highlighting and static analysis can be enabled by specifying the
language::

    class EditorForm(forms.Form):
        text = forms.CharField(widget=AceWidget(mode='css'))

Themes are also supported::

    class EditorForm(forms.Form):
        text = forms.CharField(widget=AceWidget(mode='css', theme='twilight'))

All options, and their default values, are::

    class EditorForm(forms.Form):
        text = forms.CharField(widget=AceWidget(
            mode=None,  # try for example "python"
            theme=None,  # try for example "twilight"
            wordwrap=False,
            width="500px",  # Deprecated, pass None and use CSS
            height="300px",  # Deprecated, pass None and use CSS
            minlines=None,
            maxlines=None,
            showprintmargin=True,
            showinvisibles=False,
            usesofttabs=True,
            tabsize=None,
            fontsize=None,
            toolbar=True,
            readonly=False,
            showgutter=True,  # To hide/show line numbers
            behaviours=True,  # To disable auto-append of quote when quotes are entered
            useworker=True,
            extensions=None,
            basicautocompletion=False,
            liveautocompletion=False,
        ))


For a ``ModelForm``, for example in Django admin, it can be used like this::

    class PageForm(forms.ModelForm):
        class Meta:
            model = Page
            fields = ("title", "body")
            widgets = {
                "body": AceWidget(
                    mode="markdown", theme="twilight", width=None, height=None
                ),
            }

    class PageAdmin(admin.ModelAdmin):
        form = PageForm


Install
=======

1. Install using pip::

    pip install django_ace

2. Update ``INSTALLED_APPS``::

    INSTALLED_APPS = (
        # ...
        'django_ace',
    )


Example Project
===============

There's an example project included in the source, to try it do::

    cd example/
    python -m venv .venv
    source .venv/bin/activate
    pip install -e ..
    ./manage.py migrate
    ./manage.py runserver

Then browser to ``http://localhost:8000``.


Change log
==========

v1.43.5
-------

- Update ACE editor to version v143.5.
- Added a flag to activate vim keybindings.
- Added a flag to disable current line highlighing.


v1.43.3
-------

- Update ACE editor to version v1.43.3.


v1.42.0
-------

- Update ACE editor to version v1.42.0.


v1.39.1
-------

- Update ACE editor to version v1.39.1.

Add a `useStrictCSP` option.


v1.38.0
-------

- Update ACE editor to version v1.38.

The ``width`` and ``height`` arguments (which sets the HTML ``style``
attribute) are starting a slow change of their default
values. Starting from this version do not rely in their default
arguments, give them explicitly!

They are changing from ``width="500px", height="300px"`` (setting the
HTML ``style`` argument) to ``width=None, height=None`` (relying on
the CSS).

The default CSS uses ``width: 500px; height: 300px``, so changing from
no ``width`` and no ``height`` to ``width=None, height=None`` is an
easy correct move.

If you need custom size, prefer using ``width=None, height=None`` (the
future default values) and use CSS to customize the size, this permits
more secure CSP rules.


v1.37.5
-------

- Update ACE editor to version v1.37.5.
- Use minified and non-conflict ACE instead of basic.
- Expose two new options: enablebasicautocompletion and enableliveautocompletion.

v1.36.2
-------

- Update ACE editor to version v1.36.2.

v1.32.4
-------

- Expose useworker, contributed by @mounirmesselmeni.

v1.32.3
-------

- Update ACE editor to version v1.32.3.

v1.32.0
-------

- Update ACE editor to version v1.32.0.
- Expose extensions, contributed by @okaycj.

v1.31.1
-------

- Update ACE editor to version v1.31.1.

v1.26.0
-------

- Update ACE editor to version v1.26.0.

v1.24.1
-------

- Update ACE editor to version v1.24.1.

v1.23.4
-------

- Update ACE editor to version v1.23.4.

v1.22.1
-------

- Update ACE editor to version v1.22.1.

v1.19.0
-------

- Update ACE editor to version v1.19.0.

v1.15.4
-------

- Added CSS to work with new admin in Django 4.2. Now you can use `width="100%"` without breaking the layout.

v1.15.3
-------

- Update ACE editor to version v1.15.3.

v1.14.0
-------

- Update ACE editor to version v1.14.0.
- Follow ACE version numbers.

v1.0.13
-------

- Update ACE editor to version v1.11.2.


v1.0.12
-------

- Update ACE editor to version v1.5.0.

v1.0.11
-------

- Support Grappelli inlines.


v1.0.10
-------

- FIX JavaScript error when using ``JavaScriptCatalog``.


v1.0.9
------

- New widget option ``showgutters`` to hide line numbers.
- New widget option ``behaviours`` to avoid auto-insert of quotes.


v1.0.8
------

- New widget option ``readonly``.
- Update ACE editor to version v1.4.12.


v1.0.7
------

- New widget option ``toolbar``.
- Update ACE editor to version v1.4.8.


v1.0.6
------

- New widget option ``fontsize``.
- Update ACE editor to version v1.4.7.


v1.0.5
------

- New widget option ``tabsize``.
- Upgrade ACE editor to version v1.4.2.


v1.0.4
------

- Update Django compatibility to ``>1.11,<=2.1``
- New widget options ``minLines``, ``maxLines``, ``showinvisibles``, ``usesofttabs``.
- Upgrade ACE editor to version v1.4.0.
- Updated example for Django 1.11
- PEP8 improvements

v1.0.2
------

- Upgrade ACE editor to version 1.1.8
- Add support for showprintmargin

v1.0.1
------

- Add support for Django 1.7 by removing deprecated imports.

v1.0.0
------

- Initial release.
