# Flask Humanize

Provides an interface between [Flask](http://flask.pocoo.org/) web framework
and [humanize](https://github.com/jmoiron/humanize) library.

## Features

- Add new filter `humanize` to jinja environment, which can be easily used for
  humanize different objects:

    + Integer numbers:

      ```jinja
      {{ 12345|humanize('intcomma') }} -> 12,345
      {{ 12345591313|humanize('intword') }} -> 12.3 billion
      {{ 5|humanize('aphumber') }} -> five
      ```

    + Floating point numbers:

      ```jinja
      {{ 0.3|humanize('fractional') }} -> 1/3
      {{ 1.5|humanize('fractional') }} -> 1 1/2
      ```

    + File sizes:

      ```jinja
      {{ 1000000|humanize('naturalsize') }} -> 1.0 MB
      {{ 1000000|humanize('naturalsize', binary=True) }} -> 976.6 KiB
      ```

    + Date & times:

      ```jinja
      {{ datetime.datetime.now()|humanize('naturalday') }} -> today
      {{ datetime.date(2014,4,21)|humanize('naturaldate') }} -> Apr 21 2014
      {{ (datetime.datetime.now() - datetime.timedelta(hours=1))|humanize() }} -> an hour ago
      ```

- Runtime i18n/l10n

    ```python
    from flask import Flask
    from flask_humanize import Humanize
    
    app = Flask(__name__)
    humanize = Humanize(app)
    
    @humanize.localeselector
    def get_locale():
        return 'ru_RU'
    ```

    ```jinja
    {{ datetime.datetime.now()|humanize }} -> сейчас
    ```

- In order to use UTC time instead of local time for humanize date and time
  methods use `HUMANIZE_USE_UTC` option, which is disabled by default:

    ```python
    HUMANIZE_USE_UTC = True
    ```

## Issues

Don't hesitate to open [GitHub Issues](https://github.com/vitalk/flask-humanize/issues) for any bug or suggestions.
