"""Module to handle launching a URL no matter the environment."""

import os
import sys
import webbrowser


def launch(url):
  """Attempt to launch the given URL."""
  if os.environ.get('DISPLAY') is not None:
    webbrowser.open(url)
    return


if __name__ == '__main__':
  launch(sys.argv[1])
