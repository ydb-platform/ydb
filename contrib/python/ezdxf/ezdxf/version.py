# version scheme: (major, minor, micro, release_level)
#
# major:
#   0 .. not all planned features done
#   1 .. all features available
#   2 .. if significant API change (2, 3, ...)
#
# minor:
#   changes with new features or minor API changes
#
# micro:
#   changes with bug fixes, maybe also minor API changes
#
# release_state:
#   a .. alpha: adding new features - non-public development state
#   b .. beta: testing new features - public development state
#   rc .. release candidate: testing release - public testing
#   release: public release
#
# examples:
#   major pre-release alpha 2: VERSION = "0.9.0a2"; version = (0, 9, 0, 'a2')
#   major release candidate 0: VERSION = "0.9.0rc0"; version = (0, 9, 0, 'rc0')
#   major release: VERSION = "0.9.0"; version = (0, 9, 0, 'release')
#   1. bug fix release beta0: VERSION = "0.9.1b0"; version = (0, 9, 1, 'b0')
#   2. bug fix release: VERSION = "0.9.2"; version = (0, 9, 2, 'release')

version = (1, 4, 3, "release")
__version__ = "1.4.3"
