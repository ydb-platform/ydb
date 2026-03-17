import pytest  # NOQA

# import srsly.ruamel_yaml
from .roundtrip import round_trip


class TestProgramConfig:
    def test_application_arguments(self):
        # application configur
        round_trip(
            """
        args:
          username: anthon
          passwd: secret
          fullname: Anthon van der Neut
          tmux:
            session-name: test
          loop:
            wait: 10
        """
        )

    def test_single(self):
        # application configuration
        round_trip(
            """
        # default arguments for the program
        args:  # needed to prevent comment wrapping
        # this should be your username
          username: anthon
          passwd: secret        # this is plaintext don't reuse \
# important/system passwords
          fullname: Anthon van der Neut
          tmux:
            session-name: test  # make sure this doesn't clash with
                                # other sessions
          loop:   # looping related defaults
            # experiment with the following
            wait: 10
          # no more argument info to pass
        """
        )

    def test_multi(self):
        # application configuration
        round_trip(
            """
        # default arguments for the program
        args:  # needed to prevent comment wrapping
        # this should be your username
          username: anthon
          passwd: secret        # this is plaintext don't reuse
                                # important/system passwords
          fullname: Anthon van der Neut
          tmux:
            session-name: test  # make sure this doesn't clash with
                                # other sessions
          loop:   # looping related defaults
            # experiment with the following
            wait: 10
          # no more argument info to pass
        """
        )
