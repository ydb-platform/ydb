from .roundtrip import YAML
import pytest  # NOQA


def test_example_2_1():
    yaml = YAML()
    yaml.round_trip(
        """
    - Mark McGwire
    - Sammy Sosa
    - Ken Griffey
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_2():
    yaml = YAML()
    yaml.mapping_value_align = True
    yaml.round_trip(
        """
    hr:  65    # Home runs
    avg: 0.278 # Batting average
    rbi: 147   # Runs Batted In
    """
    )


def test_example_2_3():
    yaml = YAML()
    yaml.indent(sequence=4, offset=2)
    yaml.round_trip(
        """
    american:
      - Boston Red Sox
      - Detroit Tigers
      - New York Yankees
    national:
      - New York Mets
      - Chicago Cubs
      - Atlanta Braves
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_4():
    yaml = YAML()
    yaml.mapping_value_align = True
    yaml.round_trip(
        """
    -
      name: Mark McGwire
      hr:   65
      avg:  0.278
    -
      name: Sammy Sosa
      hr:   63
      avg:  0.288
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_5():
    yaml = YAML()
    yaml.flow_sequence_element_align = True
    yaml.round_trip(
        """
    - [name        , hr, avg  ]
    - [Mark McGwire, 65, 0.278]
    - [Sammy Sosa  , 63, 0.288]
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_6():
    yaml = YAML()
    # yaml.flow_mapping_final_comma = False
    yaml.flow_mapping_one_element_per_line = True
    yaml.round_trip(
        """
    Mark McGwire: {hr: 65, avg: 0.278}
    Sammy Sosa: {
        hr: 63,
        avg: 0.288
      }
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_7():
    yaml = YAML()
    yaml.round_trip_all(
        """
    # Ranking of 1998 home runs
    ---
    - Mark McGwire
    - Sammy Sosa
    - Ken Griffey

    # Team ranking
    ---
    - Chicago Cubs
    - St Louis Cardinals
    """
    )


def test_example_2_8():
    yaml = YAML()
    yaml.explicit_start = True
    yaml.explicit_end = True
    yaml.round_trip_all(
        """
    ---
    time: 20:03:20
    player: Sammy Sosa
    action: strike (miss)
    ...
    ---
    time: 20:03:47
    player: Sammy Sosa
    action: grand slam
    ...
    """
    )


def test_example_2_9():
    yaml = YAML()
    yaml.explicit_start = True
    yaml.indent(sequence=4, offset=2)
    yaml.round_trip(
        """
    ---
    hr: # 1998 hr ranking
      - Mark McGwire
      - Sammy Sosa
    rbi:
      # 1998 rbi ranking
      - Sammy Sosa
      - Ken Griffey
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_10():
    yaml = YAML()
    yaml.explicit_start = True
    yaml.indent(sequence=4, offset=2)
    yaml.round_trip(
        """
    ---
    hr:
      - Mark McGwire
      # Following node labeled SS
      - &SS Sammy Sosa
    rbi:
      - *SS # Subsequent occurrence
      - Ken Griffey
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_11():
    yaml = YAML()
    yaml.round_trip(
        """
    ? - Detroit Tigers
      - Chicago cubs
    :
      - 2001-07-23

    ? [ New York Yankees,
        Atlanta Braves ]
    : [ 2001-07-02, 2001-08-12,
        2001-08-14 ]
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_12():
    yaml = YAML()
    yaml.explicit_start = True
    yaml.round_trip(
        """
    ---
    # Products purchased
    - item    : Super Hoop
      quantity: 1
    - item    : Basketball
      quantity: 4
    - item    : Big Shoes
      quantity: 1
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_13():
    yaml = YAML()
    yaml.round_trip(
        r"""
    # ASCII Art
    --- |
      \//||\/||
      // ||  ||__
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_14():
    yaml = YAML()
    yaml.explicit_start = True
    yaml.indent(root_scalar=2)  # needs to be added
    yaml.round_trip(
        """
    --- >
      Mark McGwire's
      year was crippled
      by a knee injury.
    """
    )


@pytest.mark.xfail(strict=True)
def test_example_2_15():
    yaml = YAML()
    yaml.round_trip(
        """
    >
     Sammy Sosa completed another
     fine season with great stats.

       63 Home Runs
       0.288 Batting Average

     What a year!
    """
    )


def test_example_2_16():
    yaml = YAML()
    yaml.round_trip(
        """
    name: Mark McGwire
    accomplishment: >
      Mark set a major league
      home run record in 1998.
    stats: |
      65 Home Runs
      0.278 Batting Average
    """
    )


@pytest.mark.xfail(
    strict=True, reason="cannot YAML dump escape sequences (\n) as hex and normal"
)
def test_example_2_17():
    yaml = YAML()
    yaml.allow_unicode = False
    yaml.preserve_quotes = True
    yaml.round_trip(
        r"""
    unicode: "Sosa did fine.\u263A"
    control: "\b1998\t1999\t2000\n"
    hex esc: "\x0d\x0a is \r\n"

    single: '"Howdy!" he cried.'
    quoted: ' # Not a ''comment''.'
    tie-fighter: '|\-*-/|'
    """
    )


@pytest.mark.xfail(
    strict=True, reason="non-literal/folding multiline scalars not supported"
)
def test_example_2_18():
    yaml = YAML()
    yaml.round_trip(
        """
    plain:
      This unquoted scalar
      spans many lines.

    quoted: "So does this
      quoted scalar.\n"
    """
    )


@pytest.mark.xfail(strict=True, reason="leading + on decimal dropped")
def test_example_2_19():
    yaml = YAML()
    yaml.round_trip(
        """
    canonical: 12345
    decimal: +12345
    octal: 0o14
    hexadecimal: 0xC
    """
    )


@pytest.mark.xfail(strict=True, reason="case of NaN not preserved")
def test_example_2_20():
    yaml = YAML()
    yaml.round_trip(
        """
    canonical: 1.23015e+3
    exponential: 12.3015e+02
    fixed: 1230.15
    negative infinity: -.inf
    not a number: .NaN
    """
    )


def Xtest_example_2_X():
    yaml = YAML()
    yaml.round_trip(
        """
    """
    )
