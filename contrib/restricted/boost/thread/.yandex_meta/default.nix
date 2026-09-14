self: super: with self; {
  boost_thread = stdenv.mkDerivation rec {
    pname = "boost_thread";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "thread";
      rev = "boost-${version}";
      hash = "sha256-sA8oOX25pto5VAofPx9NQPNza6MHrbu6LjFxYZA5B3Q=";
    };
  };
}
