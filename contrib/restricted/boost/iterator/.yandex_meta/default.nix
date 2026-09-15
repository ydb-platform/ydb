self: super: with self; {
  boost_iterator = stdenv.mkDerivation rec {
    pname = "boost_iterator";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "iterator";
      rev = "boost-${version}";
      hash = "sha256-/E0mBplcSHgsg0ObJA9MOb43sWwXQ2A4587hRNDI/Hw=";
    };
  };
}
