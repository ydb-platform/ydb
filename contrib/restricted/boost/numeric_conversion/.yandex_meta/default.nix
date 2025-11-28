self: super: with self; {
  boost_numeric_conversion = stdenv.mkDerivation rec {
    pname = "boost_numeric_conversion";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "numeric_conversion";
      rev = "boost-${version}";
      hash = "sha256-u9v7e7yLKsfiV8Btz4+hKNR9eI7BTFK7AWte8oGvreg=";
    };
  };
}
