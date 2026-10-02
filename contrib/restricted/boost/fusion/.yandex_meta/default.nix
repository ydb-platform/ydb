self: super: with self; {
  boost_fusion = stdenv.mkDerivation rec {
    pname = "boost_fusion";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "fusion";
      rev = "boost-${version}";
      hash = "sha256-jdQ2tTBKBvHyv7cyN6yXRfyzfH3yV/3V2O++3UZU7hA=";
    };
  };
}
