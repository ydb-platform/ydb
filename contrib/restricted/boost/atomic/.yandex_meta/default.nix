self: super: with self; {
  boost_atomic = stdenv.mkDerivation rec {
    pname = "boost_atomic";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "atomic";
      rev = "boost-${version}";
      hash = "sha256-3OjP8NRc6Y7tDbxUC0jupZPAY2+r1SL3nIoNaJP09FA=";
    };
  };
}
