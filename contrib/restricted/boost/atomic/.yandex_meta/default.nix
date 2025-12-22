self: super: with self; {
  boost_atomic = stdenv.mkDerivation rec {
    pname = "boost_atomic";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "atomic";
      rev = "boost-${version}";
      hash = "sha256-slm3tKEBvVCG2COochBvg4jn4hPw8uk6Vevnz0dRRZU=";
    };
  };
}
