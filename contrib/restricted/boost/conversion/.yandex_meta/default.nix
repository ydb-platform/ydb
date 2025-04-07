self: super: with self; {
  boost_conversion = stdenv.mkDerivation rec {
    pname = "boost_conversion";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "conversion";
      rev = "boost-${version}";
      hash = "sha256-STQcRLg4FPxQ3MxMPx2IsqVV0PoyAPgGHfv+EeluyXs=";
    };
  };
}
