self: super: with self; {
  boost_convert = stdenv.mkDerivation rec {
    pname = "boost_convert";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "convert";
      rev = "boost-${version}";
      hash = "sha256-SHEQGDHNhBtcNbCX9hVYXxo8yN3Eg6i8fxgY095zWPw=";
    };
  };
}
