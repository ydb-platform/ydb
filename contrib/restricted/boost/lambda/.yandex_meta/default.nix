self: super: with self; {
  boost_lambda = stdenv.mkDerivation rec {
    pname = "boost_lambda";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "lambda";
      rev = "boost-${version}";
      hash = "sha256-5revBfxl90zjfbwipY7Se4J/h/RBWsoGICc66WVSsmY=";
    };
  };
}
