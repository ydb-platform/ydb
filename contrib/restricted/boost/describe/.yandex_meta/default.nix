self: super: with self; {
  boost_describe = stdenv.mkDerivation rec {
    pname = "boost_describe";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "describe";
      rev = "boost-${version}";
      hash = "sha256-cjsYoyl4Vd8g5gSHyHQOJDUujWJOa6mPt6qAaOv9jFg=";
    };
  };
}
