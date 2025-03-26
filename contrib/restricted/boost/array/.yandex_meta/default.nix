self: super: with self; {
  boost_array = stdenv.mkDerivation rec {
    pname = "boost_array";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "array";
      rev = "boost-${version}";
      hash = "sha256-KlUpm9POv39C7BBpC2foHzTUCE9t2uEEka9kMdRVgcA=";
    };
  };
}
