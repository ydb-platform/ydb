self: super: with self; {
  boost_format = stdenv.mkDerivation rec {
    pname = "boost_format";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "format";
      rev = "boost-${version}";
      hash = "sha256-S46TJZNFw3sDw8D5Sa6zVTt5dkfvjGahLHJ5OC5dHXs=";
    };
  };
}
