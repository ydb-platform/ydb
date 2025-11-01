self: super: with self; {
  boost_filesystem = stdenv.mkDerivation rec {
    pname = "boost_filesystem";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "filesystem";
      rev = "boost-${version}";
      hash = "sha256-knhjAm7vz2gEIpIX7uxboIqIy6/2O/tXsxXD8uwR6yI=";
    };
  };
}
