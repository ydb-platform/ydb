self: super: with self; {
  boost_filesystem = stdenv.mkDerivation rec {
    pname = "boost_filesystem";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "filesystem";
      rev = "boost-${version}";
      hash = "sha256-ASgDbzY7ArGuBxwxrBbVLn2mvIoVl8xBYg3egysdRbU=";
    };
  };
}
