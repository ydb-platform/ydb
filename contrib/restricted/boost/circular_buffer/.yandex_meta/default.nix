self: super: with self; {
  boost_circular_buffer = stdenv.mkDerivation rec {
    pname = "boost_circular_buffer";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "circular_buffer";
      rev = "boost-${version}";
      hash = "sha256-t5fSfuQFGNb1cQUkcdxu4i8FbEQAfddhtYAEen6bCGE=";
    };
  };
}
