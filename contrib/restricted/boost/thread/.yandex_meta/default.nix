self: super: with self; {
  boost_thread = stdenv.mkDerivation rec {
    pname = "boost_thread";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "thread";
      rev = "boost-${version}";
      hash = "sha256-b/71ovr7i4YorEnFSYHXwYgv4fgSWUobCORaX7hmxmw=";
    };
  };
}
