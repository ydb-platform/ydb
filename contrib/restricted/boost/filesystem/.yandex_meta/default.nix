self: super: with self; {
  boost_filesystem = stdenv.mkDerivation rec {
    pname = "boost_filesystem";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "filesystem";
      rev = "boost-${version}";
      hash = "sha256-Oujyrojgc8lxVEzaTY03kmMro6lkpBcqRntZLLZjXzU=";
    };
  };
}
