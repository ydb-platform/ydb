self: super: with self; {
  boost_move = stdenv.mkDerivation rec {
    pname = "boost_move";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "move";
      rev = "boost-${version}";
      hash = "sha256-9blsgfGTy+4IHUA/EOXh5OeO49V8UzUZcsnZB9YaHrg=";
    };
  };
}
