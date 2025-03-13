self: super: with self; {
  boost_move = stdenv.mkDerivation rec {
    pname = "boost_move";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "move";
      rev = "boost-${version}";
      hash = "sha256-nV7rrJoYowMnqQcUSjJPWjMG6vuu0GvCjqXeWexWVSY=";
    };
  };
}
