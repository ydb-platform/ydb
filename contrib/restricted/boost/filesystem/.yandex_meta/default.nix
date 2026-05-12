self: super: with self; {
  boost_filesystem = stdenv.mkDerivation rec {
    pname = "boost_filesystem";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "filesystem";
      rev = "boost-${version}";
      hash = "sha256-v8wVrhkZFAiEjjvEeUgfi5hBB8ISJFh+O5w81ce2cN8=";
    };
  };
}
