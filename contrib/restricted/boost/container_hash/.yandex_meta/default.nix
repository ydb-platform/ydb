self: super: with self; {
  boost_container_hash = stdenv.mkDerivation rec {
    pname = "boost_container_hash";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "container_hash";
      rev = "boost-${version}";
      hash = "sha256-vQixawo5FBgRLu5WoVl1HWPSjUVb/vKdnJ+9srANCWs=";
    };
  };
}
