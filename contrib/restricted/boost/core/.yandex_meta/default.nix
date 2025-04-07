self: super: with self; {
  boost_core = stdenv.mkDerivation rec {
    pname = "boost_core";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "core";
      rev = "boost-${version}";
      hash = "sha256-jfMBu2K9peGzT/rtvrBXHdN5z17GiwQjUQ0cO6OWU18=";
    };
  };
}
