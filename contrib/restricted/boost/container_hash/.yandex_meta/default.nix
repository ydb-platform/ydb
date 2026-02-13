self: super: with self; {
  boost_container_hash = stdenv.mkDerivation rec {
    pname = "boost_container_hash";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "container_hash";
      rev = "boost-${version}";
      hash = "sha256-Yu3Qc7yRFkusfnS107Rrs+yXFcBQuNhfZ9lAAMsHbLA=";
    };
  };
}
