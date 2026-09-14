self: super: with self; {
  boost_type_index = stdenv.mkDerivation rec {
    pname = "boost_type_index";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "type_index";
      rev = "boost-${version}";
      hash = "sha256-6m4Dgvh5rrG0maKyki0FHikvHM7Pd9cIHZL8aDZk7ks=";
    };
  };
}
