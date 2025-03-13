self: super: with self; {
  boost_algorithm = stdenv.mkDerivation rec {
    pname = "boost_algorithm";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "algorithm";
      rev = "boost-${version}";
      hash = "sha256-+rnClgwIlt25oJ0MVzjaJn3N7eD3QXi0rf0v+cZDgvw=";
    };
  };
}
