self: super: with self; {
  boost_context = stdenv.mkDerivation rec {
    pname = "boost_context";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "context";
      rev = "boost-${version}";
      hash = "sha256-MFJgat3HX5IRW4xaXzjAdbo77e103mt3HnNgG8PV+pw=";
    };
  };
}
