self: super: with self; {
  boost_scope = stdenv.mkDerivation rec {
    pname = "boost_scope";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "scope";
      rev = "boost-${version}";
      hash = "sha256-874Rsb4NdXeS6xe8koz00ZVNmDlsApO7RtI0h871nVU=";
    };
  };
}
