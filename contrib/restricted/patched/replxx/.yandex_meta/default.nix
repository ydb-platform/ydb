self: super: with self; {
  yamaker-replxx = stdenv.mkDerivation rec {
    name = "replxx";
    version = "2022-09-09";
    revision = "5d04501f93a4fb7f0bb8b73b8f614bc986f9e25b";

    src = fetchFromGitHub {
      owner = "ClickHouse";
      repo = "replxx";
      rev = "${revision}";

      hash = "sha256-JFlkt3s5YOk1/97O0HKF0qdKmDmL0jxsPkPvbi2fcTM=";
    };

    nativeBuildInputs = [
      cmake
      ninja
    ];

    cmakeFlags = [
      "-DREPLXX_BUILD_EXAMPLES=OFF"
      "-DBUILD_SHARED_LIBS=OFF"
    ];
  };
}
