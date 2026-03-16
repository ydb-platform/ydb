self: super: with self; rec {
  pname = "crc32c";
  version = "1.1.2";

  cmakeFlags = [
      "-DCRC32C_BUILD_TESTS=OFF"
      "-DCRC32C_BUILD_BENCHMARKS=OFF"
      "-DCRC32C_USE_GLOG=OFF"
  ];

  src = fetchFromGitHub {
    owner = "google";
    repo = "crc32c";
    rev = version;
    hash = "sha256-8lylNeaKkGSOJVQcbSEpMxT1IFF1OsCAzpYVPrynxiQ=";
  };
}
