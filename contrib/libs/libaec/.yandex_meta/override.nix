pkgs: attrs: with pkgs; rec {
  version = "1.1.6";

  src = fetchFromGitHub {
    owner = "MathisRosenhauer";
    repo = "libaec";
    rev = "${version}-release";
    hash = "sha256-cxDP+JNwokxgzH9hO2zw+rIcz8XG7E8ujbAbWpgUEW8=";
  };

  cmakeFlags = [
    "-DBUILD_TESTING=OFF"
    "-DCMAKE_BUILD_TYPE=Release"
  ];
}
