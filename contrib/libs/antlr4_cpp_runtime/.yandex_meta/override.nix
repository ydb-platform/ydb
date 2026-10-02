self: super: with self; rec {
  version = "4.13.2";

  src = fetchFromGitHub {
    owner = "antlr";
    repo = "antlr4";
    rev = version;
    hash = "sha256-DxxRL+FQFA+x0RudIXtLhewseU50aScHKSCDX7DE9bY=";
  };

  patches = [ ];

  nativeBuildInputs = [ cmake antlr4 ];

  buildInputs = [];

  cmakeFlags = [
    "-DANTLR_BUILD_CPP_TESTS=OFF"
  ];
}
