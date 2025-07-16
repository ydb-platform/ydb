pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.12.0";

  src = fetchFromGitHub {
    owner = "nlohmann";
    repo = "json";
    rev = "v${version}";
    hash = "sha256-cECvDOLxgX7Q9R3IE86Hj9JJUxraDQvhoyPDF03B2CY=";
  };

  patches = [];
}
