pkgs: attrs: with pkgs; with python310.pkgs; with attrs; rec {
  pname = "psycopg2";
  version = "2.9.11";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-lk0xyvco4hfGl/936mnCughl+kHsILsA8Jd+Yv3MUuM=";
  };

  prePatch = "";
  patches = [];

  propagatedBuildInputs = [];
}
